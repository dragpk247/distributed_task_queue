//! # Queue Engine Module
//!
//! Implements high-throughput, low-latency concurrent task queues with:
//! - Thread-safe state managed by `parking_lot::RwLock`.
//! - True FIFO queue ordering via `VecDeque`.
//! - At-least-once task leasing with visibility timeouts (`InFlightTask`).
//! - Automatic Dead-Letter Queue (DLQ) routing upon exceeding maximum retries.
//! - Non-busy event-driven notification channels (`tokio::sync::broadcast`) for async `BRPOP` workers.

use parking_lot::RwLock;
use std::collections::{BinaryHeap, HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::broadcast;

/// Default visibility timeout granted to a worker when leasing a task without explicit duration.
pub const DEFAULT_VISIBILITY_TIMEOUT_SECS: u64 = 30;

/// Default maximum retry attempts before moving a poisoned/failing task to the Dead-Letter Queue.
pub const DEFAULT_MAX_RETRIES: u32 = 3;

/// A discrete unit of work residing inside a FIFO queue.
#[derive(Debug, Clone)]
pub struct TaskItem {
    /// Raw payload bytes of the task (e.g. JSON, Protobuf, binary).
    pub payload: Vec<u8>,
    /// Number of times this task was leased and subsequently failed or timed out.
    pub retry_count: u32,
    /// Threshold of retry attempts before permanent escalation to the DLQ.
    pub max_retries: u32,
    /// Task priority level (higher values dequeued first, default 0).
    pub priority: u8,
}

/// An active task currently checked out/leased by an external worker thread.
#[derive(Debug, Clone)]
pub struct InFlightTask {
    /// Unique identifier for this lease instance (e.g. "task-1").
    pub id: String,
    /// Underlying task data and retry history.
    pub item: TaskItem,
    /// Timestamp when this lease was granted or last renewed via `TASKTOUCH`.
    pub leased_at: Instant,
    /// Max duration the worker has to ACK before the background reaper reclaims the task.
    pub visibility_timeout: Duration,
}

/// A scheduled task delayed until a future instant.
///
/// Implements [`Ord`] and [`PartialOrd`] in reverse order so that
/// [`BinaryHeap<DelayedTask>`] acts as a Min-Heap ordered by `execute_at`
/// (earliest deadline pops first).
#[derive(Debug, Clone)]
pub struct DelayedTask {
    /// Monotonically assigned unique identifier for the delayed task.
    pub id: u64,
    /// Target queue where the task will be pushed once ready.
    pub queue: String,
    /// Raw payload bytes to enqueue.
    pub payload: Vec<u8>,
    /// Future instant after which the task is eligible for promotion.
    pub execute_at: Instant,
}

impl PartialEq for DelayedTask {
    fn eq(&self, other: &Self) -> bool {
        self.execute_at == other.execute_at && self.id == other.id
    }
}

impl Eq for DelayedTask {}

impl Ord for DelayedTask {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Reverse ordering so BinaryHeap functions as a min-heap by execute_at,
        // using id as a secondary tie-breaker (smaller id first).
        other
            .execute_at
            .cmp(&self.execute_at)
            .then_with(|| other.id.cmp(&self.id))
    }
}

impl PartialOrd for DelayedTask {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Finds the index of the task with the highest priority closest to the tail.
///
/// Iterating from tail (back) to head (front) ensures that among tasks with equal
/// highest priority, the one closest to the tail (the oldest, preserving FIFO) is chosen.
#[inline]
pub(crate) fn find_highest_priority_tail_index(q: &VecDeque<TaskItem>) -> Option<usize> {
    if q.is_empty() {
        return None;
    }
    let mut best_idx = q.len() - 1;
    let mut max_priority = q[best_idx].priority;

    // Scan backwards from tail towards head
    for idx in (0..q.len()).rev() {
        if q[idx].priority > max_priority {
            max_priority = q[idx].priority;
            best_idx = idx;
        }
    }
    Some(best_idx)
}

/// Internal shared mutable state protected by a read-write lock.
#[derive(Default)]
pub struct EngineInner {
    /// Mapping: `queue_name` -> FIFO deque of tasks awaiting workers.
    pub queues: HashMap<String, VecDeque<TaskItem>>,
    /// Mapping: `queue_name` -> (`task_id` -> active `InFlightTask`).
    pub in_flight: HashMap<String, HashMap<String, InFlightTask>>,
    /// Mapping: `queue_name` -> list of permanently failed task payloads (DLQ).
    pub dead_letter_queues: HashMap<String, Vec<Vec<u8>>>,
    /// Priority min-heap storing pending delayed tasks ordered by scheduled execution time.
    pub delayed_tasks: BinaryHeap<DelayedTask>,
    /// Monotonically increasing counter for assigning unique delayed task identifiers.
    pub delayed_counter: u64,
}

/// The core concurrent queue engine.
pub struct QueueEngine {
    /// Thread-safe in-memory state wrapped in a low-overhead RwLock.
    pub inner: RwLock<EngineInner>,
    /// Broadcast notifier to wake up waiting `BRPOP` / `BRPOPLEASE` workers when new tasks arrive.
    notifier: broadcast::Sender<String>,
}

impl Default for QueueEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl QueueEngine {
    /// Creates a new, empty `QueueEngine` instance with an active notification channel.
    pub fn new() -> Self {
        let (tx, _rx) = broadcast::channel(1024);
        Self {
            inner: RwLock::new(EngineInner::default()),
            notifier: tx,
        }
    }

    /// Pushes an element to the head/front of the queue with priority (`LPUSH_PRIORITY`).
    ///
    /// Returns the new total length of the queue. Also broadcasts an event
    /// to awaken any suspended async workers waiting on this queue.
    pub fn lpush_priority(&self, queue: &str, priority: u8, payload: Vec<u8>) -> usize {
        let item = TaskItem {
            payload,
            retry_count: 0,
            max_retries: DEFAULT_MAX_RETRIES,
            priority,
        };
        let len = {
            let mut lock = self.inner.write();
            let q = lock.queues.entry(queue.to_string()).or_default();
            q.push_front(item);
            q.len()
        };
        // Wake up any workers awaiting work on this specific queue
        let _ = self.notifier.send(queue.to_string());
        len
    }

    /// Pushes an element to the head/front of the queue (`LPUSH`).
    ///
    /// Standard tasks default to priority 0. Returns the new total length of the queue.
    /// Also broadcasts an event to awaken any suspended async workers waiting on this queue.
    pub fn lpush(&self, queue: &str, payload: Vec<u8>) -> usize {
        self.lpush_priority(queue, 0, payload)
    }

    /// Pops an element from the queue (`RPOP`) without leasing, respecting task priorities.
    ///
    /// Tasks with higher priority are dequeued first. Within the same priority level,
    /// strict FIFO ordering is maintained (oldest item closest to tail pops first).
    pub fn rpop(&self, queue: &str) -> Option<Vec<u8>> {
        let mut lock = self.inner.write();
        let q = lock.queues.get_mut(queue)?;
        let idx = find_highest_priority_tail_index(q)?;
        q.remove(idx).map(|item| item.payload)
    }

    /// Atomically pops from the tail of `source` (priority-aware) and prepends to the head of `destination` (`RPOPLPUSH`).
    pub fn rpoplpush(&self, source: &str, destination: &str) -> Option<Vec<u8>> {
        let mut lock = self.inner.write();
        let item = {
            let src_q = lock.queues.get_mut(source)?;
            let idx = find_highest_priority_tail_index(src_q)?;
            src_q.remove(idx)?
        };
        let payload = item.payload.clone();
        let dest_q = lock.queues.entry(destination.to_string()).or_default();
        dest_q.push_front(item);
        let _ = self.notifier.send(destination.to_string());
        Some(payload)
    }

    /// Leased Pop: Pops the next task (priority-aware) and tracks it under `in_flight`
    /// with an active visibility timeout.
    ///
    /// If the worker fails to ACK or crashes, the background lease reaper will
    /// reclaim the task after `visibility_timeout` expires.
    pub fn rpop_with_lease(
        &self,
        queue: &str,
        task_id: String,
        visibility_timeout: Duration,
    ) -> Option<Vec<u8>> {
        let mut lock = self.inner.write();
        let item = {
            let q = lock.queues.get_mut(queue)?;
            let idx = find_highest_priority_tail_index(q)?;
            q.remove(idx)?
        };
        let payload = item.payload.clone();

        let in_flight = InFlightTask {
            id: task_id.clone(),
            item,
            leased_at: Instant::now(),
            visibility_timeout,
        };

        lock.in_flight
            .entry(queue.to_string())
            .or_default()
            .insert(task_id, in_flight);

        Some(payload)
    }

    /// Renews or extends an active lease visibility timeout (`TASKTOUCH`).
    ///
    /// Used by long-running workers to prevent the background reaper from reclaiming tasks in progress.
    pub fn task_touch(&self, queue: &str, task_id: &str, extend_by: Duration) -> bool {
        let mut lock = self.inner.write();
        if let Some(queue_tasks) = lock.in_flight.get_mut(queue) {
            if let Some(task) = queue_tasks.get_mut(task_id) {
                task.leased_at = Instant::now();
                task.visibility_timeout = extend_by;
                return true;
            }
        }
        false
    }

    /// Acknowledges successful processing of an in-flight task (`TASKACK`).
    ///
    /// Permanently removes the task from the system. Returns `true` if found and deleted.
    pub fn task_ack(&self, queue: &str, task_id: &str) -> bool {
        let mut lock = self.inner.write();
        if let Some(queue_tasks) = lock.in_flight.get_mut(queue) {
            queue_tasks.remove(task_id).is_some()
        } else {
            false
        }
    }

    /// Negatively acknowledges a task (`TASKNACK`).
    ///
    /// Increments the task retry count. If it exceeds `max_retries`, routes the payload
    /// directly to the Dead-Letter Queue. Otherwise, puts it back into the ready queue.
    pub fn task_nack(&self, queue: &str, task_id: &str) -> bool {
        let mut lock = self.inner.write();
        let task_opt = lock
            .in_flight
            .get_mut(queue)
            .and_then(|queue_tasks| queue_tasks.remove(task_id));

        if let Some(mut in_flight) = task_opt {
            in_flight.item.retry_count += 1;
            if in_flight.item.retry_count >= in_flight.item.max_retries {
                // Maximum retries exceeded: Route to Dead Letter Queue
                lock.dead_letter_queues
                    .entry(queue.to_string())
                    .or_default()
                    .push(in_flight.item.payload);
            } else {
                // Re-queue to back of FIFO deque for next available worker
                lock.queues
                    .entry(queue.to_string())
                    .or_default()
                    .push_back(in_flight.item);
                let _ = self.notifier.send(queue.to_string());
            }
            true
        } else {
            false
        }
    }

    /// Asynchronous blocking pop on one or more queues with timeout (`BRPOP`).
    ///
    /// Suspends asynchronously using Tokio broadcast events without busy-spinning CPU cycles.
    pub async fn brpop(
        self: &Arc<Self>,
        queues: &[String],
        timeout: Duration,
    ) -> Option<(String, Vec<u8>)> {
        // Fast-path: check if any queue already has items ready
        for q in queues {
            if let Some(payload) = self.rpop(q) {
                return Some((q.clone(), payload));
            }
        }

        if timeout.is_zero() {
            return self.wait_for_any_queue(queues).await;
        }

        tokio::select! {
            result = self.wait_for_any_queue(queues) => result,
            _ = tokio::time::sleep(timeout) => None,
        }
    }

    /// Asynchronous blocking pop with atomic lease creation (`BRPOPLEASE`).
    pub async fn brpop_lease(
        self: &Arc<Self>,
        queue: &str,
        timeout: Duration,
        visibility_timeout: Duration,
        task_id: String,
    ) -> Option<Vec<u8>> {
        // Fast-path check
        if let Some(payload) = self.rpop_with_lease(queue, task_id.clone(), visibility_timeout) {
            return Some(payload);
        }

        let wait_future = async {
            let mut rx = self.notifier.subscribe();
            loop {
                if let Some(payload) =
                    self.rpop_with_lease(queue, task_id.clone(), visibility_timeout)
                {
                    return Some(payload);
                }
                match rx.recv().await {
                    Ok(q_name) if q_name == queue => {
                        if let Some(payload) =
                            self.rpop_with_lease(queue, task_id.clone(), visibility_timeout)
                        {
                            return Some(payload);
                        }
                    }
                    Ok(_) => {}
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        if let Some(payload) =
                            self.rpop_with_lease(queue, task_id.clone(), visibility_timeout)
                        {
                            return Some(payload);
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => return None,
                }
            }
        };

        if timeout.is_zero() {
            wait_future.await
        } else {
            tokio::select! {
                res = wait_future => res,
                _ = tokio::time::sleep(timeout) => None,
            }
        }
    }

    async fn wait_for_any_queue(&self, queues: &[String]) -> Option<(String, Vec<u8>)> {
        let mut rx = self.notifier.subscribe();
        loop {
            // Check queues first
            for q in queues {
                if let Some(payload) = self.rpop(q) {
                    return Some((q.clone(), payload));
                }
            }

            match rx.recv().await {
                Ok(q_name) => {
                    if queues.contains(&q_name) {
                        if let Some(payload) = self.rpop(&q_name) {
                            return Some((q_name, payload));
                        }
                    }
                }
                Err(broadcast::error::RecvError::Lagged(_)) => {
                    for q in queues {
                        if let Some(payload) = self.rpop(q) {
                            return Some((q.clone(), payload));
                        }
                    }
                }
                Err(broadcast::error::RecvError::Closed) => return None,
            }
        }
    }

    /// Background reaper scanning in-flight tasks and reclaiming any whose visibility timeout expired.
    ///
    /// If an expired task has hit `max_retries`, it routes directly to the DLQ.
    /// Otherwise, it re-queues to `queues` and broadcasts an event to wake up idle workers.
    /// Returns the total number of reaped tasks.
    pub fn reap_expired_leases(&self) -> usize {
        let mut reaped = 0;
        let now = Instant::now();
        let mut expired_tasks: Vec<(String, InFlightTask)> = Vec::new();

        // Pass 1: Extract expired task IDs without holding write locks on the whole state
        {
            let mut lock = self.inner.write();
            for (q, tasks) in lock.in_flight.iter_mut() {
                let mut expired_ids = Vec::new();
                for (id, task) in tasks.iter() {
                    if now.duration_since(task.leased_at) >= task.visibility_timeout {
                        expired_ids.push(id.clone());
                    }
                }
                for id in expired_ids {
                    if let Some(task) = tasks.remove(&id) {
                        expired_tasks.push((q.clone(), task));
                    }
                }
            }
        }

        if expired_tasks.is_empty() {
            return 0;
        }

        // Pass 2: Re-queue or route to DLQ
        let mut lock = self.inner.write();
        for (q, mut task) in expired_tasks {
            reaped += 1;
            task.item.retry_count += 1;
            if task.item.retry_count >= task.item.max_retries {
                lock.dead_letter_queues
                    .entry(q.clone())
                    .or_default()
                    .push(task.item.payload);
            } else {
                lock.queues
                    .entry(q.clone())
                    .or_default()
                    .push_back(task.item);
                let _ = self.notifier.send(q);
            }
        }

        reaped
    }

    /// Schedules a task to be pushed after a specified delay duration (`LPUSH_DELAY`).
    ///
    /// Stores the task in an in-memory priority min-heap ordered by `execute_at`.
    /// Returns the unique delayed task identifier assigned to it.
    pub fn lpush_delayed(&self, queue: &str, delay: Duration, payload: Vec<u8>) -> u64 {
        let execute_at = Instant::now() + delay;
        let mut lock = self.inner.write();
        lock.delayed_counter += 1;
        let id = lock.delayed_counter;
        let task = DelayedTask {
            id,
            queue: queue.to_string(),
            payload,
            execute_at,
        };
        lock.delayed_tasks.push(task);
        id
    }

    /// Pops and promotes all scheduled delayed tasks whose execution deadline has passed (`execute_at <= Instant::now()`).
    ///
    /// For each ready task:
    /// 1. Prepends the task item to `inner.queues` for that queue.
    /// 2. Emits a notification on `inner.notifier` to wake up blocking workers (e.g. `BRPOP`).
    ///
    /// Returns a vector of promoted tasks as `(queue_name, payload)`.
    pub fn pop_ready_delayed_tasks(&self) -> Vec<(String, Vec<u8>)> {
        let now = Instant::now();
        let mut promoted = Vec::new();
        let mut notifications = Vec::new();

        {
            let mut lock = self.inner.write();
            while let Some(task) = lock.delayed_tasks.peek() {
                if task.execute_at <= now {
                    let task = lock.delayed_tasks.pop().unwrap();
                    let item = TaskItem {
                        payload: task.payload.clone(),
                        retry_count: 0,
                        max_retries: DEFAULT_MAX_RETRIES,
                        priority: 0,
                    };
                    lock.queues
                        .entry(task.queue.clone())
                        .or_default()
                        .push_front(item);
                    notifications.push(task.queue.clone());
                    promoted.push((task.queue, task.payload));
                } else {
                    break;
                }
            }
        }

        // Notify waiting workers for each queue that received promoted tasks
        for q in notifications {
            let _ = self.notifier.send(q);
        }

        promoted
    }

    /// Gathers high-level telemetry and status metrics across all queues and leases.
    pub fn get_stats(&self) -> EngineStats {
        let lock = self.inner.read();
        let mut queue_lengths = HashMap::new();
        let mut in_flight_counts = HashMap::new();
        let mut dlq_counts = HashMap::new();

        for (q, items) in lock.queues.iter() {
            queue_lengths.insert(q.clone(), items.len());
        }
        for (q, items) in lock.in_flight.iter() {
            in_flight_counts.insert(q.clone(), items.len());
        }
        for (q, items) in lock.dead_letter_queues.iter() {
            dlq_counts.insert(q.clone(), items.len());
        }

        EngineStats {
            queue_lengths,
            in_flight_counts,
            dlq_counts,
            delayed_tasks_count: lock.delayed_tasks.len(),
        }
    }

    /// Re-queues all dead-letter queue (DLQ) tasks back to the ready queue for retry.
    /// Returns the number of items requeued.
    pub fn requeue_dlq(&self, queue: &str) -> usize {
        let mut lock = self.inner.write();
        if let Some(dlq_items) = lock.dead_letter_queues.remove(queue) {
            let count = dlq_items.len();
            let q = lock.queues.entry(queue.to_string()).or_default();
            for payload in dlq_items {
                let item = TaskItem {
                    payload,
                    retry_count: 0,
                    max_retries: DEFAULT_MAX_RETRIES,
                    priority: 0,
                };
                q.push_back(item);
            }
            if count > 0 {
                let _ = self.notifier.send(queue.to_string());
            }
            count
        } else {
            0
        }
    }

    /// Clears and purges all dead-letter queue (DLQ) tasks for the specified queue.
    /// Returns the number of items purged.
    pub fn purge_dlq(&self, queue: &str) -> usize {
        let mut lock = self.inner.write();
        if let Some(dlq_items) = lock.dead_letter_queues.remove(queue) {
            dlq_items.len()
        } else {
            0
        }
    }
}

/// Snapshot of queue engine state for monitoring and telemetry.
#[derive(Debug, Clone, Default)]
pub struct EngineStats {
    /// Number of items ready per queue.
    pub queue_lengths: HashMap<String, usize>,
    /// Number of items currently leased per queue.
    pub in_flight_counts: HashMap<String, usize>,
    /// Number of items routed to dead-letter queue per queue.
    pub dlq_counts: HashMap<String, usize>,
    /// Total count of scheduled delayed tasks awaiting execution time.
    pub delayed_tasks_count: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lpush_rpop_fifo() {
        let engine = QueueEngine::new();
        engine.lpush("jobs", b"job1".to_vec());
        engine.lpush("jobs", b"job2".to_vec());

        // LPUSH prepends to front, RPOP removes from back -> FIFO order
        assert_eq!(engine.rpop("jobs"), Some(b"job1".to_vec()));
        assert_eq!(engine.rpop("jobs"), Some(b"job2".to_vec()));
        assert_eq!(engine.rpop("jobs"), None);
    }

    #[test]
    fn test_rpoplpush() {
        let engine = QueueEngine::new();
        engine.lpush("source", b"taskA".to_vec());
        let popped = engine.rpoplpush("source", "processing");

        assert_eq!(popped, Some(b"taskA".to_vec()));
        assert_eq!(engine.rpop("source"), None);
        assert_eq!(engine.rpop("processing"), Some(b"taskA".to_vec()));
    }

    #[tokio::test]
    async fn test_brpop_notification() {
        let engine = Arc::new(QueueEngine::new());
        let engine_clone = Arc::clone(&engine);

        let handle = tokio::spawn(async move {
            engine_clone
                .brpop(&["async_queue".to_string()], Duration::from_millis(500))
                .await
        });

        tokio::time::sleep(Duration::from_millis(50)).await;
        engine.lpush("async_queue", b"async_payload".to_vec());

        let result = handle.await.unwrap();
        assert_eq!(
            result,
            Some(("async_queue".to_string(), b"async_payload".to_vec()))
        );
    }

    #[test]
    fn test_ack_and_nack_dlq() {
        let engine = QueueEngine::new();
        engine.lpush("work", b"critical_task".to_vec());

        // Lease the task
        let leased = engine.rpop_with_lease("work", "id-1".to_string(), Duration::from_secs(10));
        assert_eq!(leased, Some(b"critical_task".to_vec()));

        // NACK 1 -> retry count 1, re-queued
        assert!(engine.task_nack("work", "id-1"));
        let leased2 = engine.rpop_with_lease("work", "id-2".to_string(), Duration::from_secs(10));
        assert_eq!(leased2, Some(b"critical_task".to_vec()));

        // NACK 2 -> retry count 2
        assert!(engine.task_nack("work", "id-2"));
        let leased3 = engine.rpop_with_lease("work", "id-3".to_string(), Duration::from_secs(10));
        assert_eq!(leased3, Some(b"critical_task".to_vec()));

        // NACK 3 -> hits max retries (3) -> routed to Dead Letter Queue
        assert!(engine.task_nack("work", "id-3"));

        let inner = engine.inner.read();
        let dlq = inner.dead_letter_queues.get("work");
        assert!(dlq.is_some(), "DLQ should exist for 'work'");
        assert_eq!(dlq.unwrap().len(), 1);
        assert_eq!(dlq.unwrap()[0], b"critical_task");
    }

    #[test]
    fn test_delayed_tasks_heap_order() {
        let engine = QueueEngine::new();
        // Schedule three tasks with different delays
        let id1 = engine.lpush_delayed("q1", Duration::from_millis(200), b"task200".to_vec());
        let id2 = engine.lpush_delayed("q1", Duration::from_millis(50), b"task50".to_vec());
        let id3 = engine.lpush_delayed("q1", Duration::from_millis(100), b"task100".to_vec());

        assert_eq!(id1, 1);
        assert_eq!(id2, 2);
        assert_eq!(id3, 3);

        // Before any delay elapses, none should be popped
        let ready = engine.pop_ready_delayed_tasks();
        assert!(ready.is_empty());

        // Sleep to allow all tasks to become ready
        std::thread::sleep(Duration::from_millis(220));

        let promoted = engine.pop_ready_delayed_tasks();
        assert_eq!(promoted.len(), 3);
        // The earliest task (task50) was popped first, then task100, then task200
        assert_eq!(promoted[0], ("q1".to_string(), b"task50".to_vec()));
        assert_eq!(promoted[1], ("q1".to_string(), b"task100".to_vec()));
        assert_eq!(promoted[2], ("q1".to_string(), b"task200".to_vec()));

        // In the queue, they were prepended sequentially (LPUSH),
        // so RPOP (from the back) pops the first one pushed: task50, then task100, then task200
        assert_eq!(engine.rpop("q1"), Some(b"task50".to_vec()));
        assert_eq!(engine.rpop("q1"), Some(b"task100".to_vec()));
        assert_eq!(engine.rpop("q1"), Some(b"task200".to_vec()));
        assert_eq!(engine.rpop("q1"), None);
    }

    #[tokio::test]
    async fn test_delayed_task_notifies_brpop() {
        let engine = Arc::new(QueueEngine::new());
        let engine_clone = Arc::clone(&engine);

        // Schedule delayed task with 50ms delay
        engine.lpush_delayed(
            "delayed_q",
            Duration::from_millis(50),
            b"delayed_item".to_vec(),
        );

        // Worker waiting with BRPOP
        let handle = tokio::spawn(async move {
            engine_clone
                .brpop(&["delayed_q".to_string()], Duration::from_millis(500))
                .await
        });

        // Sleep 60ms and promote
        tokio::time::sleep(Duration::from_millis(60)).await;
        let promoted = engine.pop_ready_delayed_tasks();
        assert_eq!(promoted.len(), 1);

        let result = handle.await.unwrap();
        assert_eq!(
            result,
            Some(("delayed_q".to_string(), b"delayed_item".to_vec()))
        );
    }

    #[test]
    fn test_requeue_and_purge_dlq() {
        let engine = QueueEngine::new();
        engine.lpush("dlq_test", b"dead_payload".to_vec());

        // Lease and NACK 3 times to send to DLQ
        for i in 1..=3 {
            let task_id = format!("t-{}", i);
            let _ = engine.rpop_with_lease("dlq_test", task_id.clone(), Duration::from_secs(10));
            assert!(engine.task_nack("dlq_test", &task_id));
        }

        let stats = engine.get_stats();
        assert_eq!(stats.dlq_counts.get("dlq_test"), Some(&1));
        assert_eq!(stats.queue_lengths.get("dlq_test"), Some(&0));

        // Requeue
        let requeued = engine.requeue_dlq("dlq_test");
        assert_eq!(requeued, 1);

        let stats_after = engine.get_stats();
        assert_eq!(stats_after.dlq_counts.get("dlq_test"), None);
        assert_eq!(stats_after.queue_lengths.get("dlq_test"), Some(&1));

        // Re-lease and NACK to DLQ again
        for i in 4..=6 {
            let task_id = format!("t-{}", i);
            let _ = engine.rpop_with_lease("dlq_test", task_id.clone(), Duration::from_secs(10));
            assert!(engine.task_nack("dlq_test", &task_id));
        }

        // Purge
        let purged = engine.purge_dlq("dlq_test");
        assert_eq!(purged, 1);
        let stats_purged = engine.get_stats();
        assert_eq!(stats_purged.dlq_counts.get("dlq_test"), None);
    }

    #[test]
    fn test_task_priority_ordering() {
        let engine = QueueEngine::new();
        // Push tasks with varying priorities:
        // Priority 0: "low1", "low2"
        // Priority 5: "med1", "med2"
        // Priority 10: "high1", "high2"
        engine.lpush("prio_q", b"low1".to_vec());
        engine.lpush("prio_q", b"low2".to_vec());
        engine.lpush_priority("prio_q", 10, b"high1".to_vec());
        engine.lpush_priority("prio_q", 5, b"med1".to_vec());
        engine.lpush_priority("prio_q", 10, b"high2".to_vec());
        engine.lpush_priority("prio_q", 5, b"med2".to_vec());

        // Dequeuing order should be:
        // 1. Priority 10: "high1" then "high2" (FIFO within priority 10)
        // 2. Priority 5: "med1" then "med2" (FIFO within priority 5)
        // 3. Priority 0: "low1" then "low2" (FIFO within priority 0)
        assert_eq!(engine.rpop("prio_q"), Some(b"high1".to_vec()));
        assert_eq!(engine.rpop("prio_q"), Some(b"high2".to_vec()));
        assert_eq!(engine.rpop("prio_q"), Some(b"med1".to_vec()));
        assert_eq!(engine.rpop("prio_q"), Some(b"med2".to_vec()));
        assert_eq!(engine.rpop("prio_q"), Some(b"low1".to_vec()));
        assert_eq!(engine.rpop("prio_q"), Some(b"low2".to_vec()));
        assert_eq!(engine.rpop("prio_q"), None);
    }

    #[test]
    fn test_task_priority_lease_and_transfer() {
        let engine = QueueEngine::new();
        engine.lpush("prio_lease", b"low".to_vec());
        engine.lpush_priority("prio_lease", 8, b"urgent".to_vec());

        // rpop_with_lease should yield urgent (priority 8) first
        let leased = engine.rpop_with_lease(
            "prio_lease",
            "task-prio-1".to_string(),
            Duration::from_secs(30),
        );
        assert_eq!(leased, Some(b"urgent".to_vec()));

        // rpoplpush should transfer remaining task "low"
        let transferred = engine.rpoplpush("prio_lease", "dest_q");
        assert_eq!(transferred, Some(b"low".to_vec()));
        assert_eq!(engine.rpop("prio_lease"), None);
        assert_eq!(engine.rpop("dest_q"), Some(b"low".to_vec()));
    }
}
