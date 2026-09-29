use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};
use parking_lot::RwLock;
use tokio::sync::broadcast;

pub const DEFAULT_VISIBILITY_TIMEOUT_SECS: u64 = 30;
pub const DEFAULT_MAX_RETRIES: u32 = 3;

#[derive(Debug, Clone)]
pub struct TaskItem {
    pub payload: Vec<u8>,
    pub retry_count: u32,
    pub max_retries: u32,
}

#[derive(Debug, Clone)]
pub struct InFlightTask {
    pub id: String,
    pub item: TaskItem,
    pub leased_at: Instant,
    pub visibility_timeout: Duration,
}

#[derive(Default)]
pub struct EngineInner {
    /// queue_name -> FIFO tasks (ready for workers)
    pub queues: HashMap<String, VecDeque<TaskItem>>,
    /// queue_name -> in-flight tasks keyed by task ID
    pub in_flight: HashMap<String, HashMap<String, InFlightTask>>,
    /// Dead letter queues: queue_name -> list of failed payloads
    pub dead_letter_queues: HashMap<String, Vec<Vec<u8>>>,
}

pub struct QueueEngine {
    pub inner: RwLock<EngineInner>,
    /// Broadcast notifier to wake up any awaiting BRPOP / BRPOPLPUSH listeners
    notifier: broadcast::Sender<String>,
}

impl Default for QueueEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl QueueEngine {
    pub fn new() -> Self {
        let (tx, _rx) = broadcast::channel(1024);
        Self {
            inner: RwLock::new(EngineInner::default()),
            notifier: tx,
        }
    }

    /// Push an element to the front/head of the queue (LPUSH)
    pub fn lpush(&self, queue: &str, payload: Vec<u8>) -> usize {
        let item = TaskItem {
            payload,
            retry_count: 0,
            max_retries: DEFAULT_MAX_RETRIES,
        };
        let len = {
            let mut lock = self.inner.write();
            let q = lock.queues.entry(queue.to_string()).or_default();
            q.push_front(item);
            q.len()
        };
        let _ = self.notifier.send(queue.to_string());
        len
    }

    /// Pop an element from the tail of the queue (RPOP)
    pub fn rpop(&self, queue: &str) -> Option<Vec<u8>> {
        let mut lock = self.inner.write();
        lock.queues.get_mut(queue).and_then(|q| q.pop_back().map(|item| item.payload))
    }

    /// Atomically pops from tail of source and prepends to head of destination (RPOPLPUSH)
    pub fn rpoplpush(&self, source: &str, destination: &str) -> Option<Vec<u8>> {
        let mut lock = self.inner.write();
        let item = lock.queues.get_mut(source).and_then(|q| q.pop_back())?;
        let payload = item.payload.clone();
        let dest_q = lock.queues.entry(destination.to_string()).or_default();
        dest_q.push_front(item);
        let _ = self.notifier.send(destination.to_string());
        Some(payload)
    }

    /// Leased pop: Pops task and tracks it in `in_flight` under visibility timeout
    pub fn rpop_with_lease(
        &self,
        queue: &str,
        task_id: String,
        visibility_timeout: Duration,
    ) -> Option<Vec<u8>> {
        let mut lock = self.inner.write();
        let item = lock.queues.get_mut(queue).and_then(|q| q.pop_back())?;
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

    /// Acknowledge successful processing of an in-flight task (TASKACK)
    pub fn task_ack(&self, queue: &str, task_id: &str) -> bool {
        let mut lock = self.inner.write();
        if let Some(queue_tasks) = lock.in_flight.get_mut(queue) {
            queue_tasks.remove(task_id).is_some()
        } else {
            false
        }
    }

    /// Negative-acknowledge: fail task immediately and trigger retry or DLQ (TASKNACK)
    pub fn task_nack(&self, queue: &str, task_id: &str) -> bool {
        let mut lock = self.inner.write();
        let task_opt = lock
            .in_flight
            .get_mut(queue)
            .and_then(|queue_tasks| queue_tasks.remove(task_id));

        if let Some(mut in_flight) = task_opt {
            in_flight.item.retry_count += 1;
            if in_flight.item.retry_count >= in_flight.item.max_retries {
                // Route to Dead Letter Queue
                lock.dead_letter_queues
                    .entry(queue.to_string())
                    .or_default()
                    .push(in_flight.item.payload);
            } else {
                // Re-queue to tail (or front)
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

    /// Extend lease visibility timeout for long running tasks (TASKTOUCH)
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

    /// Asynchronous blocking pop with lease
    pub async fn brpop_lease(
        self: &Arc<Self>,
        queue: &str,
        timeout: Duration,
        visibility_timeout: Duration,
        task_id: String,
    ) -> Option<Vec<u8>> {
        // Fast-path: check if queue already has items
        if let Some(payload) = self.rpop_with_lease(queue, task_id.clone(), visibility_timeout) {
            return Some(payload);
        }

        if timeout.is_zero() {
            let mut rx = self.notifier.subscribe();
            loop {
                if let Some(payload) = self.rpop_with_lease(queue, task_id.clone(), visibility_timeout) {
                    return Some(payload);
                }
                match rx.recv().await {
                    Ok(q_name) if q_name == queue => {
                        if let Some(payload) = self.rpop_with_lease(queue, task_id.clone(), visibility_timeout) {
                            return Some(payload);
                        }
                    }
                    Ok(_) => {}
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        if let Some(payload) = self.rpop_with_lease(queue, task_id.clone(), visibility_timeout) {
                            return Some(payload);
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => return None,
                }
            }
        }

        tokio::select! {
            result = async {
                let mut rx = self.notifier.subscribe();
                loop {
                    if let Some(payload) = self.rpop_with_lease(queue, task_id.clone(), visibility_timeout) {
                        return Some(payload);
                    }
                    match rx.recv().await {
                        Ok(q_name) if q_name == queue => {
                            if let Some(payload) = self.rpop_with_lease(queue, task_id.clone(), visibility_timeout) {
                                return Some(payload);
                            }
                        }
                        Ok(_) => {}
                        Err(broadcast::error::RecvError::Lagged(_)) => {
                            if let Some(payload) = self.rpop_with_lease(queue, task_id.clone(), visibility_timeout) {
                                return Some(payload);
                            }
                        }
                        Err(broadcast::error::RecvError::Closed) => return None,
                    }
                }
            } => result,
            _ = tokio::time::sleep(timeout) => None,
        }
    }

    /// Asynchronous blocking pop on one or more queues with timeout (BRPOP)
    pub async fn brpop(
        self: &Arc<Self>,
        queues: &[String],
        timeout: Duration,
    ) -> Option<(String, Vec<u8>)> {
        // Fast-path: check if any queue already has items
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

    /// Background reaper to reclaim tasks whose visibility timeouts have expired
    pub fn reap_expired_leases(&self) -> usize {
        let mut reaped = 0;
        let now = Instant::now();
        let mut expired_tasks: Vec<(String, InFlightTask)> = Vec::new();

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

        // Small delay, then push
        tokio::time::sleep(Duration::from_millis(50)).await;
        engine.lpush("async_queue", b"async_payload".to_vec());

        let result = handle.await.unwrap();
        assert_eq!(result, Some(("async_queue".to_string(), b"async_payload".to_vec())));
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
}
