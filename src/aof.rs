//! # Append-Only File (AOF) Persistence Module
//!
//! Provides durable mutation logging and crash-recovery snapshotting:
//! - Appends raw RESP command frames directly to physical disk ledger.
//! - Startup recovery: replays logged frames sequentially to reconstruct state.
//! - Online AOF compaction (`BGREWRITEAOF`): writes clean, minimal state to a temporary file
//!   and atomically swaps files via OS `rename` to prevent disk bloat.

use bytes::BytesMut;
use parking_lot::Mutex;
use std::collections::{HashMap, VecDeque};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::engine::{TaskItem, DEFAULT_MAX_RETRIES};
use crate::protocol::{parse_command, Command};

/// Manages the physical append-only transaction file on disk.
pub struct AofManager {
    /// Filesystem path to the active `.aof` file.
    file_path: PathBuf,
    /// Thread-safe file handle for append mutations.
    writer: Mutex<File>,
    /// Rolling counter of recorded mutation transactions.
    mutation_count: AtomicUsize,
}

impl AofManager {
    /// Opens or creates the AOF ledger at the given path in append mode.
    pub fn open<P: AsRef<Path>>(path: P) -> std::io::Result<Self> {
        let file_path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&file_path)?;

        Ok(Self {
            file_path,
            writer: Mutex::new(file),
            mutation_count: AtomicUsize::new(0),
        })
    }

    /// Appends a raw RESP command frame directly to the persistence ledger.
    pub fn append(&self, raw_frame: &[u8]) -> std::io::Result<()> {
        let mut lock = self.writer.lock();
        lock.write_all(raw_frame)?;
        lock.flush()?;
        self.mutation_count.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Explicitly flushes buffered AOF mutations to disk.
    pub fn flush(&self) -> std::io::Result<()> {
        let mut lock = self.writer.lock();
        lock.flush()
    }

    /// Replays the AOF ledger sequentially upon server startup to reconstruct in-memory queues.
    pub fn replay(&self) -> std::io::Result<HashMap<String, VecDeque<TaskItem>>> {
        let mut queues: HashMap<String, VecDeque<TaskItem>> = HashMap::new();
        let file = OpenOptions::new().read(true).open(&self.file_path);
        let Ok(f) = file else {
            return Ok(queues);
        };

        let mut reader = BufReader::new(f);
        let mut file_bytes = Vec::new();
        reader.read_to_end(&mut file_bytes)?;

        let mut buffer = BytesMut::from(&file_bytes[..]);
        let mut count = 0;

        // Parse and replay commands in exact historical order
        while !buffer.is_empty() {
            match parse_command(&mut buffer) {
                Ok(Some((cmd, _))) => match cmd {
                    Command::Lpush { queue, payload } => {
                        let item = TaskItem {
                            payload,
                            retry_count: 0,
                            max_retries: DEFAULT_MAX_RETRIES,
                            priority: 0,
                        };
                        queues.entry(queue).or_default().push_front(item);
                        count += 1;
                    }
                    Command::LpushPriority {
                        queue,
                        priority,
                        payload,
                    } => {
                        let item = TaskItem {
                            payload,
                            retry_count: 0,
                            max_retries: DEFAULT_MAX_RETRIES,
                            priority,
                        };
                        queues.entry(queue).or_default().push_front(item);
                        count += 1;
                    }
                    Command::Rpop { queue } => {
                        if let Some(q) = queues.get_mut(&queue) {
                            q.pop_back();
                        }
                        count += 1;
                    }
                    Command::Rpoplpush {
                        source,
                        destination,
                    } => {
                        if let Some(item) = queues.get_mut(&source).and_then(|q| q.pop_back()) {
                            queues.entry(destination).or_default().push_front(item);
                        }
                        count += 1;
                    }
                    _ => {}
                },
                Ok(None) => break,
                Err(_) => {
                    tracing::warn!("AOF log recovery encountered truncated/corrupted frame; recovered up to frame {}", count);
                    break;
                }
            }
        }

        tracing::info!("AOF replay finished. Recovered {} mutations.", count);
        Ok(queues)
    }

    /// Compacts the current in-memory queue state into a temporary file and atomically
    /// swaps it over the existing AOF log (`fs::rename`).
    ///
    /// This removes historical dead commands (e.g. pushed items that were subsequently popped),
    /// bounding disk space usage.
    pub fn compact(
        &self,
        current_state: &HashMap<String, VecDeque<TaskItem>>,
    ) -> std::io::Result<usize> {
        let temp_path = self.file_path.with_extension("aof.tmp");
        let mut temp_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&temp_path)?;

        let mut written_commands = 0;

        // Reconstruct minimal LPUSH frames for each active queue item in FIFO order
        for (queue, items) in current_state.iter() {
            for item in items.iter().rev() {
                let frame = format!(
                    "*3\r\n$5\r\nLPUSH\r\n${}\r\n{}\r\n${}\r\n",
                    queue.len(),
                    queue,
                    item.payload.len()
                );
                temp_file.write_all(frame.as_bytes())?;
                temp_file.write_all(&item.payload)?;
                temp_file.write_all(b"\r\n")?;
                written_commands += 1;
            }
        }

        temp_file.flush()?;
        drop(temp_file);

        // Atomic file swap: replace the old AOF with the new compacted file
        let mut lock = self.writer.lock();
        std::fs::rename(&temp_path, &self.file_path)?;

        // Re-open append writer pointing to the new compacted file
        *lock = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&self.file_path)?;

        tracing::info!(
            "AOF Compaction complete: wrote {} items cleanly.",
            written_commands
        );
        Ok(written_commands)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_aof_append_and_replay() {
        let test_dir = std::env::temp_dir();
        let aof_path = test_dir.join("test_queue_append.aof");
        let _ = std::fs::remove_file(&aof_path);

        let aof = AofManager::open(&aof_path).unwrap();
        // LPUSH tasks task1
        let frame1 = b"*3\r\n$5\r\nLPUSH\r\n$5\r\ntasks\r\n$5\r\ntask1\r\n";
        aof.append(frame1).unwrap();

        // LPUSH tasks task2
        let frame2 = b"*3\r\n$5\r\nLPUSH\r\n$5\r\ntasks\r\n$5\r\ntask2\r\n";
        aof.append(frame2).unwrap();

        let restored = aof.replay().unwrap();
        assert_eq!(restored.get("tasks").unwrap().len(), 2);
        assert_eq!(restored.get("tasks").unwrap()[1].payload, b"task1"); // oldest item at back
        assert_eq!(restored.get("tasks").unwrap()[0].payload, b"task2"); // newest item at front

        let _ = std::fs::remove_file(&aof_path);
    }

    #[test]
    fn test_aof_compaction() {
        let test_dir = std::env::temp_dir();
        let aof_path = test_dir.join("test_queue_compact.aof");
        let _ = std::fs::remove_file(&aof_path);

        let aof = AofManager::open(&aof_path).unwrap();

        let mut state = HashMap::new();
        let mut deque = VecDeque::new();
        deque.push_front(TaskItem {
            payload: b"task1".to_vec(),
            retry_count: 0,
            max_retries: 3,
            priority: 0,
        });
        deque.push_front(TaskItem {
            payload: b"task2".to_vec(),
            retry_count: 0,
            max_retries: 3,
            priority: 0,
        });
        state.insert("work".to_string(), deque);

        let count = aof.compact(&state).unwrap();
        assert_eq!(count, 2);

        // Replay compacted log
        let replayed = aof.replay().unwrap();
        assert_eq!(replayed.get("work").unwrap().len(), 2);
        assert_eq!(replayed.get("work").unwrap()[1].payload, b"task1");
        assert_eq!(replayed.get("work").unwrap()[0].payload, b"task2");

        let _ = std::fs::remove_file(&aof_path);
    }

    #[test]
    fn test_aof_replay_priority() {
        let test_dir = std::env::temp_dir();
        let aof_path = test_dir.join("test_queue_prio.aof");
        let _ = std::fs::remove_file(&aof_path);

        let aof = AofManager::open(&aof_path).unwrap();
        // LPUSH tasks low
        let frame1 = b"*3\r\n$5\r\nLPUSH\r\n$5\r\ntasks\r\n$3\r\nlow\r\n";
        aof.append(frame1).unwrap();

        // LPUSH_PRIORITY tasks 10 high
        let frame2 = b"*4\r\n$14\r\nLPUSH_PRIORITY\r\n$5\r\ntasks\r\n$2\r\n10\r\n$4\r\nhigh\r\n";
        aof.append(frame2).unwrap();

        let restored = aof.replay().unwrap();
        let tasks = restored.get("tasks").unwrap();
        assert_eq!(tasks.len(), 2);
        assert_eq!(tasks[0].payload, b"high");
        assert_eq!(tasks[0].priority, 10);
        assert_eq!(tasks[1].payload, b"low");
        assert_eq!(tasks[1].priority, 0);

        let _ = std::fs::remove_file(&aof_path);
    }
}
