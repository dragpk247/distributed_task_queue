# Distributed Task Queue (DTQ) - Python SDK

A lightweight, high-performance, pure-Python client and background worker library for `distributed_task_queue`. Communicates over the standard Redis Serialization Protocol (RESP) wire format with **zero third-party dependencies**.

---

## Features

- **Pure Python Standard Library**: Built directly on `socket` and `threading` with zero external dependencies.
- **Full RESP Command Suite**:
  - `ping()` / `auth(password)`
  - `enqueue(queue, payload, delay=None)` (`LPUSH` / `LPUSH_DELAY`)
  - `pop(queue)` (`RPOP`)
  - `blocking_pop(queues, timeout=0.0)` (`BRPOP`)
  - `lease(queue, visibility_secs=30.0)` (`RPOPLEASE`)
  - `blocking_lease(queue, timeout=5.0, visibility_secs=30.0)` (`BRPOPLEASE`)
  - `touch(queue, task_id, extend_secs=30.0)` (`TASKTOUCH`)
  - `ack(queue, task_id)` (`TASKACK`)
  - `nack(queue, task_id)` (`TASKNACK`)
  - `dlq_list(queue, limit=None)` (`DLQ_LIST`)
  - `dlq_purge(queue)` (`DLQ_PURGE`)
  - `dlq_replay(queue)` (`DLQ_REPLAY`)
  - `info()` (`INFO` server metrics & queue stats)
- **Production-Ready Worker Pattern**:
  - `@worker.task` decorator for concise queue handler definition.
  - Automated background heartbeat threads sending `TASKTOUCH` periodically while tasks execute.
  - Automatic `TASKACK` on successful task execution.
  - Automatic `TASKNACK` with retry counter escalation on uncaught exceptions.
  - Graceful shutdown on `SIGINT` / `SIGTERM` or programmatic `worker.stop()`.
  - Supports flexible handler signatures (`func(payload)`, `func(task_id, payload)`, `func()`).

---

## Installation

```bash
cd sdk/python
pip install .
```

Or for editable development:

```bash
pip install -e .
```

---

## Quickstart

### 1. Producer Example

Publish immediate tasks or schedule tasks with future execution delay:

```python
from dtq import TaskQueueClient

# Connect to the distributed task queue server
client = TaskQueueClient(host="127.0.0.1", port=6379, password="optional_password")

with client:
    # Health check
    if client.ping():
        print("Connected to Task Queue server!")

    # 1. Enqueue an immediate task
    queue_length = client.enqueue("emails", "send_welcome_email: user_123")
    print(f"Task enqueued! Queue length is now: {queue_length}")

    # 2. Enqueue a delayed task (executes in 60 seconds)
    task_id = client.enqueue("reminders", "send_followup: user_123", delay=60.0)
    print(f"Delayed task scheduled with ID: {task_id}")

    # 3. Query server statistics & metrics
    stats = client.info()
    print("Delayed tasks:", stats.get("delayed_tasks"))
    print("Queue stats:", stats.get("queues"))
```

---

### 2. Worker Example

Decorate your task handler functions with `@worker.task`. The worker automatically acquires a visibility lease, spawns a background heartbeat thread to extend the lease during processing, and settles the task with `ACK` or `NACK`.

```python
import time
from dtq import Worker

# Create a worker instance
worker = Worker(host="127.0.0.1", port=6379, password="optional_password")

@worker.task(queue="emails", visibility_secs=30.0, heartbeat_interval=10.0)
def handle_email(payload: bytes):
    email_data = payload.decode("utf-8")
    print(f"Processing email task: {email_data}")
    # Simulating long-running job:
    # Heartbeat thread automatically issues TASKTOUCH every 10 seconds!
    time.sleep(2)
    print("Email sent successfully!")

@worker.task(queue="billing", visibility_secs=60.0, heartbeat_interval=15.0)
def process_payment(task_id: str, payload: bytes):
    print(f"Processing payment for task {task_id} with data {payload}")
    # If an uncaught exception is raised:
    # Worker automatically issues TASKNACK to trigger server retry or DLQ
    if b"fail" in payload:
        raise RuntimeError("Payment gateway unavailable")

if __name__ == "__main__":
    print("Starting worker... Press Ctrl+C to stop.")
    # worker.run() listens for SIGINT / SIGTERM and stops gracefully
    worker.run()
```

---

### 3. Manual Task Consumption & Leases

If you prefer lower-level control over task lifecycle without using the `Worker` abstraction:

```python
from dtq import TaskQueueClient

client = TaskQueueClient(host="127.0.0.1", port=6379)

with client:
    # Block for up to 5 seconds waiting for a task with a 30-second visibility lease
    lease = client.blocking_lease("orders", timeout=5.0, visibility_secs=30.0)

    if lease is not None:
        task_id, payload = lease
        print(f"Leased task {task_id}: {payload}")

        try:
            # Periodically extend lease if job takes longer than expected:
            client.touch("orders", task_id, extend_secs=30.0)

            # Do work...
            # Settle task as successfully completed:
            client.ack("orders", task_id)
            print("Task completed successfully!")
        except Exception:
            # Negative acknowledge to trigger retry or move to DLQ:
            client.nack("orders", task_id)
            print("Task failed and was NACKed.")
    else:
        print("No tasks available in queue.")
```

---

### 4. Dead-Letter Queue (DLQ) Management

Inspect, replay, or purge dead-lettered tasks that have exceeded their maximum retry limit:

```python
from dtq import TaskQueueClient

client = TaskQueueClient(host="127.0.0.1", port=6379)

with client:
    # 1. Inspect tasks in DLQ (optionally pass a limit)
    dead_tasks = client.dlq_list("orders", limit=10)
    print(f"DLQ contains {len(dead_tasks)} tasks")
    for payload in dead_tasks:
        print("Dead task payload:", payload)

    # 2. Replay all DLQ tasks back into the active queue for processing
    requeued_count = client.dlq_replay("orders")
    print(f"Requeued {requeued_count} tasks back into 'orders'")

    # 3. Or purge DLQ tasks permanently
    purged_count = client.dlq_purge("orders")
    print(f"Purged {purged_count} tasks from DLQ")
```

---

## API Reference

### `TaskQueueClient`

```python
TaskQueueClient(host="127.0.0.1", port=6379, password=None, socket_timeout=None)
```

- `ping() -> bool`: Server health probe (`PING`).
- `auth(password: str) -> bool`: Authenticate connection (`AUTH`).
- `enqueue(queue: str, payload: Union[str, bytes], delay: Optional[float] = None) -> int`: Enqueue immediate or delayed task (`LPUSH` / `LPUSH_DELAY`).
- `pop(queue: str) -> Optional[bytes]`: Non-blocking pop from tail (`RPOP`).
- `blocking_pop(queues: Union[str, List[str]], timeout: float = 0.0) -> Optional[Tuple[str, bytes]]`: Blocking pop (`BRPOP`).
- `lease(queue: str, visibility_secs: float = 30.0) -> Optional[Tuple[str, bytes]]`: Non-blocking lease with visibility window (`RPOPLEASE`).
- `blocking_lease(queue: str, timeout: float = 5.0, visibility_secs: float = 30.0) -> Optional[Tuple[str, bytes]]`: Blocking lease with visibility window (`BRPOPLEASE`).
- `touch(queue: str, task_id: str, extend_secs: float = 30.0) -> bool`: Extend active lease window (`TASKTOUCH`).
- `ack(queue: str, task_id: str) -> bool`: Acknowledge completion (`TASKACK`).
- `nack(queue: str, task_id: str) -> bool`: Negative-acknowledge failure (`TASKNACK`).
- `dlq_list(queue: str, limit: Optional[int] = None) -> List[bytes]`: Inspect dead-lettered task payloads (`DLQ_LIST`).
- `dlq_purge(queue: str) -> int`: Purge all dead-lettered tasks (`DLQ_PURGE`).
- `dlq_replay(queue: str) -> int`: Re-queue dead-lettered tasks back to active queue (`DLQ_REPLAY`).
- `info() -> Dict[str, Any]`: Retrieve engine metrics and queue stats (`INFO`).
- `connect()` / `close()`: Manage connection lifecycle. Supports `with` context manager.


### `Worker`

```python
Worker(client=None, host="127.0.0.1", port=6379, password=None, poll_timeout=2.0)
```

- `@worker.task(queue=None, visibility_secs=30.0, heartbeat_interval=10.0)`: Task registration decorator.
- `run()`: Starts blocking event loop until stopped or interrupted (`SIGINT`/`SIGTERM`).
- `run_once(timeout=None) -> bool`: Single-iteration poll across registered queues.
- `stop()`: Signals the worker to finish the current task and exit cleanly.

---

## Running Tests

Unit and integration tests use Python's built-in `unittest` runner:

```bash
python3 -m unittest discover sdk/python/tests
```
