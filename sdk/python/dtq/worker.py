"""
Distributed Task Queue (DTQ) Worker Abstraction.

Provides a decorated worker pattern with automated background heartbeats (TASKTOUCH),
lifecycle acknowledgement (TASKACK / TASKNACK), and graceful shutdown handling.
"""

from __future__ import annotations

import inspect
import logging
import signal
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Union

from dtq.client import TaskQueueClient

logger = logging.getLogger("dtq.worker")


@dataclass
class TaskDefinition:
    """Metadata describing a registered task handler."""
    func: Callable
    queue: str
    visibility_secs: float = 30.0
    heartbeat_interval: float = 10.0


class Worker:
    """
    Background task worker that polls queues, invokes registered task handlers,
    maintains task visibility leases via background heartbeat threads, and settles
    tasks with ACK on success or NACK on uncaught exceptions.
    """

    def __init__(
        self,
        client: Optional[TaskQueueClient] = None,
        host: str = "127.0.0.1",
        port: int = 6379,
        password: Optional[str] = None,
        poll_timeout: float = 2.0,
    ):
        """
        Initialize a new Worker instance.

        :param client: Existing TaskQueueClient instance. If None, one will be created.
        :param host: Server IP address if creating a new client (default: "127.0.0.1").
        :param port: Server port if creating a new client (default: 6379).
        :param password: Password for authentication if required.
        :param poll_timeout: Timeout in seconds for blocking poll per queue iteration (default: 2.0).
        """
        if client is not None:
            self.client = client
        else:
            self.client = TaskQueueClient(host=host, port=port, password=password)

        self.poll_timeout = poll_timeout
        self.tasks: Dict[str, TaskDefinition] = {}
        self._stop_event = threading.Event()
        self._running = False
        self._registered_signals = False

    def task(
        self,
        func: Optional[Callable] = None,
        *,
        queue: Optional[str] = None,
        visibility_secs: float = 30.0,
        heartbeat_interval: float = 10.0,
    ) -> Callable:
        """
        Decorator to register a task handler function for a specific queue.

        Usage:
            @worker.task(queue="emails", visibility_secs=30.0, heartbeat_interval=10.0)
            def handle_email(payload: bytes):
                ...

            # Or using function name as queue:
            @worker.task
            def emails(payload: bytes):
                ...

        :param func: Function being decorated (when used without parentheses).
        :param queue: Name of queue to pull tasks from. Defaults to function name.
        :param visibility_secs: Initial task lease visibility in seconds (default: 30.0).
        :param heartbeat_interval: Interval in seconds between TASKTOUCH heartbeats (default: 10.0).
        """
        def decorator(fn: Callable) -> Callable:
            q = queue or fn.__name__
            self.register(
                fn,
                queue=q,
                visibility_secs=visibility_secs,
                heartbeat_interval=heartbeat_interval,
            )
            return fn

        if func is not None and callable(func):
            return decorator(func)
        return decorator

    def register(
        self,
        func: Callable,
        queue: str,
        visibility_secs: float = 30.0,
        heartbeat_interval: float = 10.0,
    ) -> None:
        """
        Explicitly registers a task handler function for a queue.

        :param func: Callable task handler.
        :param queue: Queue name to consume tasks from.
        :param visibility_secs: Lease visibility timeout in seconds.
        :param heartbeat_interval: Seconds between TASKTOUCH heartbeats while running.
        """
        if heartbeat_interval >= visibility_secs:
            logger.warning(
                "heartbeat_interval (%.1fs) >= visibility_secs (%.1fs) on queue '%s'. "
                "Consider setting heartbeat_interval smaller to avoid lease expiry.",
                heartbeat_interval,
                visibility_secs,
                queue,
            )

        self.tasks[queue] = TaskDefinition(
            func=func,
            queue=queue,
            visibility_secs=visibility_secs,
            heartbeat_interval=heartbeat_interval,
        )
        logger.debug("Registered task handler for queue '%s': %s", queue, func.__name__)

    def _invoke_handler(self, task_def: TaskDefinition, task_id: str, payload: bytes) -> Any:
        """Invokes user task callable inspecting signature to inject arguments properly."""
        sig = inspect.signature(task_def.func)
        param_names = list(sig.parameters.keys())
        param_count = len(param_names)

        if param_count == 0:
            return task_def.func()
        elif param_count == 1:
            return task_def.func(payload)
        elif param_count == 2:
            if param_names[0] in ("task_id", "id", "job_id"):
                return task_def.func(task_id, payload)
            else:
                return task_def.func(payload, task_id)
        else:
            return task_def.func(payload)

    def _execute_task(self, task_def: TaskDefinition, task_id: str, payload: bytes) -> bool:
        """
        Executes a leased task:
        1. Spawns background heartbeat thread sending TASKTOUCH every heartbeat_interval seconds.
        2. Executes task handler function.
        3. Calls TASKACK on success or TASKNACK on uncaught exception.
        4. Terminates heartbeat thread cleanly.

        :return: True if task succeeded and was ACKed, False if failed and NACKed.
        """
        stop_heartbeat = threading.Event()

        def heartbeat_loop():
            while not stop_heartbeat.wait(timeout=task_def.heartbeat_interval):
                if stop_heartbeat.is_set():
                    break
                try:
                    touched = self.client.touch(
                        task_def.queue,
                        task_id,
                        extend_secs=task_def.visibility_secs,
                    )
                    if not touched:
                        logger.warning(
                            "Heartbeat TASKTOUCH failed for task %s on queue '%s' (lease may have expired).",
                            task_id,
                            task_def.queue,
                        )
                    else:
                        logger.debug(
                            "Heartbeat TASKTOUCH renewed lease for task %s (+%.1fs)",
                            task_id,
                            task_def.visibility_secs,
                        )
                except Exception as e:
                    logger.warning("Error during heartbeat TASKTOUCH for task %s: %s", task_id, e)

        hb_thread = threading.Thread(
            target=heartbeat_loop,
            name=f"heartbeat-{task_def.queue}-{task_id}",
            daemon=True,
        )
        hb_thread.start()

        success = False
        try:
            logger.info("Executing task %s on queue '%s'", task_id, task_def.queue)
            self._invoke_handler(task_def, task_id, payload)
            success = True
            logger.info("Task %s completed successfully", task_id)
        except Exception as exc:
            logger.error("Task %s failed with uncaught exception: %s", task_id, exc, exc_info=True)
            success = False
        except BaseException:
            success = False
            raise
        finally:
            stop_heartbeat.set()
            hb_thread.join(timeout=2.0)

            if success:
                try:
                    self.client.ack(task_def.queue, task_id)
                    logger.debug("Task %s acknowledged (TASKACK)", task_id)
                except Exception as err:
                    logger.error("Failed to send TASKACK for task %s: %s", task_id, err)
            else:
                try:
                    self.client.nack(task_def.queue, task_id)
                    logger.warning("Task %s negative-acknowledged (TASKNACK)", task_id)
                except Exception as err:
                    logger.error("Failed to send TASKNACK for task %s: %s", task_id, err)

        return success

    def run_once(self, timeout: Optional[float] = None) -> bool:
        """
        Polls registered queues for a single task and processes it.

        :param timeout: Maximum seconds to block for task lease (defaults to poll_timeout).
        :return: True if a task was leased and executed, False if timed out or stopped.
        """
        if self._stop_event.is_set():
            return False

        if not self.tasks:
            time.sleep(0.1)
            return False

        effective_timeout = timeout if timeout is not None else self.poll_timeout

        for queue_name, task_def in list(self.tasks.items()):
            if self._stop_event.is_set():
                return False

            try:
                lease_result = self.client.blocking_lease(
                    queue=queue_name,
                    timeout=effective_timeout,
                    visibility_secs=task_def.visibility_secs,
                )
            except Exception as e:
                logger.error("Error polling queue '%s': %s", queue_name, e)
                continue

            if lease_result is not None:
                task_id, payload = lease_result
                self._execute_task(task_def, task_id, payload)
                return True

        return False

    def stop(self) -> None:
        """Signals the worker event loop to stop cleanly after current task completes."""
        logger.info("Worker stop requested.")
        self._stop_event.set()

    def _install_signal_handlers(self) -> None:
        """Installs SIGINT and SIGTERM handlers for graceful shutdown if running in main thread."""
        if threading.current_thread() is not threading.main_thread():
            return
        if self._registered_signals:
            return

        def _handler(signum, frame):
            sig_name = signal.Signals(signum).name if hasattr(signal, "Signals") else str(signum)
            logger.info("Received termination signal %s. Shutting down worker gracefully...", sig_name)
            self.stop()

        for sig in (getattr(signal, "SIGINT", None), getattr(signal, "SIGTERM", None)):
            if sig is not None:
                try:
                    signal.signal(sig, _handler)
                except (ValueError, OSError):
                    pass
        self._registered_signals = True

    def run(self) -> None:
        """
        Main worker execution loop. Continuously polls queues and executes tasks
        until worker.stop() is called or a SIGINT/SIGTERM signal is received.
        """
        self._install_signal_handlers()
        self._stop_event.clear()
        self._running = True

        queue_names = list(self.tasks.keys())
        logger.info("Worker started, listening on queues: %s", queue_names)

        try:
            while not self._stop_event.is_set():
                self.run_once(timeout=self.poll_timeout)
        except KeyboardInterrupt:
            logger.info("Worker interrupted via KeyboardInterrupt.")
            self.stop()
        finally:
            self._running = False
            logger.info("Worker event loop finished.")
