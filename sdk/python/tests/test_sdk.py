"""
Unit and integration tests for Distributed Task Queue (DTQ) Python Client & Worker SDK.
"""

import io
import os
import socket
import subprocess
import sys
import threading
import time
import unittest
from typing import Any, List, Optional

# Ensure sdk/python is in sys.path
SDK_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if SDK_ROOT not in sys.path:
    sys.path.insert(0, SDK_ROOT)

from dtq import (
    TaskQueueAuthError,
    TaskQueueClient,
    TaskQueueConnectionError,
    TaskQueueError,
    TaskQueueProtocolError,
    TaskQueueResponseError,
    Worker,
    encode_command,
)


class TestRespEncoding(unittest.TestCase):
    """Unit tests for RESP command wire serialization."""

    def test_encode_simple_command(self):
        wire = encode_command("PING")
        self.assertEqual(wire, b"*1\r\n$4\r\nPING\r\n")

    def test_encode_command_with_arguments(self):
        wire = encode_command("LPUSH", "orders", "payload-123")
        expected = b"*3\r\n$5\r\nLPUSH\r\n$6\r\norders\r\n$11\r\npayload-123\r\n"
        self.assertEqual(wire, expected)

    def test_encode_command_with_bytes_and_numbers(self):
        wire = encode_command("LPUSH_DELAY", "emails", 5.5, b"\x00\xffbinary")
        expected = (
            b"*4\r\n"
            b"$11\r\nLPUSH_DELAY\r\n"
            b"$6\r\nemails\r\n"
            b"$3\r\n5.5\r\n"
            b"$8\r\n\x00\xffbinary\r\n"
        )
        self.assertEqual(wire, expected)


class MockSocketPair:
    """Creates a connected pair of mock socket streams for deterministic protocol testing."""

    def __init__(self):
        self.server_sock, self.client_sock = socket.socketpair()
        self.server_rfile = self.server_sock.makefile("rb")

    def client_client(self, **kwargs) -> TaskQueueClient:
        c = TaskQueueClient(**kwargs)
        c._sock = self.client_sock
        c._rfile = self.client_sock.makefile("rb", buffering=65536)
        return c

    def server_respond(self, data: bytes):
        self.server_sock.sendall(data)

    def server_recv_command(self) -> bytes:
        # Read until client finishes sending current command
        line = self.server_rfile.readline()
        if not line.startswith(b"*"):
            return line
        count = int(line[1:-2])
        parts = [line]
        for _ in range(count):
            len_line = self.server_rfile.readline()
            parts.append(len_line)
            length = int(len_line[1:-2])
            payload = self.server_rfile.read(length + 2)
            parts.append(payload)
        return b"".join(parts)

    def close(self):
        self.server_rfile.close()
        self.server_sock.close()
        self.client_sock.close()


class TestMockClientProtocol(unittest.TestCase):
    """Unit tests for RESP parsing and client commands using socket pairs."""

    def setUp(self):
        self.pair = MockSocketPair()
        self.client = self.pair.client_client()

    def tearDown(self):
        self.client.close()
        self.pair.close()

    def test_ping_success(self):
        def respond():
            _ = self.pair.server_recv_command()
            self.pair.server_respond(b"+PONG\r\n")

        t = threading.Thread(target=respond)
        t.start()
        self.assertTrue(self.client.ping())
        t.join()

    def test_auth_success_and_failure(self):
        # Successful auth
        def respond_ok():
            _ = self.pair.server_recv_command()
            self.pair.server_respond(b"+OK\r\n")

        t = threading.Thread(target=respond_ok)
        t.start()
        self.assertTrue(self.client.auth("secret"))
        t.join()

        # Failed auth
        def respond_err():
            _ = self.pair.server_recv_command()
            self.pair.server_respond(b"-WRONGPASS invalid password\r\n")

        t2 = threading.Thread(target=respond_err)
        t2.start()
        self.assertFalse(self.client.auth("wrong"))
        t2.join()

    def test_enqueue_immediate_and_delayed(self):
        # Immediate LPUSH
        def respond_lpush():
            cmd = self.pair.server_recv_command()
            self.assertIn(b"LPUSH", cmd)
            self.pair.server_respond(b":4\r\n")

        t = threading.Thread(target=respond_lpush)
        t.start()
        q_len = self.client.enqueue("tasks", "task-body")
        self.assertEqual(q_len, 4)
        t.join()

        # Delayed LPUSH_DELAY
        def respond_delay():
            cmd = self.pair.server_recv_command()
            self.assertIn(b"LPUSH_DELAY", cmd)
            self.pair.server_respond(b":101\r\n")

        t2 = threading.Thread(target=respond_delay)
        t2.start()
        task_id = self.client.enqueue("tasks", "future-body", delay=10.0)
        self.assertEqual(task_id, 101)
        t2.join()

    def test_pop_available_and_nil(self):
        # Item available
        def respond_item():
            _ = self.pair.server_recv_command()
            self.pair.server_respond(b"$5\r\nhello\r\n")

        t = threading.Thread(target=respond_item)
        t.start()
        item = self.client.pop("tasks")
        self.assertEqual(item, b"hello")
        t.join()

        # Queue empty (nil)
        def respond_nil():
            _ = self.pair.server_recv_command()
            self.pair.server_respond(b"$-1\r\n")

        t2 = threading.Thread(target=respond_nil)
        t2.start()
        item_nil = self.client.pop("tasks")
        self.assertIsNone(item_nil)
        t2.join()

    def test_blocking_pop(self):
        # Found item
        def respond_brpop():
            _ = self.pair.server_recv_command()
            self.pair.server_respond(b"*2\r\n$5\r\ntasks\r\n$4\r\ndata\r\n")

        t = threading.Thread(target=respond_brpop)
        t.start()
        res = self.client.blocking_pop(["tasks"], timeout=2.0)
        self.assertEqual(res, ("tasks", b"data"))
        t.join()

        # Timeout (nil)
        def respond_brpop_timeout():
            _ = self.pair.server_recv_command()
            self.pair.server_respond(b"$-1\r\n")

        t2 = threading.Thread(target=respond_brpop_timeout)
        t2.start()
        res2 = self.client.blocking_pop("tasks", timeout=1.0)
        self.assertIsNone(res2)
        t2.join()

    def test_lease_and_blocking_lease(self):
        # RPOPLEASE
        def respond_lease():
            _ = self.pair.server_recv_command()
            self.pair.server_respond(b"*2\r\n$6\r\ntask-1\r\n$7\r\npayload\r\n")

        t = threading.Thread(target=respond_lease)
        t.start()
        res = self.client.lease("tasks", visibility_secs=15.0)
        self.assertEqual(res, ("task-1", b"payload"))
        t.join()

        # BRPOPLEASE
        def respond_brpop_lease():
            _ = self.pair.server_recv_command()
            self.pair.server_respond(b"*2\r\n$6\r\ntask-2\r\n$5\r\nwork2\r\n")

        t2 = threading.Thread(target=respond_brpop_lease)
        t2.start()
        res2 = self.client.blocking_lease("tasks", timeout=5.0, visibility_secs=20.0)
        self.assertEqual(res2, ("task-2", b"work2"))
        t2.join()

    def test_touch_ack_nack(self):
        # Touch OK
        def respond_touch():
            _ = self.pair.server_recv_command()
            self.pair.server_respond(b"+OK\r\n")

        t = threading.Thread(target=respond_touch)
        t.start()
        self.assertTrue(self.client.touch("tasks", "task-1", extend_secs=30.0))
        t.join()

        # Touch not found (-ERR)
        def respond_touch_err():
            _ = self.pair.server_recv_command()
            self.pair.server_respond(b"-ERR Task ID not found in-flight\r\n")

        t2 = threading.Thread(target=respond_touch_err)
        t2.start()
        self.assertFalse(self.client.touch("tasks", "task-99"))
        t2.join()

        # ACK OK
        def respond_ack():
            _ = self.pair.server_recv_command()
            self.pair.server_respond(b"+OK\r\n")

        t3 = threading.Thread(target=respond_ack)
        t3.start()
        self.assertTrue(self.client.ack("tasks", "task-1"))
        t3.join()

        # NACK OK
        def respond_nack():
            _ = self.pair.server_recv_command()
            self.pair.server_respond(b"+OK\r\n")

        t4 = threading.Thread(target=respond_nack)
        t4.start()
        self.assertTrue(self.client.nack("tasks", "task-1"))
        t4.join()

    def test_info_parsing(self):
        info_raw = (
            b"# QueueEngine\r\n"
            b"delayed_tasks:2\r\n"
            b"# Queues\r\n"
            b"queue_orders:10;in_flight:3;dlq:1\r\n"
            b"queue_emails:0;in_flight:0;dlq:0\r\n"
        )

        def respond_info():
            _ = self.pair.server_recv_command()
            self.pair.server_respond(f"${len(info_raw)}\r\n".encode("ascii") + info_raw + b"\r\n")

        t = threading.Thread(target=respond_info)
        t.start()
        info = self.client.info()
        t.join()

        self.assertEqual(info["delayed_tasks"], 2)
        self.assertIn("orders", info["queues"])
        self.assertEqual(info["queues"]["orders"]["length"], 10)
        self.assertEqual(info["queues"]["orders"]["in_flight"], 3)
        self.assertEqual(info["queues"]["orders"]["dlq"], 1)
        self.assertEqual(info["queues"]["emails"]["length"], 0)


class MockQueueClient:
    """Mock client for testing Worker logic in complete isolation without sockets."""

    def __init__(self):
        self.leased_items = []
        self.touched = []
        self.acked = []
        self.nacked = []
        self.lock = threading.Lock()

    def blocking_lease(self, queue: str, timeout: float = 5.0, visibility_secs: float = 30.0):
        with self.lock:
            if self.leased_items:
                return self.leased_items.pop(0)
            return None

    def touch(self, queue: str, task_id: str, extend_secs: float = 30.0) -> bool:
        with self.lock:
            self.touched.append((queue, task_id, extend_secs))
            return True

    def ack(self, queue: str, task_id: str) -> bool:
        with self.lock:
            self.acked.append((queue, task_id))
            return True

    def nack(self, queue: str, task_id: str) -> bool:
        with self.lock:
            self.nacked.append((queue, task_id))
            return True


class TestWorkerAbstraction(unittest.TestCase):
    """Unit tests for Worker registration, execution, heartbeats, and ACK/NACK."""

    def setUp(self):
        self.mock_client = MockQueueClient()
        self.worker = Worker(client=self.mock_client, poll_timeout=0.05)

    def test_worker_registration_decorator(self):
        @self.worker.task(queue="custom_q", visibility_secs=45.0, heartbeat_interval=15.0)
        def my_task(payload):
            pass

        self.assertIn("custom_q", self.worker.tasks)
        task_def = self.worker.tasks["custom_q"]
        self.assertEqual(task_def.visibility_secs, 45.0)
        self.assertEqual(task_def.heartbeat_interval, 15.0)
        self.assertIs(task_def.func, my_task)

    def test_worker_registration_implicit_name(self):
        @self.worker.task
        def default_queue_task(payload):
            pass

        self.assertIn("default_queue_task", self.worker.tasks)

    def test_worker_successful_task_execution_acks(self):
        executed_payloads = []

        @self.worker.task(queue="orders")
        def handle_order(payload: bytes):
            executed_payloads.append(payload)

        self.mock_client.leased_items.append(("task-10", b"order_data"))
        did_work = self.worker.run_once(timeout=0.01)

        self.assertTrue(did_work)
        self.assertEqual(executed_payloads, [b"order_data"])
        self.assertEqual(self.mock_client.acked, [("orders", "task-10")])
        self.assertEqual(self.mock_client.nacked, [])

    def test_worker_failed_task_execution_nacks(self):
        @self.worker.task(queue="reports")
        def handle_report(payload: bytes):
            raise ValueError("Processing exploded!")

        self.mock_client.leased_items.append(("task-11", b"bad_report"))
        did_work = self.worker.run_once(timeout=0.01)

        self.assertTrue(did_work)
        self.assertEqual(self.mock_client.acked, [])
        self.assertEqual(self.mock_client.nacked, [("reports", "task-11")])

    def test_worker_heartbeat_during_long_task(self):
        @self.worker.task(queue="long_jobs", visibility_secs=1.0, heartbeat_interval=0.1)
        def slow_job(payload: bytes):
            time.sleep(0.35)

        self.mock_client.leased_items.append(("task-12", b"slow_payload"))
        did_work = self.worker.run_once(timeout=0.01)

        self.assertTrue(did_work)
        self.assertEqual(self.mock_client.acked, [("long_jobs", "task-12")])
        # Heartbeat should have touched at least 2 times during 0.35s sleep with 0.1s interval
        self.assertGreaterEqual(len(self.mock_client.touched), 2)
        queue, task_id, extend_secs = self.mock_client.touched[0]
        self.assertEqual(queue, "long_jobs")
        self.assertEqual(task_id, "task-12")
        self.assertEqual(extend_secs, 1.0)

    def test_worker_supports_signature_with_task_id(self):
        received_args = []

        @self.worker.task(queue="dual_args")
        def handler_with_id(task_id: str, payload: bytes):
            received_args.append((task_id, payload))

        self.mock_client.leased_items.append(("task-77", b"hello_77"))
        self.worker.run_once(timeout=0.01)
        self.assertEqual(received_args, [("task-77", b"hello_77")])

    def test_worker_graceful_stop(self):
        @self.worker.task(queue="jobs")
        def dummy(payload):
            pass

        def stop_after_delay():
            time.sleep(0.1)
            self.worker.stop()

        t = threading.Thread(target=stop_after_delay)
        t.start()
        # run() should terminate once worker.stop() is called
        self.worker.run()
        t.join()
        self.assertTrue(self.worker._stop_event.is_set())


class TestLiveServerIntegration(unittest.TestCase):
    """
    End-to-end integration tests spawning the compiled Rust server binary
    and verifying TaskQueueClient & Worker over actual TCP sockets.
    """

    SERVER_BIN = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "../../../target/release/distributed_task_queue")
    )

    @classmethod
    def setUpClass(cls):
        if not os.path.exists(cls.SERVER_BIN):
            raise unittest.SkipTest(f"Compiled binary not found at {cls.SERVER_BIN}")

        # Pick dynamic free port
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(("127.0.0.1", 0))
        cls.port = s.getsockname()[1]
        s.close()

        cls.aof_file = f"/tmp/test_dtq_{cls.port}.aof"
        if os.path.exists(cls.aof_file):
            try:
                os.remove(cls.aof_file)
            except OSError:
                pass

        cls.proc = subprocess.Popen(
            [
                cls.SERVER_BIN,
                "--bind",
                f"127.0.0.1:{cls.port}",
                "--aof",
                cls.aof_file,
                "--requirepass",
                "testpass123",
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        # Wait for server to accept connections
        time.sleep(0.4)

    @classmethod
    def tearDownClass(cls):
        if hasattr(cls, "proc") and cls.proc:
            cls.proc.terminate()
            cls.proc.wait()
        if hasattr(cls, "aof_file") and os.path.exists(cls.aof_file):
            try:
                os.remove(cls.aof_file)
            except OSError:
                pass

    def test_live_auth_and_ping(self):
        client = TaskQueueClient(host="127.0.0.1", port=self.port, password="testpass123")
        with client:
            self.assertTrue(client.ping())

        # Bad password raises TaskQueueAuthError
        bad_client = TaskQueueClient(host="127.0.0.1", port=self.port, password="wrongpassword")
        try:
            with self.assertRaises(TaskQueueAuthError):
                bad_client.connect()
        finally:
            bad_client.close()

    def test_live_producer_worker_pipeline(self):
        producer = TaskQueueClient(host="127.0.0.1", port=self.port, password="testpass123")
        worker_client = TaskQueueClient(host="127.0.0.1", port=self.port, password="testpass123")
        worker = Worker(client=worker_client, poll_timeout=0.1)

        processed = []

        @worker.task(queue="live_orders", visibility_secs=5.0, heartbeat_interval=1.0)
        def handle_order(payload: bytes):
            processed.append(payload.decode("utf-8"))

        try:
            with producer:
                producer.enqueue("live_orders", "order_1")
                producer.enqueue("live_orders", "order_2")

                # Worker processes order 1
                self.assertTrue(worker.run_once(timeout=1.0))
                # Worker processes order 2
                self.assertTrue(worker.run_once(timeout=1.0))

                self.assertEqual(processed, ["order_1", "order_2"])

                # Info stats show queue was drained
                info = producer.info()
                self.assertIn("live_orders", info["queues"])
                self.assertEqual(info["queues"]["live_orders"]["length"], 0)
                self.assertEqual(info["queues"]["live_orders"]["in_flight"], 0)
        finally:
            worker_client.close()


if __name__ == "__main__":
    unittest.main()
