"""
Distributed Task Queue (DTQ) RESP Client Implementation.

Provides a pure-Python, zero-dependency client communicating with the
distributed task queue server via Redis Serialization Protocol (RESP).
"""

from __future__ import annotations

import io
import socket
import threading
from typing import Any, Dict, List, Optional, Tuple, Union


class TaskQueueError(Exception):
    """Base exception for all Distributed Task Queue errors."""
    pass


class TaskQueueConnectionError(TaskQueueError, ConnectionError):
    """Raised when network connection to the queue server fails or is terminated."""
    pass


class TaskQueueResponseError(TaskQueueError):
    """Raised when the server returns a RESP error frame (-ERR or similar)."""
    pass


class TaskQueueAuthError(TaskQueueResponseError):
    """Raised when server authentication fails or authentication is required."""
    pass


class TaskQueueProtocolError(TaskQueueError):
    """Raised when encountering malformed or unexpected RESP protocol frames."""
    pass


# Convenience aliases
ConnectionError = TaskQueueConnectionError
ResponseError = TaskQueueResponseError
ProtocolError = TaskQueueProtocolError


def encode_command(*args: Union[str, bytes, int, float]) -> bytes:
    """
    Encodes command name and arguments into a standard RESP Array of Bulk Strings.

    Format:
        *<num_args>\\r\\n$<len0>\\r\\n<arg0>\\r\\n...
    """
    b_args: List[bytes] = []
    for arg in args:
        if isinstance(arg, bytes):
            b_args.append(arg)
        elif isinstance(arg, str):
            b_args.append(arg.encode("utf-8"))
        elif isinstance(arg, (int, float)):
            b_args.append(str(arg).encode("ascii"))
        else:
            b_args.append(str(arg).encode("utf-8"))

    parts: List[bytes] = [f"*{len(b_args)}\r\n".encode("ascii")]
    for b in b_args:
        parts.append(f"${len(b)}\r\n".encode("ascii"))
        parts.append(b)
        parts.append(b"\r\n")

    return b"".join(parts)


class TaskQueueClient:
    """
    Pure-Python RESP client for interacting with the distributed task queue server.

    Zero third-party dependencies required. Thread-safe for multi-threaded usage.
    """

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 6379,
        password: Optional[str] = None,
        socket_timeout: Optional[float] = None,
    ):
        """
        Initialize a new TaskQueueClient instance.

        :param host: Server IP address or hostname (default: "127.0.0.1").
        :param port: Server listening TCP port (default: 6379).
        :param password: Optional authentication password configured via --requirepass.
        :param socket_timeout: Optional network socket timeout in seconds.
        """
        self.host = host
        self.port = port
        self.password = password
        self.socket_timeout = socket_timeout

        self._sock: Optional[socket.socket] = None
        self._rfile: Optional[io.BufferedReader] = None
        self._lock = threading.RLock()

    @property
    def is_connected(self) -> bool:
        """Returns True if the socket is currently connected."""
        return self._sock is not None

    def connect(self) -> None:
        """
        Establish a TCP connection to the queue server and authenticate if password was configured.

        :raises TaskQueueConnectionError: If connection fails.
        :raises TaskQueueAuthError: If authentication fails.
        """
        with self._lock:
            if self._sock is not None:
                return

            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                if self.socket_timeout is not None:
                    sock.settimeout(self.socket_timeout)

                sock.connect((self.host, self.port))
                self._sock = sock
                self._rfile = sock.makefile("rb", buffering=65536)

            except (socket.error, OSError) as e:
                self.close()
                raise TaskQueueConnectionError(
                    f"Failed to connect to task queue server at {self.host}:{self.port}: {e}"
                ) from e

            # Perform authentication handshake if a password is provided
            if self.password is not None:
                if not self.auth(self.password):
                    self.close()
                    raise TaskQueueAuthError("Authentication handshake failed with configured password.")

    def close(self) -> None:
        """Closes the active connection and releases socket resources."""
        with self._lock:
            if self._rfile is not None:
                try:
                    self._rfile.close()
                except Exception:
                    pass
                self._rfile = None

            if self._sock is not None:
                try:
                    self._sock.shutdown(socket.SHUT_RDWR)
                except Exception:
                    pass
                try:
                    self._sock.close()
                except Exception:
                    pass
                self._sock = None

    disconnect = close

    def __enter__(self) -> "TaskQueueClient":
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def execute_command(self, *args: Union[str, bytes, int, float], cmd_timeout: Optional[float] = None) -> Any:
        """
        Encodes and transmits a command frame across the TCP stream, returning the parsed RESP result.

        :param args: Command name and arguments.
        :param cmd_timeout: Optional timeout in seconds to override socket timeout for blocking commands.
        :return: Parsed RESP response object.
        """
        with self._lock:
            if self._sock is None:
                self.connect()

            orig_timeout = self._sock.gettimeout() if self._sock else None
            if cmd_timeout is not None and self._sock is not None:
                if cmd_timeout <= 0:
                    # Timeout of 0 means block indefinitely
                    self._sock.settimeout(None)
                else:
                    effective_timeout = max(orig_timeout or 0, cmd_timeout + 5.0)
                    self._sock.settimeout(effective_timeout)

            wire = encode_command(*args)
            try:
                self._sock.sendall(wire)
                response = self._read_response()
                return response
            except (socket.error, OSError) as e:
                self.close()
                raise TaskQueueConnectionError(f"Network error during command execution: {e}") from e
            finally:
                if cmd_timeout is not None and self._sock is not None:
                    self._sock.settimeout(orig_timeout)

    def _read_exact(self, length: int) -> bytes:
        """Reads exactly `length` bytes from the socket read stream."""
        if length == 0:
            return b""
        data = bytearray()
        while len(data) < length:
            chunk = self._rfile.read(length - len(data))
            if not chunk:
                raise TaskQueueConnectionError("Connection closed while reading bulk data payload")
            data.extend(chunk)
        return bytes(data)

    def _read_line(self) -> bytes:
        """Reads a CRLF-terminated line from the stream buffer."""
        line = self._rfile.readline()
        if not line:
            raise TaskQueueConnectionError("Connection closed unexpectedly by server")
        if not line.endswith(b"\r\n"):
            raise TaskQueueProtocolError(f"Malformed RESP line (missing CRLF): {line!r}")
        return line[:-2]

    def _read_response(self) -> Any:
        """Parses a single RESP frame from the active connection stream."""
        marker = self._rfile.read(1)
        if not marker:
            raise TaskQueueConnectionError("Connection closed unexpectedly by server")

        if marker == b"+":
            # Simple String: +<str>\r\n
            return self._read_line().decode("utf-8", errors="replace")

        elif marker == b"-":
            # Error String: -<msg>\r\n
            err_msg = self._read_line().decode("utf-8", errors="replace")
            if err_msg.startswith("WRONGPASS") or err_msg.startswith("NOAUTH"):
                raise TaskQueueAuthError(err_msg)
            raise TaskQueueResponseError(err_msg)

        elif marker == b":":
            # Integer: :<num>\r\n
            line = self._read_line()
            try:
                return int(line)
            except ValueError as e:
                raise TaskQueueProtocolError(f"Invalid integer in RESP response: {line!r}") from e

        elif marker == b"$":
            # Bulk String: $<length>\r\n<data>\r\n or $-1\r\n
            len_line = self._read_line()
            try:
                length = int(len_line)
            except ValueError as e:
                raise TaskQueueProtocolError(f"Invalid bulk length in RESP response: {len_line!r}") from e

            if length == -1:
                return None

            data = self._read_exact(length)
            crlf = self._read_exact(2)
            if crlf != b"\r\n":
                raise TaskQueueProtocolError(f"Missing CRLF terminator after bulk string data: {crlf!r}")
            return data

        elif marker == b"*":
            # Array: *<count>\r\n... or *-1\r\n
            count_line = self._read_line()
            try:
                count = int(count_line)
            except ValueError as e:
                raise TaskQueueProtocolError(f"Invalid array count in RESP response: {count_line!r}") from e

            if count == -1:
                return None

            return [self._read_response() for _ in range(count)]

        else:
            raise TaskQueueProtocolError(f"Unrecognized RESP type marker: {marker!r}")

    # =========================================================================
    # High-level Client API Methods
    # =========================================================================

    def ping(self) -> bool:
        """
        Sends health-check probe (PING) to the server.

        :return: True if the server responded with PONG, False otherwise.
        """
        try:
            return self.execute_command("PING") == "PONG"
        except (TaskQueueError, OSError):
            return False

    def auth(self, password: str) -> bool:
        """
        Authenticate connection against a server configured with --requirepass.

        :param password: Authentication secret string.
        :return: True if authentication succeeded, False otherwise.
        """
        try:
            return self.execute_command("AUTH", password) == "OK"
        except TaskQueueResponseError:
            return False

    def enqueue(self, queue: str, payload: Union[str, bytes], delay: Optional[float] = None) -> int:
        """
        Push a task item into the queue.

        Uses LPUSH for immediate tasks and LPUSH_DELAY for tasks with delayed execution.

        :param queue: Name of destination queue.
        :param payload: Task payload as string or raw bytes.
        :param delay: Optional delay in seconds before task becomes eligible for consumption.
        :return: Queue length (for immediate task) or assigned task ID integer (for delayed task).
        """
        if isinstance(payload, str):
            payload_bytes = payload.encode("utf-8")
        else:
            payload_bytes = payload

        if delay is not None and delay > 0:
            return int(self.execute_command("LPUSH_DELAY", queue, delay, payload_bytes))
        else:
            return int(self.execute_command("LPUSH", queue, payload_bytes))

    def pop(self, queue: str) -> Optional[bytes]:
        """
        Non-blocking pop from the tail of the queue (RPOP).

        :param queue: Name of queue.
        :return: Raw payload bytes if available, or None if queue is empty.
        """
        res = self.execute_command("RPOP", queue)
        if res is None:
            return None
        return res if isinstance(res, bytes) else str(res).encode("utf-8")

    def blocking_pop(self, queues: Union[str, List[str]], timeout: float = 0.0) -> Optional[Tuple[str, bytes]]:
        """
        Non-busy blocking pop across one or more queues (BRPOP).

        :param queues: Queue name or list of queue names to pop from.
        :param timeout: Maximum seconds to block (0.0 blocks indefinitely).
        :return: Tuple of (queue_name, payload) or None if timeout elapsed.
        """
        if isinstance(queues, str):
            q_list = [queues]
        else:
            q_list = list(queues)

        args = ["BRPOP"] + q_list + [timeout]
        res = self.execute_command(*args, cmd_timeout=timeout if timeout > 0 else 0)
        if res is None:
            return None

        q_name = res[0].decode("utf-8") if isinstance(res[0], bytes) else str(res[0])
        payload = res[1] if isinstance(res[1], bytes) else str(res[1]).encode("utf-8")
        return (q_name, payload)

    def lease(self, queue: str, visibility_secs: float = 30.0) -> Optional[Tuple[str, bytes]]:
        """
        Non-blocking pop and lease task with visibility timeout window (RPOPLEASE).

        If the task is not settled (via ack/nack) within `visibility_secs`, the server
        background lease reaper will automatically re-queue it or escalate to DLQ.

        :param queue: Name of queue.
        :param visibility_secs: Duration in seconds for visibility lease window (default: 30.0).
        :return: Tuple of (task_id, payload) or None if queue is empty.
        """
        res = self.execute_command("RPOPLEASE", queue, visibility_secs)
        if res is None:
            return None

        task_id = res[0].decode("utf-8") if isinstance(res[0], bytes) else str(res[0])
        payload = res[1] if isinstance(res[1], bytes) else str(res[1]).encode("utf-8")
        return (task_id, payload)

    def blocking_lease(
        self,
        queue: str,
        timeout: float = 5.0,
        visibility_secs: float = 30.0,
    ) -> Optional[Tuple[str, bytes]]:
        """
        Blocking pop and lease task with visibility timeout window (BRPOPLEASE).

        Blocks until a task is available or `timeout` seconds expire.

        :param queue: Name of queue.
        :param timeout: Maximum seconds to wait for a task (default: 5.0).
        :param visibility_secs: Duration in seconds for visibility lease window (default: 30.0).
        :return: Tuple of (task_id, payload) or None if timeout expired.
        """
        res = self.execute_command(
            "BRPOPLEASE",
            queue,
            timeout,
            visibility_secs,
            cmd_timeout=timeout if timeout > 0 else 0,
        )
        if res is None:
            return None

        task_id = res[0].decode("utf-8") if isinstance(res[0], bytes) else str(res[0])
        payload = res[1] if isinstance(res[1], bytes) else str(res[1]).encode("utf-8")
        return (task_id, payload)

    def touch(self, queue: str, task_id: str, extend_secs: float = 30.0) -> bool:
        """
        Heartbeat command to extend active visibility lease of in-flight task (TASKTOUCH).

        :param queue: Name of queue task was leased from.
        :param task_id: Unique task identifier assigned during lease.
        :param extend_secs: Additional visibility seconds to add (default: 30.0).
        :return: True if lease was extended, False if task was not found or expired.
        """
        try:
            return self.execute_command("TASKTOUCH", queue, task_id, extend_secs) == "OK"
        except TaskQueueResponseError:
            return False

    def ack(self, queue: str, task_id: str) -> bool:
        """
        Acknowledge successful completion of a leased task (TASKACK).

        Removes task permanently from the in-flight lease map.

        :param queue: Name of queue.
        :param task_id: Unique task identifier.
        :return: True if task was acknowledged, False if task was not found.
        """
        try:
            return self.execute_command("TASKACK", queue, task_id) == "OK"
        except TaskQueueResponseError:
            return False

    def nack(self, queue: str, task_id: str) -> bool:
        """
        Negative-acknowledge a failed task (TASKNACK).

        Increments retry count; immediately re-queues the task or escalates it
        to the Dead-Letter Queue (DLQ) if maximum retries are exceeded.

        :param queue: Name of queue.
        :param task_id: Unique task identifier.
        :return: True if task was nacked, False if task was not found.
        """
        try:
            return self.execute_command("TASKNACK", queue, task_id) == "OK"
        except TaskQueueResponseError:
            return False

    def info(self) -> Dict[str, Any]:
        """
        Query diagnostic server statistics and queue engine metrics (INFO).

        :return: Dictionary containing parsed engine statistics and per-queue metrics.
        """
        raw = self.execute_command("INFO")
        text = raw.decode("utf-8") if isinstance(raw, bytes) else str(raw)
        return self._parse_info(text)

    @staticmethod
    def _parse_info(text: str) -> Dict[str, Any]:
        """Parses server INFO bulk response text into structured dictionary."""
        result: Dict[str, Any] = {
            "queues": {},
            "raw": text,
        }

        for line in text.splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue

            if ";" in line:
                # Per-queue entry: queue_<name>:<len>;in_flight:<in_flight>;dlq:<dlq>
                segments = line.split(";")
                q_info: Dict[str, int] = {}
                queue_name = None
                for seg in segments:
                    if ":" in seg:
                        k, v = seg.split(":", 1)
                        k, v = k.strip(), v.strip()
                        val = int(v) if v.isdigit() else v
                        if k.startswith("queue_"):
                            queue_name = k[len("queue_"):]
                            q_info["length"] = val
                        else:
                            q_info[k] = val
                if queue_name:
                    result["queues"][queue_name] = q_info
            elif ":" in line:
                k, v = line.split(":", 1)
                k, v = k.strip(), v.strip()
                val = int(v) if v.isdigit() else v
                result[k] = val

        return result
