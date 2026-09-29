"""
Distributed Task Queue (DTQ) Python Client & Worker SDK.

A lightweight, pure-Python client and background worker framework for
high-performance distributed task queues communicating via RESP.
"""

from dtq.client import (
    ConnectionError,
    ProtocolError,
    ResponseError,
    TaskQueueAuthError,
    TaskQueueClient,
    TaskQueueConnectionError,
    TaskQueueError,
    TaskQueueProtocolError,
    TaskQueueResponseError,
    encode_command,
)
from dtq.worker import TaskDefinition, Worker

__version__ = "0.1.0"

__all__ = [
    "TaskQueueClient",
    "Worker",
    "TaskDefinition",
    "TaskQueueError",
    "TaskQueueConnectionError",
    "TaskQueueResponseError",
    "TaskQueueAuthError",
    "TaskQueueProtocolError",
    "ConnectionError",
    "ResponseError",
    "ProtocolError",
    "encode_command",
]
