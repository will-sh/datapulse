from __future__ import annotations

import threading
import time
from collections import deque
from dataclasses import dataclass


@dataclass(frozen=True)
class StreamEvent:
    message: str
    kafka_timestamp: str | None
    received_at: float


class EventStore:
    def __init__(self, max_events: int = 200) -> None:
        self._events: deque[StreamEvent] = deque(maxlen=max_events)
        self._lock = threading.Lock()
        self.total_received = 0
        self.last_error: str | None = None
        self.stream_active = False
        self.spark_status = "initializing"

    def add(self, message: str, kafka_timestamp: str | None = None) -> None:
        event = StreamEvent(message=message, kafka_timestamp=kafka_timestamp, received_at=time.time())
        with self._lock:
            self._events.appendleft(event)
            self.total_received += 1

    def set_error(self, message: str) -> None:
        with self._lock:
            self.last_error = message
            self.stream_active = False
            self.spark_status = "error"

    def set_status(self, status: str, active: bool | None = None) -> None:
        with self._lock:
            self.spark_status = status
            if active is not None:
                self.stream_active = active

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "total_received": self.total_received,
                "last_error": self.last_error,
                "stream_active": self.stream_active,
                "spark_status": self.spark_status,
                "events": [
                    {
                        "message": event.message,
                        "kafka_timestamp": event.kafka_timestamp,
                        "received_at": event.received_at,
                    }
                    for event in list(self._events)
                ],
            }


STORE = EventStore()
