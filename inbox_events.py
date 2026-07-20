"""Inbox SSE connection hub and Redis pub/sub fanout across containers."""
from __future__ import annotations

import asyncio
import json
import os
import threading
import time
from collections import defaultdict
from typing import Any, Dict, Optional, Set

try:
    import redis as redis_lib
except Exception:
    redis_lib = None

INBOX_EVENTS_CHANNEL = "inbox:events"
HEARTBEAT_SECONDS = 30


def format_sse_event(event: str, data: Dict[str, Any]) -> str:
    return f"event: {event}\ndata: {json.dumps(data)}\n\n"


class InboxEventHub:
    def __init__(self) -> None:
        self._queues: Dict[str, Set[asyncio.Queue]] = defaultdict(set)
        self._lock = threading.Lock()
        self._pub_client = None
        self._listener_thread: Optional[threading.Thread] = None
        self._started = False

    def _get_pub_client(self):
        if self._pub_client is not None:
            return self._pub_client
        url = os.environ.get("REDIS_URL")
        if not redis_lib or not url:
            return None
        try:
            self._pub_client = redis_lib.Redis.from_url(url, decode_responses=True)
            self._pub_client.ping()
        except Exception as exc:
            print(f"WARNING: Inbox events Redis publish client unavailable: {exc}")
            self._pub_client = None
        return self._pub_client

    def register(self, user_id: str, queue: asyncio.Queue) -> None:
        with self._lock:
            self._queues[user_id].add(queue)

    def unregister(self, user_id: str, queue: asyncio.Queue) -> None:
        with self._lock:
            conns = self._queues.get(user_id)
            if not conns:
                return
            conns.discard(queue)
            if not conns:
                self._queues.pop(user_id, None)

    def _deliver_local(self, user_id: str, payload: Dict[str, Any]) -> None:
        with self._lock:
            queues = list(self._queues.get(user_id, ()))
        for queue in queues:
            try:
                queue.put_nowait(payload)
            except Exception:
                pass

    def handle_redis_message(self, raw: str) -> None:
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            return
        user_id = payload.get("user_id")
        if not user_id:
            return
        self._deliver_local(str(user_id), payload)

    def publish_unread_changed(self, user_id: str, unread_count: int) -> None:
        payload = {"user_id": str(user_id), "unread_count": max(0, int(unread_count))}
        client = self._get_pub_client()
        if client is not None:
            try:
                client.publish(INBOX_EVENTS_CHANNEL, json.dumps(payload))
                return
            except Exception as exc:
                print(f"WARNING: Inbox events Redis publish failed: {exc}")
        self._deliver_local(str(user_id), payload)

    def start_listener(self) -> None:
        if self._started:
            return
        self._started = True
        url = os.environ.get("REDIS_URL")
        if not redis_lib or not url:
            print("Inbox events: Redis pub/sub listener disabled (no REDIS_URL)")
            return
        self._listener_thread = threading.Thread(
            target=self._redis_listen_loop,
            name="inbox-events-redis",
            daemon=True,
        )
        self._listener_thread.start()

    def _redis_listen_loop(self) -> None:
        url = os.environ.get("REDIS_URL")
        while True:
            try:
                client = redis_lib.Redis.from_url(url, decode_responses=True)
                pubsub = client.pubsub(ignore_subscribe_messages=True)
                pubsub.subscribe(INBOX_EVENTS_CHANNEL)
                print(f"Inbox events: subscribed to {INBOX_EVENTS_CHANNEL}")
                for message in pubsub.listen():
                    if message.get("type") != "message":
                        continue
                    data = message.get("data")
                    if isinstance(data, str):
                        self.handle_redis_message(data)
            except Exception as exc:
                print(f"WARNING: Inbox events Redis listener error: {exc}")
                time.sleep(5)


hub = InboxEventHub()


def notify_inbox_unread_changed(user_id: str, unread_count: Optional[int] = None) -> None:
    if unread_count is None:
        from inbox_db import InboxDb

        unread_count, err = InboxDb().inbox_unread_count(str(user_id))
        if err:
            return
    hub.publish_unread_changed(str(user_id), unread_count)


async def inbox_sse_generator(user_id: str):
    queue: asyncio.Queue = asyncio.Queue()
    hub.register(user_id, queue)
    try:
        from inbox_db import InboxDb

        count, _ = InboxDb().inbox_unread_count(user_id)
        yield format_sse_event("inbox", {"unread_count": count})
        while True:
            try:
                payload = await asyncio.wait_for(queue.get(), timeout=HEARTBEAT_SECONDS)
                yield format_sse_event(
                    "inbox",
                    {"unread_count": int(payload.get("unread_count", 0))},
                )
            except asyncio.TimeoutError:
                yield ": heartbeat\n\n"
    finally:
        hub.unregister(user_id, queue)


def start_inbox_event_listener() -> None:
    hub.start_listener()
