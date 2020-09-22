from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, tzinfo
from typing import Dict, Optional

import pytz
import zmq
import zmq.asyncio

from cron import CronEntry

log = logging.getLogger("serpent")


class ScheduledPayloadTask:

    __slots__ = ("schedule", "payload", "uuid", "zone")

    def __init__(self, schedule: CronEntry, payload: bytes, uuid: bytes, zone: tzinfo):
        self.schedule: CronEntry = schedule
        self.payload: bytes = payload
        self.uuid: bytes = uuid
        self.zone: tzinfo = zone

    def check_time(self, when: datetime) -> bool:
        return self.schedule.field_match(when.astimezone(self.zone))

    def __hash__(self):
        return hash((type(self), self.uuid))

    def __eq__(self, other):
        if isinstance(other, type(self)):
            return self.uuid == other.uuid
        return NotImplemented


class Scheduler:
    def __init__(self, socket: zmq.asyncio.Socket):
        asyncio.get_running_loop()  # we don't want this being created outside a running event loop.
        self.socket: zmq.asyncio.Socket = socket
        self._tasks: Dict[bytes, ScheduledPayloadTask] = {}
        self._iter_lock = asyncio.Lock()
        self._loop_task: Optional[asyncio.Task] = None

    async def __aenter__(self):
        if self._loop_task is not None:
            raise RuntimeError("Can't reuse this context manager")

        self._loop_task = asyncio.create_task(self.scheduling_loop())
        return self

    async def __aexit__(self, *args):
        self._loop_task.cancel()

    async def add_task(self, task: ScheduledPayloadTask):
        async with self._iter_lock:
            self._tasks[task.uuid] = task

    async def remove_task(self, uuid: bytes):
        async with self._iter_lock:
            self._tasks.pop(uuid, None)

    async def safe_send(self, payload):
        try:
            await self.socket.send(payload)
        except Exception as exc:
            log.exception(
                "Issue with sending scheduled payload %s", payload, exc_info=exc
            )

    async def scheduling_loop(self):

        now = datetime.utcnow().replace(second=0, microsecond=0, tzinfo=pytz.utc)
        last = now - timedelta(minutes=1)
        while True:
            await asyncio.sleep(15)
            now = datetime.utcnow().replace(second=0, microsecond=0, tzinfo=pytz.utc)
            if now <= last:
                continue

            async with self._iter_lock:
                tsks = {
                    self.safe_send(t.payload)
                    for t in self._tasks.values()
                    if t.check_time(now)
                }
                f = asyncio.gather(*tsks, return_exceptions=True)
                asyncio.create_task(f)
            last = now
