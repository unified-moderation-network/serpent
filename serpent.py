"""
A scheduler of sorts
"""

import asyncio
from datetime import datetime, timezone
from typing import Set

import msgpack
import zmq
import zmq.asyncio

MULTICAST_SUBSCRIBE_ADDR = "tcp://127.0.0.1:5555"
PULL_REMOTE_ADDR = "tcp://127.0.0.1:5556"

CREATE_SCHEDULED_PAYLOAD = "serpent.start"
REMOVE_SCHEDULED_PAYLOAD = "serpent.stop"

MAX_ASYNCIO_SLEEP = 3456000


class UTCScheduledTask:

    __slots__ = ("payload", "initial", "recur")

    def __init__(self, payload, initial, recur):
        self.payload = payload
        self.initial = initial
        self.recur = recur
        self.running = False

    def __eq__(self, other):
        if isinstance(other, UTCScheduledTask):
            self.payload, self.initial, self.recur == other.payload, other.initial, other.recur
        return NotImplemented

    def __hash__(self):
        return hash((type(self), self.payload, self.sched))

    def get_delay_till_next(self) -> float:

        now = datetime.now(timezone.utc)

        if self.recur and now >= self.initial:
            raw_interval = self.recur.total_seconds()
            return raw_interval - ((now - self.initial).total_seconds() % raw_interval)

        return (self.initial - now).total_seconds()


class SchedulingLoop:
    def __init__(self, zmq_ctx):
        self.ctx = zmq_ctx
        self.push_socket = zmq_ctx.socket(zmq.PUSH)
        self.tasks: Set[UTCScheduledTask] = set()
        self._scheduled: Set[asyncio.Task] = set()

    async def delayed_send(self, delay: float, task: UTCScheduledTask):
        await asyncio.sleep(delay)
        await self.push_socket.send(msgpack.packb(task.payload))
        task.running = False

    async def run(self):
        while True:
            self._scheduled = {t for t in self._scheduled if not t.done()}

            to_remove = []
            for task in self.tasks:
                if task.running:
                    continue

                delay = task.next_call_delay
                if delay < 30:
                    task.running = True
                    fut = asyncio.create_task(self.delayed_send(delay, task))
                    self._scheduled.add(fut)

                    if not task.recur:
                        to_remove.append(task)

            self.tasks.difference_update(to_remove)
            await asyncio.sleep(15)

    async def __aenter__(self):
        self.push_socket.connect(PULL_REMOTE_ADDR)
        return self

    async def __aexit__(self, *args):
        for task in self.tasks:
            if not task.cancelled():
                task.cancel()


async def main():
    ctx = zmq.asyncio.Context()
    async with SchedulingLoop(ctx) as sl:
        load_stored_tasks(sl)
        await asyncio.gather(recv_loop(ctx, sl), sl.run())


def load_stored_tasks(schedule_loop_obj: SchedulingLoop):
    ...


async def create_scheduled_payload(sl, payload, schedule_spec):
    ...


async def remove_scheduled_payload(sl, payload, schedule_spec):
    ...


async def recv_loop(ctx, sl):
    recv_sock = ctx.socket(zmq.SUB)
    recv_sock.setsockopt(zmq.SUBSCRIBE, b"")  #: TODO
    recv_sock.connect(MULTICAST_SUBSCRIBE_ADDR)

    while True:
        raw_payload = await recv_sock.recv()
        topic, maybe_payload = msgpack.unpackb(raw_payload)
        if topic == CREATE_SCHEDULED_PAYLOAD:
            await create_scheduled_payload(sl, *maybe_payload)
        elif topic == REMOVE_SCHEDULED_PAYLOAD:
            await remove_scheduled_payload(sl, *maybe_payload)


if __name__ == "__main__":
    try:
        import uvloop
    except ImportError:
        pass
    else:
        uvloop.install()

    asyncio.run(main())
