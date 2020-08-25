"""
A scheduler of sorts
"""

import asyncio

import msgpack
import zmq
import zmq.asyncio

MULTICAST_SUBSCRIBE_ADDR = "tcp://127.0.0.1:5555"
PULL_REMOTE_ADDR = "tcp://127.0.0.1:5556"

CREATE_SCHEDULED_PAYLOAD = "serpent.start"
REMOVE_SCHEDULED_PAYLOAD = "serpent.stop"


class ScheduledTask:

    __slots__ = ("payload", "sched")

    def __init__(self, payload, sched):
        self.payload = payload
        self.sched = sched

    def __eq__(self, other):
        if isinstance(other, ScheduledTask):
            self.payload == other.payload and self.sched == other.sched
        return NotImplemented

    def __hash__(self):
        return hash((type(self), self.payload, self.sched))

    def delay_till_next(self) -> float:
        ...


class SchedulingLoop:

    def __init__(self, zmq_ctx):
        self.ctx = zmq_ctx
        self.tasks = []

    async def run(self):
        ...

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        ...


async def main():
    ctx = zmq.asyncio.Context()
    async with SchedulingLoop(ctx) as sl:
        load_stored_tasks(sl)
        await asyncio.gather(recv_loop(ctx), sl.run())


def load_stored_tasks(schedule_loop_obj: SchedulingLoop):
    ...


async def create_scheduled_payload(payload, schedule_spec):
    ...


async def remove_scheduled_payload(payload, schedule_spec):
    ...


async def recv_loop(ctx):
    recv_sock = ctx.socket(zmq.SUB)
    recv_sock.setsockopt(zmq.SUBSCRIBE, b"")
    recv_sock.connect(MULTICAST_SUBSCRIBE_ADDR)

    while True:
        raw_payload = await recv_sock.recv()
        topic, maybe_payload = msgpack.unpackb(raw_payload)
        if topic == CREATE_SCHEDULED_PAYLOAD:
            await create_scheduled_payload(*maybe_payload)
        elif topic == REMOVE_SCHEDULED_PAYLOAD:
            await remove_scheduled_payload(*maybe_payload)


if __name__ == "__main__":
    try:
        import uvloop
    except ImportError:
        pass
    else:
        uvloop.install()

    asyncio.run(main())
