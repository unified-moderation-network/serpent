"""
A scheduler of sorts
"""

import asyncio
import logging
import os
import sys
from logging.handlers import RotatingFileHandler

import msgpack
import pytz
import zmq
import zmq.asyncio

from .tasks import Scheduler

MULTICAST_SUBSCRIBE_ADDR = "tcp://127.0.0.1:5555"
PULL_REMOTE_ADDR = "tcp://127.0.0.1:5556"

CREATE_SCHEDULED_PAYLOAD = "serpent.start"
REMOVE_SCHEDULED_PAYLOAD = "serpent.stop"

CRON = "cron"
INTERVAL = "interval"
ONCE = "once"

log = logging.getLogger("serpent")


async def main(timezone):
    ctx = zmq.asyncio.Context()
    push_socket = ctx.socket(zmq.PUSH)
    push_socket.connect(PULL_REMOTE_ADDR)
    async with Scheduler(timezone, push_socket) as sched:
        await recv_loop(ctx, sched)


async def recv_loop(ctx: zmq.asyncio.Context, scheduler: Scheduler):
    recv_sock = ctx.socket(zmq.SUB)
    recv_sock.setsockopt(zmq.SUBSCRIBE, b"")  #: TODO
    recv_sock.connect(MULTICAST_SUBSCRIBE_ADDR)

    while True:
        raw_payload = await recv_sock.recv()
        topic, maybe_payload = msgpack.unpackb(raw_payload)
        if topic == CREATE_SCHEDULED_PAYLOAD:
            try:
                ...
            except Exception as exc:
                log.exception("Bad payload %s ", raw_payload, exc_info=exc)

        elif topic == REMOVE_SCHEDULED_PAYLOAD:
            ...


if __name__ == "__main__":
    rotating_file_handler = RotatingFileHandler(
        "serpent.log", maxBytes=10000000, backupCount=5
    )
    formatter = logging.Formatter(
        "[%(asctime)s] [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        style="%",
    )
    rotating_file_handler.setFormatter(formatter)
    log.addHandler(rotating_file_handler)

    if _tz := os.getenv("SERPENT_TZ"):
        try:
            TIMEZONE = pytz.timezone(_tz)
        except pytz.UnknownTimeZoneError:
            log.exception("Could not get timezone info for %s", _tz)
            sys.exit(1)
    else:
        TIMEZONE = pytz.utc

    if __debug__:
        log.setLevel(logging.INFO)
    else:
        log.setLevel(logging.WARNING)

    try:
        import uvloop
    except ImportError:
        uvloop = None
    else:
        uvloop.install()

    asyncio.run(main(TIMEZONE))
