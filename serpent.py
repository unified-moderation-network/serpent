"""
A scheduler of sorts
"""

import contextlib
import logging
import os
import sys
from logging.handlers import RotatingFileHandler
from typing import Optional

import msgpack
import pytz
import zmq
from apscheduler.jobstores.base import JobLookupError
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger

MULTICAST_SUBSCRIBE_ADDR = "tcp://127.0.0.1:5555"
PULL_REMOTE_ADDR = "tcp://127.0.0.1:5556"

CREATE_SCHEDULED_PAYLOAD = "serpent.start"
REMOVE_SCHEDULED_PAYLOAD = "serpent.stop"

CRON = "cron"
INTERVAL = "interval"
DATE = "date"

TRIGGER_DICT = {
    CRON: CronTrigger,
    INTERVAL: IntervalTrigger,
    DATE: DateTrigger,
}


log = logging.getLogger("serpent")
ctx: Optional[zmq.Context] = None
push_socket: Optional[zmq.Socket] = None


def main(timezone):
    scheduler = BackgroundScheduler(timezone=timezone)
    scheduler.add_jobstore("sqlalchemy", url="sqlite:///serpent.sqlite")
    try:
        scheduler.start()
        await recv_loop(ctx, scheduler)
    finally:
        scheduler.shutdown()


def send_payload(payload):
    if push_socket is not None:
        push_socket.send(payload)


def recv_loop(ctx, scheduler):
    recv_sock = ctx.socket(zmq.SUB)
    recv_sock.setsockopt(zmq.SUBSCRIBE, b"")  #: TODO
    recv_sock.connect(MULTICAST_SUBSCRIBE_ADDR)

    while True:
        raw_payload = recv_sock.recv()
        topic, maybe_payload = msgpack.unpackb(raw_payload)
        if topic == CREATE_SCHEDULED_PAYLOAD:
            try:
                (uuid, trigger_type, trigger_kwargs, payload) = maybe_payload
                trigger = TRIGGER_DICT[trigger_type](**trigger_kwargs)
                re_serial_payload = msgpack.packb(payload)
                scheduler.add_job(
                    send_payload,
                    trigger=trigger,
                    args=(re_serial_payload,),
                    replace_existing=True,
                )
            except Exception as exc:
                log.exception("Bad payload %s ", raw_payload, exc_info=exc)

        elif topic == REMOVE_SCHEDULED_PAYLOAD:
            with contextlib.suppress(JobLookupError):
                scheduler.remove_job(maybe_payload)


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

    ctx = zmq.Context()
    push_socket = ctx.socket(zmq.PUSH)
    push_socket.connect(PULL_REMOTE_ADDR)
    main(TIMEZONE)
