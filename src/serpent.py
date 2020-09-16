"""
A scheduler of sorts
"""

import asyncio
import logging
import os
import sys
from contextlib import closing
from datetime import tzinfo
from logging.handlers import RotatingFileHandler
from typing import Generator

import apsw
import msgpack
import pytz
import zmq
import zmq.asyncio

from .cron import CronEntry
from .tasks import ScheduledPayloadTask, Scheduler

MULTICAST_SUBSCRIBE_ADDR = "tcp://127.0.0.1:5555"
PULL_REMOTE_ADDR = "tcp://127.0.0.1:5556"

CREATE_SCHEDULED_PAYLOAD = "serpent.start"
REMOVE_SCHEDULED_PAYLOAD = "serpent.stop"

CRON = "cron"
INTERVAL = "interval"  # TODO
ONCE = "once"  # TODO

log = logging.getLogger("serpent")


async def main(timezone: tzinfo):
    ctx = zmq.asyncio.Context()
    push_socket = ctx.socket(zmq.PUSH)
    push_socket.connect(PULL_REMOTE_ADDR)
    with closing(prep_database()) as connection:
        async with Scheduler(push_socket) as sched:
            for task in get_all_tasks(connection):
                sched.add_task(task)
            await recv_loop(ctx, sched, connection, timezone)


def prep_database() -> apsw.Connection:
    con: apsw.Connection = apsw.Connection("serpent.sqlite")
    with closing(con.cursor()) as cursor:
        cursor.execute("""PRAGMA journal_mode=wal""")
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS crontab (
                uuid BLOB PRIMARY KEY NOT NULL,
                payload BLOB NOT NULL,
                schedule TEXT NOT NULL,
                version INTEGER NOT NULL,
                tzinfo TEXT NOT NULL
            )
            """
        )
    return con


def get_all_tasks(con: apsw.Connection) -> Generator[ScheduledPayloadTask, None, None]:
    with closing(con.cursor()) as cursor:
        for (uuid, payload, schedule, tzi) in cursor.execute(
            """
            SELECT uuid, payload, schedule, tzinfo
            FROM crontab
            WHERE version=1
            """
        ):
            ce = CronEntry.parse(schedule)
            yield ScheduledPayloadTask(ce, payload, uuid, pytz.timezone(tzi))


def recv_to_task(
    s_type: str, s_parsable: str, tz: tzinfo, uuid: bytes, payload: bytes
) -> ScheduledPayloadTask:
    if s_type != CRON:
        raise ValueError("Currently only supports cron style scheduling")

    ce = CronEntry.parse(s_parsable)
    return ScheduledPayloadTask(ce, payload, uuid, tz)


def store_task(connection: apsw.Connection, task: ScheduledPayloadTask):
    with closing(connection.cursor()) as cursor:
        zone_str = task.zone.zone  # type: ignore
        s_type, s_ver, s_parse = task.schedule.to_store()
        if s_type == CRON:
            cursor.execute(
                """
                INSERT INTO crontab(uuid, payload, schedule, tzinfo, version)
                VALUES(?,?,?,?,?)
                """,
                (task.uuid, task.payload, s_parse, zone_str, s_ver),
            )
        else:
            # If this happens, I forgot to add logic for the others after adding them
            raise RuntimeError("Unexpected type: %s" % s_type)


def unstore_task(connection: apsw.Connection, s_type: str, uuid: bytes):
    if s_type == CRON:
        with closing(connection.cursor()) as cursor:
            cursor.execute(
                """
                DELETE FROM crontab WHERE uuid=?
                """,
                (uuid,),
            )
    else:
        raise RuntimeError("Unexpected type: %s" % s_type)


async def recv_loop(
    ctx: zmq.asyncio.Context,
    scheduler: Scheduler,
    connection: apsw.Connection,
    tz: tzinfo,
):
    recv_sock = ctx.socket(zmq.SUB)
    recv_sock.setsockopt(zmq.SUBSCRIBE, b"")  #: TODO
    recv_sock.connect(MULTICAST_SUBSCRIBE_ADDR)

    while True:
        raw_payload = await recv_sock.recv()
        topic, maybe_payload = msgpack.unpackb(raw_payload)
        try:
            if topic == CREATE_SCHEDULED_PAYLOAD:
                (uuid, (s_type, s_parsable), tzi, payload) = maybe_payload
                tzi = pytz.timezone(tzi) if tzi is not None else tz
                task = recv_to_task(s_type, s_parsable, tzi, uuid, payload)
                scheduler.add_task(task)
                store_task(connection, task)
            elif topic == REMOVE_SCHEDULED_PAYLOAD:
                s_type, uuid = maybe_payload
                scheduler.remove_task(uuid)
                unstore_task(connection, s_type, uuid)
        except Exception as exc:
            log.exception("Bad payload %s ", raw_payload, exc_info=exc)


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
