#   Copyright 2020 Michael Hall
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.


from __future__ import annotations

import contextlib
import re
from datetime import datetime
from typing import Dict, Final, Literal, Optional, Sequence

#########################################################################
#                     Limited cron spec adhered to                      #
# +-------------------------------------------------------------------+ #
# | Position      | Field         | Literal Ranges | Special Values   | #
# +-------------------------------------------------------------------+ #
# | 0             | Minutes       | 0-59           | * ,              | #
# | 1             | Hours         | 0-23           | * , -            | #
# | 2             | Day of month  | 1-31           | * , -            | #
# | 3             | Month         | 1-12   JAN-DEC | * ,              | #
# | 4             | Day of week   | 0-6    MON-SUN | * , -            | #
# +-------------------------------------------------------------------+ #
# Only one special value may be used per Field, but the comma can be    #
# repeated when it's the one in use                                     #
#########################################################################


# fmt: off
VALID_MINUTES = Literal[
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
    22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40,
    41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59,
]

VALID_HOURS = Literal[
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12,
    13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23,
]

VALID_DAYS_OF_MONTH = Literal[
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
    17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
]

VALID_WEEKDAYS = Literal[0, 1, 2, 3, 4, 5, 6]

VALID_MONTHS = Literal[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]

WILDCARD: Final[str] = "*"

WEEKDAYS: Final[Dict[str, VALID_WEEKDAYS]] = {"MON": 0, "TUE": 1, "WED": 2, "THU": 3, "FRI": 4, "SAT": 5, "SUN": 6}

MONTHS: Final[Dict[str, VALID_MONTHS]] = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4,
    "MAY": 5, "JUN": 6, "JUL": 7, "AUG": 8,
    "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
}
# fmt: on

DIGIT_RANGE_RE: Final[re.Pattern] = re.compile(r"^(\d{1,2})\-(\d{1,2})$")


def try_minute(string: str) -> Optional[VALID_MINUTES]:
    with contextlib.suppress(ValueError):
        return parse_minute(string)
    return None


def parse_minute(string: str) -> VALID_MINUTES:
    val = int(string, base=10)
    if not (0 <= val < 60):
        raise ValueError()
    return val  # type: ignore


def try_hour(string: str) -> Optional[VALID_HOURS]:
    with contextlib.suppress(ValueError):
        return parse_hour(string)
    return None


def parse_hour(string: str) -> VALID_HOURS:
    val = int(string, base=10)
    if not (0 <= val < 24):
        raise ValueError()
    return val  # type: ignore


def try_day_of_month(string: str) -> Optional[VALID_HOURS]:
    with contextlib.suppress(ValueError):
        return parse_day_of_month(string)
    return None


def parse_day_of_month(string: str) -> VALID_HOURS:
    val = int(string, base=10)
    if not (1 <= val <= 31):
        raise ValueError()
    return val  # type: ignore


def try_month(string: str) -> Optional[VALID_MONTHS]:
    with contextlib.suppress(ValueError):
        return parse_month(string)
    return None


def parse_month(string: str) -> VALID_MONTHS:
    if lit := MONTHS.get(string.upper(), None):
        return lit
    val = int(string, base=10)
    if not (1 <= val <= 12):
        raise ValueError()
    return val  # type: ignore


def try_weekday(string: str) -> Optional[VALID_WEEKDAYS]:
    with contextlib.suppress(ValueError):
        return parse_weekday(string)
    return None


def parse_weekday(string: str) -> VALID_WEEKDAYS:
    if (lit := WEEKDAYS.get(string.upper(), None)) is not None:
        return lit
    val = int(string, base=10)
    if not (0 <= val < 7):
        raise ValueError()
    return val  # type: ignore


# There's a decent amount of code duplication below. While some of this could be made more generic,
# the amount of effort to get this working well with type checking isn't worth it
# (maybe after python + mypy handle generics better this will get refactored)


class MinutesField:
    __slots__ = ("_all", "_specifics")

    def __init__(
        self,
        *,
        is_all: bool = False,
        specifics: Optional[Sequence[VALID_MINUTES]] = None,
    ):
        self._all: bool = is_all
        self._specifics: Optional[Sequence[VALID_MINUTES]] = specifics

    def __str__(self):
        if self._all:
            return WILDCARD
        elif self._specifics:
            return ",".join(map(str, self._specifics))

    def __repr__(self):
        return f"<MinutesField(is_all={self._all}, specifics={self._specifics}>"

    @classmethod
    def parse(cls, string: str):
        if string == WILDCARD:
            return cls(is_all=True)
        elif lit := try_minute(string):
            return cls(specifics=(lit,))

        try:
            items = [parse_minute(i) for i in string.split(",")]
        except ValueError:
            raise ValueError(
                "Cannot parse %s as minute field for cron" % string
            ) from None

        # py 3.7+ efficient de-duplication
        specs = tuple(dict.fromkeys(sorted(items)))
        return cls(specifics=specs)

    def field_match(self, when: datetime) -> bool:
        if self._specifics:
            return when.minute in self._specifics
        return True  # invariant: self._all is True

    # TODO: use this later on for more efficient scheduling
    # def get_next(self, when: datetime) -> Optional[int]:
    #     if self._specifics:
    #         return next((m for m in self._specifics if m > when.minute), None)
    #     return when.minute + 1 if when.minute < 59 else None  # invariant: self._all is True


class HoursField:
    __slots__ = ("_all", "_start", "_stop", "_specifics")

    def __init__(
        self,
        *,
        is_all: bool = False,
        start: Optional[VALID_HOURS] = None,
        stop: Optional[VALID_HOURS] = None,
        specifics: Optional[Sequence[VALID_HOURS]] = None,
    ):
        self._specifics: Optional[Sequence[VALID_HOURS]] = specifics
        self._all: bool = is_all
        self._start: Optional[VALID_HOURS] = start
        self._stop: Optional[VALID_HOURS] = stop
        if start is not None and stop is not None:
            if start == stop:
                self._specifics = (start,)
                self._start = None
                self._stop = None
            elif start < stop:
                self._specifics = tuple(range(start, stop + 1))  # type: ignore
            else:
                self._specifics = tuple((*range(0, start + 1), *range(stop, 24)))  # type: ignore

    def __str__(self):
        if self._all:
            return WILDCARD
        elif self._start is not None and self._stop is not None:
            return f"{self._start}-{self._stop}"
        elif self._specifics:
            return ",".join(map(str, self._specifics))

    def __repr__(self):
        return f"<HoursField(is_all={self._all}, start={self._start}, stop={self._stop}, specifics={self._specifics}>"

    @classmethod
    def parse(cls, string: str):
        if string == WILDCARD:
            return cls(is_all=True)
        elif lit := try_hour(string):
            return cls(specifics=(lit,))
        elif match := DIGIT_RANGE_RE.match(string):
            try:
                start, stop = (parse_hour(part) for part in match.groups())
            except ValueError:
                raise ValueError(
                    "Cannot parse %s as hour field for cron" % string
                ) from None
            else:
                return cls(start=start, stop=stop)
        else:
            try:
                items = [parse_hour(i) for i in string.split(",")]
            except ValueError:
                raise ValueError(
                    "Cannot parse %s as hour field for cron" % string
                ) from None

            # py 3.7+ efficient de-duplication
            specs = tuple(dict.fromkeys(sorted(items)))
            return cls(specifics=specs)

    def field_match(self, when: datetime) -> bool:
        if self._specifics:
            return when.hour in self._specifics
        return True  # invariant: self._all is True

    # TODO: use this later on for more efficient scheduling
    # def get_next(self, when: datetime) -> Optional[int]:
    #     if self._specifics:
    #         return next((m for m in self._specifics if m > when.hour), None)
    #     return when.minute + 1 if when.hour < 23 else None  # invariant: self._all is True


class DaysOfMonthField:
    __slots__ = ("_all", "_start", "_stop", "_specifics")

    def __init__(
        self,
        *,
        is_all: bool = False,
        start: Optional[VALID_DAYS_OF_MONTH] = None,
        stop: Optional[VALID_DAYS_OF_MONTH] = None,
        specifics: Optional[Sequence[VALID_DAYS_OF_MONTH]] = None,
    ):
        self._all: bool = is_all
        self._specifics: Optional[Sequence[VALID_DAYS_OF_MONTH]] = None
        self._start: Optional[VALID_DAYS_OF_MONTH] = start
        self._stop: Optional[VALID_DAYS_OF_MONTH] = stop
        if start is not None and stop is not None:
            if start == stop:
                self._specifics = (start,)
                self._start = None
                self._stop = None
            elif start < stop:
                self._specifics = tuple(range(start, stop + 1))  # type: ignore
            else:
                self._specifics = tuple((*range(1, start + 1), *range(stop, 32)))  # type: ignore

    def __str__(self):
        if self._all:
            return WILDCARD
        elif self._start is not None and self._stop is not None:
            return f"{self._start}-{self._stop}"
        elif self._specifics:
            return ",".join(map(str, self._specifics))

    def __repr__(self):
        return f"<DaysOfMonthField(is_all={self._all}, start={self._start}, stop={self._stop}, specifics={self._specifics}>"

    @classmethod
    def parse(cls, string: str):
        if string == WILDCARD:
            return cls(is_all=True)
        elif lit := try_day_of_month(string):
            return cls(specifics=(lit,))
        elif match := DIGIT_RANGE_RE.match(string):
            try:
                start, stop = (parse_day_of_month(part) for part in match.groups())
            except ValueError:
                raise ValueError(
                    "Cannot parse %s as day of month field for cron" % string
                ) from None
            else:
                return cls(start=start, stop=stop)
        else:
            try:
                items = [parse_day_of_month(i) for i in string.split(",")]
            except ValueError:
                raise ValueError(
                    "Cannot parse %s as day of month field for cron" % string
                ) from None
            # py 3.7+ efficient de-duplication
            specs = tuple(dict.fromkeys(sorted(items)))
            return cls(specifics=specs)

    def field_match(self, when: datetime) -> bool:
        if self._specifics:
            return when.day in self._specifics
        return True  # invariant: self._all is True


class MonthsField:
    __slots__ = ("_all", "_start", "_stop", "_specifics")

    def __init__(
        self,
        *,
        is_all: bool = False,
        start: Optional[VALID_MONTHS] = None,
        stop: Optional[VALID_MONTHS] = None,
        specifics: Optional[Sequence[VALID_MONTHS]] = None,
    ):
        self._specifics: Optional[Sequence[VALID_MONTHS]] = specifics
        self._all: bool = is_all
        self._start: Optional[VALID_MONTHS] = start
        self._stop: Optional[VALID_MONTHS] = stop
        if start is not None and stop is not None:
            if start == stop:
                self._specifics = (start,)
                self._start = None
                self._stop = None
            elif start < stop:
                self._specifics = tuple(range(start, stop + 1))  # type: ignore
            else:
                self._specifics = tuple((*range(0, start + 1), *range(stop, 24)))  # type: ignore

    def __str__(self):
        if self._all:
            return WILDCARD
        elif self._start is not None and self._stop is not None:
            return f"{self._start}-{self._stop}"
        elif self._specifics:
            return ",".join(map(str, self._specifics))

    def __repr__(self):
        return f"<MonthsField(is_all={self._all}, start={self._start}, stop={self._stop}, specifics={self._specifics}>"

    @classmethod
    def parse(cls, string: str):
        if string == WILDCARD:
            return cls(is_all=True)
        elif lit := try_month(string):
            return cls(specifics=(lit,))
        elif match := DIGIT_RANGE_RE.match(string):
            try:
                start, stop = (parse_month(part) for part in match.groups())
            except ValueError:
                raise ValueError(
                    "Cannot parse %s as month field for cron" % string
                ) from None
            else:
                return cls(start=start, stop=stop)
        else:
            try:
                items = [parse_month(i) for i in string.split(",")]
            except ValueError:
                raise ValueError(
                    "Cannot parse %s as month field for cron" % string
                ) from None
            # py 3.7+ efficient de-duplication
            specs = tuple(dict.fromkeys(sorted(items)))
            return cls(specifics=specs)

    def field_match(self, when: datetime) -> bool:
        if self._specifics:
            return when.month in self._specifics
        return True  # invariant: self._all is True


class WeekdaysField:
    __slots__ = ("_all", "_start", "_stop", "_specifics")

    def __init__(
        self,
        *,
        is_all: bool = False,
        start: Optional[VALID_WEEKDAYS] = None,
        stop: Optional[VALID_WEEKDAYS] = None,
        specifics: Optional[Sequence[VALID_WEEKDAYS]] = None,
    ):
        self._all: bool = is_all
        self._specifics: Optional[Sequence[VALID_WEEKDAYS]] = None
        self._start: Optional[VALID_WEEKDAYS] = start
        self._stop: Optional[VALID_WEEKDAYS] = stop
        if start is not None and stop is not None:
            if start == stop:
                self._specifics = (start,)
                self._start = None
                self._stop = None
            elif start < stop:
                self._specifics = tuple(range(start, stop + 1))  # type: ignore
            else:
                self._specifics = tuple((*range(1, start + 1), *range(stop, 32)))  # type: ignore

    def __str__(self):
        if self._all:
            return WILDCARD
        elif self._start is not None and self._stop is not None:
            return f"{self._start}-{self._stop}"
        elif self._specifics:
            return ",".join(map(str, self._specifics))

    def __repr__(self):
        return f"<WeekdaysField(is_all={self._all}, start={self._start}, stop={self._stop}, specifics={self._specifics}>"

    @classmethod
    def parse(cls, string: str):
        if string == WILDCARD:
            return cls(is_all=True)
        elif lit := try_weekday(string):
            return cls(specifics=(lit,))
        elif match := DIGIT_RANGE_RE.match(string):
            try:
                start, stop = (parse_weekday(part) for part in match.groups())
            except ValueError:
                raise ValueError(
                    "Cannot parse %s as weekday field for cron" % string
                ) from None
            else:
                return cls(start=start, stop=stop)
        else:
            try:
                items = [parse_weekday(i) for i in string.split(",")]
            except ValueError:
                raise ValueError(
                    "Cannot parse %s as weekday field for cron" % string
                ) from None
            # py 3.7+ efficient de-duplication
            specs = tuple(dict.fromkeys(sorted(items)))
            return cls(specifics=specs)

    def field_match(self, when: datetime) -> bool:
        if self._specifics:
            return when.weekday() in self._specifics
        return True  # invariant: self._all is True


class CronEntry:
    __slots__ = ("_minutes", "_hours", "_dom", "_month", "_wd")

    def __init__(
        self,
        m: MinutesField,
        h: HoursField,
        dom: DaysOfMonthField,
        mon: MonthsField,
        dow: WeekdaysField,
        /,
    ):
        self._minutes: MinutesField = m
        self._hours: HoursField = h
        self._dom: DaysOfMonthField = dom
        self._month: MonthsField = mon
        self._wd: WeekdaysField = dow

    def field_match(self, when: datetime) -> bool:
        return (
            self._minutes.field_match(when)
            and self._hours.field_match(when)
            and self._dom.field_match(when)
            and self._month.field_match(when)
            and self._wd.field_match(when)
        )

    def __str__(self):
        return " ".join(
            map(str, (self._minutes, self._hours, self._dom, self._month, self._wd))
        )

    def __repr__(self):
        return f"<CronEntry: {self}>"

    @classmethod
    def parse(cls, string: str):
        parts = string.split()
        if len(parts) != 5:
            raise ValueError(
                "Invalid cron entrty (Valid entires have 5 parts): %s" % string
            )
        p0, p1, p2, p3, p4 = parts
        return cls(
            MinutesField.parse(p0),
            HoursField.parse(p1),
            DaysOfMonthField.parse(p2),
            MonthsField.parse(p3),
            WeekdaysField.parse(p4),
        )

    def to_store(self):
        return ("cron", 1, str(self))

    @classmethod
    def from_store(cls, typ, ver, data):
        if typ != "cron":
            raise TypeError("Got a different schedule type than expected cron: %s", typ)
        if (
            ver != 1
        ):  # unlikely to change in a breaking way, but let's plan for it as a possibility.
            raise ValueError("Can't parse newer schedule version.")
        return cls.parse(data)
