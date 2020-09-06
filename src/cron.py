from __future__ import annotations

import contextlib
import re
from datetime import datetime
from typing import Literal, Optional, Sequence

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

WILDCARD = "*"

WEEKDAYS = {"MON": 0, "TUE": 1, "WED": 2, "THU": 3, "FRI": 4, "SAT": 5, "SUN": 6}

MONTHS = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4,
    "MAY": 5, "JUN": 6, "JUL": 7, "AUG": 8,
    "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
}
# fmt: on

HOUR_RANGE_RE = re.compile(r"^(\d{1,2})\-(\d{1,2})$")


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
            # do not switch to f-string: string is untrusted
            raise ValueError(
                "Cannot parse %s as minute field for cron" % string
            ) from None

        specs = tuple(dict.fromkeys(sorted(items)))  # py 3.7+ efficient de-duplication
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
        elif match := HOUR_RANGE_RE.match(string):
            try:
                start, stop = (parse_hour(part) for part in match.groups())
            except ValueError:
                # do not switch to f-string: string is untrusted
                raise ValueError(
                    "Cannot parse %s as hour field for cron" % string
                ) from None
            else:
                return cls(start=start, stop=stop)
        else:
            try:
                items = [parse_hour(i) for i in string.split(",")]
            except ValueError:
                # do not switch to f-string: string is untrusted
                raise ValueError(
                    "Cannot parse %s as hour field for cron" % string
                ) from None
            specs = tuple(
                dict.fromkeys(sorted(items))
            )  # py 3.7+ efficient de-duplication
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


#: TODO
class DaysOfMonthField:
    ...


class MonthField:
    ...


class DayOfWeekField:
    ...
