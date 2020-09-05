from __future__ import annotations

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

import contextlib
from datetime import datetime
from typing import Literal, Optional, Sequence


VALID_MINUTES = Literal[
    0,
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9,
    10,
    11,
    12,
    13,
    14,
    15,
    16,
    17,
    18,
    19,
    20,
    21,
    22,
    23,
    24,
    25,
    26,
    27,
    28,
    29,
    30,
    31,
    32,
    33,
    34,
    35,
    36,
    37,
    38,
    39,
    40,
    41,
    42,
    43,
    44,
    45,
    46,
    47,
    48,
    49,
    50,
    51,
    52,
    53,
    54,
    55,
    56,
    57,
    58,
    59,
]

VALID_MONTHS = Literal[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]

WILDCARD = "*"

WEEKDAYS = {
    "MON": 1,
    "TUE": 2,
    "WED": 3,
    "THU": 4,
    "FRI": 5,
    "SAT": 6,
    "SUN": 7,
}

MONTHS = {
    "JAN": 1,
    "FEB": 2,
    "MAR": 3,
    "APR": 4,
    "MAY": 5,
    "JUN": 6,
    "JUL": 7,
    "AUG": 8,
    "SEP": 9,
    "OCT": 10,
    "NOV": 11,
    "DEC": 12,
}


def try_minute(string: str) -> Optional[VALID_MINUTES]:
    with contextlib.suppress(ValueError):
        return parse_minute(string)
    return None


def parse_minute(string: str) -> VALID_MINUTES:
    val = int(string, base=10)
    if not (0 <= val < 60):
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

    def get_next(self, when: datetime) -> int:
        if self._specifics:
            return next((m for m in self._specifics if m > when.minute), next(iter(self._specifics)))
        return when.minute + 1 if when.minute < 59 else 0  # invariant: self._all is True
