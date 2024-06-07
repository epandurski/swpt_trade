from __future__ import annotations
import re
import math
import array
from typing import Self
from enum import Enum
from dataclasses import dataclass
from hashlib import md5
from datetime import datetime, timedelta, timezone
from itertools import islice
from collections import defaultdict
from swpt_pythonlib.utils import i64_to_u64, u64_to_i64

RE_PERIOD = re.compile(r"^([\d.eE+-]+)([smhdw]?)\s*$")
RE_TRANSFER_NOTE = re.compile(
    r'^Trading session: ([0-9A-Fa-f]{1,16})\r?\n'
    r'(Buyer|From|Seller): ([0-9A-Fa-f]{1,16})\r?\n'
    r'(Seller|To|Buyer): ([0-9A-Fa-f]{1,16})(?:\r?\n)?$'
)
DATETIME0 = datetime(2024, 1, 1, tzinfo=timezone.utc)  # 2024-01-01 is Monday.
MIN_INT64 = -1 << 63
MAX_INT64 = (1 << 63) - 1
SECONDS_IN_DAY = 24 * 60 * 60
SECONDS_IN_YEAR = 365.25 * SECONDS_IN_DAY


@dataclass
class TransferNote:
    turn_id: int
    note_kind: TransferNote.Kind
    first_id: int
    second_id: int

    class Kind(Enum):
        COLLECTING = ("Buyer", "Seller")
        SENDING = ("From", "To")
        DISPATCHING = ("Seller", "Buyer")

        @classmethod
        def from_strings(cls, first: str, second: str) -> Self:
            for enum_instance in cls:
                if enum_instance.value == (first, second):
                    return enum_instance
            raise ValueError

    @classmethod
    def parse(cls, s: str) -> Self:
        m = RE_TRANSFER_NOTE.fullmatch(s)
        if m:
            return cls(
                turn_id=u32_to_i32(int(m[1])),
                note_kind=cls.Kind.from_strings(m[2], m[4]),
                first_id=u64_to_i64(int(m[3], 16)),
                second_id=u64_to_i64(int(m[5], 16)),
            )
        raise ValueError

    def __str__(self) -> str:
        first_label, second_label = self.note_kind.value
        return (
            f"Trading session: {i32_to_u32(self.turn_id)}\n"
            f"{first_label}: {i64_to_u64(self.first_id):x}\n"
            f"{second_label}: {i64_to_u64(self.second_id):x}\n"
        )


class DispatchingData:
    def __init__(self, turn_id):
        self._turn_id = turn_id
        self._empty_value_tuple = (0, 0, 0, 0, 0)
        self._data = defaultdict(self._create_empty_value)

    def _create_empty_value(self):
        return array.array("q", self._empty_value_tuple)

    def _get_value(self, *args):
        return self._data[*args]

    def register_collecting(self, collector_id, turn_id, debtor_id, amount):
        assert turn_id == self._turn_id
        value = self._get_value(collector_id, turn_id, debtor_id)
        value[0] = contain_principal_overflow(value[0] + amount)

    def register_sending(self, collector_id, turn_id, debtor_id, amount):
        assert turn_id == self._turn_id
        value = self._get_value(collector_id, turn_id, debtor_id)
        value[1] = contain_principal_overflow(value[1] + amount)

    def register_receiving(self, collector_id, turn_id, debtor_id, amount):
        assert turn_id == self._turn_id
        value = self._get_value(collector_id, turn_id, debtor_id)
        value[2] = contain_principal_overflow(value[2] + amount)
        value[3] += 1

    def register_dispatching(self, collector_id, turn_id, debtor_id, amount):
        assert turn_id == self._turn_id
        value = self._get_value(collector_id, turn_id, debtor_id)
        value[4] = contain_principal_overflow(value[4] + amount)

    def statuses_iter(self):
        current_ts = datetime.now(tz=timezone.utc)

        for key, value in self._data.items():
            yield {
                "collector_id": key[0],
                "turn_id": key[1],
                "debtor_id": key[2],
                "inserted_at": current_ts,
                "amount_to_collect": value[0],
                "total_collected_amount": None,
                "amount_to_send": value[1],
                "all_sent": False,
                "amount_to_receive": value[2],
                "number_to_receive": value[3],
                "total_received_amount": None,
                "all_received": False,
                "amount_to_dispatch": value[4],
            }


def parse_timedelta(s: str) -> timedelta:
    """Parse a string to a timedelta object.

    The string must be in the format: "<number><unit>". <unit> can be
    `s` (seconds), `m` (minutes), `h` (hours), `d` (days), or `w`
    (weeks). If <unit> is not specified (en empty string), it defaults
    to seconds. For example:

    >>> parse_timedelta("20d")
    datetime.timedelta(days=20)
    >>> parse_timedelta("20")
    datetime.timedelta(seconds=20)
    """
    m = RE_PERIOD.match(s)
    if m:
        n = float(m[1])
        if n >= 0:
            unit = m[2]
            if unit == "" or unit == "s":
                return timedelta(seconds=n)
            if unit == "m":
                return timedelta(minutes=n)
            elif unit == "h":
                return timedelta(hours=n)
            elif unit == "d":
                return timedelta(days=n)
            else:
                assert unit == 'w'
                return timedelta(weeks=n)

    raise ValueError(f"invalid time interval: {s}")


def can_start_new_turn(
    *,
    turn_period: timedelta,
    turn_period_offset: timedelta,
    latest_turn_started_at: datetime,
    current_ts: datetime,
) -> bool:
    """Decide whether a new turn can be started.

    Turns should be started after a "turn starting point", but and as
    close as possible to it. Turn starting points are separated by an
    exact time interval (`turn_period`), and the first starting point
    is calculated to be at `DATETIME0 + turn_period_offset`.
    """
    if not turn_period:
        return False

    start_of_first_turn = DATETIME0 + turn_period_offset
    time_since_latest_turn = current_ts - latest_turn_started_at
    overdue = (current_ts - start_of_first_turn) % turn_period
    return overdue < 0.5 * turn_period < time_since_latest_turn


def batched(iterable, n):
    """Batch data from the iterable into tuples of length n.
    """
    if n < 1:
        raise ValueError('n must be at least one')
    it = iter(iterable)
    while batch := tuple(islice(it, n)):
        yield batch


def calc_hash(n: int) -> int:
    """Calculate the MD5 hash of `n`, and return the highest 16 bits
    as a signed 16-bits integer.
    """
    m = md5()
    m.update(n.to_bytes(8, byteorder="big", signed=True))
    return int.from_bytes(m.digest()[:2], byteorder="big", signed=True)


def i16_to_u16(value: int) -> int:
    """Convert a signed 16-bit integer to unsigned 16-bit integer.
    """
    if value > 0x7fff or value < -0x8000:
        raise ValueError()
    if value >= 0:
        return value
    return value + 0x10000


def u16_to_i16(value: int) -> int:
    """Convert an unsigned 16-bit integer to a signed 16-bit integer.
    """
    if value > 0xffff or value < 0:
        raise ValueError()
    if value <= 0x7fff:
        return value
    return value - 0x10000


def i32_to_u32(value: int) -> int:
    """Convert a signed 32-bit integer to unsigned 32-bit integer.
    """
    if value > 0x7fffffff or value < -0x80000000:
        raise ValueError()
    if value >= 0:
        return value
    return value + 0x100000000


def u32_to_i32(value: int) -> int:
    """Convert an unsigned 32-bit integer to a signed 32-bit integer.
    """
    if value > 0xffffffff or value < 0:
        raise ValueError()
    if value <= 0x7fffffff:
        return value
    return value - 0x100000000


def contain_principal_overflow(value: int) -> int:
    if value <= MIN_INT64:
        return -MAX_INT64
    if value > MAX_INT64:
        return MAX_INT64
    return value


def calc_k(interest_rate: float) -> float:
    return math.log(1.0 + interest_rate / 100.0) / SECONDS_IN_YEAR


def calc_demurrage(demurrage_rate: float, period: timedelta) -> float:
    if demurrage_rate < -99.9999:
        return 0.0

    k = calc_k(demurrage_rate)
    t = period.total_seconds()
    return min(math.exp(k * t), 1.0)
