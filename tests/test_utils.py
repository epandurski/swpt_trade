import pytest
import math
from datetime import timedelta, datetime, timezone
from swpt_trade.utils import (
    SECONDS_IN_DAY,
    SECONDS_IN_YEAR,
    TT_BUYER,
    TT_COLLECTOR,
    TT_SELLER,
    parse_timedelta,
    can_start_new_turn,
    batched,
    calc_hash,
    i16_to_u16,
    u16_to_i16,
    contain_principal_overflow,
    calc_k,
    calc_demurrage,
    parse_transfer_note,
    generate_transfer_note,
)


def test_parse_timedelta():
    assert parse_timedelta("3w") == timedelta(weeks=3)
    assert parse_timedelta("33d") == timedelta(days=33)
    assert parse_timedelta("123h") == timedelta(hours=123)
    assert parse_timedelta("0.5h") == timedelta(minutes=30)
    assert parse_timedelta(".5h") == timedelta(minutes=30)
    assert parse_timedelta("30.0m") == timedelta(minutes=30)
    assert parse_timedelta("5e-1h") == timedelta(minutes=30)
    assert parse_timedelta("0.05e1h") == timedelta(minutes=30)
    assert parse_timedelta("0.05e+1h") == timedelta(minutes=30)
    assert parse_timedelta("1234m") == timedelta(minutes=1234)
    assert parse_timedelta("1000s") == timedelta(seconds=1000)
    assert parse_timedelta("1000s ") == timedelta(seconds=1000)
    assert parse_timedelta("1000s\n") == timedelta(seconds=1000)
    assert parse_timedelta("1000") == timedelta(seconds=1000)
    assert parse_timedelta("1000 \n") == timedelta(seconds=1000)
    assert parse_timedelta("0") == timedelta(seconds=0)

    with pytest.raises(ValueError):
        parse_timedelta("1.2.3")
    with pytest.raises(ValueError):
        parse_timedelta("3x")
    with pytest.raises(ValueError):
        parse_timedelta("?s")
    with pytest.raises(ValueError):
        parse_timedelta("-5s")
    with pytest.raises(ValueError):
        parse_timedelta(" 1s")


def test_can_start_new_turn():
    t = datetime(2025, 1, 1, 2, tzinfo=timezone.utc)
    h = timedelta(hours=1)

    def f(x, y=0):
        return can_start_new_turn(
            turn_period=timedelta(days=1),
            turn_period_offset=timedelta(hours=2),
            latest_turn_started_at=t + y * h,
            current_ts=t + x * h,
        )

    # Can not start a turn because the latest turn was too soon.
    assert not f(0)
    assert f(0, -1000)
    assert not f(11)
    assert f(11, -2)

    # Can not start a turn in the second half of the period.
    assert not f(12)
    assert not f(12, -1000)
    assert not f(13)
    assert not f(13, -1000)
    assert not f(23)
    assert not f(23, -1000)

    # Can start a turn in the first half of the period.
    assert f(24)
    assert f(24, 11)
    assert f(35)
    assert f(35, 22)

    # Can not start a turn in the second half of the period.
    assert not f(36)
    assert not f(36, -1000)

    # Can never start when the period is zero.
    assert not can_start_new_turn(
        turn_period=timedelta(seconds=0),
        turn_period_offset=timedelta(seconds=0),
        latest_turn_started_at=t - 1000 * h,
        current_ts=t,
    )


def test_batched():
    assert list(batched('', 3)) == []
    assert list(batched('ABCDEFG', 3)) == [
        tuple("ABC"),
        tuple("DEF"),
        tuple("G"),
    ]
    assert list(batched('', 3)) == []

    with pytest.raises(ValueError):
        list(batched('ABCDEFG', 0))


def test_calc_hash():
    assert calc_hash(123) == u16_to_i16(0b1111110000010000)


def test_i16_to_u16():
    assert i16_to_u16(-0x8000) == 0x8000
    assert i16_to_u16(-0x7fff) == 0x8001
    assert i16_to_u16(-1) == 0xffff
    assert i16_to_u16(1) == 0x0001
    assert i16_to_u16(0x7fff) == 0x7fff

    with pytest.raises(ValueError):
        i16_to_u16(0x8000)
    with pytest.raises(ValueError):
        i16_to_u16(-0x8001)


def test_u16_to_i16():
    assert u16_to_i16(0x8000) == -0x8000
    assert u16_to_i16(0x8001) == -0x7fff
    assert u16_to_i16(0xffff) == -1
    assert u16_to_i16(0x0001) == 1
    assert u16_to_i16(0x7fff) == 0x7fff

    with pytest.raises(ValueError):
        u16_to_i16(-1)
    with pytest.raises(ValueError):
        u16_to_i16(0x10000)


def test_contain_principal_overflow():
    assert contain_principal_overflow(0) == 0
    assert contain_principal_overflow(1) == 1
    assert contain_principal_overflow(-1) == -1
    assert contain_principal_overflow(-1 << 63 + 1) == (-1 << 63) + 1
    assert contain_principal_overflow(-1 << 63) == (-1 << 63) + 1
    assert contain_principal_overflow((1 << 63) - 1) == (1 << 63) - 1
    assert contain_principal_overflow(1 << 63) == (1 << 63) - 1


def test_calc_k():
    eps = 1e-15
    assert SECONDS_IN_DAY == 24 * 3600
    assert 365 * SECONDS_IN_DAY < SECONDS_IN_YEAR < 366 * SECONDS_IN_DAY
    assert abs(math.exp(calc_k(0.0) * SECONDS_IN_YEAR) - 1.0) < eps
    assert abs(math.exp(calc_k(50.0) * SECONDS_IN_YEAR) - 1.5) < eps
    assert abs(math.exp(calc_k(80.0) * SECONDS_IN_YEAR) - 1.8) < eps
    assert abs(math.exp(calc_k(-50.0) * SECONDS_IN_YEAR) - 0.5) < eps
    assert abs(math.exp(calc_k(-80.0) * SECONDS_IN_YEAR) - 0.2) < eps


def test_calc_demurrage():
    assert 0.94 < calc_demurrage(-50, timedelta(days=30)) < 0.95
    assert 0.89 < calc_demurrage(-50, timedelta(days=60)) < 0.90
    assert calc_demurrage(-50, timedelta(days=0)) == 1.0
    assert calc_demurrage(50, timedelta(days=30)) == 1.0
    assert calc_demurrage(-50, timedelta(days=-30)) == 1.0


def test_parse_transfer_note():
    assert (
        parse_transfer_note("Trading session: a\nBuyer: b\n")
        == (10, "Buyer", 11)
    )
    assert (
        parse_transfer_note("Trading session: A\r\nBuyer: b\r\n")
        == (10, "Buyer", 11)
    )
    assert (
        parse_transfer_note("Trading session: a\r\nBuyer: B")
        == (10, "Buyer", 11)
    )
    with pytest.raises(ValueError):
        parse_transfer_note("")

    assert (
        parse_transfer_note(generate_transfer_note(123, TT_BUYER, 456))
        == (123, TT_BUYER, 456)
    )
    assert (
        parse_transfer_note(generate_transfer_note(123, TT_SELLER, 456))
        == (123, TT_SELLER, 456)
    )
    assert (
        parse_transfer_note(generate_transfer_note(123, TT_COLLECTOR, 456))
        == (123, TT_COLLECTOR, 456)
    )
