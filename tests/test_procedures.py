import pytest
from datetime import timedelta
from swpt_trade import procedures as p
from swpt_trade.models import (
    Turn,
    DebtorInfo,
    ConfirmedDebtor,
    CurrencyInfo,
    CollectorSending,
    DebtorLocatorClaim,
    FetchDebtorInfoSignal,
    DebtorInfoFetch,
    DebtorInfoDocument,
    TS0,
)


@pytest.fixture(params=[True, False])
def turn_may_exist(request, db_session):
    if request.param:
        db_session.add(
            Turn(
                started_at=TS0,
                base_debtor_info_locator="https://example.com/101",
                base_debtor_id=101,
                max_distance_to_base=5,
                min_trade_amount=5000,
                phase=4,
                phase_deadline=TS0,
                collection_started_at=TS0,
                collection_deadline=TS0,
            )
        )
        db_session.commit()

    return request.param


def test_start_new_turn_if_possible(current_ts, turn_may_exist):
    midnight = current_ts.replace(hour=0, minute=0, second=0, microsecond=0)

    # Successfully starts a new turn.
    turns = p.start_new_turn_if_possible(
        turn_period=timedelta(days=1),
        turn_period_offset=current_ts - midnight,
        phase1_duration=timedelta(hours=1),
        base_debtor_info_locator="https://example.com/101",
        base_debtor_id=101,
        max_distance_to_base=5,
        min_trade_amount=5000,
    )
    assert len(turns) == 1
    assert turns[0].base_debtor_info_locator == "https://example.com/101"
    assert turns[0].base_debtor_id == 101
    assert turns[0].max_distance_to_base == 5
    assert turns[0].min_trade_amount == 5000
    assert turns[0].phase == 1
    assert turns[0].phase_deadline is not None
    all_turns = Turn.query.all()
    assert len(all_turns) == 2 if turn_may_exist else 1
    all_turns.sort(key=lambda t: t.phase)
    assert all_turns[0].phase == 1
    assert all_turns[0].phase_deadline is not None

    # Does not start a new turn.
    turns = p.start_new_turn_if_possible(
        turn_period=timedelta(days=1),
        turn_period_offset=current_ts - midnight,
        phase1_duration=timedelta(hours=1),
        base_debtor_info_locator="https://example.com/101",
        base_debtor_id=101,
        max_distance_to_base=5,
        min_trade_amount=5000,
    )
    assert len(turns) == 1
    assert turns[0].base_debtor_info_locator == "https://example.com/101"
    assert turns[0].base_debtor_id == 101
    assert turns[0].max_distance_to_base == 5
    assert turns[0].min_trade_amount == 5000
    assert turns[0].phase == 1
    assert turns[0].phase_deadline is not None
    all_turns = Turn.query.all()
    assert len(all_turns) == 2 if turn_may_exist else 1
    all_turns.sort(key=lambda t: t.phase)
    assert all_turns[0].phase == 1
    assert all_turns[0].phase_deadline is not None


def test_try_to_advance_turn_to_phase2(db_session):
    turn = Turn(
        phase_deadline=TS0,
        base_debtor_info_locator="https://example.com/101",
        base_debtor_id=101,
        max_distance_to_base=5,
        min_trade_amount=5000,
    )
    db_session.add(turn)
    db_session.flush()
    db_session.commit()
    turn_id = turn.turn_id

    db_session.add(
        DebtorInfo(
            turn_id=turn_id,
            debtor_info_locator='http://example.com/101',
            debtor_id=101,
            peg_debtor_info_locator=None,
            peg_debtor_id=None,
            peg_exchange_rate=None,
        )
    )
    db_session.add(
        DebtorInfo(
            turn_id=turn_id,
            debtor_info_locator='http://example.com/102',
            debtor_id=102,
            peg_debtor_info_locator='http://example.com/101',
            peg_debtor_id=101,
            peg_exchange_rate=2.0,
        )
    )
    db_session.add(
        ConfirmedDebtor(
            turn_id=turn_id,
            debtor_id=102,
            debtor_info_locator='http://example.com/102',
        )
    )
    db_session.commit()
    assert len(DebtorInfo.query.all()) != 0
    assert len(ConfirmedDebtor.query.all()) != 0
    assert len(db_session.query(CurrencyInfo).all()) == 0

    # Successful advance.
    p.try_to_advance_turn_to_phase2(
        turn_id=turn_id,
        phase2_duration=timedelta(hours=1),
        max_commit_period=timedelta(days=30),
    )

    currencies = db_session.query(CurrencyInfo).all()
    assert len(currencies) == 2
    currencies.sort(key=lambda c: c.debtor_id)

    assert currencies[0].turn_id == turn_id
    assert currencies[0].debtor_info_locator == 'http://example.com/101'
    assert currencies[0].debtor_id == 101
    assert currencies[0].peg_debtor_info_locator is None
    assert currencies[0].peg_debtor_id is None
    assert currencies[0].peg_exchange_rate is None
    assert currencies[0].is_confirmed is False

    assert currencies[1].turn_id == turn_id
    assert currencies[1].debtor_info_locator == 'http://example.com/102'
    assert currencies[1].debtor_id == 102
    assert currencies[1].peg_debtor_info_locator == 'http://example.com/101'
    assert currencies[1].peg_debtor_id == 101
    assert currencies[1].peg_exchange_rate == 2.0
    assert currencies[1].is_confirmed is True

    all_turns = Turn.query.all()
    assert len(all_turns) == 1
    assert all_turns[0].phase == 2
    assert all_turns[0].phase_deadline is not None
    assert all_turns[0].phase_deadline != TS0

    assert len(DebtorInfo.query.all()) == 0
    assert len(ConfirmedDebtor.query.all()) == 0

    # Wrong turn_id or phase.
    p.try_to_advance_turn_to_phase2(
        turn_id=-1,
        phase2_duration=timedelta(hours=1),
        max_commit_period=timedelta(days=30),
    )
    p.try_to_advance_turn_to_phase2(
        turn_id=turn_id,
        phase2_duration=timedelta(hours=1),
        max_commit_period=timedelta(days=30),
    )
    all_turns = Turn.query.all()
    assert len(all_turns) == 1
    assert all_turns[0].phase == 2
    assert all_turns[0].phase_deadline is not None
    assert all_turns[0].phase_deadline != TS0


def test_try_to_advance_turn_to_phase4(db_session):
    turn = Turn(
        phase=3,
        phase_deadline=TS0,
        collection_started_at=TS0,
        collection_deadline=TS0,
        base_debtor_info_locator="https://example.com/101",
        base_debtor_id=101,
        max_distance_to_base=5,
        min_trade_amount=5000,
    )
    db_session.add(turn)
    db_session.flush()
    turn_id = turn.turn_id
    db_session.add(
        CollectorSending(
            turn_id=turn_id,
            debtor_id=102,
            from_collector_id=1,
            to_collector_id=2,
            from_collector_hash=123,
            amount=100000,
        )
    )
    db_session.commit()

    # Can not advance with pending rows.
    p.try_to_advance_turn_to_phase4(turn_id)
    all_turns = Turn.query.all()
    assert len(all_turns) == 1
    assert all_turns[0].phase == 3

    CollectorSending.query.delete()

    # Can advance without pending rows.
    p.try_to_advance_turn_to_phase4(turn_id)
    all_turns = Turn.query.all()
    assert len(all_turns) == 1
    assert all_turns[0].phase == 4
    assert all_turns[0].phase_deadline is None

    # Wrong turn_id or phase.
    p.try_to_advance_turn_to_phase4(-1)
    p.try_to_advance_turn_to_phase4(turn_id)
    all_turns = Turn.query.all()
    assert len(all_turns) == 1
    assert all_turns[0].phase == 4
    assert all_turns[0].phase_deadline is None


def test_store_document(db_session, current_ts):
    assert len(DebtorInfoDocument.query.all()) == 0

    # Insert
    p.store_document(
        debtor_info_locator="https://example.com/666",
        debtor_id=666,
        peg_debtor_info_locator="https://example.com/777",
        peg_debtor_id=777,
        peg_exchange_rate=3.14,
        will_not_change_until=current_ts + timedelta(days=100),
        ts=current_ts,
    )
    documents = DebtorInfoDocument.query.all()
    assert len(documents) == 1
    assert documents[0].debtor_info_locator == "https://example.com/666"
    assert documents[0].debtor_id == 666
    assert documents[0].peg_debtor_info_locator == "https://example.com/777"
    assert documents[0].peg_debtor_id == 777
    assert documents[0].peg_exchange_rate == 3.14
    assert documents[0].will_not_change_until == (
        current_ts + timedelta(days=100)
    )
    assert documents[0].fetched_at == current_ts

    # Update
    p.store_document(
        debtor_info_locator="https://example.com/666",
        debtor_id=999,
        peg_debtor_info_locator=None,
        peg_debtor_id=None,
        peg_exchange_rate=None,
        will_not_change_until=None,
        ts=current_ts + timedelta(seconds=60),
    )
    documents = DebtorInfoDocument.query.all()
    assert len(documents) == 1
    assert documents[0].debtor_info_locator == "https://example.com/666"
    assert documents[0].debtor_id == 999
    assert documents[0].peg_debtor_info_locator is None
    assert documents[0].peg_debtor_id is None
    assert documents[0].peg_exchange_rate is None
    assert documents[0].will_not_change_until is None
    assert documents[0].fetched_at == current_ts + timedelta(seconds=60)

    # Old document (does nothing)
    p.store_document(
        debtor_info_locator="https://example.com/666",
        debtor_id=666,
        peg_debtor_info_locator="https://example.com/777",
        peg_debtor_id=777,
        peg_exchange_rate=3.14,
        will_not_change_until=current_ts + timedelta(days=100),
        ts=current_ts,
    )
    documents = DebtorInfoDocument.query.all()
    assert len(documents) == 1
    assert documents[0].debtor_info_locator == "https://example.com/666"
    assert documents[0].debtor_id == 999
    assert documents[0].peg_debtor_info_locator is None
    assert documents[0].peg_debtor_id is None
    assert documents[0].peg_exchange_rate is None
    assert documents[0].will_not_change_until is None
    assert documents[0].fetched_at == current_ts + timedelta(seconds=60)


def test_schedule_debtor_info_fetch(db_session, current_ts):
    assert len(DebtorInfoFetch.query.all()) == 0

    p.schedule_debtor_info_fetch(
        iri="https://example.com/666",
        debtor_id=666,
        is_locator_fetch=True,
        is_discovery_fetch=False,
        recursion_level=4,
        ts=current_ts,
    )
    fetches = DebtorInfoFetch.query.all()
    assert len(fetches) == 1
    assert fetches[0].iri == "https://example.com/666"
    assert fetches[0].debtor_id == 666
    assert fetches[0].is_locator_fetch is True
    assert fetches[0].is_discovery_fetch is False
    assert fetches[0].recursion_level == 4
    assert fetches[0].attempts_count == 0

    # Schedule a fetch for the same IRI and debtor ID.
    p.schedule_debtor_info_fetch(
        iri="https://example.com/666",
        debtor_id=666,
        is_locator_fetch=False,
        is_discovery_fetch=True,
        recursion_level=2,
        ts=current_ts,
    )
    fetches = DebtorInfoFetch.query.all()
    assert len(fetches) == 1
    assert fetches[0].iri == "https://example.com/666"
    assert fetches[0].debtor_id == 666
    assert fetches[0].is_locator_fetch is True
    assert fetches[0].is_discovery_fetch is True
    assert fetches[0].recursion_level == 2
    assert fetches[0].attempts_count == 0


def test_discover_and_confirm_debtor(db_session, current_ts):
    assert len(DebtorLocatorClaim.query.all()) == 0
    assert len(FetchDebtorInfoSignal.query.all()) == 0

    # Process a discover message.
    p.discover_debtor(
        debtor_id=666,
        iri="https:/example.com/666",
        ts=current_ts,
        locator_claim_expiry_period=timedelta(days=30),
    )

    claims = DebtorLocatorClaim.query.all()
    assert len(claims) == 1
    assert claims[0].debtor_id == 666
    assert claims[0].debtor_info_locator is None
    assert claims[0].latest_locator_fetch_at is None
    assert claims[0].latest_discovery_fetch_at >= current_ts

    fetch_signals = FetchDebtorInfoSignal.query.all()
    assert len(fetch_signals) == 1
    assert fetch_signals[0].iri == "https:/example.com/666"
    assert fetch_signals[0].debtor_id == 666
    assert fetch_signals[0].is_locator_fetch is False
    assert fetch_signals[0].is_discovery_fetch is True
    assert fetch_signals[0].recursion_level == 0

    # Process the same discover message again (does nothing).
    p.discover_debtor(
        debtor_id=666,
        iri="https:/example.com/666",
        ts=current_ts,
        locator_claim_expiry_period=timedelta(days=30),
    )
    claims = DebtorLocatorClaim.query.all()
    assert len(claims) == 1
    assert len(FetchDebtorInfoSignal.query.all()) == 1

    # Process a confirm message.
    p.confirm_debtor(
        debtor_id=666,
        debtor_info_locator="https:/example.com/old-locator",
        ts=current_ts + timedelta(seconds=10),
        max_message_delay=timedelta(days=14),
    )
    claims = DebtorLocatorClaim.query.all()
    assert len(claims) == 1
    assert claims[0].debtor_id == 666
    assert claims[0].debtor_info_locator == "https:/example.com/old-locator"
    assert (
        claims[0].latest_locator_fetch_at == current_ts + timedelta(seconds=10)
    )

    # Process another confirm message for this debtor.
    p.confirm_debtor(
        debtor_id=666,
        debtor_info_locator="https:/example.com/locator",
        ts=current_ts + timedelta(seconds=30),
        max_message_delay=timedelta(days=14),
    )
    claims = DebtorLocatorClaim.query.all()
    assert len(claims) == 1
    assert claims[0].debtor_id == 666
    assert claims[0].debtor_info_locator == "https:/example.com/locator"
    assert (
        claims[0].latest_locator_fetch_at == current_ts + timedelta(seconds=30)
    )

    # Process a very old confirm message (does nothing).
    p.confirm_debtor(
        debtor_id=666,
        debtor_info_locator="https:/example.com/very-old-locator",
        ts=current_ts - timedelta(days=15),
        max_message_delay=timedelta(days=14),
    )
    claims = DebtorLocatorClaim.query.all()
    assert len(claims) == 1
    assert claims[0].debtor_id == 666
    assert claims[0].debtor_info_locator == "https:/example.com/locator"
    assert (
        claims[0].latest_locator_fetch_at == current_ts + timedelta(seconds=30)
    )

    # Process the same discover message again, but this time with
    # expired debtor locator claim.
    claims[0].latest_discovery_fetch_at = current_ts - timedelta(days=40)
    claims[0].debtor_info_locator = "https:/example.com/locator"
    claims[0].latest_locator_fetch_at = current_ts - timedelta(days=39)
    db_session.commit()

    p.discover_debtor(
        debtor_id=666,
        iri="https:/example.com/777",
        ts=current_ts,
        locator_claim_expiry_period=timedelta(days=30),
    )

    claims = DebtorLocatorClaim.query.all()
    assert len(claims) == 1
    assert claims[0].debtor_id == 666
    assert claims[0].debtor_info_locator == "https:/example.com/locator"
    assert claims[0].latest_locator_fetch_at == current_ts - timedelta(days=39)
    assert claims[0].latest_discovery_fetch_at >= current_ts

    fetch_signals = FetchDebtorInfoSignal.query.all()
    assert len(fetch_signals) == 2
    fetch_signals.sort(key=lambda signal: signal.iri)
    assert fetch_signals[0].iri == "https:/example.com/666"
    assert fetch_signals[1].iri == "https:/example.com/777"
    assert fetch_signals[1].debtor_id == 666
    assert fetch_signals[1].is_locator_fetch is False
    assert fetch_signals[1].is_discovery_fetch is True
    assert fetch_signals[1].recursion_level == 0

    # Process a confirm message for another debtor.
    p.confirm_debtor(
        debtor_id=1234,
        debtor_info_locator="https:/example.com/locator1234",
        ts=current_ts,
        max_message_delay=timedelta(days=14),
    )
    claims = DebtorLocatorClaim.query.all()
    assert len(claims) == 2
    claims.sort(key=lambda claim: claim.debtor_id)
    assert claims[1].debtor_id == 1234
    assert claims[1].debtor_info_locator == "https:/example.com/locator1234"
    assert claims[1].latest_locator_fetch_at == current_ts
