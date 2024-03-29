import pytest
from datetime import timedelta, date
from swpt_trade import procedures as p
from swpt_trade.models import (
    Turn,
    DebtorInfo,
    CollectorAccount,
    ConfirmedDebtor,
    CurrencyInfo,
    CollectorSending,
    NeededWorkerAccount,
    WorkerAccount,
    DebtorLocatorClaim,
    FetchDebtorInfoSignal,
    DiscoverDebtorSignal,
    ActivateCollectorSignal,
    ConfigureAccountSignal,
    DebtorInfoFetch,
    DebtorInfoDocument,
    TradingPolicy,
    TS0,
    DATE0,
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
        ignore_cache=False,
        recursion_level=4,
        ts=current_ts,
    )
    fetches = DebtorInfoFetch.query.all()
    assert len(fetches) == 1
    assert fetches[0].iri == "https://example.com/666"
    assert fetches[0].debtor_id == 666
    assert fetches[0].is_locator_fetch is True
    assert fetches[0].is_discovery_fetch is False
    assert fetches[0].ignore_cache is False
    assert fetches[0].recursion_level == 4
    assert fetches[0].attempts_count == 0

    # Schedule a fetch for the same IRI and debtor ID.
    p.schedule_debtor_info_fetch(
        iri="https://example.com/666",
        debtor_id=666,
        is_locator_fetch=False,
        is_discovery_fetch=True,
        ignore_cache=True,
        recursion_level=2,
        ts=current_ts,
    )
    fetches = DebtorInfoFetch.query.all()
    assert len(fetches) == 1
    assert fetches[0].iri == "https://example.com/666"
    assert fetches[0].debtor_id == 666
    assert fetches[0].is_locator_fetch is True
    assert fetches[0].is_discovery_fetch is True
    assert fetches[0].ignore_cache is True
    assert fetches[0].recursion_level == 2
    assert fetches[0].attempts_count == 0


def test_discover_and_confirm_debtor(db_session, current_ts):
    assert len(DebtorLocatorClaim.query.all()) == 0
    assert len(FetchDebtorInfoSignal.query.all()) == 0

    # Process a discover message.
    p.discover_debtor(
        debtor_id=666,
        iri="https:/example.com/666",
        force_locator_refetch=False,
        ts=current_ts,
        debtor_info_expiry_period=timedelta(days=7),
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
    assert fetch_signals[0].ignore_cache is True
    assert fetch_signals[0].recursion_level == 0

    # Process the same discover message again (does nothing).
    p.discover_debtor(
        debtor_id=666,
        iri="https:/example.com/666",
        force_locator_refetch=False,
        ts=current_ts,
        debtor_info_expiry_period=timedelta(days=7),
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
    assert claims[0].latest_locator_fetch_at >= current_ts
    fetch_signals = FetchDebtorInfoSignal.query.all()
    assert len(fetch_signals) == 2
    fetch_signals.sort(key=lambda signal: signal.signal_id)
    assert fetch_signals[1].iri == "https:/example.com/old-locator"
    assert fetch_signals[1].debtor_id == 666
    assert fetch_signals[1].is_locator_fetch is True
    assert fetch_signals[1].is_discovery_fetch is False
    assert fetch_signals[1].ignore_cache is True
    assert fetch_signals[1].recursion_level == 0

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
    assert claims[0].latest_locator_fetch_at >= current_ts
    fetch_signals = FetchDebtorInfoSignal.query.all()
    assert len(fetch_signals) == 3
    fetch_signals.sort(key=lambda signal: signal.signal_id)
    assert fetch_signals[2].iri == "https:/example.com/locator"
    assert fetch_signals[2].debtor_id == 666
    assert fetch_signals[2].is_locator_fetch is True
    assert fetch_signals[2].is_discovery_fetch is False
    assert fetch_signals[2].ignore_cache is True
    assert fetch_signals[2].recursion_level == 0

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
    assert claims[0].latest_locator_fetch_at >= current_ts
    assert len(FetchDebtorInfoSignal.query.all()) == 3

    # Process the same discover message again, but this time with
    # expired debtor locator claim, and old `latest_locator_fetch_at`.
    claims[0].latest_discovery_fetch_at = current_ts - timedelta(days=40)
    claims[0].debtor_info_locator = "https:/example.com/locator"
    claims[0].latest_locator_fetch_at = current_ts - timedelta(days=39)
    db_session.commit()
    p.discover_debtor(
        debtor_id=666,
        iri="https:/example.com/777",
        force_locator_refetch=True,
        ts=current_ts,
        debtor_info_expiry_period=timedelta(days=7),
        locator_claim_expiry_period=timedelta(days=30),
    )
    claims = DebtorLocatorClaim.query.all()
    assert len(claims) == 1
    assert claims[0].debtor_id == 666
    assert claims[0].debtor_info_locator == "https:/example.com/locator"
    assert claims[0].latest_locator_fetch_at >= current_ts
    assert claims[0].latest_discovery_fetch_at >= current_ts
    fetch_signals = FetchDebtorInfoSignal.query.all()
    assert len(fetch_signals) == 5
    fetch_signals.sort(key=lambda signal: signal.signal_id)
    fetch_signals = sorted(fetch_signals[3:], key=lambda signal: signal.iri)
    assert fetch_signals[0].iri == "https:/example.com/777"
    assert fetch_signals[0].debtor_id == 666
    assert fetch_signals[0].is_locator_fetch is False
    assert fetch_signals[0].is_discovery_fetch is True
    assert fetch_signals[0].ignore_cache is True
    assert fetch_signals[0].recursion_level == 0
    assert fetch_signals[1].iri == "https:/example.com/locator"
    assert fetch_signals[1].debtor_id == 666
    assert fetch_signals[1].is_locator_fetch is True
    assert fetch_signals[1].is_discovery_fetch is False
    assert fetch_signals[1].ignore_cache is True
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
    assert claims[1].latest_locator_fetch_at >= current_ts
    fetch_signals = FetchDebtorInfoSignal.query.all()
    assert len(fetch_signals) == 6
    fetch_signals.sort(key=lambda signal: signal.signal_id)
    assert fetch_signals[5].iri == "https:/example.com/locator1234"
    assert fetch_signals[5].debtor_id == 1234
    assert fetch_signals[5].is_locator_fetch is True
    assert fetch_signals[5].is_discovery_fetch is False
    assert fetch_signals[5].ignore_cache is True
    assert fetch_signals[5].recursion_level == 0


def test_process_updated_ledger_signal(db_session, current_ts):
    assert len(TradingPolicy.query.all()) == 0

    p.process_updated_ledger_signal(
        creditor_id=-777,
        debtor_id=666,
        update_id=123,
        account_id="test_account",
        creation_date=date(2020, 5, 17),
        principal=10000,
        last_transfer_number=456,
        ts=current_ts,
    )
    tps = TradingPolicy.query.all()
    assert len(tps) == 1
    assert tps[0].creditor_id == -777
    assert tps[0].debtor_id == 666
    assert tps[0].latest_ledger_update_id == 123
    assert tps[0].latest_ledger_update_ts == current_ts
    assert tps[0].account_id == "test_account"
    assert tps[0].creation_date == date(2020, 5, 17)
    assert tps[0].principal == 10000
    assert tps[0].last_transfer_number == 456

    # Receiving an older signal.
    p.process_updated_ledger_signal(
        creditor_id=-777,
        debtor_id=666,
        update_id=122,
        account_id="test_account",
        creation_date=date(2020, 5, 17),
        principal=20000,
        last_transfer_number=457,
        ts=current_ts + timedelta(hours=1),
    )
    tps = TradingPolicy.query.all()
    assert len(tps) == 1
    assert tps[0].creditor_id == -777
    assert tps[0].debtor_id == 666
    assert tps[0].latest_ledger_update_id == 123
    assert tps[0].latest_ledger_update_ts == current_ts
    assert tps[0].account_id == "test_account"
    assert tps[0].creation_date == date(2020, 5, 17)
    assert tps[0].principal == 10000
    assert tps[0].last_transfer_number == 456

    # Receiving an newer signal.
    p.process_updated_ledger_signal(
        creditor_id=-777,
        debtor_id=666,
        update_id=124,
        account_id="new_account_id",
        creation_date=date(2021, 6, 18),
        principal=20000,
        last_transfer_number=457,
        ts=current_ts + timedelta(hours=1),
    )
    tps = TradingPolicy.query.all()
    assert len(tps) == 1
    assert tps[0].creditor_id == -777
    assert tps[0].debtor_id == 666
    assert tps[0].latest_ledger_update_id == 124
    assert tps[0].latest_ledger_update_ts == current_ts + timedelta(hours=1)
    assert tps[0].account_id == "new_account_id"
    assert tps[0].creation_date == date(2021, 6, 18)
    assert tps[0].principal == 20000
    assert tps[0].last_transfer_number == 457


def test_process_updated_policy_signal(db_session, current_ts):
    assert len(TradingPolicy.query.all()) == 0

    p.process_updated_policy_signal(
        creditor_id=-777,
        debtor_id=666,
        update_id=123,
        policy_name="test_policy",
        min_principal=2000,
        max_principal=6000,
        peg_exchange_rate=3.14,
        peg_debtor_id=999,
        ts=current_ts,
    )
    tps = TradingPolicy.query.all()
    assert len(tps) == 1
    assert tps[0].creditor_id == -777
    assert tps[0].debtor_id == 666
    assert tps[0].latest_policy_update_id == 123
    assert tps[0].latest_policy_update_ts == current_ts
    assert tps[0].policy_name == "test_policy"
    assert tps[0].min_principal == 2000
    assert tps[0].max_principal == 6000
    assert tps[0].peg_exchange_rate == 3.14
    assert tps[0].peg_debtor_id == 999

    # Receiving an older signal.
    p.process_updated_policy_signal(
        creditor_id=-777,
        debtor_id=666,
        update_id=122,
        policy_name=None,
        min_principal=20000,
        max_principal=60000,
        peg_exchange_rate=None,
        peg_debtor_id=None,
        ts=current_ts + timedelta(hours=1),
    )
    tps = TradingPolicy.query.all()
    assert len(tps) == 1
    assert tps[0].creditor_id == -777
    assert tps[0].debtor_id == 666
    assert tps[0].latest_policy_update_id == 123
    assert tps[0].latest_policy_update_ts == current_ts
    assert tps[0].policy_name == "test_policy"
    assert tps[0].min_principal == 2000
    assert tps[0].max_principal == 6000
    assert tps[0].peg_exchange_rate == 3.14
    assert tps[0].peg_debtor_id == 999

    # Receiving an newer signal.
    p.process_updated_policy_signal(
        creditor_id=-777,
        debtor_id=666,
        update_id=124,
        policy_name=None,
        min_principal=20000,
        max_principal=60000,
        peg_exchange_rate=None,
        peg_debtor_id=None,
        ts=current_ts + timedelta(hours=1),
    )
    tps = TradingPolicy.query.all()
    assert len(tps) == 1
    assert tps[0].creditor_id == -777
    assert tps[0].debtor_id == 666
    assert tps[0].latest_policy_update_id == 124
    assert tps[0].latest_policy_update_ts == current_ts + timedelta(hours=1)
    assert tps[0].policy_name is None
    assert tps[0].min_principal == 20000
    assert tps[0].max_principal == 60000
    assert tps[0].peg_exchange_rate is None
    assert tps[0].peg_debtor_id is None


def test_process_updated_flags_signal(db_session, current_ts):
    assert len(TradingPolicy.query.all()) == 0

    p.process_updated_flags_signal(
        creditor_id=-777,
        debtor_id=666,
        update_id=123,
        config_flags=7890,
        ts=current_ts,
    )
    tps = TradingPolicy.query.all()
    assert len(tps) == 1
    assert tps[0].creditor_id == -777
    assert tps[0].debtor_id == 666
    assert tps[0].latest_flags_update_id == 123
    assert tps[0].latest_flags_update_ts == current_ts
    assert tps[0].config_flags == 7890

    # Receiving an older signal.
    p.process_updated_flags_signal(
        creditor_id=-777,
        debtor_id=666,
        update_id=122,
        config_flags=4567,
        ts=current_ts + timedelta(hours=1),
    )
    tps = TradingPolicy.query.all()
    assert len(tps) == 1
    assert tps[0].creditor_id == -777
    assert tps[0].debtor_id == 666
    assert tps[0].latest_flags_update_id == 123
    assert tps[0].latest_flags_update_ts == current_ts
    assert tps[0].config_flags == 7890

    # Receiving an newer signal.
    p.process_updated_flags_signal(
        creditor_id=-777,
        debtor_id=666,
        update_id=124,
        config_flags=4567,
        ts=current_ts + timedelta(hours=1),
    )
    tps = TradingPolicy.query.all()
    assert len(tps) == 1
    assert tps[0].creditor_id == -777
    assert tps[0].debtor_id == 666
    assert tps[0].latest_flags_update_id == 124
    assert tps[0].latest_flags_update_ts == current_ts + timedelta(hours=1)
    assert tps[0].config_flags == 4567


@pytest.mark.parametrize("status", [0, 1])
def test_activate_collector(db_session, current_ts, status):
    db_session.add(
        CollectorAccount(
            debtor_id=666,
            collector_id=123,
            status=status,
            latest_status_change_at=current_ts - timedelta(days=1),
        )
    )
    db_session.add(
        CollectorAccount(
            debtor_id=777,
            collector_id=123,
            status=status,
            latest_status_change_at=current_ts - timedelta(days=1),
        )
    )
    db_session.add(
        CollectorAccount(
            debtor_id=666,
            collector_id=321,
            status=status,
            latest_status_change_at=current_ts - timedelta(days=1),
        )
    )
    db_session.commit()

    # non-existing account
    assert not p.activate_collector(
        debtor_id=666,
        collector_id=999,
        account_id="acconut999",
    )

    # existing account
    assert p.activate_collector(
        debtor_id=666,
        collector_id=123,
        account_id="acconut123",
    )

    # already activated account
    assert not p.activate_collector(
        debtor_id=666,
        collector_id=123,
        account_id="changed-acconut-id",
    )

    cas = CollectorAccount.query.all()
    cas.sort(key=lambda r: (r.debtor_id, r.collector_id))
    assert len(cas) == 3
    assert cas[0].status == 2
    assert cas[0].account_id == "acconut123"
    assert cas[0].latest_status_change_at >= current_ts
    assert cas[1].status == status
    assert cas[1].account_id == ""
    assert cas[2].status == status
    assert cas[2].account_id == ""


def test_mark_requested_collector(db_session, current_ts):
    db_session.add(
        CollectorAccount(
            debtor_id=666,
            collector_id=123,
            latest_status_change_at=current_ts - timedelta(days=1),
        )
    )
    db_session.add(
        CollectorAccount(
            debtor_id=777,
            collector_id=123,
            latest_status_change_at=current_ts - timedelta(days=1),
        )
    )
    db_session.add(
        CollectorAccount(
            debtor_id=666,
            collector_id=321,
            latest_status_change_at=current_ts - timedelta(days=1),
        )
    )
    db_session.commit()

    # non-existing account
    assert not p.mark_requested_collector(
        debtor_id=666,
        collector_id=999,
    )

    # existing account
    assert p.mark_requested_collector(
        debtor_id=666,
        collector_id=123,
    )

    # already marked account
    assert not p.mark_requested_collector(
        debtor_id=666,
        collector_id=123,
    )

    cas = CollectorAccount.query.all()
    cas.sort(key=lambda r: (r.debtor_id, r.collector_id))
    assert len(cas) == 3
    assert cas[0].status == 1
    assert cas[0].latest_status_change_at >= current_ts
    assert cas[1].status == 0
    assert cas[2].status == 0


def test_ensure_collector_accounts(db_session):
    p.ensure_collector_accounts(
        debtor_id=666,
        min_collector_id=1000,
        max_collector_id=2000,
        number_of_accounts=3,
    )

    cas = CollectorAccount.query.all()
    assert len(cas) == 3
    for ca in cas:
        assert ca.status == 0
        assert 1000 <= ca.collector_id <= 2000

    db_session.add(CollectorAccount(debtor_id=777, collector_id=1, status=2))
    db_session.add(CollectorAccount(debtor_id=666, collector_id=1, status=3))
    db_session.commit()

    p.ensure_collector_accounts(
        debtor_id=666,
        min_collector_id=1000,
        max_collector_id=2000,
        number_of_accounts=4,
    )

    cas = CollectorAccount.query.filter_by(debtor_id=666).all()
    cas.sort(key=lambda x: x.status)
    assert len(cas) == 5
    for ca in cas[:-1]:
        assert ca.status == 0
        assert 1000 <= ca.collector_id <= 2000

    assert cas[-1].status == 3
    assert len(CollectorAccount.query.filter_by(debtor_id=777).all()) == 1

    with pytest.raises(RuntimeError):
        p.ensure_collector_accounts(
            debtor_id=666,
            min_collector_id=1,
            max_collector_id=2,
            number_of_accounts=40,
        )


def test_process_account_purge_signal(db_session, current_ts):
    nwa1 = NeededWorkerAccount(debtor_id=666, creditor_id=123)
    wa1 = WorkerAccount(
        creditor_id=123,
        debtor_id=666,
        creation_date=DATE0,
        last_change_ts=current_ts,
        last_change_seqnum=1,
        principal=0,
        interest=0.0,
        interest_rate=0.0,
        last_interest_rate_change_ts=TS0,
        config_flags=0,
        account_id="Account123",
        last_transfer_number=0,
        last_transfer_committed_at=current_ts,
        demurrage_rate=-50.0,
        commit_period=1000000,
        transfer_note_max_bytes=500,
        last_heartbeat_ts=current_ts,
    )
    wa2 = WorkerAccount(
        creditor_id=124,
        debtor_id=666,
        creation_date=DATE0,
        last_change_ts=current_ts,
        last_change_seqnum=1,
        principal=0,
        interest=0.0,
        interest_rate=0.0,
        last_interest_rate_change_ts=TS0,
        config_flags=0,
        account_id="Account124",
        last_transfer_number=0,
        last_transfer_committed_at=current_ts,
        demurrage_rate=-50.0,
        commit_period=1000000,
        transfer_note_max_bytes=500,
        last_heartbeat_ts=current_ts,
    )
    db_session.add(nwa1)
    db_session.add(wa1)
    db_session.add(wa2)
    db_session.commit()
    assert len(WorkerAccount.query.all()) == 2

    assert p.process_account_purge_signal(
        debtor_id=666, creditor_id=123, creation_date=DATE0,
    )
    was = WorkerAccount.query.all()
    assert len(was) == 1
    assert was[0].creditor_id == 124

    assert not p.process_account_purge_signal(
        debtor_id=666, creditor_id=124, creation_date=DATE0,
    )
    assert len(WorkerAccount.query.all()) == 0

    assert not p.process_account_purge_signal(
        debtor_id=666, creditor_id=125, creation_date=DATE0,
    )


def test_process_account_update_signal(db_session, current_ts):
    nwa1 = NeededWorkerAccount(debtor_id=666, creditor_id=123)
    wa1 = WorkerAccount(
        creditor_id=123,
        debtor_id=666,
        creation_date=DATE0,
        last_change_ts=current_ts - timedelta(hours=1),
        last_change_seqnum=1,
        principal=0,
        interest=0.0,
        interest_rate=0.0,
        last_interest_rate_change_ts=TS0,
        config_flags=0,
        account_id="",
        last_transfer_number=0,
        last_transfer_committed_at=current_ts - timedelta(hours=1),
        demurrage_rate=-50.0,
        commit_period=1000000,
        transfer_note_max_bytes=500,
        last_heartbeat_ts=current_ts - timedelta(hours=1),
    )
    wa2 = WorkerAccount(
        creditor_id=124,
        debtor_id=666,
        creation_date=DATE0,
        last_change_ts=current_ts - timedelta(hours=1),
        last_change_seqnum=1,
        principal=0,
        interest=0.0,
        interest_rate=0.0,
        last_interest_rate_change_ts=TS0,
        config_flags=0,
        account_id="Account124",
        last_transfer_number=0,
        last_transfer_committed_at=current_ts - timedelta(hours=1),
        demurrage_rate=-50.0,
        commit_period=1000000,
        transfer_note_max_bytes=500,
        last_heartbeat_ts=current_ts - timedelta(hours=1),
    )
    nwa3 = NeededWorkerAccount(debtor_id=666, creditor_id=125)
    db_session.add(nwa1)
    db_session.add(wa1)
    db_session.add(wa2)
    db_session.add(nwa3)
    db_session.commit()

    params = {
        "debtor_id": 666,
        "creation_date": DATE0,
        "principal": 100,
        "interest": 31.4,
        "interest_rate": 5.0,
        "last_interest_rate_change_ts": TS0 + timedelta(days=10),
        "config_flags": 0,
        "account_id": "Account123",
        "last_transfer_number": 2,
        "last_transfer_committed_at": TS0 + timedelta(days=20),
        "demurrage_rate": -50.0,
        "commit_period": 1000000,
        "transfer_note_max_bytes": 500,
        "negligible_amount": 1e30,
        "debtor_info_iri": "https://example.com/666",
        "ttl": 10000,
    }

    p.process_account_update_signal(
        **params,
        creditor_id=123,
        last_change_ts=current_ts - timedelta(hours=1),
        last_change_seqnum=1,
        ts=current_ts - timedelta(seconds=11000),
    )
    wa = WorkerAccount.query.filter_by(debtor_id=666, creditor_id=123).one()
    assert wa.debtor_info_iri is None
    assert wa.account_id == ""
    assert wa.last_change_ts == current_ts - timedelta(hours=1)
    assert wa.last_change_seqnum == 1
    assert len(DiscoverDebtorSignal.query.all()) == 0
    assert len(ActivateCollectorSignal.query.all()) == 0

    # Old last_change_ts/seqnum:
    p.process_account_update_signal(
        **params,
        creditor_id=123,
        last_change_ts=current_ts - timedelta(hours=1),
        last_change_seqnum=1,
        ts=current_ts,
    )
    wa = WorkerAccount.query.filter_by(debtor_id=666, creditor_id=123).one()
    assert wa.debtor_info_iri is None
    assert wa.account_id == ""
    assert wa.last_change_ts == current_ts - timedelta(hours=1)
    assert wa.last_change_seqnum == 1
    assert len(DiscoverDebtorSignal.query.all()) == 0
    assert len(ActivateCollectorSignal.query.all()) == 0
    assert len(ConfigureAccountSignal.query.all()) == 0

    # Expired TTL:
    p.process_account_update_signal(
        **params,
        creditor_id=123,
        last_change_ts=current_ts,
        last_change_seqnum=2,
        ts=current_ts - timedelta(seconds=11000),
    )
    wa = WorkerAccount.query.filter_by(debtor_id=666, creditor_id=123).one()
    assert wa.debtor_info_iri is None
    assert wa.account_id == ""
    assert wa.last_change_ts == current_ts - timedelta(hours=1)
    assert wa.last_change_seqnum == 1
    assert len(DiscoverDebtorSignal.query.all()) == 0
    assert len(ActivateCollectorSignal.query.all()) == 0
    assert len(ConfigureAccountSignal.query.all()) == 0

    # Successful update of existing WorkerAccount:
    p.process_account_update_signal(
        **params,
        creditor_id=123,
        last_change_ts=current_ts,
        last_change_seqnum=2,
        ts=current_ts,
    )
    wa = WorkerAccount.query.filter_by(debtor_id=666, creditor_id=123).one()
    assert wa.debtor_info_iri == "https://example.com/666"
    assert wa.account_id == "Account123"
    assert wa.last_change_ts == current_ts
    assert wa.last_change_seqnum == 2
    dds = DiscoverDebtorSignal.query.one()
    assert dds.debtor_id == 666
    assert dds.iri == "https://example.com/666"
    assert dds.force_locator_refetch is True
    acs = ActivateCollectorSignal.query.one()
    assert acs.debtor_id == 666
    assert acs.creditor_id == 123
    assert acs.account_id == "Account123"
    assert len(ConfigureAccountSignal.query.all()) == 0

    # Receiving AccountUpdate message for account that is not needed:
    p.process_account_update_signal(
        **params,
        creditor_id=124,
        last_change_ts=current_ts,
        last_change_seqnum=2,
        ts=current_ts,
    )
    wa = WorkerAccount.query.filter_by(debtor_id=666, creditor_id=124).one()
    assert wa.debtor_info_iri == "https://example.com/666"
    assert wa.account_id == "Account124"  # account_id should not change.
    assert wa.last_change_ts == current_ts
    assert wa.last_change_seqnum == 2
    assert len(DiscoverDebtorSignal.query.all()) == 1
    assert len(ActivateCollectorSignal.query.all()) == 1
    cas = ConfigureAccountSignal.query.one()
    assert cas.debtor_id == 666
    assert cas.creditor_id == 124
    assert cas.ts >= current_ts
    assert cas.negligible_amount >= 1e20
    assert cas.config_data == ""
    assert cas.config_flags & WorkerAccount.CONFIG_SCHEDULED_FOR_DELETION_FLAG

    # Receiving AccountUpdate message for the first time for a needed account:
    p.process_account_update_signal(
        **params,
        creditor_id=125,
        last_change_ts=current_ts,
        last_change_seqnum=10,
        ts=current_ts,
    )
    wa = WorkerAccount.query.filter_by(debtor_id=666, creditor_id=125).one()
    assert wa.creation_date == DATE0
    assert wa.last_change_ts == current_ts
    assert wa.last_change_seqnum == 10
    assert wa.principal == 100
    assert wa.interest == 31.4
    assert wa.interest_rate == 5.0
    assert wa.last_interest_rate_change_ts == TS0 + timedelta(days=10)
    assert wa.config_flags == 0
    assert wa.account_id == "Account123"
    assert wa.debtor_info_iri == "https://example.com/666"
    assert wa.last_transfer_number == 2
    assert wa.last_transfer_committed_at == TS0 + timedelta(days=20)
    assert wa.demurrage_rate == -50.0
    assert wa.commit_period == 1000000
    assert wa.transfer_note_max_bytes == 500
    assert wa.last_heartbeat_ts == current_ts
    assert len(ConfigureAccountSignal.query.all()) == 1
    assert len(ActivateCollectorSignal.query.all()) == 2
    assert len(DiscoverDebtorSignal.query.all()) == 2
