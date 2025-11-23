import pytest
from datetime import timedelta, date
from swpt_trade import procedures as p
from swpt_trade import utils
from swpt_trade.models import (
    Turn,
    DebtorInfo,
    CollectorAccount,
    ConfirmedDebtor,
    CurrencyInfo,
    CollectorSending,
    NeededWorkerAccount,
    WorkerAccount,
    InterestRateChange,
    DebtorLocatorClaim,
    FetchDebtorInfoSignal,
    DiscoverDebtorSignal,
    ActivateCollectorSignal,
    ConfigureAccountSignal,
    PrepareTransferSignal,
    FinalizeTransferSignal,
    AccountIdResponseSignal,
    AccountIdRequestSignal,
    DebtorInfoFetch,
    DebtorInfoDocument,
    TradingPolicy,
    RecentlyNeededCollector,
    WorkerTurn,
    AccountLock,
    UsableCollector,
    CreditorParticipation,
    DispatchingStatus,
    WorkerCollecting,
    WorkerReceiving,
    WorkerSending,
    WorkerDispatching,
    TransferAttempt,
    TS0,
    DATE0,
    MAX_INT32,
    MIN_INT64,
    AGENT_TRANSFER_NOTE_FORMAT,
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
    db_session.add(
        CollectorAccount(
            debtor_id=102,
            collector_id=0x0000010000000000,
            account_id="TestCollectorAccount0",
            status=2,
        )
    )
    db_session.add(
        CollectorAccount(
            debtor_id=102,
            collector_id=0x0000010000000001,
            account_id="TestCollectorAccount1",
            status=2,
        )
    )
    db_session.add(
        CollectorAccount(
            debtor_id=123456,
            collector_id=0x0000010000000001,
            account_id="TestCollectorAccount3",
            status=2,
        )
    )
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
        forced_iri=None,
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
        forced_iri=None,
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
    assert fetch_signals[0].recursion_level == 0
    assert fetch_signals[1].iri == "https:/example.com/locator"
    assert fetch_signals[1].debtor_id == 666
    assert fetch_signals[1].is_locator_fetch is True
    assert fetch_signals[1].is_discovery_fetch is False
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
    assert tps[0].account_id_is_obsolete is False
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
    assert tps[0].account_id_is_obsolete is False
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
    assert tps[0].account_id_is_obsolete is False
    assert tps[0].creation_date == date(2021, 6, 18)
    assert tps[0].principal == 20000
    assert tps[0].last_transfer_number == 457

    # Receiving an newer signal with empty account ID.
    p.process_updated_ledger_signal(
        creditor_id=-777,
        debtor_id=666,
        update_id=125,
        account_id="",
        creation_date=date(2021, 6, 18),
        principal=20000,
        last_transfer_number=457,
        ts=current_ts + timedelta(hours=1),
    )
    tps = TradingPolicy.query.all()
    assert len(tps) == 1
    assert tps[0].creditor_id == -777
    assert tps[0].debtor_id == 666
    assert tps[0].latest_ledger_update_id == 125
    assert tps[0].latest_ledger_update_ts == current_ts + timedelta(hours=1)
    assert tps[0].account_id == "new_account_id"
    assert tps[0].account_id_is_obsolete is True
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


def test_insert_collector_accounts(db_session):
    p.insert_collector_accounts([(666, 1000)])
    p.insert_collector_accounts([(666, 1000), (666, 1001)])
    cas = CollectorAccount.query.all()
    cas.sort(key=lambda x: (x.debtor_id, x.collector_id))
    assert len(cas) == 2
    assert cas[0].debtor_id == 666
    assert cas[0].collector_id == 1000
    assert cas[0].status == 0
    assert cas[1].debtor_id == 666
    assert cas[1].collector_id == 1001
    assert cas[0].status == 0


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
        interest_rate=15.0,
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
    db_session.add(
        InterestRateChange(
            creditor_id=123,
            debtor_id=666,
            change_ts=TS0,
            interest_rate=0.0,
        )
    )
    db_session.add(wa2)
    db_session.add(
        InterestRateChange(
            creditor_id=124,
            debtor_id=666,
            change_ts=TS0,
            interest_rate=15.0,
        )
    )
    db_session.add(nwa3)
    db_session.commit()
    assert len(InterestRateChange.query.all()) == 2

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

    # Old last_change_ts/seqnum:
    p.process_account_update_signal(
        **params,
        creditor_id=123,
        last_change_ts=current_ts - timedelta(hours=2),
        last_change_seqnum=0,
        ts=current_ts,
    )
    wa = WorkerAccount.query.filter_by(debtor_id=666, creditor_id=123).one()
    assert wa.debtor_info_iri is None
    assert wa.account_id == ""
    assert wa.last_change_ts == current_ts - timedelta(hours=1)
    assert wa.last_change_seqnum == 1
    assert wa.interest_rate == 0.0
    assert len(DiscoverDebtorSignal.query.all()) == 0
    assert len(ActivateCollectorSignal.query.all()) == 0
    assert len(ConfigureAccountSignal.query.all()) == 0
    ircs = InterestRateChange.query.all()
    ircs.sort(key=lambda t: (
        t.creditor_id, t.debtor_id, t.change_ts, t.interest_rate
    ))
    assert len(ircs) == 3
    assert ircs[0].creditor_id == 123
    assert ircs[0].debtor_id == 666
    assert ircs[0].change_ts == TS0
    assert ircs[0].interest_rate == 0.0
    assert ircs[1].creditor_id == 123
    assert ircs[1].debtor_id == 666
    assert ircs[1].change_ts == TS0 + timedelta(days=10)
    assert ircs[1].interest_rate == 5.0
    assert ircs[2].creditor_id == 124
    assert ircs[2].debtor_id == 666
    assert ircs[2].change_ts == TS0
    assert ircs[2].interest_rate == 15.0

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
    assert wa.interest_rate == 0.0
    assert len(DiscoverDebtorSignal.query.all()) == 0
    assert len(ActivateCollectorSignal.query.all()) == 0
    assert len(ConfigureAccountSignal.query.all()) == 0
    assert len(InterestRateChange.query.all()) == 3

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
    assert wa.interest_rate == 5.0
    dds = DiscoverDebtorSignal.query.one()
    assert dds.debtor_id == 666
    assert dds.iri == "https://example.com/666"
    assert dds.force_locator_refetch is True
    acs = ActivateCollectorSignal.query.one()
    assert acs.debtor_id == 666
    assert acs.creditor_id == 123
    assert acs.account_id == "Account123"
    assert len(ConfigureAccountSignal.query.all()) == 0
    assert len(InterestRateChange.query.all()) == 3

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
    ircs = InterestRateChange.query.all()
    ircs.sort(key=lambda t: (
        t.creditor_id, t.debtor_id, t.change_ts, t.interest_rate
    ))
    assert len(ircs) == 3
    assert ircs[0].creditor_id == 123
    assert ircs[0].debtor_id == 666
    assert ircs[0].change_ts == TS0
    assert ircs[0].interest_rate == 0.0
    assert ircs[1].creditor_id == 123
    assert ircs[1].debtor_id == 666
    assert ircs[1].change_ts == TS0 + timedelta(days=10)
    assert ircs[1].interest_rate == 5.0
    assert ircs[2].creditor_id == 124
    assert ircs[2].debtor_id == 666
    assert ircs[2].change_ts == TS0
    assert ircs[2].interest_rate == 15.0

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
    ircs = InterestRateChange.query.all()
    ircs.sort(key=lambda t: (
        t.creditor_id, t.debtor_id, t.change_ts, t.interest_rate
    ))
    assert len(ircs) == 4
    assert ircs[0].creditor_id == 123
    assert ircs[0].debtor_id == 666
    assert ircs[0].change_ts == TS0
    assert ircs[0].interest_rate == 0.0
    assert ircs[1].creditor_id == 123
    assert ircs[1].debtor_id == 666
    assert ircs[1].change_ts == TS0 + timedelta(days=10)
    assert ircs[1].interest_rate == 5.0
    assert ircs[2].creditor_id == 124
    assert ircs[2].debtor_id == 666
    assert ircs[2].change_ts == TS0
    assert ircs[2].interest_rate == 15.0
    assert ircs[3].creditor_id == 125
    assert ircs[3].debtor_id == 666
    assert ircs[3].change_ts == TS0 + timedelta(days=10)
    assert ircs[3].interest_rate == 5.0


def test_mark_as_recently_needed_collector(db_session, current_ts):
    assert len(RecentlyNeededCollector.query.all()) == 0
    assert p.is_recently_needed_collector(666) is False
    assert p.is_recently_needed_collector(777) is False
    assert len(RecentlyNeededCollector.query.all()) == 0
    p.mark_as_recently_needed_collector(666)
    assert p.is_recently_needed_collector(666) is True
    assert p.is_recently_needed_collector(777) is False
    rncs = RecentlyNeededCollector.query.all()
    assert len(rncs) == 1
    assert rncs[0].debtor_id == 666
    assert rncs[0].needed_at >= current_ts


def test_try_to_compact_interest_rate_changes(db_session, current_ts):
    assert len(InterestRateChange.query.all()) == 0
    assert p.store_interest_rate_change(
        creditor_id=123,
        debtor_id=666,
        change_ts=current_ts,
        interest_rate=10.0,
    )
    assert len(InterestRateChange.query.all()) == 1
    assert not p.store_interest_rate_change(
        creditor_id=123,
        debtor_id=666,
        change_ts=current_ts,
        interest_rate=10.0,
    )
    assert len(InterestRateChange.query.all()) == 1

    assert p.store_interest_rate_change(
        creditor_id=124,
        debtor_id=777,
        change_ts=current_ts - timedelta(hours=1),
        interest_rate=5.0,
    )
    ircs = InterestRateChange.query.all()
    ircs.sort(key=lambda r: (r.creditor_id, r.debtor_id, r.change_ts))
    assert ircs[0].creditor_id == 123
    assert ircs[0].debtor_id == 666
    assert ircs[0].change_ts == current_ts
    assert ircs[0].interest_rate == 10.0
    assert ircs[1].creditor_id == 124
    assert ircs[1].debtor_id == 777
    assert ircs[1].change_ts == current_ts - timedelta(hours=1)
    assert ircs[1].interest_rate == 5.0

    p.compact_interest_rate_changes(
        creditor_id=123,
        debtor_id=666,
        cutoff_ts=current_ts - timedelta(days=100),
        max_number_of_changes=100,
    )
    assert len(InterestRateChange.query.all()) == 2

    p.compact_interest_rate_changes(
        creditor_id=124,
        debtor_id=777,
        cutoff_ts=current_ts - timedelta(days=100),
        max_number_of_changes=1,
    )
    assert len(InterestRateChange.query.all()) == 2

    assert p.store_interest_rate_change(
        creditor_id=124,
        debtor_id=777,
        change_ts=current_ts,
        interest_rate=4.0,
    )
    assert len(InterestRateChange.query.all()) == 3

    p.compact_interest_rate_changes(
        creditor_id=124,
        debtor_id=777,
        cutoff_ts=current_ts - timedelta(days=100),
        max_number_of_changes=1,
    )
    ircs = InterestRateChange.query.all()
    ircs.sort(key=lambda r: (r.creditor_id, r.debtor_id, r.change_ts))
    assert len(ircs) == 2
    assert ircs[0].creditor_id == 123
    assert ircs[0].debtor_id == 666
    assert ircs[0].change_ts == current_ts
    assert ircs[0].interest_rate == 10.0
    assert ircs[1].creditor_id == 124
    assert ircs[1].debtor_id == 777
    assert ircs[1].change_ts == current_ts
    assert ircs[1].interest_rate == 4.0

    p.compact_interest_rate_changes(
        creditor_id=123,
        debtor_id=666,
        cutoff_ts=current_ts - timedelta(days=100),
        max_number_of_changes=0,
    )
    ircs = InterestRateChange.query.all()
    ircs.sort(key=lambda r: (r.creditor_id, r.debtor_id, r.change_ts))
    assert len(ircs) == 1
    assert ircs[0].creditor_id == 124
    assert ircs[0].debtor_id == 777
    assert ircs[0].change_ts == current_ts
    assert ircs[0].interest_rate == 4.0

    assert p.store_interest_rate_change(
        creditor_id=124,
        debtor_id=777,
        change_ts=current_ts - timedelta(hours=12),
        interest_rate=-2.0,
    )
    assert len(InterestRateChange.query.all()) == 2

    p.compact_interest_rate_changes(
        creditor_id=124,
        debtor_id=777,
        cutoff_ts=current_ts - timedelta(days=100),
        max_number_of_changes=2,
    )
    ircs = InterestRateChange.query.all()
    ircs.sort(key=lambda r: (r.creditor_id, r.debtor_id, r.change_ts))
    assert len(ircs) == 2
    assert ircs[0].creditor_id == 124
    assert ircs[0].debtor_id == 777
    assert ircs[0].change_ts == current_ts - timedelta(hours=12)
    assert ircs[0].interest_rate == -2.0
    assert ircs[1].creditor_id == 124
    assert ircs[1].debtor_id == 777
    assert ircs[1].change_ts == current_ts
    assert ircs[1].interest_rate == 4.0

    p.compact_interest_rate_changes(
        creditor_id=124,
        debtor_id=777,
        cutoff_ts=current_ts + timedelta(hours=1),
        max_number_of_changes=2,
    )
    ircs = InterestRateChange.query.all()
    ircs.sort(key=lambda r: (r.creditor_id, r.debtor_id, r.change_ts))
    assert len(ircs) == 1
    assert ircs[0].creditor_id == 124
    assert ircs[0].debtor_id == 777
    assert ircs[0].change_ts == current_ts
    assert ircs[0].interest_rate == 4.0


@pytest.mark.parametrize("has_released_lock", [True, False])
def test_process_candidate_offer_signal(
        db_session,
        current_ts,
        has_released_lock,
):
    assert len(AccountLock.query.all()) == 0
    assert len(PrepareTransferSignal.query.all()) == 0
    assert len(WorkerTurn.query.all()) == 0

    wt0 = WorkerTurn(
        turn_id=0,
        started_at=current_ts,
        base_debtor_info_locator="https://example.com/666",
        base_debtor_id=666,
        max_distance_to_base=10,
        min_trade_amount=1000,
        phase=2,
        phase_deadline=current_ts + timedelta(hours=10),
        collection_started_at=None,
        collection_deadline=current_ts + timedelta(days=30),
        worker_turn_subphase=5,
    )
    wt1 = WorkerTurn(
        turn_id=1,
        started_at=current_ts,
        base_debtor_info_locator="https://example.com/666",
        base_debtor_id=666,
        max_distance_to_base=10,
        min_trade_amount=1000,
        phase=2,
        phase_deadline=current_ts + timedelta(hours=10),
        collection_started_at=None,
        collection_deadline=current_ts + timedelta(days=30),
        worker_turn_subphase=5,
    )
    wt2 = WorkerTurn(
        turn_id=2,
        started_at=current_ts,
        base_debtor_info_locator="https://example.com/666",
        base_debtor_id=666,
        max_distance_to_base=10,
        min_trade_amount=1000,
        phase=2,
        phase_deadline=current_ts + timedelta(hours=10),
        collection_started_at=None,
        collection_deadline=current_ts + timedelta(days=30),
        worker_turn_subphase=5,
    )
    db_session.add(wt0)
    db_session.add(wt1)
    db_session.add(wt2)
    db_session.add(
        UsableCollector(
            debtor_id=666,
            collector_id=999,
            account_id="TestCollectorAccount999",
        )
    )
    db_session.commit()

    if has_released_lock:
        db_session.add(
            AccountLock(
                creditor_id=777,
                debtor_id=666,
                turn_id=0,
                collector_id=999,
                released_at=current_ts,
                transfer_id=123,
                finalized_at=current_ts,
                amount=80000,
                account_creation_date=date(2000, 1, 1),
                account_last_transfer_number=12345,
            )
        )
        db_session.commit()

    # successful buyer lock
    p.process_candidate_offer_signal(
        demurrage_rate=-50.0,
        turn_id=1,
        creditor_id=777,
        debtor_id=666,
        amount=20000,
        account_creation_date=date(2024, 1, 1),
        last_transfer_number=1234,
    )
    al = AccountLock.query.one()
    assert al.creditor_id == 777
    assert al.debtor_id == 666
    assert al.turn_id == 1
    assert al.amount == 20000
    assert al.collector_id == 999
    assert al.released_at is None
    assert al.initiated_at >= current_ts
    assert type(al.coordinator_request_id) is int
    assert al.transfer_id is None
    assert al.finalized_at is None
    assert al.account_creation_date is None
    assert al.account_last_transfer_number is None

    pts = PrepareTransferSignal.query.one()
    assert pts.creditor_id == 777
    assert pts.coordinator_request_id == al.coordinator_request_id
    assert pts.debtor_id == 666
    assert pts.recipient == "TestCollectorAccount999"
    assert pts.min_locked_amount == 0
    assert pts.max_locked_amount == 0
    assert pts.final_interest_rate_ts > current_ts + timedelta(days=1000)
    assert pts.max_commit_delay == MAX_INT32

    # already locked
    p.process_candidate_offer_signal(
        demurrage_rate=-50.0,
        turn_id=2,
        creditor_id=777,
        debtor_id=666,
        amount=30000,
        account_creation_date=date(2024, 1, 1),
        last_transfer_number=1234,
    )
    assert len(PrepareTransferSignal.query.all()) == 1
    al = AccountLock.query.one()
    assert al.creditor_id == 777
    assert al.debtor_id == 666
    assert al.turn_id == 1
    assert al.amount == 20000
    assert al.collector_id == 999
    assert al.released_at is None
    assert al.initiated_at >= current_ts
    assert type(al.coordinator_request_id) is int
    assert al.transfer_id is None
    assert al.finalized_at is None
    assert al.account_creation_date is None
    assert al.account_last_transfer_number is None

    # no collectors
    p.process_candidate_offer_signal(
        demurrage_rate=-50.0,
        turn_id=1,
        creditor_id=777,
        debtor_id=12345,
        amount=30000,
        account_creation_date=date(2024, 1, 1),
        last_transfer_number=1234,
    )
    assert len(AccountLock.query.all()) == 1
    assert len(PrepareTransferSignal.query.all()) == 1

    # successful seller lock
    p.process_candidate_offer_signal(
        demurrage_rate=-50.0,
        turn_id=1,
        creditor_id=888,
        debtor_id=666,
        amount=-30000,
        account_creation_date=date(2024, 1, 1),
        last_transfer_number=1234,
    )
    als = AccountLock.query.all()
    als.sort(key=lambda x: x.creditor_id)
    assert len(als) == 2
    assert als[0].creditor_id == 777
    assert als[1].creditor_id == 888
    assert als[1].debtor_id == 666
    assert als[1].turn_id == 1
    assert als[1].amount == -30000
    assert als[1].collector_id == 999
    assert als[1].released_at is None
    assert als[1].initiated_at >= current_ts
    assert type(als[1].coordinator_request_id) is int
    assert als[1].coordinator_request_id != als[0].coordinator_request_id
    assert als[1].transfer_id is None
    assert als[1].finalized_at is None
    assert als[1].account_creation_date is None
    assert als[1].account_last_transfer_number is None

    ptss = PrepareTransferSignal.query.all()
    ptss.sort(key=lambda x: x.creditor_id)
    assert len(ptss) == 2
    assert ptss[0].creditor_id == 777
    assert ptss[1].creditor_id == 888
    assert ptss[1].coordinator_request_id == als[1].coordinator_request_id
    assert ptss[1].debtor_id == 666
    assert ptss[1].recipient == "TestCollectorAccount999"
    assert ptss[1].min_locked_amount > 1050
    assert ptss[1].max_locked_amount > 31500
    assert ptss[1].final_interest_rate_ts > current_ts + timedelta(days=1000)
    assert ptss[1].max_commit_delay == MAX_INT32

    # self-lock
    p.process_candidate_offer_signal(
        demurrage_rate=-50.0,
        turn_id=1,
        creditor_id=999,
        debtor_id=666,
        amount=-50000,
        account_creation_date=date(2024, 1, 1),
        last_transfer_number=1234,
    )
    assert len(PrepareTransferSignal.query.all()) == 2
    als = AccountLock.query.all()
    als.sort(key=lambda x: x.creditor_id)
    assert len(als) == 3
    assert als[0].creditor_id == 777
    assert als[1].creditor_id == 888
    assert als[2].creditor_id == 999
    assert als[2].debtor_id == 666
    assert als[2].turn_id == 1
    assert als[2].amount == -50000  # already locked
    assert als[2].collector_id == 999
    assert als[2].released_at is None
    assert als[2].initiated_at >= current_ts
    assert type(als[2].coordinator_request_id) is int
    assert als[2].coordinator_request_id != als[0].coordinator_request_id
    assert als[2].coordinator_request_id != als[1].coordinator_request_id
    assert type(als[2].transfer_id) is int
    assert als[2].finalized_at is None
    assert als[2].account_creation_date is None
    assert als[2].account_last_transfer_number is None


@pytest.mark.parametrize("has_account_lock", [True, False])
def test_process_account_lock_rejected_transfer(
        db_session,
        current_ts,
        has_account_lock,
):
    wt = WorkerTurn(
        turn_id=0,
        started_at=current_ts,
        base_debtor_info_locator="https://example.com/666",
        base_debtor_id=666,
        max_distance_to_base=10,
        min_trade_amount=1000,
        phase=2,
        phase_deadline=current_ts + timedelta(hours=10),
        collection_started_at=None,
        collection_deadline=current_ts + timedelta(days=30),
        worker_turn_subphase=5,
    )
    db_session.add(wt)
    db_session.commit()

    if has_account_lock:
        al = AccountLock(
            creditor_id=777,
            debtor_id=666,
            turn_id=0,
            collector_id=999,
            amount=0,
        )
        db_session.add(al)
        db_session.flush()
        coordinator_request_id = al.coordinator_request_id
        db_session.commit()
    else:
        coordinator_request_id = 0

    assert p.put_rejected_transfer_through_account_locks(
        coordinator_id=777,
        coordinator_request_id=coordinator_request_id,
        status_code="TEST",
        debtor_id=666,
        creditor_id=777,
    ) == has_account_lock

    assert len(AccountLock.query.all()) == 0


@pytest.fixture(scope="function")
def wt_2_5(db_session, current_ts):
    wt = WorkerTurn(
        turn_id=1,
        started_at=current_ts,
        base_debtor_info_locator="https://example.com/666",
        base_debtor_id=666,
        max_distance_to_base=10,
        min_trade_amount=1000,
        phase=2,
        phase_deadline=current_ts + timedelta(hours=10),
        collection_started_at=None,
        collection_deadline=current_ts + timedelta(days=30),
        worker_turn_subphase=5,
    )
    db_session.add(wt)
    db_session.commit()
    return wt


@pytest.fixture(scope="function")
def collector_id(db_session, current_ts):
    db_session.add(
        UsableCollector(
            debtor_id=666,
            collector_id=999,
            account_id="TestCollectorAccount999",
        )
    )
    db_session.add(
        NeededWorkerAccount(creditor_id=999, debtor_id=666)
    )
    params = {
        "creation_date": DATE0,
        "last_change_ts": current_ts,
        "last_change_seqnum": 1,
        "principal": 100,
        "interest": 31.4,
        "interest_rate": 5.0,
        "last_interest_rate_change_ts": current_ts,
        "config_flags": 0,
        "account_id": "Account123",
        "last_transfer_number": 2,
        "last_transfer_committed_at": TS0,
        "demurrage_rate": -50.0,
        "commit_period": 1000000,
        "transfer_note_max_bytes": 500,
        "debtor_info_iri": "https://example.com/666",
    }
    db_session.add(
        WorkerAccount(creditor_id=999, debtor_id=666, **params)
    )
    db_session.add(
        InterestRateChange(
            creditor_id=999,
            debtor_id=666,
            change_ts=current_ts,
            interest_rate=5.0,
        )
    )
    db_session.add(
        InterestRateChange(
            creditor_id=999,
            debtor_id=666,
            change_ts=current_ts - timedelta(days=10),
            interest_rate=-20.0,
        )
    )
    db_session.add(
        InterestRateChange(
            creditor_id=999,
            debtor_id=666,
            change_ts=current_ts - timedelta(days=1000),
            interest_rate=-10.0,
        )
    )
    db_session.commit()
    return 999


@pytest.fixture(scope="function")
def account_lock(wt_2_5, collector_id):
    p.process_candidate_offer_signal(
        demurrage_rate=-50.0,
        turn_id=wt_2_5.turn_id,
        creditor_id=888,
        debtor_id=666,
        amount=-30000,
        account_creation_date=date(2024, 1, 1),
        last_transfer_number=1234,
    )
    al = AccountLock.query.one()
    return al


def test_process_alpt_no_lock_id(db_session, current_ts):
    assert not p.put_prepared_transfer_through_account_locks(
        debtor_id=1666,
        creditor_id=888,
        transfer_id=123,
        coordinator_id=888,
        coordinator_request_id=12345,
        locked_amount=10000,
        demurrage_rate=-49,
        deadline=current_ts + timedelta(days=1000),
        min_demurrage_rate=-50,
    )
    assert len(FinalizeTransferSignal.query.all()) == 0


def test_process_alpt_wrong_debtor_id(current_ts, account_lock):
    assert p.put_prepared_transfer_through_account_locks(
        debtor_id=1666,
        creditor_id=888,
        transfer_id=123,
        coordinator_id=888,
        coordinator_request_id=account_lock.coordinator_request_id,
        locked_amount=10000,
        demurrage_rate=-49,
        deadline=current_ts + timedelta(days=1000),
        min_demurrage_rate=-50,
    )
    fts = FinalizeTransferSignal.query.one()
    assert fts.creditor_id == 888
    assert fts.debtor_id == 1666
    assert fts.coordinator_id == 888
    assert fts.coordinator_request_id == account_lock.coordinator_request_id
    assert fts.transfer_id == 123
    assert fts.committed_amount == 0
    assert fts.transfer_note_format == ""
    assert fts.transfer_note == ""

    al = AccountLock.query.one()
    assert al.debtor_id == 666
    assert al.creditor_id == 888
    assert al.turn_id == account_lock.turn_id
    assert al.collector_id == account_lock.collector_id
    assert al.initiated_at == account_lock.initiated_at
    assert al.coordinator_request_id == account_lock.coordinator_request_id
    assert al.transfer_id is None
    assert al.amount == account_lock.amount
    assert al.finalized_at is None
    assert al.released_at is None
    assert al.account_creation_date is None
    assert al.account_last_transfer_number is None


def test_process_alpt_wrong_creditor_id(current_ts, account_lock):
    assert p.put_prepared_transfer_through_account_locks(
        debtor_id=666,
        creditor_id=1888,
        transfer_id=123,
        coordinator_id=888,
        coordinator_request_id=account_lock.coordinator_request_id,
        locked_amount=10000,
        demurrage_rate=-49,
        deadline=current_ts + timedelta(days=1000),
        min_demurrage_rate=-50,
    )
    fts = FinalizeTransferSignal.query.one()
    assert fts.creditor_id == 1888
    assert fts.debtor_id == 666
    assert fts.coordinator_id == 888
    assert fts.coordinator_request_id == account_lock.coordinator_request_id
    assert fts.transfer_id == 123
    assert fts.committed_amount == 0
    assert fts.transfer_note_format == ""
    assert fts.transfer_note == ""

    al = AccountLock.query.one()
    assert al.debtor_id == 666
    assert al.creditor_id == 888
    assert al.turn_id == account_lock.turn_id
    assert al.collector_id == account_lock.collector_id
    assert al.initiated_at == account_lock.initiated_at
    assert al.coordinator_request_id == account_lock.coordinator_request_id
    assert al.transfer_id is None
    assert al.amount == account_lock.amount
    assert al.finalized_at is None
    assert al.released_at is None
    assert al.account_creation_date is None
    assert al.account_last_transfer_number is None


def test_process_alpt_sell_success(current_ts, account_lock):
    assert p.put_prepared_transfer_through_account_locks(
        debtor_id=666,
        creditor_id=888,
        transfer_id=123,
        coordinator_id=888,
        coordinator_request_id=account_lock.coordinator_request_id,
        locked_amount=10000,
        demurrage_rate=-49,
        deadline=current_ts + timedelta(days=1000),
        min_demurrage_rate=-50,
    )
    assert len(FinalizeTransferSignal.query.all()) == 0

    al = AccountLock.query.one()
    assert al.debtor_id == 666
    assert al.creditor_id == 888
    assert al.turn_id == account_lock.turn_id
    assert al.collector_id == account_lock.collector_id
    assert al.initiated_at >= current_ts
    assert al.coordinator_request_id == account_lock.coordinator_request_id
    assert al.transfer_id == 123
    assert -9500 < al.amount < -9400
    assert al.finalized_at is None
    assert al.released_at is None
    assert al.account_creation_date is None
    assert al.account_last_transfer_number is None

    # Receive the same "PreparedTransfer" message again.
    assert p.put_prepared_transfer_through_account_locks(
        debtor_id=666,
        creditor_id=888,
        transfer_id=123,
        coordinator_id=888,
        coordinator_request_id=account_lock.coordinator_request_id,
        locked_amount=10000,
        demurrage_rate=-49,
        deadline=current_ts + timedelta(days=1000),
        min_demurrage_rate=-50,
    )
    assert len(FinalizeTransferSignal.query.all()) == 0

    al = AccountLock.query.one()
    assert al.debtor_id == 666
    assert al.creditor_id == 888
    assert al.turn_id == account_lock.turn_id
    assert al.collector_id == account_lock.collector_id
    assert al.initiated_at >= current_ts
    assert al.coordinator_request_id == account_lock.coordinator_request_id
    assert al.transfer_id == 123
    assert -9500 < al.amount < -9400
    assert al.finalized_at is None
    assert al.released_at is None
    assert al.account_creation_date is None
    assert al.account_last_transfer_number is None

    # Receive "PreparedTransfer" message with another `transfer_id`.
    assert p.put_prepared_transfer_through_account_locks(
        debtor_id=666,
        creditor_id=888,
        transfer_id=124,
        coordinator_id=888,
        coordinator_request_id=account_lock.coordinator_request_id,
        locked_amount=10000,
        demurrage_rate=-49,
        deadline=current_ts + timedelta(days=1000),
        min_demurrage_rate=-50,
    )
    fts = FinalizeTransferSignal.query.one()
    assert fts.creditor_id == 888
    assert fts.debtor_id == 666
    assert fts.coordinator_id == 888
    assert fts.coordinator_request_id == account_lock.coordinator_request_id
    assert fts.transfer_id == 124
    assert fts.committed_amount == 0
    assert fts.transfer_note_format == ""
    assert fts.transfer_note == ""

    al = AccountLock.query.one()
    assert al.debtor_id == 666
    assert al.creditor_id == 888
    assert al.turn_id == account_lock.turn_id
    assert al.collector_id == account_lock.collector_id
    assert al.initiated_at >= current_ts
    assert al.coordinator_request_id == account_lock.coordinator_request_id
    assert al.transfer_id == 123
    assert -9500 < al.amount < -9400
    assert al.finalized_at is None
    assert al.released_at is None
    assert al.account_creation_date is None
    assert al.account_last_transfer_number is None


def test_process_alpt_buy_success(db_session, current_ts, account_lock):
    al = AccountLock.query.one()
    al.amount = 50000
    db_session.commit()

    assert p.put_prepared_transfer_through_account_locks(
        debtor_id=666,
        creditor_id=888,
        transfer_id=123,
        coordinator_id=888,
        coordinator_request_id=account_lock.coordinator_request_id,
        locked_amount=0,
        demurrage_rate=-49,
        deadline=current_ts + timedelta(days=1000),
        min_demurrage_rate=-50,
    )
    assert len(FinalizeTransferSignal.query.all()) == 0

    al = AccountLock.query.one()
    assert al.debtor_id == 666
    assert al.creditor_id == 888
    assert al.turn_id == account_lock.turn_id
    assert al.collector_id == account_lock.collector_id
    assert al.initiated_at >= current_ts
    assert al.coordinator_request_id == account_lock.coordinator_request_id
    assert al.transfer_id == 123
    assert al.amount == 50000
    assert al.finalized_at is None
    assert al.released_at is None
    assert al.account_creation_date is None
    assert al.account_last_transfer_number is None


def test_process_alpt_wrong_demurrage_rate(current_ts, account_lock):
    assert p.put_prepared_transfer_through_account_locks(
        debtor_id=666,
        creditor_id=888,
        transfer_id=123,
        coordinator_id=888,
        coordinator_request_id=account_lock.coordinator_request_id,
        locked_amount=10000,
        demurrage_rate=-80,
        deadline=current_ts + timedelta(days=1000),
        min_demurrage_rate=-50,
    )
    fts = FinalizeTransferSignal.query.one()
    assert fts.creditor_id == 888
    assert fts.debtor_id == 666
    assert fts.coordinator_id == 888
    assert fts.coordinator_request_id == account_lock.coordinator_request_id
    assert fts.transfer_id == 123
    assert fts.committed_amount == 0
    assert fts.transfer_note_format == ""
    assert fts.transfer_note == ""

    assert len(AccountLock.query.all()) == 0


def test_process_alpt_wrong_deadline(current_ts, account_lock):
    assert p.put_prepared_transfer_through_account_locks(
        debtor_id=666,
        creditor_id=888,
        transfer_id=123,
        coordinator_id=888,
        coordinator_request_id=account_lock.coordinator_request_id,
        locked_amount=10000,
        demurrage_rate=-49,
        deadline=current_ts + timedelta(days=1),
        min_demurrage_rate=-50,
    )
    fts = FinalizeTransferSignal.query.one()
    assert fts.creditor_id == 888
    assert fts.debtor_id == 666
    assert fts.coordinator_id == 888
    assert fts.coordinator_request_id == account_lock.coordinator_request_id
    assert fts.transfer_id == 123
    assert fts.committed_amount == 0
    assert fts.transfer_note_format == ""
    assert fts.transfer_note == ""

    assert len(AccountLock.query.all()) == 0


def test_process_alpt_already_dismissed(db_session, current_ts, account_lock):
    al = AccountLock.query.one()
    al.transfer_id = 123
    al.amount = 0
    al.finalized_at = current_ts
    db_session.commit()

    assert p.put_prepared_transfer_through_account_locks(
        debtor_id=666,
        creditor_id=888,
        transfer_id=123,
        coordinator_id=888,
        coordinator_request_id=account_lock.coordinator_request_id,
        locked_amount=10000,
        demurrage_rate=-49,
        deadline=current_ts + timedelta(days=1),
        min_demurrage_rate=-50,
    )
    fts = FinalizeTransferSignal.query.one()
    assert fts.creditor_id == 888
    assert fts.debtor_id == 666
    assert fts.coordinator_id == 888
    assert fts.coordinator_request_id == account_lock.coordinator_request_id
    assert fts.transfer_id == 123
    assert fts.committed_amount == 0
    assert fts.transfer_note_format == ""
    assert fts.transfer_note == ""

    al = AccountLock.query.one()
    assert al.debtor_id == 666
    assert al.creditor_id == 888
    assert al.turn_id == account_lock.turn_id
    assert al.collector_id == account_lock.collector_id
    assert al.initiated_at >= current_ts
    assert al.coordinator_request_id == account_lock.coordinator_request_id
    assert al.transfer_id == 123
    assert al.amount == 0
    assert al.finalized_at == current_ts
    assert al.released_at is None
    assert al.account_creation_date is None
    assert al.account_last_transfer_number is None


def test_process_alpt_already_committed(db_session, current_ts, account_lock):
    al = AccountLock.query.one()
    al.transfer_id = 123
    al.finalized_at = current_ts
    al.amount = -9000
    db_session.commit()

    assert p.put_prepared_transfer_through_account_locks(
        debtor_id=666,
        creditor_id=888,
        transfer_id=123,
        coordinator_id=888,
        coordinator_request_id=account_lock.coordinator_request_id,
        locked_amount=10000,
        demurrage_rate=-49,
        deadline=current_ts + timedelta(days=1),
        min_demurrage_rate=-50,
    )
    fts = FinalizeTransferSignal.query.one()
    assert fts.creditor_id == 888
    assert fts.debtor_id == 666
    assert fts.coordinator_id == 888
    assert fts.coordinator_request_id == account_lock.coordinator_request_id
    assert fts.transfer_id == 123
    assert fts.committed_amount == 9000
    assert fts.transfer_note_format == "-cXchge"
    assert fts.transfer_note == (
        f"Trading session: {account_lock.turn_id}\n"
        f"Buyer: {account_lock.collector_id:x}\n"
        f"Seller: {account_lock.creditor_id:x}\n"
    )

    al = AccountLock.query.one()
    assert al.debtor_id == 666
    assert al.creditor_id == 888
    assert al.turn_id == account_lock.turn_id
    assert al.collector_id == account_lock.collector_id
    assert al.initiated_at >= current_ts
    assert al.coordinator_request_id == account_lock.coordinator_request_id
    assert al.transfer_id == 123
    assert al.amount == -9000
    assert al.finalized_at == current_ts
    assert al.released_at is None
    assert al.account_creation_date is None
    assert al.account_last_transfer_number is None


@pytest.fixture(scope="function")
def wt_3_10(db_session, current_ts):
    wt = WorkerTurn(
        turn_id=1,
        started_at=current_ts,
        base_debtor_info_locator="https://example.com/666",
        base_debtor_id=666,
        max_distance_to_base=10,
        min_trade_amount=1000,
        phase=3,
        phase_deadline=None,
        collection_started_at=current_ts + timedelta(hours=-1),
        collection_deadline=current_ts + timedelta(days=30),
        worker_turn_subphase=10,
    )
    db_session.add(wt)
    db_session.commit()
    return wt


@pytest.mark.parametrize("amount", [80000, -80000])
@pytest.mark.parametrize("transfer_id", [None, 123])
def test_process_revise_account_lock_signal_delete_lock(
        db_session,
        current_ts,
        wt_3_10,
        collector_id,
        amount,
        transfer_id,
):
    turn_id = wt_3_10.turn_id
    db_session.add(
        AccountLock(
            creditor_id=777,
            debtor_id=666,
            turn_id=turn_id,
            collector_id=collector_id,
            coordinator_request_id=7890,
            transfer_id=transfer_id,
            amount=amount,
        )
    )
    assert len(CreditorParticipation.query.all()) == 0
    assert len(AccountLock.query.all()) == 1
    assert len(FinalizeTransferSignal.query.all()) == 0
    p.process_revise_account_lock_signal(
        creditor_id=777,
        debtor_id=666,
        turn_id=turn_id,
    )
    assert len(CreditorParticipation.query.all()) == 0
    assert len(AccountLock.query.all()) == 0

    if transfer_id is None:
        assert len(FinalizeTransferSignal.query.all()) == 0
    else:
        fts = FinalizeTransferSignal.query.one()
        assert fts.debtor_id == 666
        assert fts.creditor_id == 777
        assert fts.transfer_id == transfer_id
        assert fts.coordinator_id == 777
        assert fts.committed_amount == 0
        assert fts.coordinator_request_id == 7890
        assert fts.transfer_note_format == ""
        assert fts.transfer_note == ""


def test_process_revise_account_lock_signal_seller(
        db_session,
        current_ts,
        wt_3_10,
        collector_id,
):
    turn_id = wt_3_10.turn_id
    db_session.add(
        AccountLock(
            creditor_id=777,
            debtor_id=666,
            turn_id=turn_id,
            collector_id=collector_id,
            coordinator_request_id=7890,
            transfer_id=123,
            amount=-80000,
        )
    )
    db_session.add(
        CreditorParticipation(
            creditor_id=777,
            debtor_id=666,
            turn_id=turn_id,
            amount=-50000,
            collector_id=collector_id,
        )
    )
    assert len(CreditorParticipation.query.all()) == 1
    assert len(AccountLock.query.all()) == 1
    assert len(FinalizeTransferSignal.query.all()) == 0
    p.process_revise_account_lock_signal(
        creditor_id=777,
        debtor_id=666,
        turn_id=turn_id,
    )
    assert len(CreditorParticipation.query.all()) == 0
    al = AccountLock.query.one()
    assert al.creditor_id == 777
    assert al.debtor_id == 666
    assert al.turn_id == turn_id
    assert al.collector_id == collector_id
    assert al.coordinator_request_id == 7890
    assert al.amount == -50000
    assert al.transfer_id == 123
    assert al.finalized_at >= current_ts
    assert al.released_at is None
    assert al.account_creation_date is None
    assert al.account_last_transfer_number is None
    assert al.has_been_revised is True
    fts = FinalizeTransferSignal.query.one()
    assert fts.debtor_id == 666
    assert fts.creditor_id == 777
    assert fts.transfer_id == 123
    assert fts.coordinator_id == 777
    assert fts.committed_amount == 50000
    assert fts.coordinator_request_id == 7890
    assert fts.transfer_note_format == AGENT_TRANSFER_NOTE_FORMAT
    print(fts.transfer_note)
    assert (
        utils.TransferNote.parse(fts.transfer_note)
        == utils.TransferNote(
            turn_id, utils.TransferNote.Kind.COLLECTING, collector_id, 777
        )
    )

    # process again (must be a noop)
    p.process_revise_account_lock_signal(
        creditor_id=777,
        debtor_id=666,
        turn_id=turn_id,
    )
    assert len(CreditorParticipation.query.all()) == 0
    assert len(FinalizeTransferSignal.query.all()) == 1
    assert len(AccountLock.query.all()) == 1


def test_process_revise_account_lock_signal_buyer(
        db_session,
        current_ts,
        wt_3_10,
        collector_id,
):
    turn_id = wt_3_10.turn_id
    db_session.add(
        AccountLock(
            creditor_id=777,
            debtor_id=666,
            turn_id=turn_id,
            collector_id=collector_id,
            coordinator_request_id=7890,
            transfer_id=123,
            amount=80000,
        )
    )
    db_session.add(
        CreditorParticipation(
            creditor_id=777,
            debtor_id=666,
            turn_id=turn_id,
            amount=50000,
            collector_id=collector_id,
        )
    )
    assert len(CreditorParticipation.query.all()) == 1
    assert len(AccountLock.query.all()) == 1
    assert len(FinalizeTransferSignal.query.all()) == 0
    p.process_revise_account_lock_signal(
        creditor_id=777,
        debtor_id=666,
        turn_id=turn_id,
    )
    assert len(CreditorParticipation.query.all()) == 0
    al = AccountLock.query.one()
    assert al.creditor_id == 777
    assert al.debtor_id == 666
    assert al.turn_id == turn_id
    assert al.collector_id == collector_id
    assert al.coordinator_request_id == 7890
    assert al.amount == 50000
    assert al.transfer_id == 123
    assert al.finalized_at is None
    assert al.released_at is None
    assert al.account_creation_date is None
    assert al.account_last_transfer_number is None
    assert al.has_been_revised is True
    assert len(FinalizeTransferSignal.query.all()) == 0

    # process again (must be a noop)
    p.process_revise_account_lock_signal(
        creditor_id=777,
        debtor_id=666,
        turn_id=turn_id,
    )
    assert len(CreditorParticipation.query.all()) == 0
    assert len(FinalizeTransferSignal.query.all()) == 0
    assert len(AccountLock.query.all()) == 1


@pytest.mark.parametrize("amount", [80000, -80000])
def test_process_revise_account_lock_signal_self_lock(
        db_session,
        current_ts,
        wt_3_10,
        collector_id,
        amount,
):
    turn_id = wt_3_10.turn_id
    db_session.add(
        AccountLock(
            creditor_id=collector_id,
            debtor_id=666,
            turn_id=turn_id,
            collector_id=collector_id,
            coordinator_request_id=7890,
            transfer_id=123,
            amount=amount,
        )
    )
    db_session.add(
        CreditorParticipation(
            creditor_id=collector_id,
            debtor_id=666,
            turn_id=turn_id,
            amount=amount,
            collector_id=collector_id,
        )
    )
    assert len(CreditorParticipation.query.all()) == 1
    assert len(AccountLock.query.all()) == 1
    assert len(FinalizeTransferSignal.query.all()) == 0
    p.process_revise_account_lock_signal(
        creditor_id=collector_id,
        debtor_id=666,
        turn_id=turn_id,
    )
    assert len(CreditorParticipation.query.all()) == 0
    al = AccountLock.query.one()
    assert al.creditor_id == collector_id
    assert al.debtor_id == 666
    assert al.turn_id == turn_id
    assert al.collector_id == collector_id
    assert al.coordinator_request_id == 7890
    assert al.amount == amount
    assert al.transfer_id == 123
    assert al.finalized_at >= current_ts
    assert al.released_at >= current_ts
    assert al.account_creation_date is not None
    assert al.account_last_transfer_number is not None
    assert al.has_been_revised is True
    assert len(FinalizeTransferSignal.query.all()) == 0
    wa = WorkerAccount.query.one()
    assert wa.debtor_id == 666
    assert wa.creditor_id == collector_id
    assert wa.surplus_spent_amount == (-amount if amount < 0 else 0)
    assert wa.surplus_last_transfer_number == 1

    # process again (must be a noop)
    p.process_revise_account_lock_signal(
        creditor_id=collector_id,
        debtor_id=666,
        turn_id=turn_id,
    )
    assert len(CreditorParticipation.query.all()) == 0
    assert len(FinalizeTransferSignal.query.all()) == 0
    assert len(AccountLock.query.all()) == 1
    wa = WorkerAccount.query.one()
    assert wa.debtor_id == 666
    assert wa.creditor_id == collector_id
    assert wa.surplus_spent_amount == (-amount if amount < 0 else 0)
    assert wa.surplus_last_transfer_number == 1


@pytest.mark.parametrize("is_collector_trade", [True, False])
def test_release_seller_account_lock(
        db_session,
        wt_3_10,
        current_ts,
        is_collector_trade,
):
    turn_id = wt_3_10.turn_id
    db_session.add(
        AccountLock(
            creditor_id=777,
            debtor_id=666,
            turn_id=turn_id,
            collector_id=999,
            transfer_id=123,
            finalized_at=current_ts,
            amount=-80000,
        )
    )
    db_session.commit()

    assert len(AccountLock.query.all()) == 1
    assert len(FinalizeTransferSignal.query.all()) == 0
    p.release_seller_account_lock(
        creditor_id=777,
        debtor_id=666,
        turn_id=turn_id,
        acquired_amount=-80000,
        account_creation_date=current_ts.date(),
        account_transfer_number=7890,
        is_collector_trade=is_collector_trade,
    )
    al = AccountLock.query.one()
    assert al.released_at >= current_ts
    if is_collector_trade:
        assert al.account_creation_date == DATE0
        assert al.account_last_transfer_number == 0
    else:
        assert al.account_creation_date == current_ts.date()
        assert al.account_last_transfer_number == 7890
    assert len(FinalizeTransferSignal.query.all()) == 0

    # release once more
    p.release_seller_account_lock(
        creditor_id=777,
        debtor_id=666,
        turn_id=turn_id,
        acquired_amount=-80000,
        account_creation_date=current_ts.date(),
        account_transfer_number=7890,
        is_collector_trade=is_collector_trade,
    )
    al = AccountLock.query.one()
    assert al.released_at >= current_ts
    if is_collector_trade:
        assert al.account_creation_date == DATE0
        assert al.account_last_transfer_number == 0
    else:
        assert al.account_creation_date == current_ts.date()
        assert al.account_last_transfer_number == 7890
    assert len(FinalizeTransferSignal.query.all()) == 0


def test_update_worker_collecting_record(db_session, current_ts):
    wt = WorkerTurn(
        turn_id=1,
        started_at=current_ts,
        base_debtor_info_locator="https://example.com/666",
        base_debtor_id=666,
        max_distance_to_base=10,
        min_trade_amount=10000,
        phase=3,
        phase_deadline=None,
        collection_started_at=current_ts - timedelta(days=1),
        collection_deadline=current_ts - timedelta(days=49),
        worker_turn_subphase=5,
    )
    db_session.add(wt)
    db_session.add(
        WorkerCollecting(
            collector_id=999,
            turn_id=1,
            debtor_id=666,
            creditor_id=123,
            amount=1000,
            purge_after=current_ts + timedelta(days=1000),
        )
    )
    db_session.commit()

    other_kwargs = dict(
        transfer_number=123,
        creation_date=current_ts.date(),
        coordinator_type="agent",
        sender="321",
        recipient="123",
        transfer_note_format="",
        transfer_note="test note",
        committed_at=current_ts,
        principal=1000,
        previous_transfer_number=122,
        ts=current_ts,
    )
    assert len(WorkerCollecting.query.all()) == 1
    p.update_worker_collecting_record(
        collector_id=999,
        turn_id=1,
        debtor_id=666,
        creditor_id=123,
        acquired_amount=1000,
        **other_kwargs
    )
    wc = WorkerCollecting.query.one()
    assert wc.amount == 1000
    assert wc.collected is True

    # update once more
    p.update_worker_collecting_record(
        collector_id=999,
        turn_id=1,
        debtor_id=666,
        creditor_id=123,
        acquired_amount=1000,
        **other_kwargs
    )
    wc = WorkerCollecting.query.one()
    assert wc.amount == 1000
    assert wc.collected is True


def test_delete_worker_sending_record(db_session, current_ts):
    db_session.add(
        WorkerSending(
            from_collector_id=999,
            turn_id=1,
            debtor_id=666,
            to_collector_id=888,
            amount=1000,
            purge_after=current_ts + timedelta(days=1000),
        )
    )
    db_session.commit()

    assert len(WorkerSending.query.all()) == 1
    p.delete_worker_sending_record(
        from_collector_id=999,
        turn_id=1,
        debtor_id=666,
        to_collector_id=888,
    )
    assert len(WorkerSending.query.all()) == 0

    # delete once more
    p.delete_worker_sending_record(
        from_collector_id=999,
        turn_id=1,
        debtor_id=666,
        to_collector_id=888,
    )
    assert len(WorkerSending.query.all()) == 0


def test_update_worker_receiving_record(db_session, current_ts):
    wt = WorkerTurn(
        turn_id=1,
        started_at=current_ts,
        base_debtor_info_locator="https://example.com/666",
        base_debtor_id=666,
        max_distance_to_base=10,
        min_trade_amount=10000,
        phase=3,
        phase_deadline=None,
        collection_started_at=current_ts - timedelta(days=1),
        collection_deadline=current_ts - timedelta(days=49),
        worker_turn_subphase=5,
    )
    db_session.add(wt)
    db_session.add(
        WorkerReceiving(
            to_collector_id=999,
            turn_id=1,
            debtor_id=666,
            from_collector_id=888,
            expected_amount=1001,
            purge_after=current_ts + timedelta(days=1000),
        )
    )
    db_session.commit()

    other_kwargs = dict(
        creditor_id=123,
        transfer_number=123,
        creation_date=current_ts.date(),
        coordinator_type="agent",
        sender="321",
        recipient="123",
        transfer_note_format="",
        transfer_note="test note",
        committed_at=current_ts,
        principal=1000,
        previous_transfer_number=122,
        ts=current_ts,
    )
    assert len(WorkerReceiving.query.all()) == 1
    p.update_worker_receiving_record(
        to_collector_id=999,
        turn_id=1,
        debtor_id=666,
        from_collector_id=888,
        acquired_amount=1000,
        **other_kwargs
    )
    wr = WorkerReceiving.query.one()
    assert wr.expected_amount == 1001
    assert wr.received_amount == 1000

    # update once more
    p.update_worker_receiving_record(
        to_collector_id=999,
        turn_id=1,
        debtor_id=666,
        from_collector_id=888,
        acquired_amount=1000,
        **other_kwargs
    )
    wr = WorkerReceiving.query.one()
    assert wr.expected_amount == 1001
    assert wr.received_amount == 1000


def test_delete_worker_dispatching_record(db_session, current_ts):
    db_session.add(
        WorkerDispatching(
            collector_id=999,
            turn_id=1,
            debtor_id=666,
            creditor_id=123,
            amount=1000,
            purge_after=current_ts + timedelta(days=1000),
        )
    )
    db_session.commit()

    assert len(WorkerDispatching.query.all()) == 1
    p.delete_worker_dispatching_record(
        collector_id=999,
        turn_id=1,
        debtor_id=666,
        creditor_id=888,
    )
    assert len(WorkerSending.query.all()) == 0

    # delete once more
    p.delete_worker_dispatching_record(
        collector_id=999,
        turn_id=1,
        debtor_id=666,
        creditor_id=888,
    )
    assert len(WorkerSending.query.all()) == 0


@pytest.mark.parametrize("is_collector_trade", [True, False])
def test_release_buyer_account_lock(
        db_session,
        wt_3_10,
        current_ts,
        is_collector_trade,
):
    turn_id = wt_3_10.turn_id
    db_session.add(
        AccountLock(
            creditor_id=777,
            debtor_id=666,
            turn_id=turn_id,
            collector_id=999,
            coordinator_request_id=56789,
            transfer_id=123,
            amount=80000,
        )
    )
    db_session.commit()

    assert len(AccountLock.query.all()) == 1
    assert len(FinalizeTransferSignal.query.all()) == 0
    p.release_buyer_account_lock(
        creditor_id=777,
        debtor_id=666,
        turn_id=turn_id,
        acquired_amount=80000,
        account_creation_date=current_ts.date(),
        account_transfer_number=7890,
        is_collector_trade=is_collector_trade,
    )
    al = AccountLock.query.one()
    assert al.finalized_at >= current_ts
    assert al.released_at >= current_ts
    if is_collector_trade:
        assert al.account_creation_date == DATE0
        assert al.account_last_transfer_number == 0
    else:
        assert al.account_creation_date == current_ts.date()
        assert al.account_last_transfer_number == 7890
    fts = FinalizeTransferSignal.query.one()
    assert fts.creditor_id == 777
    assert fts.debtor_id == 666
    assert fts.transfer_id == 123
    assert fts.coordinator_id == 777
    assert fts.coordinator_request_id == 56789
    assert fts.committed_amount == 0
    assert fts.transfer_note_format == ""
    assert fts.transfer_note == ""

    # release once more
    p.release_buyer_account_lock(
        creditor_id=777,
        debtor_id=666,
        turn_id=turn_id,
        acquired_amount=80000,
        account_creation_date=current_ts.date(),
        account_transfer_number=7890,
        is_collector_trade=is_collector_trade,
    )
    al = AccountLock.query.one()
    assert al.finalized_at >= current_ts
    assert al.released_at >= current_ts
    if is_collector_trade:
        assert al.account_creation_date == DATE0
        assert al.account_last_transfer_number == 0
    else:
        assert al.account_creation_date == current_ts.date()
        assert al.account_last_transfer_number == 7890
    assert len(FinalizeTransferSignal.query.all()) == 1


def test_process_account_id_request_signal(db_session, current_ts):
    db_session.add(
        TradingPolicy(
            creditor_id=666,
            debtor_id=1,
            latest_ledger_update_id=123,
            account_id='account1',
        )
    )
    db_session.add(
        WorkerAccount(
            creditor_id=999,
            debtor_id=1,
            last_change_ts=current_ts,
            last_change_seqnum=1,
            principal=100,
            interest=31.4,
            interest_rate=5.0,
            last_interest_rate_change_ts=current_ts,
            config_flags=0,
            account_id="account2",
            last_transfer_number=2,
            last_transfer_committed_at=current_ts,
            demurrage_rate=-50.0,
            commit_period=1000000,
            transfer_note_max_bytes=500,
            debtor_info_iri="https://example.com/666",
            creation_date=DATE0 + timedelta(days=5),
        )
    )
    db_session.commit()

    p.process_account_id_request_signal(
        collector_id=888,
        turn_id=3,
        debtor_id=1,
        creditor_id=666,
        is_dispatching=True
    )
    rss = AccountIdResponseSignal.query.all()
    assert len(rss) == 1
    assert rss[0].collector_id == 888
    assert rss[0].turn_id == 3
    assert rss[0].debtor_id == 1
    assert rss[0].creditor_id == 666
    assert rss[0].is_dispatching is True
    assert rss[0].account_id == "account1"
    assert rss[0].account_id_version == 123
    AccountIdResponseSignal.query.delete()

    p.process_account_id_request_signal(
        collector_id=888,
        turn_id=3,
        debtor_id=1,
        creditor_id=999,
        is_dispatching=False
    )
    rss = AccountIdResponseSignal.query.all()
    assert len(rss) == 1
    assert rss[0].collector_id == 888
    assert rss[0].turn_id == 3
    assert rss[0].debtor_id == 1
    assert rss[0].creditor_id == 999
    assert rss[0].is_dispatching is False
    assert rss[0].account_id == "account2"
    assert rss[0].account_id_version == 5
    AccountIdResponseSignal.query.delete()

    p.process_account_id_request_signal(
        collector_id=888,
        turn_id=3,
        debtor_id=1,
        creditor_id=555,
        is_dispatching=False
    )
    rss = AccountIdResponseSignal.query.all()
    assert len(rss) == 1
    assert rss[0].collector_id == 888
    assert rss[0].turn_id == 3
    assert rss[0].debtor_id == 1
    assert rss[0].creditor_id == 555
    assert rss[0].is_dispatching is False
    assert rss[0].account_id == ""
    assert rss[0].account_id_version == MIN_INT64


def test_transfer_attempt_success(db_session, collector_id, current_ts):
    db_session.add(
        TransferAttempt(
            collector_id=collector_id,
            turn_id=1,
            debtor_id=666,
            creditor_id=123,
            is_dispatching=True,
            nominal_amount=1e9,
            collection_started_at=current_ts - timedelta(days=30),
        )
    )
    db_session.commit()

    p.process_account_id_response_signal(
        collector_id=collector_id,
        turn_id=1,
        debtor_id=666,
        creditor_id=123,
        is_dispatching=True,
        account_id="account123",
        account_id_version=1,
        transfers_healthy_max_commit_delay=timedelta(hours=3),
        transfers_amount_cut=1e-8,
    )
    assert len(FinalizeTransferSignal.query.all()) == 0

    pts = PrepareTransferSignal.query.one()
    assert pts.creditor_id == collector_id
    assert pts.debtor_id == 666
    assert pts.coordinator_request_id is not None
    assert pts.recipient == "account123"
    assert pts.min_locked_amount == 0
    assert pts.max_locked_amount == 0
    assert pts.max_commit_delay == int(timedelta(hours=3).total_seconds())
    assert pts.inserted_at >= current_ts
    coordinator_request_id = pts.coordinator_request_id
    final_interest_rate_ts = pts.final_interest_rate_ts
    assert coordinator_request_id is not None
    assert final_interest_rate_ts > current_ts - timedelta(hours=1)

    ta = TransferAttempt.query.one()
    assert ta.attempted_at >= current_ts
    assert ta.recipient == "account123"
    assert ta.recipient_version == 1
    assert ta.coordinator_request_id == coordinator_request_id
    assert ta.transfer_id is None
    assert ta.finalized_at is None
    assert ta.failure_code is None
    assert ta.fatal_error is None
    assert ta.final_interest_rate_ts == final_interest_rate_ts
    assert ta.backoff_counter == 0
    amount = ta.amount
    assert 0.980e-9 <= amount <= 0.983e9

    assert p.put_prepared_transfer_through_transfer_attempts(
        debtor_id=666,
        creditor_id=collector_id,
        transfer_id=12345,
        coordinator_id=collector_id,
        coordinator_request_id=coordinator_request_id,
    )
    ta = TransferAttempt.query.one()
    assert ta.recipient == "account123"
    assert ta.recipient_version == 1
    assert ta.coordinator_request_id == coordinator_request_id
    assert ta.transfer_id == 12345
    assert ta.finalized_at >= current_ts
    assert ta.failure_code is None
    assert ta.fatal_error is None
    assert ta.final_interest_rate_ts == final_interest_rate_ts
    assert ta.amount == amount
    assert ta.backoff_counter == 0

    fts = FinalizeTransferSignal.query.one()
    assert fts.creditor_id == collector_id
    assert fts.debtor_id == 666
    assert fts.transfer_id == 12345
    assert fts.coordinator_id == collector_id
    assert fts.coordinator_request_id == coordinator_request_id
    assert fts.committed_amount == amount
    assert fts.transfer_note_format == AGENT_TRANSFER_NOTE_FORMAT
    transfser_note = utils.TransferNote.parse(fts.transfer_note)
    assert transfser_note.turn_id == 1
    assert transfser_note.note_kind == utils.TransferNote.Kind.DISPATCHING
    assert transfser_note.first_id == collector_id
    assert transfser_note.second_id == 123

    # Processing the same "PreparedTransfer" signal again leads to
    # sending the same "FinalizeTransfer" message again.
    assert p.put_prepared_transfer_through_transfer_attempts(
        debtor_id=666,
        creditor_id=collector_id,
        transfer_id=12345,
        coordinator_id=collector_id,
        coordinator_request_id=coordinator_request_id,
    )
    ta = TransferAttempt.query.one()
    assert ta.recipient == "account123"
    assert ta.recipient_version == 1
    assert ta.coordinator_request_id == coordinator_request_id
    assert ta.transfer_id == 12345
    assert ta.finalized_at >= current_ts
    assert ta.failure_code is None
    assert ta.fatal_error is None
    assert ta.final_interest_rate_ts == final_interest_rate_ts
    assert ta.amount == amount
    assert ta.backoff_counter == 0

    ftss = FinalizeTransferSignal.query.all()
    assert len(ftss) == 2
    for fts in ftss:
        assert fts.creditor_id == collector_id
        assert fts.debtor_id == 666
        assert fts.transfer_id == 12345
        assert fts.coordinator_id == collector_id
        assert fts.coordinator_request_id == coordinator_request_id
        assert fts.committed_amount == amount
        assert fts.transfer_note_format == AGENT_TRANSFER_NOTE_FORMAT
        transfser_note = utils.TransferNote.parse(fts.transfer_note)
        assert transfser_note.turn_id == 1
        assert transfser_note.note_kind == utils.TransferNote.Kind.DISPATCHING
        assert transfser_note.first_id == collector_id
        assert transfser_note.second_id == 123

    p.put_finalized_transfer_through_transfer_attempts(
        debtor_id=666,
        creditor_id=collector_id,
        transfer_id=12345,
        coordinator_id=collector_id,
        coordinator_request_id=coordinator_request_id,
        committed_amount=amount,
        status_code="OK",
        transfers_healthy_max_commit_delay=timedelta(hours=3),
    )
    assert len(TransferAttempt.query.all()) == 0


@pytest.mark.parametrize("status", [
    ("TIMEOUT",
     TransferAttempt.TIMEOUT),
    ("NEWER_INTEREST_RATE",
     TransferAttempt.NEWER_INTEREST_RATE),
    ("RECIPIENT_IS_UNREACHABLE",
     TransferAttempt.RECIPIENT_IS_UNREACHABLE),
    ("INSUFFICIENT_AVAILABLE_AMOUNT",
     TransferAttempt.INSUFFICIENT_AVAILABLE_AMOUNT),
    ("FATAL_ERROR",
     TransferAttempt.UNSPECIFIED_FAILURE),
])
def test_transfer_attempt_failure(
        db_session,
        collector_id,
        current_ts,
        status,
):
    db_session.add(
        TransferAttempt(
            collector_id=collector_id,
            turn_id=1,
            debtor_id=666,
            creditor_id=123,
            is_dispatching=True,
            nominal_amount=1e9,
            collection_started_at=current_ts - timedelta(days=30),
            backoff_counter=100,
        )
    )
    db_session.commit()

    p.process_account_id_response_signal(
        collector_id=collector_id,
        turn_id=1,
        debtor_id=666,
        creditor_id=123,
        is_dispatching=True,
        account_id="account123",
        account_id_version=1,
        transfers_healthy_max_commit_delay=timedelta(hours=3),
        transfers_amount_cut=1e-8,
    )
    pts = PrepareTransferSignal.query.one()
    coordinator_request_id = pts.coordinator_request_id

    assert p.put_prepared_transfer_through_transfer_attempts(
        debtor_id=666,
        creditor_id=collector_id,
        transfer_id=12345,
        coordinator_id=collector_id,
        coordinator_request_id=coordinator_request_id,
    )

    p.put_finalized_transfer_through_transfer_attempts(
        debtor_id=666,
        creditor_id=collector_id,
        transfer_id=12345,
        coordinator_id=collector_id,
        coordinator_request_id=coordinator_request_id,
        committed_amount=0,
        status_code=status[0],
        transfers_healthy_max_commit_delay=timedelta(hours=3),
    )
    ta = TransferAttempt.query.one()
    _assert_attempt_status(ta, status, collector_id)


def test_transfer_attempt_no_collector(db_session, current_ts):
    db_session.add(
        TransferAttempt(
            collector_id=999,
            turn_id=1,
            debtor_id=666,
            creditor_id=123,
            is_dispatching=True,
            nominal_amount=1000.5,
            collection_started_at=current_ts - timedelta(hours=3),
            recipient="account123",
            recipient_version=1,
            attempted_at=current_ts,
            amount=1000,
            coordinator_request_id=7,
            final_interest_rate_ts=current_ts,
            transfer_id=12345,
            finalized_at=current_ts,
            failure_code=TransferAttempt.UNSPECIFIED_FAILURE,
            backoff_counter=3,
            rescheduled_for=None,
        )
    )
    db_session.commit()

    p.process_trigger_transfer_signal(
        collector_id=999,
        turn_id=1,
        debtor_id=666,
        creditor_id=123,
        is_dispatching=True,
        transfers_healthy_max_commit_delay=timedelta(hours=3),
        transfers_amount_cut=1e-8,
    )
    assert len(PrepareTransferSignal.query.all()) == 0
    assert len(FinalizeTransferSignal.query.all()) == 0

    ta = TransferAttempt.query.one()
    assert ta.failure_code == TransferAttempt.UNSPECIFIED_FAILURE
    assert ta.fatal_error is None
    assert ta.backoff_counter == 3
    assert ta.rescheduled_for is not None


def test_transfer_attempt_old_interest_rate(
        db_session,
        collector_id,
        current_ts,
):
    db_session.add(
        TransferAttempt(
            collector_id=collector_id,
            turn_id=1,
            debtor_id=666,
            creditor_id=123,
            is_dispatching=True,
            nominal_amount=1000.5,
            collection_started_at=current_ts - timedelta(hours=3),
            recipient="account123",
            recipient_version=1,
            attempted_at=current_ts,
            amount=1000,
            coordinator_request_id=7,
            final_interest_rate_ts=current_ts + timedelta(days=10),
            transfer_id=12345,
            finalized_at=current_ts,
            failure_code=TransferAttempt.NEWER_INTEREST_RATE,
            backoff_counter=3,
            rescheduled_for=None,
        )
    )
    db_session.commit()

    p.process_trigger_transfer_signal(
        collector_id=collector_id,
        turn_id=1,
        debtor_id=666,
        creditor_id=123,
        is_dispatching=True,
        transfers_healthy_max_commit_delay=timedelta(hours=3),
        transfers_amount_cut=1e-8,
    )
    assert len(PrepareTransferSignal.query.all()) == 0
    assert len(FinalizeTransferSignal.query.all()) == 0

    ta = TransferAttempt.query.one()
    assert ta.failure_code == TransferAttempt.NEWER_INTEREST_RATE
    assert ta.fatal_error is None
    assert ta.backoff_counter == 3
    assert ta.rescheduled_for is not None


def test_transfer_attempt_unmatched_prepared_transfer(
        db_session,
        collector_id,
        current_ts,
):
    db_session.add(
        TransferAttempt(
            collector_id=collector_id,
            turn_id=1,
            debtor_id=666,
            creditor_id=123,
            is_dispatching=True,
            nominal_amount=1000.5,
            collection_started_at=current_ts - timedelta(hours=3),
            recipient="account123",
            recipient_version=1,
            attempted_at=current_ts,
            amount=1000,
            coordinator_request_id=7,
            final_interest_rate_ts=current_ts,
            transfer_id=12345,
            finalized_at=current_ts,
            failure_code=TransferAttempt.UNSPECIFIED_FAILURE,
            backoff_counter=3,
        )
    )
    db_session.commit()

    assert p.put_prepared_transfer_through_transfer_attempts(
        debtor_id=666,
        creditor_id=collector_id,
        transfer_id=12345,
        coordinator_id=collector_id,
        coordinator_request_id=7,
    )
    ta = TransferAttempt.query.one()
    assert ta.recipient == "account123"
    assert ta.recipient_version == 1
    assert ta.attempted_at == current_ts
    assert ta.coordinator_request_id == 7
    assert ta.transfer_id == 12345
    assert ta.finalized_at == current_ts
    assert ta.failure_code == TransferAttempt.UNSPECIFIED_FAILURE
    assert ta.fatal_error is None
    assert ta.final_interest_rate_ts == current_ts
    assert ta.amount == 1000
    assert ta.backoff_counter == 3

    assert len(PrepareTransferSignal.query.all()) == 0

    fts = FinalizeTransferSignal.query.one()
    assert fts.creditor_id == collector_id
    assert fts.debtor_id == 666
    assert fts.transfer_id == 12345
    assert fts.coordinator_id == collector_id
    assert fts.coordinator_request_id == 7
    assert fts.committed_amount == 0
    assert fts.transfer_note_format == ""
    assert fts.transfer_note == ""


@pytest.mark.parametrize("status", [
    ("TIMEOUT",
     TransferAttempt.TIMEOUT),
    ("NEWER_INTEREST_RATE",
     TransferAttempt.NEWER_INTEREST_RATE),
    ("RECIPIENT_IS_UNREACHABLE",
     TransferAttempt.RECIPIENT_IS_UNREACHABLE),
    ("INSUFFICIENT_AVAILABLE_AMOUNT",
     TransferAttempt.INSUFFICIENT_AVAILABLE_AMOUNT),
    ("FATAL_ERROR",
     TransferAttempt.UNSPECIFIED_FAILURE),
])
def test_transfer_attempt_rejected_transfer(
        db_session,
        collector_id,
        current_ts,
        status,
):
    db_session.add(
        TransferAttempt(
            collector_id=collector_id,
            turn_id=1,
            debtor_id=666,
            creditor_id=123,
            is_dispatching=True,
            nominal_amount=1000.5,
            collection_started_at=current_ts - timedelta(hours=3),
            recipient="account123",
            recipient_version=1,
            attempted_at=current_ts,
            amount=1000,
            coordinator_request_id=7,
            final_interest_rate_ts=current_ts,
            backoff_counter=100,
        )
    )
    db_session.commit()

    p.put_rejected_transfer_through_transfer_attempts(
        coordinator_id=collector_id,
        coordinator_request_id=7,
        status_code=status[0],
        debtor_id=666,
        creditor_id=123,
        transfers_healthy_max_commit_delay=timedelta(hours=3),
    )
    assert len(PrepareTransferSignal.query.all()) == 0
    assert len(FinalizeTransferSignal.query.all()) == 0
    ta = TransferAttempt.query.one()
    _assert_attempt_status(ta, status, collector_id)


def test_transfer_attempt_trigger_after_recipient_is_unreachable(
        db_session,
        collector_id,
        current_ts,
):
    db_session.add(
        TransferAttempt(
            collector_id=collector_id,
            turn_id=1,
            debtor_id=666,
            creditor_id=123,
            is_dispatching=True,
            nominal_amount=1000.5,
            collection_started_at=current_ts - timedelta(hours=3),
            recipient="account123",
            recipient_version=1,
            attempted_at=current_ts,
            amount=1000,
            coordinator_request_id=7,
            final_interest_rate_ts=current_ts,
            failure_code=TransferAttempt.RECIPIENT_IS_UNREACHABLE,
            backoff_counter=0,
        )
    )
    db_session.commit()

    p.process_trigger_transfer_signal(
        collector_id=collector_id,
        turn_id=1,
        debtor_id=666,
        creditor_id=123,
        is_dispatching=True,
        transfers_healthy_max_commit_delay=timedelta(hours=3),
        transfers_amount_cut=1e-8,
    )
    assert len(PrepareTransferSignal.query.all()) == 0
    assert len(FinalizeTransferSignal.query.all()) == 0

    ta = TransferAttempt.query.one()
    assert ta.failure_code == TransferAttempt.RECIPIENT_IS_UNREACHABLE
    assert ta.fatal_error is None
    assert ta.backoff_counter > 0
    assert ta.rescheduled_for is not None

    airs = AccountIdRequestSignal.query.one()
    assert airs.collector_id == collector_id
    assert airs.turn_id == 1
    assert airs.debtor_id == 666
    assert airs.creditor_id == 123
    assert airs.is_dispatching is True

    p.process_account_id_response_signal(
        collector_id=collector_id,
        turn_id=1,
        debtor_id=666,
        creditor_id=123,
        is_dispatching=True,
        account_id="account123new",
        account_id_version=2,
        transfers_healthy_max_commit_delay=timedelta(hours=3),
        transfers_amount_cut=1e-8,
    )
    ta = TransferAttempt.query.one()
    assert ta.recipient == "account123new"
    assert ta.recipient_version == 2
    assert ta.failure_code == TransferAttempt.UNSPECIFIED_FAILURE
    assert ta.fatal_error is None
    assert ta.rescheduled_for is not None


def _assert_attempt_status(ta, status, collector_id):
    assert ta.failure_code == status[1]
    assert ta.fatal_error == (
        None
        if status[0] != "FATAL_ERROR"
        else status[0]
    )
    assert ta.backoff_counter == (
        100
        if status[0] in ["NEWER_INTEREST_RATE", "FATAL_ERROR"]
        else 101
    )
    assert (ta.rescheduled_for is None) is (
        False
        if status[0] != "FATAL_ERROR"
        else True
    )
    if status[0] == "RECIPIENT_IS_UNREACHABLE":
        airs = AccountIdRequestSignal.query.one()
        assert airs.collector_id == collector_id
        assert airs.turn_id == 1
        assert airs.debtor_id == 666
        assert airs.creditor_id == 123
        assert airs.is_dispatching is True
    else:
        assert len(AccountIdRequestSignal.query.all()) == 0


def test_process_start_sending_signal(db_session, wt_3_10, current_ts):
    turn_id = wt_3_10.turn_id

    db_session.add(
        DispatchingStatus(
            collector_id=999,
            turn_id=turn_id,
            debtor_id=666,
            amount_to_collect=10000,
            amount_to_send=3000,
            amount_to_receive=0,
            number_to_receive=0,
            amount_to_dispatch=7000,
            started_sending=False,
            awaiting_signal_flag=True,
        )
    )
    db_session.add(
        WorkerCollecting(
            collector_id=999,
            turn_id=turn_id,
            debtor_id=666,
            creditor_id=123,
            amount=8000,
            collected=True,
            purge_after=current_ts,
        )
    )
    db_session.add(
        WorkerCollecting(
            collector_id=999,
            turn_id=turn_id,
            debtor_id=666,
            creditor_id=124,
            amount=2000,
            collected=True,
            purge_after=current_ts,
        )
    )
    db_session.add(
        WorkerSending(
            from_collector_id=999,
            turn_id=turn_id,
            debtor_id=666,
            to_collector_id=888,
            amount=3000,
            purge_after=current_ts,
        )
    )
    db_session.commit()

    p.process_start_sending_signal(
        collector_id=999,
        turn_id=turn_id,
        debtor_id=666,
    )
    assert len(WorkerCollecting.query.all()) == 0
    assert len(WorkerSending.query.all()) == 1

    ds = DispatchingStatus.query.one()
    assert ds.started_sending is True
    assert ds.awaiting_signal_flag is False
    assert ds.total_collected_amount == 10000
    assert ds.started_dispatching is False

    ta = TransferAttempt.query.one()
    assert ta.collector_id == 999
    assert ta.turn_id == turn_id
    assert ta.debtor_id == 666
    assert ta.creditor_id == 888
    assert ta.is_dispatching is False
    assert ta.nominal_amount == 3000.0
    assert ta.collection_started_at == wt_3_10.collection_started_at
    assert ta.recipient == ""
    assert ta.recipient_version == MIN_INT64
    assert ta.rescheduled_for is None
    assert ta.attempted_at is None
    assert ta.backoff_counter == 0

    airs = AccountIdRequestSignal.query.one()
    assert airs.collector_id == 999
    assert airs.turn_id == turn_id
    assert airs.debtor_id == 666
    assert airs.creditor_id == 888
    assert airs.is_dispatching is False


def test_process_start_dispatching_signal(db_session, wt_3_10, current_ts):
    turn_id = wt_3_10.turn_id

    db_session.add(
        DispatchingStatus(
            collector_id=999,
            turn_id=turn_id,
            debtor_id=666,
            amount_to_collect=10000,
            amount_to_send=3000,
            amount_to_receive=5000,
            number_to_receive=2,
            amount_to_dispatch=12000,
            total_collected_amount=10000,
            started_sending=True,
            all_sent=True,
            started_dispatching=False,
            awaiting_signal_flag=True,
        )
    )
    db_session.add(
        WorkerReceiving(
            to_collector_id=999,
            turn_id=turn_id,
            debtor_id=666,
            from_collector_id=888,
            expected_amount=1000,
            received_amount=999,
            purge_after=current_ts,
        )
    )
    db_session.add(
        WorkerReceiving(
            to_collector_id=999,
            turn_id=turn_id,
            debtor_id=666,
            from_collector_id=777,
            expected_amount=4000,
            received_amount=3996,
            purge_after=current_ts,
        )
    )
    db_session.add(
        WorkerDispatching(
            collector_id=999,
            turn_id=turn_id,
            debtor_id=666,
            creditor_id=123,
            amount=12000,
            purge_after=current_ts,
        )
    )
    db_session.commit()

    p.process_start_dispatching_signal(
        collector_id=999,
        turn_id=turn_id,
        debtor_id=666,
    )
    assert len(WorkerReceiving.query.all()) == 0
    assert len(WorkerDispatching.query.all()) == 1

    ds = DispatchingStatus.query.one()
    assert ds.started_sending is True
    assert ds.all_sent is True
    assert ds.started_dispatching is True
    assert ds.awaiting_signal_flag is False
    assert ds.total_received_amount == 4995

    ta = TransferAttempt.query.one()
    assert ta.collector_id == 999
    assert ta.turn_id == turn_id
    assert ta.debtor_id == 666
    assert ta.creditor_id == 123
    assert ta.is_dispatching is True
    assert ta.nominal_amount == 11995.0
    assert ta.collection_started_at == wt_3_10.collection_started_at
    assert ta.recipient == ""
    assert ta.recipient_version == MIN_INT64
    assert ta.rescheduled_for is None
    assert ta.attempted_at is None
    assert ta.backoff_counter == 0

    airs = AccountIdRequestSignal.query.one()
    assert airs.collector_id == 999
    assert airs.turn_id == turn_id
    assert airs.debtor_id == 666
    assert airs.creditor_id == 123
    assert airs.is_dispatching is True
