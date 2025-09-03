import pytest
import sqlalchemy
from datetime import date, datetime, timezone, timedelta
from unittest.mock import Mock
from flask import current_app
from swpt_pythonlib.utils import ShardingRealm
from swpt_trade.utils import calc_hash
from swpt_trade.extensions import db
from swpt_trade import models as m

D_ID = -1
C_ID = 4294967296


def test_consume_messages(app):
    runner = app.test_cli_runner()
    result = runner.invoke(
        args=["swpt_trade", "consume_messages", "--url=INVALID"]
    )
    assert result.exit_code == 1


def test_flush_messages(mocker, app, db_session):
    send_signalbus_message = Mock()
    mocker.patch(
        "swpt_trade.models.FinalizeTransferSignal.send_signalbus_message",
        new_callable=send_signalbus_message,
    )
    fts = m.FinalizeTransferSignal(
        creditor_id=0x0000010000000000,
        debtor_id=D_ID,
        transfer_id=666,
        coordinator_id=C_ID,
        coordinator_request_id=777,
        committed_amount=0,
        transfer_note_format="",
        transfer_note="",
    )
    db.session.add(fts)
    db.session.commit()
    assert len(m.FinalizeTransferSignal.query.all()) == 1
    db.session.commit()

    runner = app.test_cli_runner()
    result = runner.invoke(
        args=[
            "swpt_trade",
            "flush_messages",
            "FinalizeTransferSignal",
            "--wait",
            "0.1",
            "--quit-early",
        ]
    )
    assert result.exit_code != 2
    send_signalbus_message.assert_called_once()
    assert len(m.FinalizeTransferSignal.query.all()) == 0


def test_roll_turns(app, db_session):
    current_ts = datetime.now(tz=timezone.utc)
    midnight = current_ts.replace(hour=0, minute=0, second=0, microsecond=0)
    offset_seconds = (current_ts - midnight).total_seconds()
    runner = app.test_cli_runner()

    def invoke():
        return runner.invoke(
            args=[
                "swpt_trade",
                "roll_turns",
                "--period=24h",
                f"--period-offset={offset_seconds}",
                "--quit-early",
            ]
        )

    result = invoke()
    assert result.exit_code == 0
    turns = m.Turn.query.all()
    assert len(turns) == 1
    assert turns[0].phase == 1

    result = invoke()
    assert result.exit_code == 0
    turns = m.Turn.query.all()
    assert len(turns) == 1
    assert turns[0].phase == 2

    result = invoke()
    assert result.exit_code == 0
    turns = m.Turn.query.all()
    assert len(turns) == 1
    assert turns[0].phase == 3

    result = invoke()
    assert result.exit_code == 0
    turns = m.Turn.query.all()
    assert len(turns) == 1
    assert turns[0].phase == 4

    result = invoke()
    assert result.exit_code == 0
    turns = m.Turn.query.all()
    assert len(turns) == 1
    assert turns[0].phase == 4


def test_fetch_debtor_infos(mocker, app, db_session):
    from swpt_trade.fetch_debtor_infos import FetchResult

    def make_https_requests(fetches, **kwargs):
        return [
            FetchResult(
                fetch=f,
                document=m.DebtorInfoDocument(
                    debtor_info_locator=f.iri,
                    debtor_id=f.debtor_id,
                    peg_debtor_info_locator="https://example.com/777",
                    peg_debtor_id=777,
                    peg_exchange_rate=2.0,
                ),
                store_document=f.is_locator_fetch,
            ) for f in fetches
        ]

    mocker.patch(
        "swpt_trade.fetch_debtor_infos._make_https_requests",
        new=make_https_requests,
    )

    dif = m.DebtorInfoFetch(
        iri="https://example.com/666",
        debtor_id=666,
        is_locator_fetch=True,
        is_discovery_fetch=True,
        forced_iri=None,
    )
    db.session.add(dif)
    db.session.commit()
    assert len(m.DebtorInfoFetch.query.all()) == 1
    assert len(m.FetchDebtorInfoSignal.query.all()) == 0
    db.session.commit()

    runner = app.test_cli_runner()

    def invoke():
        return runner.invoke(
            args=[
                "swpt_trade",
                "fetch_debtor_infos",
                "--timeout", "0.1",
                "--quit-early",
            ]
        )

    result = invoke()
    assert result.exit_code != 2

    assert len(m.DebtorInfoFetch.query.all()) == 0
    assert len(m.DiscoverDebtorSignal.query.all()) == 0
    assert len(m.DebtorLocatorClaim.query.all()) == 0
    assert len(m.DebtorInfoDocument.query.all()) == 0

    fetch_signals = m.FetchDebtorInfoSignal.query.all()
    assert len(fetch_signals) == 1
    assert fetch_signals[0].iri == "https://example.com/777"
    assert fetch_signals[0].debtor_id == 777
    assert fetch_signals[0].is_locator_fetch is True
    assert fetch_signals[0].is_discovery_fetch is False
    assert fetch_signals[0].recursion_level == 1

    stored_signals = m.StoreDocumentSignal.query.all()
    assert len(stored_signals) == 1
    assert stored_signals[0].debtor_info_locator == "https://example.com/666"
    assert stored_signals[0].debtor_id == 666
    assert stored_signals[0].peg_debtor_info_locator == (
        "https://example.com/777"
    )
    assert stored_signals[0].peg_debtor_id == 777
    assert stored_signals[0].peg_exchange_rate == 2.0
    assert stored_signals[0].will_not_change_until is None

    confirmations = m.ConfirmDebtorSignal.query.all()
    assert len(confirmations) == 1
    assert confirmations[0].debtor_id == 666
    assert confirmations[0].debtor_info_locator == "https://example.com/666"


def test_trigger_transfers(app, db_session, current_ts):
    ta1 = m.TransferAttempt(
            collector_id=666,
            turn_id=1,
            debtor_id=222,
            creditor_id=123,
            is_dispatching=True,
            nominal_amount=1000.5,
            collection_started_at=current_ts,
            recipient="account123",
            recipient_version=1,
            rescheduled_for=current_ts - timedelta(minutes=10),
    )
    ta2 = m.TransferAttempt(
            collector_id=666,
            turn_id=1,
            debtor_id=333,
            creditor_id=123,
            is_dispatching=True,
            nominal_amount=1000.5,
            collection_started_at=current_ts,
            recipient="account123",
            recipient_version=1,
            rescheduled_for=None,
    )
    ta3 = m.TransferAttempt(
            collector_id=666,
            turn_id=1,
            debtor_id=444,
            creditor_id=123,
            is_dispatching=True,
            nominal_amount=1000.5,
            collection_started_at=current_ts,
            recipient="account123",
            recipient_version=1,
            rescheduled_for=current_ts + timedelta(days=10),
    )
    db.session.add(ta1)
    db.session.add(ta2)
    db.session.add(ta3)
    db.session.commit()

    runner = app.test_cli_runner()
    result = runner.invoke(
        args=[
            "swpt_trade",
            "trigger_transfers",
            "--quit-early",
        ]
    )
    assert result.exit_code != 2
    attempts = m.TransferAttempt.query.all()
    attempts.sort(key=lambda x: x.debtor_id)
    assert len(attempts) == 3
    assert attempts[0].debtor_id == 222
    assert attempts[0].rescheduled_for is None
    assert attempts[1].debtor_id == 333
    assert attempts[1].rescheduled_for is None
    assert attempts[2].debtor_id == 444
    assert attempts[2].rescheduled_for is not None

    tts = m.TriggerTransferSignal.query.all()
    assert len(tts) == 1
    assert tts[0].collector_id == 666
    assert tts[0].turn_id == 1
    assert tts[0].debtor_id == 222
    assert tts[0].creditor_id == 123
    assert tts[0].is_dispatching is True


def test_roll_transfers(app, db_session, current_ts):
    db.session.add(
        m.DispatchingStatus(
            collector_id=999,
            turn_id=1,
            debtor_id=666,
            amount_to_collect=1000,
            total_collected_amount=1000,
            amount_to_send=0,
            amount_to_receive=0,
            number_to_receive=0,
            total_received_amount=0,
            all_received=True,
            amount_to_dispatch=1000,
            awaiting_signal_flag=False,
            started_sending=True,
            all_sent=True,
            started_dispatching=True,
        )
    )
    db.session.commit()
    assert len(m.DispatchingStatus.query.all()) == 1

    runner = app.test_cli_runner()
    result = runner.invoke(
        args=[
            "swpt_trade",
            "roll_transfers",
            "--quit-early",
        ]
    )
    assert result.exit_code != 2
    assert len(m.DispatchingStatus.query.all()) == 0


def test_roll_delayed_account_transfers(app, db_session, current_ts):
    wt1 = m.WorkerTurn(
        turn_id=1,
        started_at=current_ts,
        base_debtor_info_locator="https://example.com/666",
        base_debtor_id=666,
        max_distance_to_base=10,
        min_trade_amount=10000,
        phase=3,
        phase_deadline=None,
        collection_started_at=current_ts - timedelta(days=1),
        collection_deadline=current_ts + timedelta(days=200),
        worker_turn_subphase=0,
    )
    wt2 = m.WorkerTurn(
        turn_id=2,
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
    db.session.add(wt1)
    db.session.add(wt2)
    db.session.add(
        m.DelayedAccountTransfer(
            turn_id=wt1.turn_id,
            creditor_id=666,
            debtor_id=123,
            creation_date=date(2021, 6, 18),
            transfer_number=2345,
            coordinator_type="agent",
            committed_at=current_ts,
            acquired_amount=1000,
            transfer_note_format="",
            transfer_note="test note",
            principal=10000,
            previous_transfer_number=2344,
            sender="667",
            recipient="666",
            ts=current_ts,
        )
    )
    db.session.add(
        m.DelayedAccountTransfer(
            turn_id=wt2.turn_id,
            creditor_id=667,
            debtor_id=123,
            creation_date=date(2021, 6, 18),
            transfer_number=3456,
            coordinator_type="agent",
            committed_at=current_ts,
            acquired_amount=-1000,
            transfer_note_format="",
            transfer_note="test note",
            principal=5000,
            previous_transfer_number=3455,
            sender="667",
            recipient="666",
            ts=current_ts,
        )
    )
    db.session.commit()
    assert len(m.DelayedAccountTransfer.query.all()) == 2
    assert len(m.ReplayedAccountTransferSignal.query.all()) == 0

    runner = app.test_cli_runner()
    result = runner.invoke(
        args=[
            "swpt_trade",
            "roll_delayed_account_transfers",
            "--quit-early",
        ]
    )
    assert result.exit_code != 2
    dat = m.DelayedAccountTransfer.query.one()
    assert dat.turn_id == 1
    rats = m.ReplayedAccountTransferSignal.query.one()
    assert rats.creditor_id == 667
    assert rats.debtor_id == 123
    assert rats.creation_date == date(2021, 6, 18)
    assert rats.transfer_number == 3456
    assert rats.coordinator_type == "agent"
    assert rats.committed_at == current_ts
    assert rats.acquired_amount == -1000
    assert rats.transfer_note_format == ""
    assert rats.transfer_note == "test note"
    assert rats.principal == 5000
    assert rats.previous_transfer_number == 3455
    assert rats.sender == "667"
    assert rats.recipient == "666"
    assert rats.ts == current_ts


def test_delete_parent_documents(app, db_session, restore_sharding_realm):
    app.config["SHARDING_REALM"] = ShardingRealm("0.#")
    app.config["DELETE_PARENT_SHARD_RECORDS"] = True

    d1 = m.DebtorInfoDocument(
        debtor_info_locator="https://example.com/666",
        debtor_id=666,
    )
    d2 = m.DebtorInfoDocument(
        debtor_info_locator="https://example.com/888",
        debtor_id=888,
    )
    d3 = m.DebtorInfoDocument(
        debtor_info_locator="https://example.com/999",
        debtor_id=999,
    )
    db.session.add(d1)
    db.session.add(d2)
    db.session.add(d3)
    db.session.commit()

    with db.engine.connect() as conn:
        conn.execute(sqlalchemy.text("ANALYZE debtor_info_document"))

    assert len(m.DebtorInfoDocument.query.all()) == 3
    runner = app.test_cli_runner()
    result = runner.invoke(
        args=[
            "swpt_trade",
            "scan_debtor_info_documents",
            "--days",
            "0.000001",
            "--quit-early",
        ]
    )
    assert result.exit_code == 0
    documents = m.DebtorInfoDocument.query.all()
    assert len(documents) == 1
    assert documents[0].debtor_info_locator == "https://example.com/666"


def test_delete_stale_documents(app, db_session, current_ts):
    d1 = m.DebtorInfoDocument(
        debtor_info_locator="https://example.com/666",
        debtor_id=666,
        fetched_at=current_ts - timedelta(days=10000),
    )
    d2 = m.DebtorInfoDocument(
        debtor_info_locator="https://example.com/888",
        debtor_id=888,
    )
    db.session.add(d1)
    db.session.add(d2)
    db.session.commit()

    with db.engine.connect() as conn:
        conn.execute(sqlalchemy.text("ANALYZE debtor_info_document"))

    assert len(m.DebtorInfoDocument.query.all()) == 2
    runner = app.test_cli_runner()
    result = runner.invoke(
        args=[
            "swpt_trade",
            "scan_debtor_info_documents",
            "--days",
            "0.000001",
            "--quit-early",
        ]
    )
    assert result.exit_code == 0
    documents = m.DebtorInfoDocument.query.all()
    assert len(documents) == 1
    assert documents[0].debtor_info_locator == "https://example.com/888"


def test_delete_parent_claims(app, db_session, restore_sharding_realm):
    app.config["SHARDING_REALM"] = ShardingRealm("0.#")
    app.config["DELETE_PARENT_SHARD_RECORDS"] = True

    c1 = m.DebtorLocatorClaim(debtor_id=666)
    c2 = m.DebtorLocatorClaim(debtor_id=777)
    c3 = m.DebtorLocatorClaim(debtor_id=888)
    db.session.add(c1)
    db.session.add(c2)
    db.session.add(c3)
    db.session.commit()

    with db.engine.connect() as conn:
        conn.execute(sqlalchemy.text("ANALYZE debtor_locator_claim"))

    assert len(m.DebtorLocatorClaim.query.all()) == 3
    runner = app.test_cli_runner()
    result = runner.invoke(
        args=[
            "swpt_trade",
            "scan_debtor_locator_claims",
            "--days",
            "0.000001",
            "--quit-early",
        ]
    )
    assert result.exit_code == 0
    claims = m.DebtorLocatorClaim.query.all()
    assert len(claims) == 1
    assert claims[0].debtor_id == 888


def test_delete_stale_claims(app, db_session, current_ts):
    c1 = m.DebtorLocatorClaim(debtor_id=666)
    c2 = m.DebtorLocatorClaim(
        debtor_id=777,
        latest_discovery_fetch_at=current_ts - timedelta(days=10),
    )
    c3 = m.DebtorLocatorClaim(
        debtor_id=888,
        debtor_info_locator="https://example.com/888",
        latest_locator_fetch_at=current_ts - timedelta(days=100),
    )
    db.session.add(c1)
    db.session.add(c2)
    db.session.add(c3)
    db.session.commit()

    with db.engine.connect() as conn:
        conn.execute(sqlalchemy.text("ANALYZE debtor_info_document"))

    assert len(m.DebtorLocatorClaim.query.all()) == 3
    runner = app.test_cli_runner()
    result = runner.invoke(
        args=[
            "swpt_trade",
            "scan_debtor_locator_claims",
            "--days",
            "0.000001",
            "--quit-early",
        ]
    )
    assert result.exit_code == 0
    claims = m.DebtorLocatorClaim.query.all()
    assert len(claims) == 1
    assert claims[0].debtor_id == 666


def test_delete_parent_trading_policies(
        app,
        db_session,
        restore_sharding_realm,
):
    app.config["SHARDING_REALM"] = ShardingRealm("0.#")
    app.config["DELETE_PARENT_SHARD_RECORDS"] = True

    tp1 = m.TradingPolicy(creditor_id=666, debtor_id=1, account_id='row1')
    tp2 = m.TradingPolicy(creditor_id=777, debtor_id=2, account_id='row2')
    tp3 = m.TradingPolicy(creditor_id=888, debtor_id=2, account_id='row3')
    db.session.add(tp1)
    db.session.add(tp2)
    db.session.add(tp3)
    db.session.commit()

    with db.engine.connect() as conn:
        conn.execute(sqlalchemy.text("ANALYZE trading_policy"))

    assert len(m.TradingPolicy.query.all()) == 3
    runner = app.test_cli_runner()
    result = runner.invoke(
        args=[
            "swpt_trade",
            "scan_trading_policies",
            "--days",
            "0.000001",
            "--quit-early",
        ]
    )
    assert result.exit_code == 0
    tps = m.TradingPolicy.query.all()
    assert len(tps) == 1
    assert tps[0].creditor_id == 888


def test_delete_useless_trading_policies(app, db_session, current_ts):
    tp1 = m.TradingPolicy(creditor_id=666, debtor_id=1)
    tp2 = m.TradingPolicy(creditor_id=777, debtor_id=2, account_id='test')
    tp3 = m.TradingPolicy(creditor_id=888, debtor_id=2)
    db.session.add(tp1)
    db.session.add(tp2)
    db.session.add(tp3)
    db.session.commit()

    with db.engine.connect() as conn:
        conn.execute(sqlalchemy.text("ANALYZE debtor_info_document"))

    assert len(m.TradingPolicy.query.all()) == 3
    runner = app.test_cli_runner()
    result = runner.invoke(
        args=[
            "swpt_trade",
            "scan_trading_policies",
            "--days",
            "0.000001",
            "--quit-early",
        ]
    )
    assert result.exit_code == 0
    tps = m.TradingPolicy.query.all()
    assert len(tps) == 1
    assert tps[0].creditor_id == 777


def test_delete_parent_recently_needed_collectors(
        app,
        db_session,
        restore_sharding_realm
):
    app.config["SHARDING_REALM"] = ShardingRealm("0.#")
    app.config["DELETE_PARENT_SHARD_RECORDS"] = True

    c1 = m.RecentlyNeededCollector(debtor_id=666)
    c2 = m.RecentlyNeededCollector(debtor_id=777)
    c3 = m.RecentlyNeededCollector(debtor_id=888)
    db.session.add(c1)
    db.session.add(c2)
    db.session.add(c3)
    db.session.commit()

    with db.engine.connect() as conn:
        conn.execute(sqlalchemy.text("ANALYZE recently_needed_collector"))

    assert len(m.RecentlyNeededCollector.query.all()) == 3
    runner = app.test_cli_runner()
    result = runner.invoke(
        args=[
            "swpt_trade",
            "scan_recently_needed_collectors",
            "--days",
            "0.000001",
            "--quit-early",
        ]
    )
    assert result.exit_code == 0
    records = m.RecentlyNeededCollector.query.all()
    assert len(records) == 1
    assert records[0].debtor_id == 888


def test_delete_stale_recently_needed_collectors(app, db_session, current_ts):
    c1 = m.RecentlyNeededCollector(debtor_id=666)
    c2 = m.RecentlyNeededCollector(
        debtor_id=777,
        needed_at=current_ts - timedelta(days=100),
    )
    c3 = m.RecentlyNeededCollector(
        debtor_id=888,
        needed_at=current_ts - timedelta(days=1000),
    )
    db.session.add(c1)
    db.session.add(c2)
    db.session.add(c3)
    db.session.commit()

    with db.engine.connect() as conn:
        conn.execute(sqlalchemy.text("ANALYZE recently_needed_collector"))

    assert len(m.RecentlyNeededCollector.query.all()) == 3
    runner = app.test_cli_runner()
    result = runner.invoke(
        args=[
            "swpt_trade",
            "scan_recently_needed_collectors",
            "--days",
            "0.000001",
            "--quit-early",
        ]
    )
    assert result.exit_code == 0
    claims = m.RecentlyNeededCollector.query.all()
    assert len(claims) == 1
    assert claims[0].debtor_id == 666


def test_delete_parent_account_locks(
        app,
        db_session,
        current_ts,
        restore_sharding_realm
):
    app.config["SHARDING_REALM"] = ShardingRealm("0.#")
    app.config["DELETE_PARENT_SHARD_RECORDS"] = True

    wt = m.WorkerTurn(
        turn_id=1,
        started_at=current_ts,
        base_debtor_info_locator="https://example.com/666",
        base_debtor_id=666,
        max_distance_to_base=10,
        min_trade_amount=10000,
        phase=2,
        phase_deadline=current_ts + timedelta(hours=10),
        collection_started_at=None,
        collection_deadline=current_ts + timedelta(days=30),
        worker_turn_subphase=10,
    )
    db.session.add(wt)
    db.session.flush()

    al1 = m.AccountLock(
        creditor_id=666,
        debtor_id=123,
        turn_id=1,
        collector_id=789,
        amount=0,
    )
    al2 = m.AccountLock(
        creditor_id=777,
        debtor_id=123,
        turn_id=1,
        collector_id=789,
        amount=0,
    )
    al3 = m.AccountLock(
        creditor_id=888,
        debtor_id=124,
        turn_id=1,
        collector_id=789,
        amount=0,
    )
    db.session.add(al1)
    db.session.add(al2)
    db.session.add(al3)
    db.session.commit()

    with db.engine.connect() as conn:
        conn.execute(sqlalchemy.text("ANALYZE account_lock"))

    assert len(m.AccountLock.query.all()) == 3
    runner = app.test_cli_runner()
    result = runner.invoke(
        args=[
            "swpt_trade",
            "scan_account_locks",
            "--days",
            "0.000001",
            "--quit-early",
        ]
    )
    assert result.exit_code == 0
    records = m.AccountLock.query.all()
    assert len(records) == 1
    assert records[0].creditor_id == 888


def test_delete_stale_account_locks(app, db_session, current_ts):
    wt = m.WorkerTurn(
        turn_id=1,
        started_at=current_ts,
        base_debtor_info_locator="https://example.com/666",
        base_debtor_id=666,
        max_distance_to_base=10,
        min_trade_amount=10000,
        phase=2,
        phase_deadline=current_ts + timedelta(hours=10),
        collection_started_at=None,
        collection_deadline=current_ts + timedelta(days=30),
        worker_turn_subphase=10,
    )
    db.session.add(wt)
    db.session.flush()

    al1 = m.AccountLock(
        creditor_id=666,
        debtor_id=123,
        turn_id=1,
        collector_id=789,
        amount=0,
        initiated_at=current_ts - timedelta(days=400),
    )
    al2 = m.AccountLock(
        creditor_id=777,
        debtor_id=123,
        turn_id=1,
        collector_id=789,
        amount=0,
        initiated_at=current_ts - timedelta(days=300),
        transfer_id=1234,
        released_at=current_ts,
        finalized_at=current_ts,
        account_creation_date=date(2024, 1, 1),
        account_last_transfer_number=789,
    )
    db.session.add(al1)
    db.session.add(al2)
    db.session.commit()

    with db.engine.connect() as conn:
        conn.execute(sqlalchemy.text("ANALYZE account_lock"))

    assert len(m.AccountLock.query.all()) == 2
    runner = app.test_cli_runner()
    result = runner.invoke(
        args=[
            "swpt_trade",
            "scan_account_locks",
            "--days",
            "0.000001",
            "--quit-early",
        ]
    )
    assert result.exit_code == 0
    records = m.AccountLock.query.all()
    assert len(records) == 1
    assert records[0].creditor_id == 777


def test_delete_parent_worker_accounts(
        app,
        db_session,
        restore_sharding_realm,
):
    app.config["SHARDING_REALM"] = ShardingRealm("0.#")
    app.config["DELETE_PARENT_SHARD_RECORDS"] = True

    params = {
        "creation_date": m.DATE0,
        "last_change_ts": m.TS0,
        "last_change_seqnum": 1,
        "principal": 100,
        "interest": 31.4,
        "interest_rate": 5.0,
        "last_interest_rate_change_ts": m.TS0,
        "config_flags": 0,
        "account_id": "Account123",
        "last_transfer_number": 2,
        "last_transfer_committed_at": m.TS0,
        "demurrage_rate": -50.0,
        "commit_period": 1000000,
        "transfer_note_max_bytes": 500,
        "debtor_info_iri": "https://example.com/666",
    }
    wa1 = m.WorkerAccount(creditor_id=666, debtor_id=1, **params)
    wa2 = m.WorkerAccount(creditor_id=777, debtor_id=2, **params)
    wa3 = m.WorkerAccount(creditor_id=888, debtor_id=2, **params)
    db.session.add(wa1)
    db.session.add(wa2)
    db.session.add(wa3)
    db.session.commit()

    with db.engine.connect() as conn:
        conn.execute(sqlalchemy.text("ANALYZE worker_account"))

    assert len(m.WorkerAccount.query.all()) == 3
    runner = app.test_cli_runner()
    result = runner.invoke(
        args=[
            "swpt_trade",
            "scan_worker_accounts",
            "--days",
            "0.000001",
            "--quit-early",
        ]
    )
    assert result.exit_code == 0
    was = m.WorkerAccount.query.all()
    assert len(was) == 1
    assert was[0].creditor_id == 888


def test_delete_dead_worker_accounts(app, db_session, current_ts):
    params = {
        "debtor_id": 666,
        "creation_date": m.DATE0,
        "last_change_ts": m.TS0,
        "last_change_seqnum": 1,
        "principal": 100,
        "interest": 31.4,
        "interest_rate": 5.0,
        "last_interest_rate_change_ts": m.TS0,
        "config_flags": 0,
        "account_id": "Account123",
        "last_transfer_number": 2,
        "last_transfer_committed_at": m.TS0,
        "demurrage_rate": -50.0,
        "commit_period": 1000000,
        "transfer_note_max_bytes": 500,
        "debtor_info_iri": "https://example.com/666",
    }
    wa1 = m.WorkerAccount(creditor_id=666, **params)
    wa2 = m.WorkerAccount(
        creditor_id=777,
        last_heartbeat_ts=current_ts - timedelta(days=100000),
        **params
    )
    db.session.add(wa1)
    db.session.add(wa2)
    db.session.commit()

    with db.engine.connect() as conn:
        conn.execute(sqlalchemy.text("ANALYZE worker_account"))

    assert len(m.WorkerAccount.query.all()) == 2
    runner = app.test_cli_runner()
    result = runner.invoke(
        args=[
            "swpt_trade",
            "scan_worker_accounts",
            "--days",
            "0.000001",
            "--quit-early",
        ]
    )
    assert result.exit_code == 0
    was = m.WorkerAccount.query.all()
    assert len(was) == 1
    assert was[0].creditor_id == 666


def test_delete_parent_interest_rate_changes(
        app,
        db_session,
        current_ts,
        restore_sharding_realm,
):
    app.config["SHARDING_REALM"] = ShardingRealm("0.#")
    app.config["DELETE_PARENT_SHARD_RECORDS"] = True

    c1 = m.InterestRateChange(
        creditor_id=666, debtor_id=1, change_ts=current_ts, interest_rate=10.0
    )
    c2 = m.InterestRateChange(
        creditor_id=777, debtor_id=2, change_ts=current_ts, interest_rate=5.0
    )
    c3 = m.InterestRateChange(
        creditor_id=888, debtor_id=2, change_ts=current_ts, interest_rate=0.0
    )
    db.session.add(c1)
    db.session.add(c2)
    db.session.add(c3)
    db.session.commit()

    with db.engine.connect() as conn:
        conn.execute(sqlalchemy.text("ANALYZE interest_rate_change"))

    assert len(m.InterestRateChange.query.all()) == 3
    runner = app.test_cli_runner()
    result = runner.invoke(
        args=[
            "swpt_trade",
            "scan_interest_rate_changes",
            "--days",
            "0.000001",
            "--quit-early",
        ]
    )
    assert result.exit_code == 0
    changes = m.InterestRateChange.query.all()
    assert len(changes) == 1
    assert changes[0].creditor_id == 888


def test_delete_parent_needed_worker_accounts(
        app,
        db_session,
        restore_sharding_realm,
):
    app.config["SHARDING_REALM"] = ShardingRealm("0.#")
    app.config["DELETE_PARENT_SHARD_RECORDS"] = True

    nwa1 = m.NeededWorkerAccount(creditor_id=666, debtor_id=1)
    nwa2 = m.NeededWorkerAccount(creditor_id=777, debtor_id=2)
    nwa3 = m.NeededWorkerAccount(creditor_id=888, debtor_id=2)
    db.session.add(nwa1)
    db.session.add(nwa2)
    db.session.add(nwa3)
    db.session.commit()

    with db.engine.connect() as conn:
        conn.execute(sqlalchemy.text("ANALYZE needed_worker_account"))

    assert len(m.NeededWorkerAccount.query.all()) == 3
    runner = app.test_cli_runner()
    result = runner.invoke(
        args=[
            "swpt_trade",
            "scan_needed_worker_accounts",
            "--days",
            "0.000001",
            "--quit-early",
        ]
    )
    assert result.exit_code == 0
    nwas = m.NeededWorkerAccount.query.all()
    assert len(nwas) == 1
    assert nwas[0].creditor_id == 888


def test_delete_parent_creditor_participations(
        app,
        db_session,
        restore_sharding_realm,
):
    app.config["SHARDING_REALM"] = ShardingRealm("0.#")
    app.config["DELETE_PARENT_SHARD_RECORDS"] = True

    cp1 = m.CreditorParticipation(
        creditor_id=666, debtor_id=1, turn_id=1, amount=100, collector_id=1
    )
    cp2 = m.CreditorParticipation(
        creditor_id=777, debtor_id=1, turn_id=1, amount=100, collector_id=1
    )
    cp3 = m.CreditorParticipation(
        creditor_id=888, debtor_id=1, turn_id=1, amount=100, collector_id=1
    )
    db.session.add(cp1)
    db.session.add(cp2)
    db.session.add(cp3)
    db.session.commit()

    with db.engine.connect() as conn:
        conn.execute(sqlalchemy.text("ANALYZE creditor_participation"))

    assert len(m.CreditorParticipation.query.all()) == 3
    runner = app.test_cli_runner()
    result = runner.invoke(
        args=[
            "swpt_trade",
            "scan_creditor_participations",
            "--days",
            "0.000001",
            "--quit-early",
        ]
    )
    assert result.exit_code == 0
    cps = m.CreditorParticipation.query.all()
    assert len(cps) == 1
    assert cps[0].creditor_id == 888


def test_delete_dispatching_statuses(
        app,
        db_session,
        restore_sharding_realm,
        current_ts,
):
    app.config["SHARDING_REALM"] = ShardingRealm("0.#")
    app.config["DELETE_PARENT_SHARD_RECORDS"] = True

    ds1 = m.DispatchingStatus(
        collector_id=666,
        turn_id=1,
        debtor_id=1,
        amount_to_collect=0,
        amount_to_receive=0,
        number_to_receive=0,
        amount_to_send=0,
        amount_to_dispatch=0,
    )
    ds2 = m.DispatchingStatus(
        collector_id=777,
        turn_id=1,
        debtor_id=1,
        amount_to_collect=0,
        amount_to_receive=0,
        number_to_receive=0,
        amount_to_send=0,
        amount_to_dispatch=0,
    )
    ds3 = m.DispatchingStatus(
        collector_id=888,
        turn_id=1,
        debtor_id=1,
        amount_to_collect=0,
        amount_to_receive=0,
        number_to_receive=0,
        amount_to_send=0,
        amount_to_dispatch=0,
    )
    db.session.add(ds1)
    db.session.add(ds2)
    db.session.add(ds3)
    db.session.commit()

    with db.engine.connect() as conn:
        conn.execute(sqlalchemy.text("ANALYZE dispatching_status"))

    assert len(m.DispatchingStatus.query.all()) == 3
    runner = app.test_cli_runner()
    result = runner.invoke(
        args=[
            "swpt_trade",
            "scan_dispatching_statuses",
            "--days",
            "0.000001",
            "--quit-early",
        ]
    )
    assert result.exit_code == 0
    dss = m.DispatchingStatus.query.all()
    assert len(dss) == 1
    assert dss[0].collector_id == 888


def test_delete_worker_collectings(
        app,
        db_session,
        restore_sharding_realm,
        current_ts,
):
    app.config["SHARDING_REALM"] = ShardingRealm("0.#")
    app.config["DELETE_PARENT_SHARD_RECORDS"] = True

    wc1 = m.WorkerCollecting(
        collector_id=666,
        turn_id=1,
        debtor_id=1,
        creditor_id=1,
        amount=1000,
        purge_after=current_ts + timedelta(days=1000),
    )
    wc2 = m.WorkerCollecting(
        collector_id=777,
        turn_id=1,
        debtor_id=1,
        creditor_id=1,
        amount=1000,
        purge_after=current_ts + timedelta(days=1000),
    )
    wc3 = m.WorkerCollecting(
        collector_id=888,
        turn_id=1,
        debtor_id=1,
        creditor_id=1,
        amount=1000,
        purge_after=current_ts + timedelta(days=1000),
    )
    wc4 = m.WorkerCollecting(
        collector_id=999,
        turn_id=1,
        debtor_id=1,
        creditor_id=1,
        amount=1000,
        purge_after=current_ts - timedelta(days=1),
    )
    db.session.add(wc1)
    db.session.add(wc2)
    db.session.add(wc3)
    db.session.add(wc4)
    db.session.commit()

    with db.engine.connect() as conn:
        conn.execute(sqlalchemy.text("ANALYZE worker_collecting"))

    assert len(m.WorkerCollecting.query.all()) == 4
    runner = app.test_cli_runner()
    result = runner.invoke(
        args=[
            "swpt_trade",
            "scan_worker_collectings",
            "--days",
            "0.000001",
            "--quit-early",
        ]
    )
    assert result.exit_code == 0
    wcs = m.WorkerCollecting.query.all()
    assert len(wcs) == 1
    assert wcs[0].collector_id == 888


def test_delete_worker_dispatchings(
        app,
        db_session,
        restore_sharding_realm,
        current_ts,
):
    app.config["SHARDING_REALM"] = ShardingRealm("0.#")
    app.config["DELETE_PARENT_SHARD_RECORDS"] = True

    wd1 = m.WorkerDispatching(
        collector_id=666,
        turn_id=1,
        debtor_id=1,
        creditor_id=1,
        amount=1000,
        purge_after=current_ts + timedelta(days=1000),
    )
    wd2 = m.WorkerDispatching(
        collector_id=777,
        turn_id=1,
        debtor_id=1,
        creditor_id=1,
        amount=1000,
        purge_after=current_ts + timedelta(days=1000),
    )
    wd3 = m.WorkerDispatching(
        collector_id=888,
        turn_id=1,
        debtor_id=1,
        creditor_id=1,
        amount=1000,
        purge_after=current_ts + timedelta(days=1000),
    )
    wd4 = m.WorkerDispatching(
        collector_id=999,
        turn_id=1,
        debtor_id=1,
        creditor_id=1,
        amount=1000,
        purge_after=current_ts - timedelta(days=1),
    )
    db.session.add(wd1)
    db.session.add(wd2)
    db.session.add(wd3)
    db.session.add(wd4)
    db.session.commit()

    with db.engine.connect() as conn:
        conn.execute(sqlalchemy.text("ANALYZE worker_dispatching"))

    assert len(m.WorkerDispatching.query.all()) == 4
    runner = app.test_cli_runner()
    result = runner.invoke(
        args=[
            "swpt_trade",
            "scan_worker_dispatchings",
            "--days",
            "0.000001",
            "--quit-early",
        ]
    )
    assert result.exit_code == 0
    wds = m.WorkerDispatching.query.all()
    assert len(wds) == 1
    assert wds[0].collector_id == 888


def test_delete_worker_receivings(
        app,
        db_session,
        restore_sharding_realm,
        current_ts,
):
    app.config["SHARDING_REALM"] = ShardingRealm("0.#")
    app.config["DELETE_PARENT_SHARD_RECORDS"] = True

    wr1 = m.WorkerReceiving(
        to_collector_id=666,
        turn_id=1,
        debtor_id=1,
        from_collector_id=1,
        expected_amount=1000,
        purge_after=current_ts + timedelta(days=1000),
    )
    wr2 = m.WorkerReceiving(
        to_collector_id=777,
        turn_id=1,
        debtor_id=1,
        from_collector_id=1,
        expected_amount=1000,
        purge_after=current_ts + timedelta(days=1000),
    )
    wr3 = m.WorkerReceiving(
        to_collector_id=888,
        turn_id=1,
        debtor_id=1,
        from_collector_id=1,
        expected_amount=1000,
        purge_after=current_ts + timedelta(days=1000),
    )
    wr4 = m.WorkerReceiving(
        to_collector_id=999,
        turn_id=1,
        debtor_id=1,
        from_collector_id=1,
        expected_amount=1000,
        purge_after=current_ts - timedelta(days=1),
    )
    db.session.add(wr1)
    db.session.add(wr2)
    db.session.add(wr3)
    db.session.add(wr4)
    db.session.commit()

    with db.engine.connect() as conn:
        conn.execute(sqlalchemy.text("ANALYZE worker_receiving"))

    assert len(m.WorkerReceiving.query.all()) == 4
    runner = app.test_cli_runner()
    result = runner.invoke(
        args=[
            "swpt_trade",
            "scan_worker_receivings",
            "--days",
            "0.000001",
            "--quit-early",
        ]
    )
    assert result.exit_code == 0
    wrs = m.WorkerReceiving.query.all()
    assert len(wrs) == 1
    assert wrs[0].to_collector_id == 888


def test_delete_worker_sendings(
        app,
        db_session,
        restore_sharding_realm,
        current_ts,
):
    app.config["SHARDING_REALM"] = ShardingRealm("0.#")
    app.config["DELETE_PARENT_SHARD_RECORDS"] = True

    ws1 = m.WorkerSending(
        from_collector_id=666,
        turn_id=1,
        debtor_id=1,
        to_collector_id=1,
        amount=1000,
        purge_after=current_ts + timedelta(days=1000),
    )
    ws2 = m.WorkerSending(
        from_collector_id=777,
        turn_id=1,
        debtor_id=1,
        to_collector_id=1,
        amount=1000,
        purge_after=current_ts + timedelta(days=1000),
    )
    ws3 = m.WorkerSending(
        from_collector_id=888,
        turn_id=1,
        debtor_id=1,
        to_collector_id=1,
        amount=1000,
        purge_after=current_ts + timedelta(days=1000),
    )
    ws4 = m.WorkerSending(
        from_collector_id=999,
        turn_id=1,
        debtor_id=1,
        to_collector_id=1,
        amount=1000,
        purge_after=current_ts - timedelta(days=1),
    )
    db.session.add(ws1)
    db.session.add(ws2)
    db.session.add(ws3)
    db.session.add(ws4)
    db.session.commit()

    with db.engine.connect() as conn:
        conn.execute(sqlalchemy.text("ANALYZE worker_sending"))

    assert len(m.WorkerSending.query.all()) == 4
    runner = app.test_cli_runner()
    result = runner.invoke(
        args=[
            "swpt_trade",
            "scan_worker_sendings",
            "--days",
            "0.000001",
            "--quit-early",
        ]
    )
    assert result.exit_code == 0
    wss = m.WorkerSending.query.all()
    assert len(wss) == 1
    assert wss[0].from_collector_id == 888


def test_delete_transfer_attempts(
        app,
        db_session,
        restore_sharding_realm,
        current_ts,
):
    app.config["SHARDING_REALM"] = ShardingRealm("0.#")
    app.config["DELETE_PARENT_SHARD_RECORDS"] = True

    ta1 = m.TransferAttempt(
        collector_id=666,
        turn_id=1,
        debtor_id=1,
        creditor_id=1,
        is_dispatching=True,
        nominal_amount=1000.0,
        collection_started_at=current_ts
    )
    ta2 = m.TransferAttempt(
        collector_id=777,
        turn_id=1,
        debtor_id=1,
        creditor_id=1,
        is_dispatching=True,
        nominal_amount=1000.0,
        collection_started_at=current_ts
    )
    ta3 = m.TransferAttempt(
        collector_id=888,
        turn_id=1,
        debtor_id=1,
        creditor_id=1,
        is_dispatching=True,
        nominal_amount=1000.0,
        collection_started_at=current_ts
    )
    ta4 = m.TransferAttempt(
        collector_id=999,
        turn_id=1,
        debtor_id=1,
        creditor_id=1,
        is_dispatching=True,
        nominal_amount=1000.0,
        collection_started_at=current_ts - timedelta(days=1000),
    )
    db.session.add(ta1)
    db.session.add(ta2)
    db.session.add(ta3)
    db.session.add(ta4)
    db.session.commit()

    with db.engine.connect() as conn:
        conn.execute(sqlalchemy.text("ANALYZE transfer_attempt"))

    assert len(m.TransferAttempt.query.all()) == 4
    runner = app.test_cli_runner()
    result = runner.invoke(
        args=[
            "swpt_trade",
            "scan_transfer_attempts",
            "--days",
            "0.000001",
            "--quit-early",
        ]
    )
    assert result.exit_code == 0
    tas = m.TransferAttempt.query.all()
    assert len(tas) == 1
    assert tas[0].collector_id == 888


def test_handle_pristine_collectors(
        app,
        db_session,
        restore_sharding_realm,
        current_ts,
):
    app.config["SHARDING_REALM"] = sr = ShardingRealm("1.#")
    app.config["DELETE_PARENT_SHARD_RECORDS"] = False

    assert sr.match(123)
    assert sr.match(127)
    assert sr.match(128)
    assert not sr.match(129)

    ca1 = m.CollectorAccount(debtor_id=666, collector_id=123, status=0)
    ca2 = m.CollectorAccount(debtor_id=666, collector_id=127, status=0)
    nwa2 = m.NeededWorkerAccount(creditor_id=127, debtor_id=666)
    wa2 = m.WorkerAccount(
        creditor_id=127,
        debtor_id=666,
        creation_date=m.DATE0,
        last_change_ts=current_ts,
        last_change_seqnum=1,
        principal=0,
        interest=0.0,
        interest_rate=0.0,
        last_interest_rate_change_ts=m.TS0,
        config_flags=0,
        account_id="Account127",
        last_transfer_number=0,
        last_transfer_committed_at=current_ts,
        demurrage_rate=-50.0,
        commit_period=1000000,
        transfer_note_max_bytes=500,
        last_heartbeat_ts=current_ts,
    )
    ca3 = m.CollectorAccount(debtor_id=666, collector_id=128, status=0)
    nwa3 = m.NeededWorkerAccount(
        creditor_id=128, debtor_id=666, configured_at=m.TS0
    )

    # Collector account from another shard (will be ignored).
    ca4 = m.CollectorAccount(debtor_id=666, collector_id=129, status=0)

    db.session.add(ca1)
    db.session.add(ca2)
    db.session.add(nwa2)
    db.session.add(wa2)
    db.session.add(ca3)
    db.session.add(nwa3)
    db.session.commit()
    db.session.add(ca4)

    assert len(m.CollectorAccount.query.all()) == 4
    runner = app.test_cli_runner()
    result = runner.invoke(
        args=[
            "swpt_trade",
            "handle_pristine_collectors",
            "--wait",
            "0.000001",
            "--quit-early",
        ]
    )
    assert result.exit_code == 0
    cas = m.CollectorAccount.query.all()
    cas.sort(key=lambda x: (x.debtor_id, x.collector_id))
    assert len(cas) == 4
    assert cas[0].status == 1
    assert cas[0].account_id == ""
    assert cas[1].status == 1
    assert cas[1].account_id == ""
    assert cas[2].status == 1
    assert cas[2].account_id == ""
    assert cas[3].status == 0
    assert cas[3].account_id == ""

    ca_signals = m.ConfigureAccountSignal.query.all()
    ca_signals.sort(key=lambda x: (x.debtor_id, x.creditor_id))
    assert len(ca_signals) == 2
    assert ca_signals[0].creditor_id == 123
    assert ca_signals[0].debtor_id == 666
    assert ca_signals[0].ts >= current_ts
    assert ca_signals[0].negligible_amount == 1e30
    assert ca_signals[0].config_data == ""
    assert ca_signals[0].config_flags == 0
    assert ca_signals[1].creditor_id == 128
    assert ca_signals[1].debtor_id == 666
    assert ca_signals[1].ts >= current_ts
    assert ca_signals[1].negligible_amount == 1e30
    assert ca_signals[1].config_data == ""
    assert ca_signals[1].config_flags == 0


def test_update_worker_turns(app, db_session, current_ts):
    t0 = m.Turn(
        base_debtor_info_locator="https://example.com/666",
        base_debtor_id=666,
        max_distance_to_base=10,
        min_trade_amount=10000,
        phase=4,
        phase_deadline=None,
        collection_started_at=current_ts - timedelta(days=71),
        collection_deadline=current_ts - timedelta(days=51),
    )
    t1 = m.Turn(
        base_debtor_info_locator="https://example.com/666",
        base_debtor_id=666,
        max_distance_to_base=10,
        min_trade_amount=10000,
        phase=2,
        phase_deadline=current_ts - timedelta(days=100),
        collection_deadline=current_ts - timedelta(days=50),
    )
    t2 = m.Turn(
        base_debtor_info_locator="https://example.com/666",
        base_debtor_id=666,
        max_distance_to_base=10,
        min_trade_amount=10000,
        phase=1,
        phase_deadline=current_ts - timedelta(days=99.1),
        collection_deadline=current_ts - timedelta(days=49),
    )
    t3 = m.Turn(
        base_debtor_info_locator="https://example.com/666",
        base_debtor_id=666,
        max_distance_to_base=10,
        min_trade_amount=10000,
        phase=3,
        phase_deadline=None,
        collection_started_at=current_ts - timedelta(days=1),
        collection_deadline=current_ts - timedelta(days=49),
    )
    db.session.add(t0)
    db.session.add(t1)
    db.session.add(t2)
    db.session.add(t3)
    db.session.flush()
    db.session.expunge(t0)
    db.session.expunge(t1)
    db.session.expunge(t2)
    db.session.expunge(t3)

    wt0 = m.WorkerTurn(
        turn_id=t0.turn_id,
        started_at=t0.started_at,
        base_debtor_info_locator="https://example.com/666",
        base_debtor_id=666,
        max_distance_to_base=10,
        min_trade_amount=10000,
        phase=2,
        phase_deadline=current_ts - timedelta(days=71.1),
        collection_started_at=current_ts - timedelta(days=71),
        collection_deadline=current_ts - timedelta(days=51),
        worker_turn_subphase=10,
    )
    wt1 = m.WorkerTurn(
        turn_id=t1.turn_id,
        started_at=t1.started_at,
        base_debtor_info_locator="https://example.com/666",
        base_debtor_id=666,
        max_distance_to_base=10,
        min_trade_amount=10000,
        phase=1,
        phase_deadline=current_ts - timedelta(days=100.1),
        worker_turn_subphase=5,
    )
    wt3 = m.WorkerTurn(
        turn_id=t3.turn_id,
        started_at=t3.started_at,
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
    db.session.add(wt0)
    db.session.add(wt1)
    db.session.add(wt3)
    db.session.commit()

    assert len(m.WorkerTurn.query.all()) == 3
    runner = app.test_cli_runner()
    result = runner.invoke(
        args=[
            "swpt_trade",
            "roll_worker_turns",
            "--quit-early",
        ]
    )
    assert result.exit_code == 0
    wts = m.WorkerTurn.query.all()
    wts.sort(key=lambda t: (t.phase, t.worker_turn_subphase), reverse=True)
    assert len(wts) == 4
    assert wts[0].phase == 3
    assert wts[0].worker_turn_subphase == 10
    assert wts[0].turn_id == t3.turn_id
    assert wts[0].started_at == t3.started_at
    assert wts[0].base_debtor_info_locator == t3.base_debtor_info_locator
    assert wts[0].base_debtor_id == t3.base_debtor_id
    assert wts[0].max_distance_to_base == t3.max_distance_to_base
    assert wts[0].min_trade_amount == t3.min_trade_amount
    assert wts[0].phase_deadline is None
    assert wts[0].collection_started_at == t3.collection_started_at
    assert wts[0].collection_deadline == t3.collection_deadline
    assert wts[1].phase == 3
    assert wts[1].worker_turn_subphase == 5
    assert wts[1].turn_id == t0.turn_id
    assert wts[1].started_at == t0.started_at
    assert wts[1].base_debtor_info_locator == t0.base_debtor_info_locator
    assert wts[1].base_debtor_id == t0.base_debtor_id
    assert wts[1].max_distance_to_base == t0.max_distance_to_base
    assert wts[1].min_trade_amount == t0.min_trade_amount
    assert wts[1].phase_deadline is None
    assert wts[1].collection_started_at == t0.collection_started_at
    assert wts[1].collection_deadline == t0.collection_deadline
    assert wts[2].phase == t1.phase
    assert wts[2].worker_turn_subphase == 5
    assert wts[2].turn_id == t1.turn_id
    assert wts[2].started_at == t1.started_at
    assert wts[2].base_debtor_info_locator == t1.base_debtor_info_locator
    assert wts[2].base_debtor_id == t1.base_debtor_id
    assert wts[2].max_distance_to_base == t1.max_distance_to_base
    assert wts[2].min_trade_amount == t1.min_trade_amount
    assert wts[2].phase_deadline == t1.phase_deadline
    assert wts[2].collection_started_at == t1.collection_started_at
    assert wts[2].collection_deadline == t1.collection_deadline
    assert wts[3].phase == t2.phase
    assert wts[3].worker_turn_subphase == 10
    assert wts[3].turn_id == t2.turn_id
    assert wts[3].started_at == t2.started_at
    assert wts[3].base_debtor_info_locator == t2.base_debtor_info_locator
    assert wts[3].base_debtor_id == t2.base_debtor_id
    assert wts[3].max_distance_to_base == t2.max_distance_to_base
    assert wts[3].min_trade_amount == t2.min_trade_amount
    assert wts[3].phase_deadline == t2.phase_deadline
    assert wts[3].collection_started_at == t2.collection_started_at
    assert wts[3].collection_deadline == t2.collection_deadline


@pytest.mark.parametrize("populated_confirmed_debtors", [True, False])
@pytest.mark.parametrize("populated_debtor_infos", [True, False])
def test_run_phase1_subphase0(
        mocker,
        app,
        db_session,
        current_ts,
        populated_debtor_infos,
        populated_confirmed_debtors,
):
    mocker.patch("swpt_trade.run_turn_subphases.INSERT_BATCH_SIZE", new=1)
    mocker.patch("swpt_trade.run_turn_subphases.SELECT_BATCH_SIZE", new=1)
    mocker.patch("swpt_trade.run_turn_subphases.BID_COUNTER_THRESHOLD", new=1)

    t1 = m.Turn(
        base_debtor_info_locator="https://example.com/666",
        base_debtor_id=666,
        max_distance_to_base=10,
        min_trade_amount=10000,
        phase=1,
        phase_deadline=current_ts + timedelta(days=100),
        collection_deadline=None,
    )
    db.session.add(t1)
    db.session.flush()

    if populated_confirmed_debtors:
        db.session.add(
            m.ConfirmedDebtor(
                turn_id=t1.turn_id,
                debtor_id=666,
                debtor_info_locator="https://example.com/666",
            )
        )
        db.session.add(
            m.ConfirmedDebtor(
                turn_id=t1.turn_id,
                debtor_id=777,
                debtor_info_locator="https://example.com/777",
            )
        )

    if populated_debtor_infos:
        db.session.add(
            m.DebtorInfo(
                turn_id=t1.turn_id,
                debtor_info_locator="https://example.com/666",
                debtor_id=666,
            )
        )
        db.session.add(
            m.DebtorInfo(
                turn_id=t1.turn_id,
                debtor_info_locator="https://example.com/777",
                debtor_id=777,
                peg_debtor_info_locator="https://example.com/666",
                peg_debtor_id=666,
                peg_exchange_rate=2.0,
            )
        )

    wt1 = m.WorkerTurn(
        turn_id=t1.turn_id,
        started_at=t1.started_at,
        base_debtor_info_locator="https://example.com/666",
        base_debtor_id=666,
        max_distance_to_base=10,
        min_trade_amount=10000,
        phase=1,
        phase_deadline=current_ts + timedelta(days=100),
        worker_turn_subphase=0,
    )
    db.session.add(wt1)
    db.session.add(
        m.DebtorInfoDocument(
            debtor_info_locator="https://example.com/777",
            debtor_id=777,
            peg_debtor_info_locator="https://example.com/666",
            peg_debtor_id=666,
            peg_exchange_rate=2.0,
        )
    )
    db.session.add(
        m.DebtorInfoDocument(
            debtor_info_locator="https://example.com/666",
            debtor_id=666,
        )
    )
    db.session.add(
        m.DebtorLocatorClaim(
            debtor_info_locator="https://example.com/666",
            debtor_id=666,
            latest_locator_fetch_at=current_ts,
        )
    )
    db.session.add(
        m.DebtorLocatorClaim(
            debtor_info_locator="https://example.com/777",
            debtor_id=777,
            latest_locator_fetch_at=current_ts,
        )
    )
    db.session.commit()

    assert len(m.WorkerTurn.query.all()) == 1
    assert len(m.DebtorInfoDocument.query.all()) == 2
    assert len(m.DebtorLocatorClaim.query.all()) == 2
    runner = app.test_cli_runner()
    result = runner.invoke(
        args=[
            "swpt_trade",
            "roll_worker_turns",
            "--quit-early",
        ]
    )
    assert result.exit_code == 0
    wt = m.WorkerTurn.query.one()
    assert wt.turn_id == t1.turn_id
    assert wt.phase == t1.phase
    assert wt.worker_turn_subphase == 10
    assert len(m.DebtorInfoDocument.query.all()) == 2
    dis = m.DebtorInfo.query.all()
    dis.sort(key=lambda x: x.debtor_info_locator)
    assert len(dis) == 2
    assert dis[0].debtor_info_locator == "https://example.com/666"
    assert dis[0].debtor_id == 666
    assert dis[0].peg_debtor_info_locator is None
    assert dis[0].peg_debtor_id is None
    assert dis[0].peg_exchange_rate is None
    assert dis[1].debtor_info_locator == "https://example.com/777"
    assert dis[1].debtor_id == 777
    assert dis[1].peg_debtor_info_locator == "https://example.com/666"
    assert dis[1].peg_debtor_id == 666
    assert dis[1].peg_exchange_rate == 2.0
    assert len(m.DebtorLocatorClaim.query.all()) == 2
    cds = m.ConfirmedDebtor.query.all()
    cds.sort(key=lambda x: x.debtor_id)
    assert len(cds) == 2
    assert cds[0].debtor_id == 666
    assert cds[0].debtor_info_locator == "https://example.com/666"
    assert cds[1].debtor_id == 777
    assert cds[1].debtor_info_locator == "https://example.com/777"


@pytest.mark.parametrize("has_active_collectors", [True, False])
def test_run_phase2_subphase0(
        mocker,
        app,
        db_session,
        current_ts,
        has_active_collectors,
):
    mocker.patch("swpt_trade.run_turn_subphases.INSERT_BATCH_SIZE", new=1)
    mocker.patch("swpt_trade.run_turn_subphases.SELECT_BATCH_SIZE", new=1)
    mocker.patch("swpt_trade.run_turn_subphases.BID_COUNTER_THRESHOLD", new=1)

    t1 = m.Turn(
        base_debtor_info_locator="https://example.com/666",
        base_debtor_id=666,
        max_distance_to_base=10,
        min_trade_amount=10000,
        phase=2,
        phase_deadline=current_ts + timedelta(days=100),
        collection_deadline=current_ts + timedelta(days=200),
    )
    db.session.add(t1)
    db.session.flush()
    db.session.add(
        m.CurrencyInfo(
            turn_id=t1.turn_id,
            debtor_info_locator="https://example.com/666",
            debtor_id=666,
            is_confirmed=True,
        )
    )
    db.session.add(
        m.CurrencyInfo(
            turn_id=t1.turn_id,
            debtor_info_locator="https://example.com/777",
            debtor_id=777,
            peg_debtor_info_locator="https://example.com/666",
            peg_debtor_id=666,
            peg_exchange_rate=1.0,
            is_confirmed=True,
        )
    )
    db.session.add(
        m.CurrencyInfo(
            turn_id=t1.turn_id,
            debtor_info_locator="https://example.com/888",
            debtor_id=888,
            peg_debtor_info_locator="https://example.com/666",
            peg_debtor_id=666,
            peg_exchange_rate=1.0,
            is_confirmed=False,
        )
    )
    db.session.add(
        m.CollectorAccount(
            debtor_id=999,
            collector_id=0x0000010000000000,
            account_id="TestCollectorAccountId",
            status=2,
        )
    )

    wt1 = m.WorkerTurn(
        turn_id=t1.turn_id,
        started_at=t1.started_at,
        base_debtor_info_locator="https://example.com/666",
        base_debtor_id=666,
        max_distance_to_base=10,
        min_trade_amount=10000,
        phase=2,
        phase_deadline=current_ts + timedelta(days=100),
        collection_deadline=current_ts + timedelta(days=200),
        worker_turn_subphase=0,
    )
    db.session.add(wt1)
    if has_active_collectors:
        db.session.add(
            m.ActiveCollector(
                debtor_id=999,
                collector_id=0x0000010000000000,
                account_id="TestCollectorAccountId",
            )
        )
    db.session.add(
        m.TradingPolicy(
            creditor_id=123,
            debtor_id=666,
            account_id="Account666-123",
            creation_date=date(2024, 4, 8),
            principal=0,
            last_transfer_number=567,
            policy_name="conservative",
            min_principal=200000,
            max_principal=300000,
        )
    )
    db.session.add(
        m.TradingPolicy(
            creditor_id=123,
            debtor_id=777,
            account_id="Account777-123",
            creation_date=date(2024, 4, 9),
            principal=150000,
            last_transfer_number=789,
            policy_name="unknown",
            min_principal=0,
            max_principal=50000,
            peg_debtor_id=666,
            peg_exchange_rate=1.0,
        )
    )
    db.session.add(
        m.TradingPolicy(
            creditor_id=124,
            debtor_id=777,
            account_id="Account777-124",
            creation_date=date(2024, 4, 10),
            principal=150000,
            last_transfer_number=890,
            policy_name="conservative",
            min_principal=0,
            max_principal=300000,
        )
    )
    db.session.add(
        m.TradingPolicy(
            creditor_id=124,
            debtor_id=888,
            account_id="Account888-124",
            creation_date=date(2024, 4, 11),
            principal=1000000,
            last_transfer_number=901,
            policy_name="conservative",
            min_principal=0,
            max_principal=0,
            peg_debtor_id=666,
            peg_exchange_rate=1.0,
        )
    )
    db.session.commit()

    assert len(m.WorkerTurn.query.all()) == 1
    assert len(m.CandidateOfferSignal.query.all()) == 0
    assert len(m.NeededCollectorSignal.query.all()) == 0
    runner = app.test_cli_runner()
    result = runner.invoke(
        args=[
            "swpt_trade",
            "roll_worker_turns",
            "--quit-early",
        ]
    )
    assert result.exit_code == 0
    wt = m.WorkerTurn.query.one()
    assert wt.turn_id == t1.turn_id
    assert wt.phase == t1.phase
    assert wt.worker_turn_subphase == 5
    cas = m.CandidateOfferSignal.query.all()
    cas.sort(key=lambda x: x.debtor_id)
    assert len(cas) == 2
    assert cas[0].turn_id == t1.turn_id
    assert cas[0].debtor_id == 666
    assert cas[0].creditor_id == 123
    assert cas[0].amount == 200000
    assert cas[0].account_creation_date == date(2024, 4, 8)
    assert cas[0].last_transfer_number == 567
    assert cas[0].inserted_at >= current_ts
    assert cas[1].turn_id == t1.turn_id
    assert cas[1].debtor_id == 777
    assert cas[1].creditor_id == 123
    assert cas[1].amount == -100000
    assert cas[1].account_creation_date == date(2024, 4, 9)
    assert cas[1].last_transfer_number == 789
    assert cas[1].inserted_at >= current_ts
    ncs = m.NeededCollectorSignal.query.all()
    assert len(ncs) == 1
    assert ncs[0].debtor_id == 888
    acs = m.ActiveCollector.query.all()
    assert len(acs) == 1
    assert acs[0].debtor_id == 999
    assert acs[0].collector_id == 0x0000010000000000
    assert acs[0].account_id == "TestCollectorAccountId"


@pytest.mark.parametrize("has_sell_offers", [True, False])
@pytest.mark.parametrize("has_buy_offers", [True, False])
def test_run_phase2_subphase5(
        mocker,
        app,
        db_session,
        current_ts,
        has_buy_offers,
        has_sell_offers,
):
    mocker.patch("swpt_trade.run_turn_subphases.INSERT_BATCH_SIZE", new=1)
    mocker.patch("swpt_trade.run_turn_subphases.SELECT_BATCH_SIZE", new=1)
    mocker.patch("swpt_trade.run_turn_subphases.BID_COUNTER_THRESHOLD", new=1)

    phase_deadline = (
        current_ts
        + current_app.config['APP_TURN_PHASE_CUSHION_PERIOD'] / 2
    )
    t1 = m.Turn(
        base_debtor_info_locator="https://example.com/666",
        base_debtor_id=666,
        started_at=current_ts - timedelta(days=10000),
        max_distance_to_base=10,
        min_trade_amount=10000,
        phase=2,
        phase_deadline=phase_deadline,
        collection_deadline=current_ts + timedelta(days=200),
    )
    db.session.add(t1)
    db.session.flush()

    if has_sell_offers:
        db.session.add(
            m.SellOffer(
                turn_id=t1.turn_id,
                creditor_id=123,
                debtor_id=666,
                amount=10000,
                collector_id=789,
            )
        )
        db.session.add(
            m.SellOffer(
                turn_id=t1.turn_id,
                creditor_id=124,
                debtor_id=666,
                amount=10001,
                collector_id=789,
            )
        )
    if has_buy_offers:
        db.session.add(
            m.BuyOffer(
                turn_id=t1.turn_id,
                creditor_id=456,
                debtor_id=777,
                amount=30000,
            )
        )
        db.session.add(
            m.BuyOffer(
                turn_id=t1.turn_id,
                creditor_id=457,
                debtor_id=777,
                amount=30001,
            )
        )

    wt1 = m.WorkerTurn(
        turn_id=t1.turn_id,
        started_at=t1.started_at,
        base_debtor_info_locator="https://example.com/666",
        base_debtor_id=666,
        max_distance_to_base=10,
        min_trade_amount=10000,
        phase=2,
        phase_deadline=phase_deadline,
        collection_deadline=current_ts + timedelta(days=200),
        worker_turn_subphase=5,
    )
    db.session.add(wt1)
    db.session.flush()

    db.session.add(
        m.AccountLock(
            creditor_id=123,
            debtor_id=666,
            turn_id=t1.turn_id,
            collector_id=789,
            transfer_id=1234,
            amount=-10000,
        )
    )
    db.session.add(
        m.AccountLock(
            creditor_id=124,
            debtor_id=666,
            turn_id=t1.turn_id,
            collector_id=789,
            transfer_id=1235,
            amount=-10001,
        )
    )
    db.session.add(
        m.AccountLock(
            creditor_id=123,
            debtor_id=777,
            turn_id=t1.turn_id,
            collector_id=890,
            transfer_id=None,
            amount=0,
        )
    )
    db.session.add(
        m.AccountLock(
            creditor_id=456,
            debtor_id=777,
            turn_id=t1.turn_id,
            collector_id=890,
            transfer_id=5678,
            amount=30000,
        )
    )
    db.session.add(
        m.AccountLock(
            creditor_id=457,
            debtor_id=777,
            turn_id=t1.turn_id,
            collector_id=890,
            transfer_id=5679,
            amount=30001,
        )
    )
    db.session.add(
        m.AccountLock(
            creditor_id=456,
            debtor_id=666,
            turn_id=t1.turn_id,
            collector_id=789,
            transfer_id=None,
            amount=50000,
        )
    )
    db.session.add(
        m.AccountLock(
            creditor_id=456,
            debtor_id=888,
            turn_id=t1.turn_id,
            collector_id=890,
            released_at=current_ts,
            transfer_id=2345,
            amount=80000,
            finalized_at=current_ts,
            account_creation_date=date(2000, 1, 1),
            account_last_transfer_number=123,
        )
    )
    db.session.commit()

    assert len(m.WorkerTurn.query.all()) == 1
    assert len(m.AccountLock.query.all()) == 7
    runner = app.test_cli_runner()
    result = runner.invoke(
        args=[
            "swpt_trade",
            "roll_worker_turns",
            "--quit-early",
        ]
    )
    assert result.exit_code == 0
    wt = m.WorkerTurn.query.one()
    assert wt.turn_id == t1.turn_id
    assert wt.phase == t1.phase
    assert wt.worker_turn_subphase == 10
    assert len(m.AccountLock.query.all()) == 7

    sos = m.SellOffer.query.all()
    sos.sort(key=lambda x: x.creditor_id)
    assert len(sos) == 2
    assert sos[0].turn_id == t1.turn_id
    assert sos[0].creditor_id == 123
    assert sos[0].debtor_id == 666
    assert sos[0].amount == 10000
    assert sos[0].collector_id == 789
    assert sos[1].turn_id == t1.turn_id
    assert sos[1].creditor_id == 124
    assert sos[1].debtor_id == 666
    assert sos[1].amount == 10001
    assert sos[1].collector_id == 789

    bos = m.BuyOffer.query.all()
    bos.sort(key=lambda x: x.creditor_id)
    assert len(bos) == 2
    assert bos[0].turn_id == t1.turn_id
    assert bos[0].creditor_id == 456
    assert bos[0].debtor_id == 777
    assert bos[0].amount == 30000
    assert bos[1].turn_id == t1.turn_id
    assert bos[1].creditor_id == 457
    assert bos[1].debtor_id == 777
    assert bos[1].amount == 30001


def test_run_phase3_subphase0(
        mocker,
        app,
        db_session,
        current_ts,
        restore_sharding_realm,
):
    app.config["SHARDING_REALM"] = ShardingRealm("1.#")
    app.config["DELETE_PARENT_SHARD_RECORDS"] = True

    mocker.patch("swpt_trade.run_turn_subphases.INSERT_BATCH_SIZE", new=1)
    mocker.patch("swpt_trade.run_turn_subphases.SELECT_BATCH_SIZE", new=1)

    t1 = m.Turn(
        base_debtor_info_locator="https://example.com/666",
        base_debtor_id=666,
        started_at=current_ts - timedelta(days=10000),
        max_distance_to_base=10,
        min_trade_amount=10000,
        phase=3,
        phase_deadline=None,
        collection_started_at=current_ts - timedelta(days=1),
        collection_deadline=current_ts + timedelta(days=200),
    )
    db.session.add(t1)
    db.session.flush()

    wt1 = m.WorkerTurn(
        turn_id=t1.turn_id,
        started_at=t1.started_at,
        base_debtor_info_locator="https://example.com/666",
        base_debtor_id=666,
        max_distance_to_base=10,
        min_trade_amount=10000,
        phase=3,
        phase_deadline=None,
        collection_started_at=current_ts - timedelta(days=1),
        collection_deadline=current_ts + timedelta(days=200),
        worker_turn_subphase=0,
    )
    db.session.add(wt1)
    db.session.flush()

    db.session.add(
        m.AccountLock(
            creditor_id=123,
            debtor_id=666,
            turn_id=t1.turn_id,
            collector_id=789,
            transfer_id=1234,
            amount=-10000,
        )
    )
    db.session.add(
        m.AccountLock(
            creditor_id=125,
            debtor_id=666,
            turn_id=t1.turn_id,
            collector_id=789,
            transfer_id=1235,
            amount=10000,
        )
    )
    db.session.add(
        m.CreditorTaking(
            turn_id=wt1.turn_id,
            creditor_id=123,
            debtor_id=666,
            creditor_hash=calc_hash(123),
            amount=10000,
            collector_id=789,
        )
    )
    db.session.add(
        # belongs to another shard
        m.CreditorTaking(
            turn_id=wt1.turn_id,
            creditor_id=12300,
            debtor_id=666,
            creditor_hash=calc_hash(12300),
            amount=10000,
            collector_id=789,
        )
    )
    db.session.add(
        m.CreditorGiving(
            turn_id=wt1.turn_id,
            creditor_id=125,
            debtor_id=666,
            creditor_hash=calc_hash(125),
            amount=10000,
            collector_id=789,
        )
    )
    db.session.add(
        # belongs to another shard
        m.CreditorGiving(
            turn_id=wt1.turn_id,
            creditor_id=12500,
            debtor_id=666,
            creditor_hash=calc_hash(12500),
            amount=10000,
            collector_id=789,
        )
    )
    db.session.add(
        m.CollectorCollecting(
            turn_id=wt1.turn_id,
            debtor_id=666,
            creditor_id=123,
            amount=10000,
            collector_id=789,
            collector_hash=calc_hash(789),
        )
    )
    db.session.add(
        # belongs to another shard
        m.CollectorCollecting(
            turn_id=wt1.turn_id,
            debtor_id=666,
            creditor_id=124,
            amount=10000,
            collector_id=78901,
            collector_hash=calc_hash(78901),
        )
    )
    db.session.add(
        m.CollectorSending(
            turn_id=wt1.turn_id,
            debtor_id=666,
            from_collector_id=789,
            to_collector_id=890,
            from_collector_hash=calc_hash(789),
            amount=10000,
        )
    )
    db.session.add(
        # belongs to another shard
        m.CollectorSending(
            turn_id=wt1.turn_id,
            debtor_id=666,
            from_collector_id=78901,
            to_collector_id=890,
            from_collector_hash=calc_hash(78901),
            amount=10000,
        )
    )
    db.session.add(
        m.CollectorReceiving(
            turn_id=wt1.turn_id,
            debtor_id=666,
            to_collector_id=890,
            from_collector_id=789,
            to_collector_hash=calc_hash(890),
            amount=10000,
        )
    )
    db.session.add(
        # belongs to another shard
        m.CollectorReceiving(
            turn_id=wt1.turn_id,
            debtor_id=666,
            to_collector_id=89001,
            from_collector_id=789,
            to_collector_hash=calc_hash(89001),
            amount=10000,
        )
    )
    db.session.add(
        m.CollectorDispatching(
            turn_id=wt1.turn_id,
            debtor_id=666,
            creditor_id=125,
            amount=10000,
            collector_id=890,
            collector_hash=calc_hash(890),
        )
    )
    db.session.add(
        # belongs to another shard
        m.CollectorDispatching(
            turn_id=wt1.turn_id,
            debtor_id=666,
            creditor_id=12300,
            amount=10000,
            collector_id=89001,
            collector_hash=calc_hash(89001),
        )
    )
    db.session.commit()

    assert len(m.WorkerTurn.query.all()) == 1
    assert len(m.AccountLock.query.all()) == 2
    assert len(m.CreditorTaking.query.all()) == 2
    assert len(m.CreditorGiving.query.all()) == 2
    assert len(m.CollectorCollecting.query.all()) == 2
    assert len(m.CollectorSending.query.all()) == 2
    assert len(m.CollectorReceiving.query.all()) == 2
    assert len(m.CollectorDispatching.query.all()) == 2
    runner = app.test_cli_runner()
    result = runner.invoke(
        args=[
            "swpt_trade",
            "roll_worker_turns",
            "--quit-early",
        ]
    )
    assert result.exit_code == 0
    wt = m.WorkerTurn.query.one()
    assert wt.turn_id == t1.turn_id
    assert wt.phase == t1.phase
    assert wt.worker_turn_subphase == 5
    assert len(m.AccountLock.query.all()) == 2
    assert len(m.CreditorTaking.query.all()) == 2
    assert len(m.CreditorGiving.query.all()) == 2
    assert len(m.CollectorCollecting.query.all()) == 2
    assert len(m.CollectorSending.query.all()) == 2
    assert len(m.CollectorReceiving.query.all()) == 2
    assert len(m.CollectorDispatching.query.all()) == 2

    cps = m.CreditorParticipation.query.all()
    cps.sort(key=lambda x: x.creditor_id)
    assert len(cps) == 2
    assert cps[0].creditor_id == 123
    assert cps[0].debtor_id == 666
    assert cps[0].turn_id == t1.turn_id
    assert cps[0].amount == -10000
    assert cps[0].collector_id == 789
    assert cps[1].creditor_id == 125
    assert cps[1].debtor_id == 666
    assert cps[1].turn_id == t1.turn_id
    assert cps[1].amount == 10000
    assert cps[1].collector_id == 789

    wc = m.WorkerCollecting.query.one()
    assert wc.collector_id == 789
    assert wc.turn_id == t1.turn_id
    assert wc.debtor_id == 666
    assert wc.creditor_id == 123
    assert wc.amount == 10000
    assert wc.collected is False
    assert wc.purge_after > current_ts

    ws = m.WorkerSending.query.one()
    assert ws.from_collector_id == 789
    assert ws.turn_id == t1.turn_id
    assert ws.debtor_id == 666
    assert ws.to_collector_id == 890
    assert ws.amount == 10000
    assert ws.purge_after > current_ts

    wr = m.WorkerReceiving.query.one()
    assert wr.to_collector_id == 890
    assert wr.turn_id == t1.turn_id
    assert wr.debtor_id == 666
    assert wr.from_collector_id == 789
    assert wr.expected_amount == 10000
    assert wr.received_amount == 0
    assert wr.purge_after > current_ts

    wr = m.WorkerDispatching.query.one()
    assert wr.collector_id == 890
    assert wr.turn_id == t1.turn_id
    assert wr.debtor_id == 666
    assert wr.creditor_id == 125
    assert wr.amount == 10000
    assert wr.purge_after > current_ts

    dss = m.DispatchingStatus.query.all()
    dss.sort(key=lambda x: x.collector_id)
    assert len(dss) == 2
    assert dss[0].collector_id == 789
    assert dss[0].turn_id == t1.turn_id
    assert dss[0].inserted_at >= current_ts
    assert dss[0].amount_to_collect == 10000
    assert dss[0].total_collected_amount is None
    assert dss[0].amount_to_send == 10000
    assert dss[0].all_sent is False
    assert dss[0].number_to_receive == 0
    assert dss[0].total_received_amount is None
    assert dss[0].all_received is False
    assert dss[0].amount_to_dispatch == 0

    assert dss[1].collector_id == 890
    assert dss[1].turn_id == t1.turn_id
    assert dss[1].inserted_at >= current_ts
    assert dss[1].amount_to_collect == 0
    assert dss[1].total_collected_amount is None
    assert dss[1].amount_to_send == 0
    assert dss[1].all_sent is False
    assert dss[1].number_to_receive == 1
    assert dss[1].total_received_amount is None
    assert dss[1].all_received is False
    assert dss[1].amount_to_dispatch == 10000

    rals = m.ReviseAccountLockSignal.query.all()
    rals.sort(key=lambda x: x.creditor_id)
    assert len(rals) == 2
    assert rals[0].creditor_id == 123
    assert rals[0].debtor_id == 666
    assert rals[0].turn_id == wt.turn_id
    assert rals[1].creditor_id == 125
    assert rals[1].turn_id == wt.turn_id
    assert rals[1].debtor_id == 666


def test_run_phase3_subphase5(
        mocker,
        app,
        db_session,
        current_ts,
        restore_sharding_realm,
):
    app.config["SHARDING_REALM"] = ShardingRealm("1.#")
    app.config["DELETE_PARENT_SHARD_RECORDS"] = True

    mocker.patch("swpt_trade.run_turn_subphases.INSERT_BATCH_SIZE", new=1)
    mocker.patch("swpt_trade.run_turn_subphases.SELECT_BATCH_SIZE", new=1)

    t1 = m.Turn(
        base_debtor_info_locator="https://example.com/666",
        base_debtor_id=666,
        started_at=current_ts - timedelta(days=10000),
        max_distance_to_base=10,
        min_trade_amount=10000,
        phase=3,
        phase_deadline=None,
        collection_started_at=current_ts - timedelta(days=1),
        collection_deadline=current_ts + timedelta(days=200),
    )
    db.session.add(t1)
    db.session.flush()

    wt1 = m.WorkerTurn(
        turn_id=t1.turn_id,
        started_at=t1.started_at,
        base_debtor_info_locator="https://example.com/666",
        base_debtor_id=666,
        max_distance_to_base=10,
        min_trade_amount=10000,
        phase=3,
        phase_deadline=None,
        collection_started_at=current_ts - timedelta(days=1),
        collection_deadline=current_ts + timedelta(days=200),
        worker_turn_subphase=5,
    )
    db.session.add(wt1)
    db.session.flush()

    db.session.add(
        m.CreditorTaking(
            turn_id=wt1.turn_id,
            creditor_id=123,
            debtor_id=666,
            creditor_hash=calc_hash(123),
            amount=10000,
            collector_id=789,
        )
    )
    db.session.add(
        # belongs to another shard
        m.CreditorTaking(
            turn_id=wt1.turn_id,
            creditor_id=124,
            debtor_id=666,
            creditor_hash=calc_hash(124),
            amount=10000,
            collector_id=789,
        )
    )
    db.session.add(
        m.CreditorGiving(
            turn_id=wt1.turn_id,
            creditor_id=125,
            debtor_id=666,
            creditor_hash=calc_hash(125),
            amount=10000,
            collector_id=789,
        )
    )
    db.session.add(
        # belongs to another shard
        m.CreditorGiving(
            turn_id=wt1.turn_id,
            creditor_id=124,
            debtor_id=666,
            creditor_hash=calc_hash(124),
            amount=10000,
            collector_id=789,
        )
    )
    db.session.add(
        m.CollectorCollecting(
            turn_id=wt1.turn_id,
            debtor_id=666,
            creditor_id=123,
            amount=10000,
            collector_id=789,
            collector_hash=calc_hash(789),
        )
    )
    db.session.add(
        # belongs to another shard
        m.CollectorCollecting(
            turn_id=wt1.turn_id,
            debtor_id=666,
            creditor_id=124,
            amount=10000,
            collector_id=78901,
            collector_hash=calc_hash(78901),
        )
    )
    db.session.add(
        m.CollectorSending(
            turn_id=wt1.turn_id,
            debtor_id=666,
            from_collector_id=789,
            to_collector_id=890,
            from_collector_hash=calc_hash(789),
            amount=10000,
        )
    )
    db.session.add(
        # belongs to another shard
        m.CollectorSending(
            turn_id=wt1.turn_id,
            debtor_id=666,
            from_collector_id=78901,
            to_collector_id=890,
            from_collector_hash=calc_hash(78901),
            amount=10000,
        )
    )
    db.session.add(
        m.CollectorReceiving(
            turn_id=wt1.turn_id,
            debtor_id=666,
            to_collector_id=890,
            from_collector_id=789,
            to_collector_hash=calc_hash(890),
            amount=10000,
        )
    )
    db.session.add(
        # belongs to another shard
        m.CollectorReceiving(
            turn_id=wt1.turn_id,
            debtor_id=666,
            to_collector_id=89001,
            from_collector_id=789,
            to_collector_hash=calc_hash(89001),
            amount=10000,
        )
    )
    db.session.add(
        m.CollectorDispatching(
            turn_id=wt1.turn_id,
            debtor_id=666,
            creditor_id=125,
            amount=10000,
            collector_id=890,
            collector_hash=calc_hash(890),
        )
    )
    db.session.add(
        # belongs to another shard
        m.CollectorDispatching(
            turn_id=wt1.turn_id,
            debtor_id=666,
            creditor_id=124,
            amount=10000,
            collector_id=89001,
            collector_hash=calc_hash(89001),
        )
    )
    db.session.commit()

    assert len(m.WorkerTurn.query.all()) == 1
    assert len(m.CreditorTaking.query.all()) == 2
    assert len(m.CreditorGiving.query.all()) == 2
    assert len(m.CollectorCollecting.query.all()) == 2
    assert len(m.CollectorSending.query.all()) == 2
    assert len(m.CollectorReceiving.query.all()) == 2
    assert len(m.CollectorDispatching.query.all()) == 2
    runner = app.test_cli_runner()
    result = runner.invoke(
        args=[
            "swpt_trade",
            "roll_worker_turns",
            "--quit-early",
        ]
    )
    assert result.exit_code == 0
    wt = m.WorkerTurn.query.one()
    assert wt.turn_id == t1.turn_id
    assert wt.phase == t1.phase
    assert wt.worker_turn_subphase == 10

    assert len(m.CreditorTaking.query.all()) == 1
    assert len(m.CreditorGiving.query.all()) == 1
    assert len(m.CollectorCollecting.query.all()) == 1
    assert len(m.CollectorSending.query.all()) == 1
    assert len(m.CollectorReceiving.query.all()) == 1
    assert len(m.CollectorDispatching.query.all()) == 1


@pytest.mark.parametrize("realm", ["0.#", "1.#"])
def test_verify_shard_content(app, db_session, realm):
    orig_sharding_realm = app.config["SHARDING_REALM"]
    app.config["SHARDING_REALM"] = ShardingRealm(realm)
    db.session.add(
        m.DebtorInfoDocument(
            debtor_info_locator="https://example.com/666",
            debtor_id=666,
        )
    )
    db.session.commit()
    runner = app.test_cli_runner()
    result = runner.invoke(
        args=["swpt_trade", "verify_shard_content"]
    )
    assert result.exit_code == int(realm[0])
    app.config["SHARDING_REALM"] = orig_sharding_realm
