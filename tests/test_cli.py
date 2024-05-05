import pytest
import sqlalchemy
from datetime import date, datetime, timezone, timedelta
from unittest.mock import Mock
from swpt_pythonlib.utils import ShardingRealm
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


def test_consume_chore_messages(app):
    runner = app.test_cli_runner()
    result = runner.invoke(
        args=["swpt_trade", "consume_chore_messages", "--url=INVALID"]
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
    assert result.exit_code == 1
    assert send_signalbus_message.called_once()
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
        ignore_cache=True,
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
    assert result.exit_code == 1

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
    assert fetch_signals[0].ignore_cache is False
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
    )
    al2 = m.AccountLock(
        creditor_id=777,
        debtor_id=123,
        turn_id=1,
        collector_id=789,
    )
    al3 = m.AccountLock(
        creditor_id=888,
        debtor_id=124,
        turn_id=1,
        collector_id=789,
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


def test_process_pristine_collectors(
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
            "process_pristine_collectors",
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
    db.session.add(t0)
    db.session.add(t1)
    db.session.add(t2)
    db.session.flush()
    db.session.expunge(t0)
    db.session.expunge(t1)
    db.session.expunge(t2)

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
    db.session.add(wt0)
    db.session.add(wt1)
    db.session.commit()

    assert len(m.WorkerTurn.query.all()) == 2
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
    wts.sort(key=lambda t: t.phase, reverse=True)
    assert len(wts) == 3
    assert wts[0].phase == 3
    assert wts[0].turn_id == t0.turn_id
    assert wts[0].started_at == t0.started_at
    assert wts[0].base_debtor_info_locator == t0.base_debtor_info_locator
    assert wts[0].base_debtor_id == t0.base_debtor_id
    assert wts[0].max_distance_to_base == t0.max_distance_to_base
    assert wts[0].min_trade_amount == t0.min_trade_amount
    assert wts[0].phase_deadline is None
    assert wts[0].collection_started_at == t0.collection_started_at
    assert wts[0].collection_deadline == t0.collection_deadline
    assert wts[1].phase == t1.phase
    assert wts[1].turn_id == t1.turn_id
    assert wts[1].started_at == t1.started_at
    assert wts[1].base_debtor_info_locator == t1.base_debtor_info_locator
    assert wts[1].base_debtor_id == t1.base_debtor_id
    assert wts[1].max_distance_to_base == t1.max_distance_to_base
    assert wts[1].min_trade_amount == t1.min_trade_amount
    assert wts[1].phase_deadline == t1.phase_deadline
    assert wts[1].collection_started_at == t1.collection_started_at
    assert wts[1].collection_deadline == t1.collection_deadline
    assert wts[2].phase == t2.phase
    assert wts[2].turn_id == t2.turn_id
    assert wts[2].started_at == t2.started_at
    assert wts[2].base_debtor_info_locator == t2.base_debtor_info_locator
    assert wts[2].base_debtor_id == t2.base_debtor_id
    assert wts[2].max_distance_to_base == t2.max_distance_to_base
    assert wts[2].min_trade_amount == t2.min_trade_amount
    assert wts[2].phase_deadline == t2.phase_deadline
    assert wts[2].collection_started_at == t2.collection_started_at
    assert wts[2].collection_deadline == t2.collection_deadline


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
        db_session, current_ts,
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
