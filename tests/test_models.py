import pytest
from datetime import date
from swpt_pythonlib.utils import (
    calc_iri_routing_key,
    calc_bin_routing_key,
    i64_to_hex_routing_key,
)
from datetime import timedelta
from swpt_pythonlib.utils import ShardingRealm
from swpt_trade import models as m
from swpt_trade import schemas


def test_sibnalbus_burst_count(app):
    assert isinstance(m.ConfigureAccountSignal.signalbus_burst_count, int)
    assert isinstance(m.PrepareTransferSignal.signalbus_burst_count, int)
    assert isinstance(m.FinalizeTransferSignal.signalbus_burst_count, int)
    assert isinstance(m.FetchDebtorInfoSignal.signalbus_burst_count, int)
    assert isinstance(m.DiscoverDebtorSignal.signalbus_burst_count, int)
    assert isinstance(m.ConfirmDebtorSignal.signalbus_burst_count, int)
    assert isinstance(m.ActivateCollectorSignal.signalbus_burst_count, int)
    assert isinstance(m.CandidateOfferSignal.signalbus_burst_count, int)
    assert isinstance(m.StoreDocumentSignal.signalbus_burst_count, int)
    assert isinstance(m.NeededCollectorSignal.signalbus_burst_count, int)
    assert isinstance(m.ReviseAccountLockSignal.signalbus_burst_count, int)
    assert isinstance(m.TriggerTransferSignal.signalbus_burst_count, int)
    assert isinstance(m.AccountIdRequestSignal.signalbus_burst_count, int)
    assert isinstance(m.AccountIdResponseSignal.signalbus_burst_count, int)
    assert isinstance(m.StartSendingSignal.signalbus_burst_count, int)
    assert isinstance(m.StartDispatchingSignal.signalbus_burst_count, int)
    assert isinstance(m.ReplayedAccountTransferSignal.signalbus_burst_count, int)


def test_sharding_realm(app, restore_sharding_realm, db_session, current_ts):
    app.config["SHARDING_REALM"] = ShardingRealm("1.#")
    app.config["DELETE_PARENT_SHARD_RECORDS"] = False

    # Sharded by debtor ID
    assert m.message_belongs_to_this_shard({
        "type": "DiscoverDebtor",
        "signal_id": 123,
        "debtor_id": 1,
        "iri": "https://example.com",
        "ts": "2024-01-01T10:00:00Z",
    })
    assert not m.message_belongs_to_this_shard({
        "type": "DiscoverDebtor",
        "signal_id": 123,
        "debtor_id": 3,
        "iri": "https://example.com",
        "ts": "2024-01-01T10:00:00Z",
    })

    # Sharded by collector ID
    assert m.message_belongs_to_this_shard({
        "type": "TriggerTransfer",
        "collector_id": 1,
        "ts": "2024-01-01T10:00:00Z",
    })
    assert not m.message_belongs_to_this_shard({
        "type": "TriggerTransfer",
        "collector_id": 3,
        "ts": "2024-01-01T10:00:00Z",
    })
    assert m.message_belongs_to_this_shard({
        "type": "AccountIdResponse",
        "collector_id": 1,
        "ts": "2024-01-01T10:00:00Z",
    })
    assert not m.message_belongs_to_this_shard({
        "type": "AccountIdResponse",
        "collector_id": 3,
        "ts": "2024-01-01T10:00:00Z",
    })

    # Sharded by debtor_info_locator
    assert m.message_belongs_to_this_shard({
        "type": "StoreDocument",
        "debtor_info_locator": "https://example.com/test",
        "debtor_id": 2,
        "ts": "2022-01-01T00:00:00Z",
    })
    assert not m.message_belongs_to_this_shard({
        "type": "StoreDocument",
        "debtor_info_locator": "https://example.com/test-other",
        "debtor_id": 2,
        "ts": "2022-01-01T00:00:00Z",
    })

    # Sharded by IRI
    assert m.message_belongs_to_this_shard({
        "type": "FetchDebtorInfo",
        "iri": "https://example.com/test",
        "debtor_id": 2,
        "is_locator_fetch": True,
        "is_discovery_fetch": False,
        "recursion_level": 5,
        "ts": "2022-01-01T00:00:00Z",
    })
    assert not m.message_belongs_to_this_shard({
        "type": "FetchDebtorInfo",
        "iri": "https://example.com/test-other",
        "debtor_id": 2,
        "is_locator_fetch": True,
        "is_discovery_fetch": False,
        "recursion_level": 5,
        "ts": "2022-01-01T00:00:00Z",
    })

    # Sharded by creditor ID
    signal1 = m.ConfigureAccountSignal(
        debtor_id=1,
        creditor_id=4294967299,
        ts=current_ts,
        seqnum=100,
        negligible_amount=3.14,
        config_data="test_config",
        config_flags=123,
    )  # correct realm
    signal2 = m.ConfigureAccountSignal(
        debtor_id=1,
        creditor_id=4294967298,
        ts=current_ts,
        seqnum=100,
        negligible_amount=3.14,
        config_data="test_config",
        config_flags=123,
    )  # incorrect realm

    db_session.add(signal1)
    db_session.add(signal2)
    db_session.flush()

    assert signal1._create_message() is not None

    with pytest.raises(RuntimeError):
        signal2._create_message()

    app.config["DELETE_PARENT_SHARD_RECORDS"] = True
    assert signal2._create_message() is None


def test_non_smp_signals(db_session):
    signal = m.FetchDebtorInfoSignal(
        iri="https://example.com",
        debtor_id=1,
        is_locator_fetch=True,
        is_discovery_fetch=False,
        recursion_level=0,
    )
    db_session.add(signal)
    db_session.flush()
    message = signal._create_message()
    assert message.mandatory
    assert message.properties.headers["message-type"] == "FetchDebtorInfo"
    assert message.properties.type == "FetchDebtorInfo"
    assert message.properties.delivery_mode == 2
    assert message.properties.content_type == "application/json"
    assert message.properties.app_id == "swpt_trade"
    assert b'https://example.com' in message.body
    assert b'creditor_id' not in message.body
    data = schemas.FetchDebtorInfoMessageSchema().loads(message.body)
    assert data['iri'] == "https://example.com"
    assert message.exchange == 'to_trade'
    assert message.routing_key == calc_iri_routing_key("https://example.com")

    signal = m.DiscoverDebtorSignal(
        iri="https://example.com",
        debtor_id=1,
        force_locator_refetch=False,
    )
    db_session.add(signal)
    db_session.flush()
    message = signal._create_message()
    assert message.mandatory
    assert message.properties.headers["message-type"] == "DiscoverDebtor"
    assert message.properties.type == "DiscoverDebtor"
    assert message.properties.delivery_mode == 2
    assert message.properties.content_type == "application/json"
    assert message.properties.app_id == "swpt_trade"
    assert b'https://example.com' in message.body
    assert b'creditor_id' not in message.body
    data = schemas.DiscoverDebtorMessageSchema().loads(message.body)
    assert data['iri'] == "https://example.com"
    assert message.exchange == 'to_trade'
    assert message.routing_key == calc_bin_routing_key(1)

    signal = m.ConfirmDebtorSignal(
        debtor_id=1,
        debtor_info_locator="https://example.com",
    )
    db_session.add(signal)
    db_session.flush()
    message = signal._create_message()
    assert message.mandatory
    assert message.properties.headers["message-type"] == "ConfirmDebtor"
    assert message.properties.type == "ConfirmDebtor"
    assert message.properties.delivery_mode == 2
    assert message.properties.content_type == "application/json"
    assert message.properties.app_id == "swpt_trade"
    assert b'https://example.com' in message.body
    assert b'creditor_id' not in message.body
    data = schemas.ConfirmDebtorMessageSchema().loads(message.body)
    assert data['debtor_info_locator'] == "https://example.com"
    assert message.exchange == 'to_trade'
    assert message.routing_key == calc_bin_routing_key(1)


def test_configure_account_signal(db_session, current_ts):
    signal = m.ConfigureAccountSignal(
        debtor_id=1,
        creditor_id=4294967297,
        ts=current_ts,
        seqnum=100,
        negligible_amount=3.14,
        config_data="test_config",
        config_flags=123,
    )
    db_session.add(signal)
    db_session.flush()
    message = signal._create_message()
    assert not message.mandatory
    assert message.properties.headers["message-type"] == "ConfigureAccount"
    assert message.properties.headers["creditor-id"] == 4294967297
    assert message.properties.headers["debtor-id"] == 1
    assert message.properties.type == "ConfigureAccount"
    assert message.properties.delivery_mode == 2
    assert message.properties.content_type == "application/json"
    assert message.properties.app_id == "swpt_trade"
    assert b'test_config' in message.body
    assert b'creditor_id' in message.body
    assert message.exchange == 'creditors_out'
    assert message.routing_key == i64_to_hex_routing_key(1)


def test_finalize_transfer_signal(
        db_session,
        app,
        restore_sharding_realm,
        current_ts,
):
    app.config["SHARDING_REALM"] = ShardingRealm("0.#")
    app.config["DELETE_PARENT_SHARD_RECORDS"] = False

    signal = m.FinalizeTransferSignal(
        debtor_id=1,
        creditor_id=4294967297,
        transfer_id=4567,
        coordinator_id=4294967298,
        coordinator_request_id=112233,
        committed_amount=1000,
        transfer_note_format="-",
        transfer_note="test_note"
    )
    db_session.add(signal)
    db_session.flush()
    message = signal._create_message()
    assert message.mandatory
    assert message.properties.headers["message-type"] == "FinalizeTransfer"
    assert message.properties.headers["creditor-id"] == 4294967297
    assert message.properties.headers["debtor-id"] == 1
    assert message.properties.headers["coordinator-id"] == 4294967298
    assert message.properties.headers["coordinator-type"] == "agent"
    assert message.properties.type == "FinalizeTransfer"
    assert message.properties.delivery_mode == 2
    assert message.properties.content_type == "application/json"
    assert message.properties.app_id == "swpt_trade"
    assert b'test_note' in message.body
    assert b'creditor_id' in message.body
    assert message.exchange == 'creditors_out'
    assert message.routing_key == i64_to_hex_routing_key(1)

    signal = m.FinalizeTransferSignal(
        debtor_id=1,
        creditor_id=2,
        transfer_id=4567,
        coordinator_id=4294967298,
        coordinator_request_id=112233,
        committed_amount=1000,
        transfer_note_format="-",
        transfer_note="test_note"
    )
    db_session.add(signal)
    db_session.flush()

    with pytest.raises(RuntimeError):
        signal._create_message()


def test_document_has_expired(current_ts):
    document = m.DebtorInfoDocument(
        debtor_info_locator="https://example.com/666",
        debtor_id=666,
        peg_debtor_info_locator=None,
        peg_debtor_id=None,
        peg_exchange_rate=None,
        will_not_change_until=None,
        fetched_at=current_ts - timedelta(days=10),
    )
    assert document.has_expired(current_ts, timedelta(days=7))

    document.will_not_change_until = current_ts + timedelta(days=1)
    assert not document.has_expired(current_ts, timedelta(days=7))


def test_trading_policy_is_useless(current_ts):
    tp = m.TradingPolicy(
        creditor_id=777,
        debtor_id=666,
        latest_ledger_update_id=1,
        latest_ledger_update_ts=current_ts,
        account_id="",
        creation_date=m.DATE0,
        principal=0,
        last_transfer_number=0,
        latest_policy_update_id=2,
        latest_policy_update_ts=current_ts,
        policy_name=None,
        min_principal=m.MIN_INT64,
        max_principal=m.MAX_INT64,
        peg_debtor_id=None,
        peg_exchange_rate=None,
        latest_flags_update_id=3,
        latest_flags_update_ts=current_ts,
        config_flags=m.DEFAULT_CONFIG_FLAGS,
    )
    assert tp.is_useless

    tp.principal = 1000
    assert not tp.is_useless


def test_account_lock_is_in_force(current_ts):
    al = m.AccountLock(
        creditor_id=777,
        debtor_id=666,
        turn_id=1,
        collector_id=123,
        released_at=current_ts,
        initiated_at=current_ts,
        coordinator_request_id=456,
        transfer_id=678,
        amount=1000,
        finalized_at=None,
        account_creation_date=date(2024, 5, 1),
        account_last_transfer_number=321,
    )
    assert al.is_in_force(date(2024, 4, 1), 322)
    assert al.is_in_force(date(2024, 5, 1), 320)
    assert not al.is_in_force(date(2024, 5, 1), 321)
    assert not al.is_in_force(date(2024, 5, 1), 322)

    al.released_at = None
    assert al.is_in_force(date(2024, 4, 1), 322)
    assert al.is_in_force(date(2024, 5, 1), 320)
    assert al.is_in_force(date(2024, 5, 1), 321)
    assert al.is_in_force(date(2024, 5, 1), 322)


def test_dispatching_status_properties(current_ts):
    ds = m.DispatchingStatus(
        collector_id=666,
        turn_id=1,
        debtor_id=1,
        amount_to_collect=50000,
        total_collected_amount=None,
        amount_to_send=5000,
        all_sent=False,
        amount_to_receive=10000,
        number_to_receive=1,
        total_received_amount=None,
        amount_to_dispatch=(50000 + 10000 - 5000),
    )
    assert not ds.finished_collecting
    assert not ds.all_collected
    assert not ds.finished_receiving
    assert not ds.all_sent
    assert not ds.all_received
    assert ds.available_amount_to_send == 0
    assert ds.available_amount_to_dispatch == 0

    ds.total_collected_amount = 46000
    ds.all_sent = True
    assert ds.finished_collecting
    assert ds.all_sent
    assert ds.available_amount_to_send == 1000
    assert ds.available_amount_to_dispatch == 45000

    ds.total_received_amount = 9900
    ds.all_received = True
    assert ds.all_received
    assert ds.available_amount_to_send == 1000
    assert ds.available_amount_to_dispatch == 46000 - 1000 + 9900


def test_transfer_attempt_properties(current_ts):
    ta = m.TransferAttempt(
        collector_id=666,
        turn_id=1,
        debtor_id=1,
        is_dispatching=True,
        nominal_amount=1000.0,
        collection_started_at=current_ts,
        recipient="",
        recipient_version=0,
        rescheduled_for=None,
        attempted_at=None,
        coordinator_request_id=None,
        final_interest_rate_ts=None,
        amount=None,
        transfer_id=None,
        finalized_at=None,
        failure_code=None,
        backoff_counter=0,
    )
    assert ta.unknown_recipient
    assert not ta.can_be_triggered
    assert ta.calc_backoff_seconds(10) == 10
    assert ta.calc_backoff_seconds(33) == 33
    assert ta.calc_backoff_seconds(0) == 0
    assert ta.calc_backoff_seconds(-10) == 0

    # The first increment is not by 1, but by 3.
    ta.increment_backoff_counter()
    assert ta.backoff_counter == 3
    assert ta.calc_backoff_seconds(10) == (2 ** 3) * 10
    assert ta.calc_backoff_seconds(33) == (2 ** 3) * 33

    ta.increment_backoff_counter()
    assert ta.backoff_counter == 4
    assert ta.calc_backoff_seconds(10) == (2 ** 4) * 10
    assert ta.calc_backoff_seconds(33) == (2 ** 4) * 33

    # `calc_backoff_seconds()` never exceeds MAX_INT32.
    ta.backoff_counter = 10000
    assert ta.calc_backoff_seconds(1) == 2 ** 31 - 1
    assert ta.calc_backoff_seconds(1000) == 2 ** 31 - 1

    # `ta.backoff_counter` never exceeds MAX_INT16.
    ta.backoff_counter = 2 ** 15 - 2
    ta.increment_backoff_counter()
    ta.increment_backoff_counter()
    ta.increment_backoff_counter()
    assert ta.backoff_counter == 2 ** 15 - 1

    ta.recipient = "123456"
    ta.recipient_version = 1
    assert not ta.unknown_recipient
    assert ta.can_be_triggered

    ta.failure_code = m.TransferAttempt.RECIPIENT_IS_UNREACHABLE
    assert ta.unknown_recipient
    assert ta.can_be_triggered

    ta.failure_code = None
    ta.rescheduled_for = current_ts
    assert not ta.can_be_triggered

    ta.rescheduled_for = None
    ta.attempted_at = current_ts
    ta.coordinator_request_id = 123
    assert not ta.can_be_triggered

    ta.failure_code = m.TransferAttempt.TIMEOUT
    assert ta.can_be_triggered

    ta.backoff_counter = 2
    ta.reschedule_failed_attempt(m.TransferAttempt.UNSPECIFIED_FAILURE, 10)
    assert not ta.can_be_triggered
    assert ta.failure_code == m.TransferAttempt.UNSPECIFIED_FAILURE
    assert ta.rescheduled_for == current_ts + timedelta(seconds=10 * (2 ** 2))
    assert ta.backoff_counter == 3

    ta.fatal_error = "Uups!"
    ta.rescheduled_for = None
    assert not ta.can_be_triggered
