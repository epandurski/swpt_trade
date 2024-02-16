import pytest
from swpt_pythonlib.utils import (
    calc_iri_routing_key,
    calc_bin_routing_key,
    i64_to_hex_routing_key,
)
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


@pytest.fixture()
def restore_sharding_realm(app):
    orig_sharding_realm = app.config["SHARDING_REALM"]
    orig_delete_parent_recs = app.config["DELETE_PARENT_SHARD_RECORDS"]
    yield
    app.config["DELETE_PARENT_SHARD_RECORDS"] = orig_delete_parent_recs
    app.config["SHARDING_REALM"] = orig_sharding_realm


def test_sharding_realm(app, restore_sharding_realm, db_session):
    app.config["SHARDING_REALM"] = ShardingRealm("1.#")
    app.config["DELETE_PARENT_SHARD_RECORDS"] = False

    signal1 = m.ConfirmDebtorSignal(
        debtor_id=1,
        debtor_info_locator="https://example.com",
    )  # correct realm
    signal2 = m.ConfirmDebtorSignal(
        debtor_id=3,
        debtor_info_locator="https://example.com",
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


def test_finalize_transfer_signal(db_session, current_ts):
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
        creditor_id=0,
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