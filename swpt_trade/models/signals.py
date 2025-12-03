from __future__ import annotations
from flask import current_app
from sqlalchemy.dialects import postgresql as pg
from marshmallow import Schema, fields
from swpt_pythonlib.utils import (
    i64_to_hex_routing_key,
    calc_bin_routing_key,
    calc_iri_routing_key,
)
from swpt_trade.extensions import (
    db,
    CREDITORS_OUT_EXCHANGE,
    TO_TRADE_EXCHANGE,
)
from .common import Signal, CT_AGENT


class classproperty(object):
    def __init__(self, f):
        self.f = f

    def __get__(self, obj, owner):
        return self.f(owner)


class ConfigureAccountSignal(Signal):
    exchange_name = CREDITORS_OUT_EXCHANGE

    class __marshmallow__(Schema):
        type = fields.Constant("ConfigureAccount")
        debtor_id = fields.Integer()
        creditor_id = fields.Integer()
        ts = fields.DateTime()
        seqnum = fields.Integer()
        negligible_amount = fields.Float()
        config_data = fields.String()
        config_flags = fields.Integer()

    __marshmallow_schema__ = __marshmallow__()

    creditor_id = db.Column(db.BigInteger, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    ts = db.Column(db.TIMESTAMP(timezone=True), primary_key=True)
    seqnum = db.Column(db.Integer, primary_key=True)
    negligible_amount = db.Column(db.REAL, nullable=False)
    config_data = db.Column(db.String, nullable=False, default="")
    config_flags = db.Column(db.Integer, nullable=False)

    @property
    def routing_key(self):  # pragma: no cover
        return i64_to_hex_routing_key(self.debtor_id)

    @classproperty
    def signalbus_burst_count(self):
        return current_app.config["APP_FLUSH_CONFIGURE_ACCOUNTS_BURST_COUNT"]


class PrepareTransferSignal(Signal):
    exchange_name = CREDITORS_OUT_EXCHANGE

    class __marshmallow__(Schema):
        type = fields.Constant("PrepareTransfer")
        creditor_id = fields.Integer()
        debtor_id = fields.Integer()
        coordinator_type = fields.Constant(CT_AGENT)
        coordinator_id = fields.Integer(
            attribute="creditor_id", dump_only=True
        )
        coordinator_request_id = fields.Integer()
        min_locked_amount = fields.Integer()
        max_locked_amount = fields.Integer()
        recipient = fields.String()
        final_interest_rate_ts = fields.DateTime()
        max_commit_delay = fields.Integer()
        inserted_at = fields.DateTime(data_key="ts")

    __marshmallow_schema__ = __marshmallow__()

    creditor_id = db.Column(db.BigInteger, primary_key=True)
    coordinator_request_id = db.Column(db.BigInteger, primary_key=True)
    debtor_id = db.Column(db.BigInteger, nullable=False)
    recipient = db.Column(db.String, nullable=False)
    min_locked_amount = db.Column(db.BigInteger, nullable=False)
    max_locked_amount = db.Column(db.BigInteger, nullable=False)
    final_interest_rate_ts = db.Column(
        db.TIMESTAMP(timezone=True), nullable=False
    )
    max_commit_delay = db.Column(db.Integer, nullable=False)

    @property
    def routing_key(self):  # pragma: no cover
        return i64_to_hex_routing_key(self.debtor_id)

    @classproperty
    def signalbus_burst_count(self):
        return current_app.config["APP_FLUSH_PREPARE_TRANSFERS_BURST_COUNT"]


class FinalizeTransferSignal(Signal):
    exchange_name = CREDITORS_OUT_EXCHANGE

    class __marshmallow__(Schema):
        type = fields.Constant("FinalizeTransfer")
        creditor_id = fields.Integer()
        debtor_id = fields.Integer()
        transfer_id = fields.Integer()
        coordinator_type = fields.Constant(CT_AGENT)
        coordinator_id = fields.Integer()
        coordinator_request_id = fields.Integer()
        committed_amount = fields.Integer()
        transfer_note_format = fields.String()
        transfer_note = fields.String()
        inserted_at = fields.DateTime(data_key="ts")

    __marshmallow_schema__ = __marshmallow__()

    creditor_id = db.Column(db.BigInteger, primary_key=True)
    signal_id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    debtor_id = db.Column(db.BigInteger, nullable=False)
    transfer_id = db.Column(db.BigInteger, nullable=False)
    coordinator_id = db.Column(db.BigInteger, nullable=False)
    coordinator_request_id = db.Column(db.BigInteger, nullable=False)
    committed_amount = db.Column(db.BigInteger, nullable=False)
    transfer_note_format = db.Column(db.String, nullable=False)
    transfer_note = db.Column(db.String, nullable=False)

    @property
    def routing_key(self):  # pragma: no cover
        return i64_to_hex_routing_key(self.debtor_id)

    @classproperty
    def signalbus_burst_count(self):
        return current_app.config["APP_FLUSH_FINALIZE_TRANSFERS_BURST_COUNT"]


class FetchDebtorInfoSignal(Signal):
    """Requests a debtor info document to be fetched from Internet.
    """
    exchange_name = TO_TRADE_EXCHANGE

    class __marshmallow__(Schema):
        type = fields.Constant("FetchDebtorInfo")
        iri = fields.String()
        debtor_id = fields.Integer()
        is_locator_fetch = fields.Boolean()
        is_discovery_fetch = fields.Boolean()
        forced_iri = fields.String()
        recursion_level = fields.Integer()
        inserted_at = fields.DateTime(data_key="ts")

    __marshmallow_schema__ = __marshmallow__()

    signal_id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    iri = db.Column(db.String, nullable=False)
    debtor_id = db.Column(db.BigInteger, nullable=False)
    is_locator_fetch = db.Column(db.BOOLEAN, nullable=False)
    is_discovery_fetch = db.Column(db.BOOLEAN, nullable=False)
    forced_iri = db.Column(db.String)
    recursion_level = db.Column(db.SmallInteger, nullable=False)

    @property
    def routing_key(self):  # pragma: no cover
        return calc_iri_routing_key(self.iri)

    @classproperty
    def signalbus_burst_count(self):
        return current_app.config["APP_FLUSH_FETCH_DEBTOR_INFO_BURST_COUNT"]


class StoreDocumentSignal(Signal):
    """Requests a debtor info document to be stored.

    NOTE: Instead of sending a `StoreDocumentSignal`, a simpler
    approach would be to directly "upsert" the document in the
    database. The problem with this is that it is prone to database
    serialization errors, because documents are usually fetched and
    stored in large batches.
    """
    exchange_name = TO_TRADE_EXCHANGE

    class __marshmallow__(Schema):
        type = fields.Constant("StoreDocument")
        debtor_info_locator = fields.String()
        debtor_id = fields.Integer()
        peg_debtor_info_locator = fields.String()
        peg_debtor_id = fields.Integer()
        peg_exchange_rate = fields.Float()
        will_not_change_until = fields.DateTime()
        inserted_at = fields.DateTime(data_key="ts")

    __marshmallow_schema__ = __marshmallow__()

    signal_id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    debtor_info_locator = db.Column(db.String, nullable=False)
    debtor_id = db.Column(db.BigInteger, nullable=False)
    peg_debtor_info_locator = db.Column(db.String)
    peg_debtor_id = db.Column(db.BigInteger)
    peg_exchange_rate = db.Column(db.FLOAT)
    will_not_change_until = db.Column(db.TIMESTAMP(timezone=True))

    @property
    def routing_key(self):  # pragma: no cover
        return calc_iri_routing_key(self.debtor_info_locator)

    @classproperty
    def signalbus_burst_count(self):
        return current_app.config["APP_FLUSH_STORE_DOCUMENT_BURST_COUNT"]


class DiscoverDebtorSignal(Signal):
    """Starts the debtor-confirmation process for a given debtor.

    The `iri` field specifies an Internationalized Resource Identifier
    (IRI), from which a debtor info document for the given debtor can
    be fetched. Note that normally the given IRI will not be the same
    as the debtor's "debtor info locator". When
    `force_locator_refetch` is `True`, this means that the debtor's
    debtor info document has been changed, and should be re-fetched
    even if a cached version exist which has not expired yet.

    NOTE: A `DiscoverDebtorSignal` should be sent periodically for
    every collector account which is "alive". The easiest way to
    achieve this is to arrange received SMP `UpdateAccount` messages
    (aka account heartbeats) to trigger the sending of
    `DiscoverDebtorSignal`s.
    """
    exchange_name = TO_TRADE_EXCHANGE

    class __marshmallow__(Schema):
        type = fields.Constant("DiscoverDebtor")
        debtor_id = fields.Integer()
        iri = fields.String()
        force_locator_refetch = fields.Boolean()
        inserted_at = fields.DateTime(data_key="ts")

    __marshmallow_schema__ = __marshmallow__()

    signal_id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    debtor_id = db.Column(db.BigInteger, nullable=False)
    force_locator_refetch = db.Column(db.Boolean, nullable=False)
    iri = db.Column(db.String, nullable=False)

    @property
    def routing_key(self):  # pragma: no cover
        return calc_bin_routing_key(self.debtor_id)

    @classproperty
    def signalbus_burst_count(self):
        return current_app.config["APP_FLUSH_DISCOVER_DEBTOR_BURST_COUNT"]


class ConfirmDebtorSignal(Signal):
    """Finalizes the debtor-confirmation process for a given debtor.

    This signal informs that the given debtor successfully claimed the
    given debtor info locator.
    """
    exchange_name = TO_TRADE_EXCHANGE

    class __marshmallow__(Schema):
        type = fields.Constant("ConfirmDebtor")
        debtor_id = fields.Integer()
        debtor_info_locator = fields.String()
        inserted_at = fields.DateTime(data_key="ts")

    __marshmallow_schema__ = __marshmallow__()

    signal_id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    debtor_id = db.Column(db.BigInteger, nullable=False)
    debtor_info_locator = db.Column(db.String, nullable=False)

    @property
    def routing_key(self):  # pragma: no cover
        return calc_bin_routing_key(self.debtor_id)

    @classproperty
    def signalbus_burst_count(self):
        return current_app.config["APP_FLUSH_CONFIRM_DEBTOR_BURST_COUNT"]


class CandidateOfferSignal(Signal):
    """Represent a candidate offer generated for a given trading turn.

    The `amount` field can be negative (the trader wants to sell), or
    positive (the trader wants to buy). The amount can not be zero,
    nor MIN_INT64.
    """
    exchange_name = TO_TRADE_EXCHANGE

    class __marshmallow__(Schema):
        type = fields.Constant("CandidateOffer")
        turn_id = fields.Integer()
        debtor_id = fields.Integer()
        creditor_id = fields.Integer()
        amount = fields.Integer()
        account_creation_date = fields.Date()
        last_transfer_number = fields.Integer()
        inserted_at = fields.DateTime(data_key="ts")

    __marshmallow_schema__ = __marshmallow__()

    turn_id = db.Column(db.SmallInteger, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    amount = db.Column(db.BigInteger, nullable=False)
    account_creation_date = db.Column(db.DATE, nullable=False)
    last_transfer_number = db.Column(db.BigInteger, nullable=False)

    @property
    def routing_key(self):  # pragma: no cover
        return calc_bin_routing_key(self.creditor_id)

    @classproperty
    def signalbus_burst_count(self):
        return current_app.config["APP_FLUSH_CANDIDATE_OFFER_BURST_COUNT"]


class NeededCollectorSignal(Signal):
    """Call for the creation of a collector account for a given
    currency.
    """
    exchange_name = TO_TRADE_EXCHANGE

    class __marshmallow__(Schema):
        type = fields.Constant("NeededCollector")
        debtor_id = fields.Integer()
        inserted_at = fields.DateTime(data_key="ts")

    __marshmallow_schema__ = __marshmallow__()

    signal_id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    debtor_id = db.Column(db.BigInteger, nullable=False)

    @property
    def routing_key(self):  # pragma: no cover
        return calc_bin_routing_key(self.debtor_id)

    @classproperty
    def signalbus_burst_count(self):
        return current_app.config["APP_FLUSH_NEEDED_COLLECTOR_BURST_COUNT"]


class ReviseAccountLockSignal(Signal):
    """A call for resolving the given account lock.
    """
    exchange_name = TO_TRADE_EXCHANGE

    class __marshmallow__(Schema):
        type = fields.Constant("ReviseAccountLock")
        creditor_id = fields.Integer()
        debtor_id = fields.Integer()
        turn_id = fields.Integer()
        inserted_at = fields.DateTime(data_key="ts")

    __marshmallow_schema__ = __marshmallow__()

    signal_id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    creditor_id = db.Column(db.BigInteger, nullable=False)
    debtor_id = db.Column(db.BigInteger, nullable=False)
    turn_id = db.Column(db.Integer, nullable=False)

    @property
    def routing_key(self):  # pragma: no cover
        return calc_bin_routing_key(self.creditor_id)

    @classproperty
    def signalbus_burst_count(self):
        return current_app.config["APP_FLUSH_REVISE_ACCOUNT_LOCK_BURST_COUNT"]


class TriggerTransferSignal(Signal):
    """Triggers a transfer attempt.
    """
    exchange_name = TO_TRADE_EXCHANGE

    class __marshmallow__(Schema):
        type = fields.Constant("TriggerTransfer")
        collector_id = fields.Integer()
        turn_id = fields.Integer()
        debtor_id = fields.Integer()
        creditor_id = fields.Integer()
        is_dispatching = fields.Boolean()
        inserted_at = fields.DateTime(data_key="ts")

    __marshmallow_schema__ = __marshmallow__()

    signal_id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    collector_id = db.Column(db.BigInteger, nullable=False)
    turn_id = db.Column(db.Integer, nullable=False)
    debtor_id = db.Column(db.BigInteger, nullable=False)
    creditor_id = db.Column(db.BigInteger, nullable=False)
    is_dispatching = db.Column(db.BOOLEAN, nullable=False)

    @property
    def routing_key(self):  # pragma: no cover
        return calc_bin_routing_key(self.collector_id)

    @classproperty
    def signalbus_burst_count(self):
        return current_app.config["APP_FLUSH_TRIGGER_TRANSFER_BURST_COUNT"]


class AccountIdRequestSignal(Signal):
    """Requests the account ID of a given creditor.

    This signal is sent when a transfer attempt must be performed (see
    the `TransferAttempt` model), but the recipient's account ID is
    unknown.
    """
    exchange_name = TO_TRADE_EXCHANGE

    class __marshmallow__(Schema):
        type = fields.Constant("AccountIdRequest")
        collector_id = fields.Integer()
        turn_id = fields.Integer()
        debtor_id = fields.Integer()
        creditor_id = fields.Integer()
        is_dispatching = fields.Boolean()
        inserted_at = fields.DateTime(data_key="ts")

    __marshmallow_schema__ = __marshmallow__()

    signal_id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    collector_id = db.Column(db.BigInteger, nullable=False)
    turn_id = db.Column(db.Integer, nullable=False)
    debtor_id = db.Column(db.BigInteger, nullable=False)
    creditor_id = db.Column(db.BigInteger, nullable=False)
    is_dispatching = db.Column(db.BOOLEAN, nullable=False)

    @property
    def routing_key(self):  # pragma: no cover
        return calc_bin_routing_key(self.creditor_id)

    @classproperty
    def signalbus_burst_count(self):
        return current_app.config["APP_FLUSH_ACCOUNT_ID_REQUEST_BURST_COUNT"]


class AccountIdResponseSignal(Signal):
    """A response to an `AccountIdRequestSignal`.
    """
    exchange_name = TO_TRADE_EXCHANGE

    class __marshmallow__(Schema):
        type = fields.Constant("AccountIdResponse")
        collector_id = fields.Integer()
        turn_id = fields.Integer()
        debtor_id = fields.Integer()
        creditor_id = fields.Integer()
        is_dispatching = fields.Boolean()
        account_id = fields.String()
        account_id_version = fields.Integer()
        inserted_at = fields.DateTime(data_key="ts")

    __marshmallow_schema__ = __marshmallow__()

    signal_id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    collector_id = db.Column(db.BigInteger, nullable=False)
    turn_id = db.Column(db.Integer, nullable=False)
    debtor_id = db.Column(db.BigInteger, nullable=False)
    creditor_id = db.Column(db.BigInteger, nullable=False)
    is_dispatching = db.Column(db.BOOLEAN, nullable=False)
    account_id = db.Column(db.String, nullable=False)
    account_id_version = db.Column(db.BigInteger, nullable=False)

    @property
    def routing_key(self):  # pragma: no cover
        return calc_bin_routing_key(self.collector_id)

    @classproperty
    def signalbus_burst_count(self):
        return current_app.config["APP_FLUSH_ACCOUNT_ID_RESPONSE_BURST_COUNT"]


class StartSendingSignal(Signal):
    """Trigger sending for a given `DispatchingStatus` record.
    """
    exchange_name = TO_TRADE_EXCHANGE

    class __marshmallow__(Schema):
        type = fields.Constant("StartSending")
        collector_id = fields.Integer()
        turn_id = fields.Integer()
        debtor_id = fields.Integer()
        inserted_at = fields.DateTime(data_key="ts")

    __marshmallow_schema__ = __marshmallow__()

    signal_id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    collector_id = db.Column(db.BigInteger, nullable=False)
    turn_id = db.Column(db.Integer, nullable=False)
    debtor_id = db.Column(db.BigInteger, nullable=False)

    @property
    def routing_key(self):  # pragma: no cover
        return calc_bin_routing_key(self.collector_id)

    @classproperty
    def signalbus_burst_count(self):
        return current_app.config["APP_FLUSH_START_SENDING_BURST_COUNT"]


class StartDispatchingSignal(Signal):
    """Trigger dispatching for a given `DispatchingStatus` record.
    """
    exchange_name = TO_TRADE_EXCHANGE

    class __marshmallow__(Schema):
        type = fields.Constant("StartDispatching")
        collector_id = fields.Integer()
        turn_id = fields.Integer()
        debtor_id = fields.Integer()
        inserted_at = fields.DateTime(data_key="ts")

    __marshmallow_schema__ = __marshmallow__()

    signal_id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    collector_id = db.Column(db.BigInteger, nullable=False)
    turn_id = db.Column(db.Integer, nullable=False)
    debtor_id = db.Column(db.BigInteger, nullable=False)

    @property
    def routing_key(self):  # pragma: no cover
        return calc_bin_routing_key(self.collector_id)

    @classproperty
    def signalbus_burst_count(self):
        return current_app.config["APP_FLUSH_START_DISPATCHING_BURST_COUNT"]


class CalculateSurplusSignal(Signal):
    """Trigger the calculation of surplus for a given `WorkerAccount`.
    """
    exchange_name = TO_TRADE_EXCHANGE

    class __marshmallow__(Schema):
        type = fields.Constant("CalculateSurplus")
        collector_id = fields.Integer()
        debtor_id = fields.Integer()
        inserted_at = fields.DateTime(data_key="ts")

    __marshmallow_schema__ = __marshmallow__()

    signal_id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    collector_id = db.Column(db.BigInteger, nullable=False)
    debtor_id = db.Column(db.BigInteger, nullable=False)

    @property
    def routing_key(self):  # pragma: no cover
        return calc_bin_routing_key(self.collector_id)

    @classproperty
    def signalbus_burst_count(self):
        return current_app.config["APP_FLUSH_CALCULATE_SURPLUS_BURST_COUNT"]


class ReplayedAccountTransferSignal(Signal):
    """Replays an already received `AccountTransfer` message.
    """
    exchange_name = TO_TRADE_EXCHANGE

    class __marshmallow__(Schema):
        type = fields.Constant("AccountTransfer")
        creditor_id = fields.Integer()
        debtor_id = fields.Integer()
        creation_date = fields.Date()
        transfer_number = fields.Integer()
        coordinator_type = fields.String()
        committed_at = fields.DateTime()
        acquired_amount = fields.Integer()
        transfer_note_format = fields.String()
        transfer_note = fields.String()
        principal = fields.Integer()
        previous_transfer_number = fields.Integer()
        sender = fields.String()
        recipient = fields.String()
        ts = fields.DateTime()

    __marshmallow_schema__ = __marshmallow__()

    signal_id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    creditor_id = db.Column(db.BigInteger, nullable=False)
    debtor_id = db.Column(db.BigInteger, nullable=False)
    creation_date = db.Column(db.DATE, nullable=False)
    transfer_number = db.Column(db.BigInteger, nullable=False)
    coordinator_type = db.Column(db.String(30), nullable=False)
    committed_at = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
    acquired_amount = db.Column(db.BigInteger, nullable=False)
    transfer_note_format = db.Column(pg.TEXT, nullable=False)
    transfer_note = db.Column(pg.TEXT, nullable=False)
    principal = db.Column(db.BigInteger, nullable=False)
    previous_transfer_number = db.Column(db.BigInteger, nullable=False)
    sender = db.Column(db.String(100), nullable=False)
    recipient = db.Column(db.String(100), nullable=False)
    ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False)

    @property
    def routing_key(self):  # pragma: no cover
        return calc_bin_routing_key(self.creditor_id)

    @classproperty
    def signalbus_burst_count(self):
        return current_app.config[
            "APP_FLUSH_REPLAYED_ACCOUNT_TRANSFERS_BURST_COUNT"
        ]
