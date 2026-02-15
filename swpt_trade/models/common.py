from __future__ import annotations
import json
from datetime import datetime, timezone
from flask import current_app
from sqlalchemy import text
from sqlalchemy.inspection import inspect
from swpt_trade.extensions import db
from swpt_pythonlib import rabbitmq
from swpt_pythonlib.utils import ShardingRealm
from swpt_trade.utils import calc_hash

MIN_INT16 = -1 << 15
MAX_INT16 = (1 << 15) - 1
MIN_INT32 = -1 << 31
MAX_INT32 = (1 << 31) - 1
MIN_INT64 = -1 << 63
MAX_INT64 = (1 << 63) - 1
MAX_UINT64 = (1 << 64) - 1
HUGE_NEGLIGIBLE_AMOUNT = 1e30
ROOT_CREDITOR_ID = 0
SECONDS_IN_DAY = 24 * 60 * 60
SECONDS_IN_YEAR = 365.25 * SECONDS_IN_DAY
TS0 = datetime(1970, 1, 1, tzinfo=timezone.utc)
T_INFINITY = datetime(9999, 12, 31, 23, 59, 59, tzinfo=timezone.utc)
DATE0 = TS0.date()
TRANSFER_NOTE_MAX_BYTES = 500
AGENT_TRANSFER_NOTE_FORMAT = "-cXchge"
DEFAULT_CONFIG_FLAGS = 0
ACCOUNT_ID_MAX_BYTES = 100
CT_AGENT = "agent"
SET_SEQSCAN_ON = text("SET LOCAL enable_seqscan = on")
SET_SEQSCAN_OFF = text("SET LOCAL enable_seqscan = off")
SET_FORCE_CUSTOM_PLAN = text("SET LOCAL plan_cache_mode = force_custom_plan")
SET_DEFAULT_PLAN_CACHE_MODE = text("SET LOCAL plan_cache_mode = DEFAULT")
SET_STATISTICS_TARGET = text("SET LOCAL default_statistics_target = 1")
DISCARD_PLANS = text("DISCARD PLANS")

SMP_MESSAGE_TYPES = set([
    "ConfigureAccount",
    "RejectedConfig",
    "AccountUpdate",
    "PrepareTransfer",
    "RejectedTransfer",
    "PreparedTransfer",
    "FinalizeTransfer",
    "FinalizedTransfer",
    "AccountTransfer",
    "AccountPurge",
])
CREDITOR_ID_SHARDED_MESSAGE_TYPES = SMP_MESSAGE_TYPES | set([
    "UpdatedLedger",
    "UpdatedPolicy",
    "UpdatedFlags",
    "CandidateOffer",
    "ReviseAccountLock",
    "AccountIdRequest",
])
DEBTOR_ID_SHARDED_MESSAGE_TYPES = set([
    "DiscoverDebtor",
    "ConfirmDebtor",
    "NeededCollector",
])
COLLECTOR_ID_SHARDED_MESSAGE_TYPES = set([
    "TriggerTransfer",
    "AccountIdResponse",
    "StartSending",
    "StartDispatching",
    "CalculateSurplus",
])
IRI_SHARDED_MESSAGE_TYPES = set([
    "FetchDebtorInfo",
])
DEBTOR_INFO_LOCATOR_SHARDED_MESSAGE_TYPES = set([
    "StoreDocument",
])


def get_now_utc() -> datetime:
    return datetime.now(tz=timezone.utc)


def calc_i64_column_hash(context, column_name) -> int:
    return calc_hash(context.get_current_parameters()[column_name])


def message_belongs_to_this_shard(
        data: dict,
        match_parent: bool = False,
) -> bool:
    sharding_realm: ShardingRealm = current_app.config["SHARDING_REALM"]
    message_type = data["type"]

    if message_type in CREDITOR_ID_SHARDED_MESSAGE_TYPES:
        return sharding_realm.match(
            data["creditor_id"], match_parent=match_parent
        )
    elif message_type in DEBTOR_ID_SHARDED_MESSAGE_TYPES:
        return sharding_realm.match(
            data["debtor_id"], match_parent=match_parent
        )
    elif message_type in COLLECTOR_ID_SHARDED_MESSAGE_TYPES:
        return sharding_realm.match(
            data["collector_id"], match_parent=match_parent
        )
    elif message_type in IRI_SHARDED_MESSAGE_TYPES:
        return sharding_realm.match_str(
            data["iri"], match_parent=match_parent
        )
    elif message_type in DEBTOR_INFO_LOCATOR_SHARDED_MESSAGE_TYPES:
        return sharding_realm.match_str(
            data["debtor_info_locator"], match_parent=match_parent
        )
    else:  # pragma: no cover
        raise RuntimeError("Unknown message type.")


class ChooseRowsMixin:
    @classmethod
    def choose_rows(cls, primary_keys: list[tuple], name: str = "chosen"):
        pktype_name = f"{cls.__table__.name}_pktype"
        bindparam_name = f"{name}_rows"
        return (
            text(f"SELECT * FROM unnest(:{bindparam_name} :: {pktype_name}[])")
            .bindparams(**{bindparam_name: primary_keys})
            .columns(**{c.key: c.type for c in inspect(cls).primary_key})
            .cte(name=name)
        )


class Signal(db.Model, ChooseRowsMixin):
    __abstract__ = True

    @classmethod
    def send_signalbus_messages(cls, objects):  # pragma: no cover
        assert all(isinstance(obj, cls) for obj in objects)
        messages = (obj._create_message() for obj in objects)
        cls.publisher.publish_messages([m for m in messages if m is not None])

    def send_signalbus_message(self):  # pragma: no cover
        self.send_signalbus_messages([self])

    def _create_message(self):
        data = self.__marshmallow_schema__.dump(self)
        message_type = data["type"]
        headers = {"message-type": message_type}
        is_smp_message = message_type in SMP_MESSAGE_TYPES

        if is_smp_message:
            if not message_belongs_to_this_shard(data):
                if (
                    current_app.config["DELETE_PARENT_SHARD_RECORDS"]
                    and message_belongs_to_this_shard(data, match_parent=True)
                ):
                    # This message most probably is a left-over from the
                    # previous splitting of the parent shard into children
                    # shards. Therefore we should just ignore it.
                    return None
                raise RuntimeError(
                    "The server is not responsible for this creditor."
                )

            headers["creditor-id"] = data["creditor_id"]
            headers["debtor-id"] = data["debtor_id"]

            if "coordinator_id" in data:
                headers["coordinator-id"] = data["coordinator_id"]
                headers["coordinator-type"] = data["coordinator_type"]

        properties = rabbitmq.MessageProperties(
            delivery_mode=2,
            app_id="swpt_trade",
            content_type="application/json",
            type=message_type,
            headers=headers,
        )
        body = json.dumps(
            data,
            ensure_ascii=False,
            check_circular=False,
            allow_nan=False,
            separators=(",", ":"),
        ).encode("utf8")

        return rabbitmq.Message(
            exchange=self.exchange_name,
            routing_key=self.routing_key,
            body=body,
            properties=properties,
            mandatory=message_type == "FinalizeTransfer" or not is_smp_message,
        )

    inserted_at = db.Column(
        db.TIMESTAMP(timezone=True), nullable=False, default=get_now_utc
    )
