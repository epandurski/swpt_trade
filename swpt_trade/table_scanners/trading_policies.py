from typing import TypeVar, Callable
from datetime import datetime, timezone, timedelta
from swpt_pythonlib.scan_table import TableScanner
from flask import current_app
from sqlalchemy.sql.expression import tuple_
from swpt_trade.extensions import db
from swpt_trade.models import (
    TradingPolicy,
    DATE0,
    MIN_INT64,
    MAX_INT64,
    DEFAULT_CONFIG_FLAGS,
)

T = TypeVar("T")
atomic: Callable[[T], T] = db.atomic


class TradingPoliciesScanner(TableScanner):
    table = TradingPolicy.__table__
    pk = tuple_(table.c.creditor_id, table.c.debtor_id)

    def __init__(self):
        super().__init__()
        cfg = current_app.config
        self.sharding_realm = cfg["SHARDING_REALM"]
        self.useless_policies_retaining_period = (
            cfg["APP_TURN_MAX_COMMIT_PERIOD"]
            + timedelta(days=cfg["APP_WORKER_DISPATCHING_SLACK_DAYS"])
            + timedelta(days=cfg["APP_EXTREME_MESSAGE_DELAY_DAYS"])
        )

    @property
    def blocks_per_query(self) -> int:
        return current_app.config["APP_TRADING_POLICIES_SCAN_BLOCKS_PER_QUERY"]

    @property
    def target_beat_duration(self) -> int:
        return current_app.config["APP_TRADING_POLICIES_SCAN_BEAT_MILLISECS"]

    @atomic
    def process_rows(self, rows):
        current_ts = datetime.now(tz=timezone.utc)

        if current_app.config["DELETE_PARENT_SHARD_RECORDS"]:
            self._delete_parent_shard_trading_policies(rows, current_ts)

        self._delete_useless_trading_policies(rows, current_ts)

    def _delete_parent_shard_trading_policies(self, rows, current_ts):
        c = self.table.c
        c_creditor_id = c.creditor_id
        c_debtor_id = c.debtor_id

        def belongs_to_parent_shard(row) -> bool:
            creditor_id = row[c_creditor_id]
            return (
                not self.sharding_realm.match(creditor_id)
                and self.sharding_realm.match(creditor_id, match_parent=True)
            )

        pks_to_delete = [
            (row[c_creditor_id], row[c_debtor_id])
            for row in rows
            if belongs_to_parent_shard(row)
        ]
        if pks_to_delete:
            to_delete = (
                TradingPolicy.query.filter(self.pk.in_(pks_to_delete))
                .with_for_update(skip_locked=True)
                .all()
            )

            for trading_policy in to_delete:
                db.session.delete(trading_policy)

            db.session.commit()

    def _delete_useless_trading_policies(self, rows, current_ts):
        c = self.table.c
        c_creditor_id = c.creditor_id
        c_debtor_id = c.debtor_id
        c_account_id = c.account_id
        c_creation_date = c.creation_date
        c_principal = c.principal
        c_last_transfer_number = c.last_transfer_number
        c_policy_name = c.policy_name
        c_min_principal = c.min_principal
        c_max_principal = c.max_principal
        c_peg_debtor_id = c.peg_debtor_id
        c_peg_exchange_rate = c.peg_exchange_rate
        c_config_flags = c.config_flags
        c_latest_ledger_update_ts = c.latest_ledger_update_ts
        c_latest_policy_update_ts = c.latest_policy_update_ts
        c_latest_flags_update_ts = c.latest_flags_update_ts
        cutoff_ts = current_ts - self.useless_policies_retaining_period

        def is_useless(row) -> bool:
            return (
                row[c_account_id] == ""
                and row[c_creation_date] == DATE0
                and row[c_principal] == 0
                and row[c_last_transfer_number] == 0
                and row[c_policy_name] is None
                and row[c_min_principal] == MIN_INT64
                and row[c_max_principal] == MAX_INT64
                and row[c_peg_debtor_id] is None
                and row[c_peg_exchange_rate] is None
                and row[c_config_flags] == DEFAULT_CONFIG_FLAGS
            )

        pks_to_delete = [
            (row[c_creditor_id], row[c_debtor_id])
            for row in rows
            if (
                    is_useless(row)
                    and row[c_latest_ledger_update_ts] < cutoff_ts
                    and row[c_latest_policy_update_ts] < cutoff_ts
                    and row[c_latest_flags_update_ts] < cutoff_ts
            )
        ]
        if pks_to_delete:
            to_delete = (
                TradingPolicy.query.filter(self.pk.in_(pks_to_delete))
                .with_for_update(skip_locked=True)
                .all()
            )

            for trading_policy in to_delete:
                if (
                        trading_policy.is_useless
                        and trading_policy.latest_ledger_update_ts < cutoff_ts
                        and trading_policy.latest_policy_update_ts < cutoff_ts
                        and trading_policy.latest_flags_update_ts < cutoff_ts
                ):
                    db.session.delete(trading_policy)

            db.session.commit()
