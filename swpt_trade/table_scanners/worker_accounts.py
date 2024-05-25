from typing import TypeVar, Callable
from datetime import datetime, timedelta, timezone
from swpt_pythonlib.scan_table import TableScanner
from flask import current_app
from sqlalchemy.sql.expression import tuple_
from swpt_trade.extensions import db
from swpt_trade.models import WorkerAccount

T = TypeVar("T")
atomic: Callable[[T], T] = db.atomic


class WorkerAccountsScanner(TableScanner):
    """Garbage collects worker accounts."""

    table = WorkerAccount.__table__
    pk = tuple_(table.c.creditor_id, table.c.debtor_id)
    columns = [
        WorkerAccount.creditor_id,
        WorkerAccount.debtor_id,
        WorkerAccount.last_heartbeat_ts,
    ]

    def __init__(self):
        super().__init__()
        self.sharding_realm = current_app.config["SHARDING_REALM"]
        self.max_heartbeat_delay = timedelta(
            days=current_app.config["APP_MAX_HEARTBEAT_DELAY_DAYS"]
        )

    @property
    def blocks_per_query(self) -> int:
        return current_app.config["APP_WORKER_ACCOUNTS_SCAN_BLOCKS_PER_QUERY"]

    @property
    def target_beat_duration(self) -> int:
        return current_app.config["APP_WORKER_ACCOUNTS_SCAN_BEAT_MILLISECS"]

    @atomic
    def process_rows(self, rows):
        current_ts = datetime.now(tz=timezone.utc)

        if current_app.config["DELETE_PARENT_SHARD_RECORDS"]:
            self._delete_parent_shard_records(rows, current_ts)

        self._delete_dead_worker_accounts(rows, current_ts)

    def _delete_parent_shard_records(self, rows, current_ts):
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
                WorkerAccount.query.filter(self.pk.in_(pks_to_delete))
                .with_for_update(skip_locked=True)
                .all()
            )

            for worker_account in to_delete:
                db.session.delete(worker_account)

            db.session.commit()

    def _delete_dead_worker_accounts(self, rows, current_ts):
        c = self.table.c
        c_creditor_id = c.creditor_id
        c_debtor_id = c.debtor_id
        c_last_heartbeat_ts = c.last_heartbeat_ts
        cutoff_ts = current_ts - self.max_heartbeat_delay

        def is_dead(row) -> bool:
            return row[c_last_heartbeat_ts] < cutoff_ts

        pks_to_delete = [
            (row[c_creditor_id], row[c_debtor_id])
            for row in rows
            if is_dead(row)
        ]
        if pks_to_delete:
            to_delete = (
                WorkerAccount.query.filter(self.pk.in_(pks_to_delete))
                .filter(WorkerAccount.last_heartbeat_ts < cutoff_ts)
                .with_for_update(skip_locked=True)
                .all()
            )

            for worker_account in to_delete:
                db.session.delete(worker_account)

            db.session.commit()
