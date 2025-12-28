from typing import TypeVar, Callable
from datetime import datetime, timezone, timedelta
from swpt_pythonlib.scan_table import TableScanner
from flask import current_app
from sqlalchemy.sql.expression import tuple_, not_, and_, null
from swpt_trade.extensions import db
from swpt_trade.models import TransferAttempt

T = TypeVar("T")
atomic: Callable[[T], T] = db.atomic


class TransferAttemptsScanner(TableScanner):
    table = TransferAttempt.__table__
    pk = tuple_(
        table.c.collector_id,
        table.c.turn_id,
        table.c.debtor_id,
        table.c.creditor_id,
        table.c.transfer_kind,
    )
    columns = [
        TransferAttempt.collector_id,
        TransferAttempt.turn_id,
        TransferAttempt.debtor_id,
        TransferAttempt.creditor_id,
        TransferAttempt.transfer_kind,
        TransferAttempt.collection_started_at,
    ]

    def __init__(self):
        super().__init__()
        cfg = current_app.config
        self.sharding_realm = cfg["SHARDING_REALM"]
        self.retaining_period = (
            cfg["APP_TURN_MAX_COMMIT_PERIOD"]
            + max(
                timedelta(days=cfg["APP_WORKER_SENDING_SLACK_DAYS"]),
                timedelta(days=cfg["APP_WORKER_DISPATCHING_SLACK_DAYS"]),
            )
        )

    @property
    def blocks_per_query(self) -> int:
        return current_app.config[
            "APP_TRANSFER_ATTEMPTS_SCAN_BLOCKS_PER_QUERY"
        ]

    @property
    def target_beat_duration(self) -> int:
        return current_app.config[
            "APP_TRANSFER_ATTEMPTS_SCAN_BEAT_MILLISECS"
        ]

    @atomic
    def process_rows(self, rows):
        current_ts = datetime.now(tz=timezone.utc)

        if current_app.config["DELETE_PARENT_SHARD_RECORDS"]:
            self._delete_parent_shard_records(rows, current_ts)

        self._delete_stale_records(rows, current_ts)

    def _delete_parent_shard_records(self, rows, current_ts):
        c = self.table.c
        c_collector_id = c.collector_id
        c_turn_id = c.turn_id
        c_debtor_id = c.debtor_id
        c_creditor_id = c.creditor_id
        c_transfer_kind = c.transfer_kind

        def belongs_to_parent_shard(row) -> bool:
            collector_id = row[c_collector_id]
            return (
                not self.sharding_realm.match(collector_id)
                and self.sharding_realm.match(collector_id, match_parent=True)
            )

        pks_to_delete = [
            (
                row[c_collector_id],
                row[c_turn_id],
                row[c_debtor_id],
                row[c_creditor_id],
                row[c_transfer_kind],
            )
            for row in rows
            if belongs_to_parent_shard(row)
        ]
        if pks_to_delete:
            to_delete = (
                TransferAttempt.query.filter(self.pk.in_(pks_to_delete))
                .with_for_update(skip_locked=True)
                .all()
            )

            for record in to_delete:
                db.session.delete(record)

            db.session.commit()

    def _delete_stale_records(self, rows, current_ts):
        c = self.table.c
        c_collector_id = c.collector_id
        c_turn_id = c.turn_id
        c_debtor_id = c.debtor_id
        c_creditor_id = c.creditor_id
        c_transfer_kind = c.transfer_kind
        c_collection_started_at = c.collection_started_at
        cutoff_ts = current_ts - self.retaining_period

        def is_stale(row) -> bool:
            return row[c_collection_started_at] < cutoff_ts

        pks_to_delete = [
            (
                row[c_collector_id],
                row[c_turn_id],
                row[c_debtor_id],
                row[c_creditor_id],
                row[c_transfer_kind],
            )
            for row in rows
            if is_stale(row)
        ]
        if pks_to_delete:
            to_delete = (
                TransferAttempt.query
                .filter(
                    self.pk.in_(pks_to_delete),
                    not_(
                        # Deleting surplus-moving transfers that have
                        # a chance to succeed may interfere with the
                        # correct calculation of surpluses. Here we do
                        # our best to avoid this.
                        and_(
                            TransferAttempt.transfer_kind
                            == TransferAttempt.KIND_MOVING,
                            TransferAttempt.finalized_at != null(),
                            TransferAttempt.failure_code == null(),
                            TransferAttempt.attempted_at >= cutoff_ts,
                        )
                    )
                )
                .with_for_update(skip_locked=True)
                .all()
            )

            for record in to_delete:
                db.session.delete(record)

            db.session.commit()
