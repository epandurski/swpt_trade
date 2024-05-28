from typing import TypeVar, Callable
from datetime import datetime, timezone
from swpt_pythonlib.scan_table import TableScanner
from flask import current_app
from sqlalchemy.sql.expression import tuple_
from swpt_trade.extensions import db
from swpt_trade.models import DispatchingStatus

T = TypeVar("T")
atomic: Callable[[T], T] = db.atomic


class DispatchingStatusesScanner(TableScanner):
    table = DispatchingStatus.__table__
    pk = tuple_(table.c.collector_id, table.c.turn_id, table.c.debtor_id)
    columns = [
        DispatchingStatus.collector_id,
        DispatchingStatus.turn_id,
        DispatchingStatus.debtor_id,
        DispatchingStatus.purge_at,
    ]

    def __init__(self):
        super().__init__()
        self.sharding_realm = current_app.config["SHARDING_REALM"]

    @property
    def blocks_per_query(self) -> int:
        return current_app.config[
            "APP_DISPATCHING_STATUSES_SCAN_BLOCKS_PER_QUERY"
        ]

    @property
    def target_beat_duration(self) -> int:
        return current_app.config[
            "APP_DISPATCHING_STATUSES_SCAN_BEAT_MILLISECS"
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

        def belongs_to_parent_shard(row) -> bool:
            collector_id = row[c_collector_id]
            return (
                not self.sharding_realm.match(collector_id)
                and self.sharding_realm.match(collector_id, match_parent=True)
            )

        pks_to_delete = [
            (row[c_collector_id], row[c_turn_id], row[c_debtor_id])
            for row in rows
            if belongs_to_parent_shard(row)
        ]
        if pks_to_delete:
            to_delete = (
                DispatchingStatus.query.filter(self.pk.in_(pks_to_delete))
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
        c_purge_at = c.purge_at

        def is_stale(row) -> bool:
            return row[c_purge_at] < current_ts

        pks_to_delete = [
            (row[c_collector_id], row[c_turn_id], row[c_debtor_id])
            for row in rows
            if is_stale(row)
        ]
        if pks_to_delete:
            to_delete = (
                DispatchingStatus.query.filter(self.pk.in_(pks_to_delete))
                .with_for_update(skip_locked=True)
                .all()
            )

            for record in to_delete:
                db.session.delete(record)

            db.session.commit()
