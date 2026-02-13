from datetime import datetime, timezone
from flask import current_app
from sqlalchemy.orm import load_only
from sqlalchemy.sql.expression import tuple_
from swpt_trade.extensions import db
from swpt_trade.models import (
    DispatchingStatus,
    SET_HASHJOIN_OFF,
    SET_MERGEJOIN_OFF,
)
from .common import ParentRecordsCleaner


class DispatchingStatusesScanner(ParentRecordsCleaner):
    table = DispatchingStatus.__table__
    pk = tuple_(
        DispatchingStatus.collector_id,
        DispatchingStatus.debtor_id,
        DispatchingStatus.turn_id,
    )
    columns = [
        DispatchingStatus.collector_id,
        DispatchingStatus.debtor_id,
        DispatchingStatus.turn_id,
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

    def process_rows(self, rows):
        assert current_app.config["DELETE_PARENT_SHARD_RECORDS"]
        self._delete_parent_shard_records(rows, datetime.now(tz=timezone.utc))
        db.session.close()

    def _delete_parent_shard_records(self, rows, current_ts):
        c = self.table.c
        c_collector_id = c.collector_id
        c_debtor_id = c.debtor_id
        c_turn_id = c.turn_id

        def belongs_to_parent_shard(row) -> bool:
            collector_id = row[c_collector_id]
            return (
                not self.sharding_realm.match(collector_id)
                and self.sharding_realm.match(collector_id, match_parent=True)
            )

        pks_to_delete = [
            (row[c_collector_id], row[c_debtor_id], row[c_turn_id])
            for row in rows
            if belongs_to_parent_shard(row)
        ]
        if pks_to_delete:
            db.session.execute(SET_MERGEJOIN_OFF)
            db.session.execute(SET_HASHJOIN_OFF)
            chosen = DispatchingStatus.choose_rows(pks_to_delete)
            to_delete = (
                DispatchingStatus.query
                .join(chosen, self.pk == tuple_(*chosen.c))
                .with_for_update(skip_locked=True)
                .options(load_only(DispatchingStatus.collector_id))
                .all()
            )

            for record in to_delete:
                db.session.delete(record)

            db.session.commit()
