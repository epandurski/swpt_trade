from datetime import datetime, timezone
from swpt_pythonlib.scan_table import TableScanner
from flask import current_app
from sqlalchemy.orm import load_only
from sqlalchemy.sql.expression import tuple_
from swpt_trade.extensions import db
from swpt_trade.models import (
    WorkerReceiving,
    SET_INDEXSCAN_ON,
    SET_INDEXSCAN_OFF,
)


class WorkerReceivingsScanner(TableScanner):
    table = WorkerReceiving.__table__
    pk = tuple_(
        WorkerReceiving.to_collector_id,
        WorkerReceiving.debtor_id,
        WorkerReceiving.turn_id,
        WorkerReceiving.from_collector_id,
    )
    columns = [
        WorkerReceiving.to_collector_id,
        WorkerReceiving.debtor_id,
        WorkerReceiving.turn_id,
        WorkerReceiving.from_collector_id,
        WorkerReceiving.received_amount,
        WorkerReceiving.purge_after,
    ]

    def __init__(self):
        super().__init__()
        self.sharding_realm = current_app.config["SHARDING_REALM"]

    @property
    def blocks_per_query(self) -> int:
        return current_app.config[
            "APP_WORKER_RECEIVINGS_SCAN_BLOCKS_PER_QUERY"
        ]

    @property
    def target_beat_duration(self) -> int:
        return current_app.config[
            "APP_WORKER_RECEIVINGS_SCAN_BEAT_MILLISECS"
        ]

    def process_rows(self, rows):
        current_ts = datetime.now(tz=timezone.utc)

        if current_app.config["DELETE_PARENT_SHARD_RECORDS"]:
            self._delete_parent_shard_records(rows, current_ts)

        self._delete_stale_records(rows, current_ts)
        db.session.close()

    def _delete_parent_shard_records(self, rows, current_ts):
        c = self.table.c
        c_to_collector_id = c.to_collector_id
        c_debtor_id = c.debtor_id
        c_turn_id = c.turn_id
        c_from_collector_id = c.from_collector_id

        def belongs_to_parent_shard(row) -> bool:
            to_collector_id = row[c_to_collector_id]
            return (
                not self.sharding_realm.match(to_collector_id)
                and self.sharding_realm.match(
                    to_collector_id, match_parent=True
                )
            )

        pks_to_delete = [
            (
                row[c_to_collector_id],
                row[c_debtor_id],
                row[c_turn_id],
                row[c_from_collector_id],
            )
            for row in rows
            if belongs_to_parent_shard(row)
        ]
        if pks_to_delete:
            db.session.execute(SET_INDEXSCAN_OFF)
            chosen = WorkerReceiving.choose_rows(pks_to_delete)
            to_delete = (
                WorkerReceiving.query
                .join(chosen, self.pk == tuple_(*chosen.c))
                .with_for_update(skip_locked=True)
                .options(load_only(WorkerReceiving.to_collector_id))
                .all()
            )
            db.session.execute(SET_INDEXSCAN_ON)

            for record in to_delete:
                db.session.delete(record)

            db.session.commit()

    def _delete_stale_records(self, rows, current_ts):
        c = self.table.c
        c_to_collector_id = c.to_collector_id
        c_debtor_id = c.debtor_id
        c_turn_id = c.turn_id
        c_from_collector_id = c.from_collector_id
        c_received_amount = c.received_amount
        c_purge_after = c.purge_after

        def is_stale(row) -> bool:
            return (
                row[c_purge_after] < current_ts
                and row[c_received_amount] == 0
            )

        pks_to_delete = [
            (
                row[c_to_collector_id],
                row[c_debtor_id],
                row[c_turn_id],
                row[c_from_collector_id],
            )
            for row in rows
            if is_stale(row)
        ]
        if pks_to_delete:
            db.session.execute(SET_INDEXSCAN_OFF)
            chosen = WorkerReceiving.choose_rows(pks_to_delete)
            to_delete = (
                WorkerReceiving.query
                .join(chosen, self.pk == tuple_(*chosen.c))
                .filter(WorkerReceiving.received_amount == 0)
                .with_for_update(skip_locked=True)
                .options(load_only(WorkerReceiving.to_collector_id))
                .all()
            )
            db.session.execute(SET_INDEXSCAN_ON)

            for record in to_delete:
                db.session.delete(record)

            db.session.commit()
