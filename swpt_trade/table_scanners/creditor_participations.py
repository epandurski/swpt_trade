from typing import TypeVar, Callable
from datetime import datetime, timezone
from flask import current_app
from sqlalchemy.orm import load_only
from sqlalchemy.sql.expression import tuple_
from swpt_trade.extensions import db
from swpt_trade.models import CreditorParticipation
from .common import ParentRecordsCleaner

T = TypeVar("T")
atomic: Callable[[T], T] = db.atomic


class CreditorParticipationsScanner(ParentRecordsCleaner):
    table = CreditorParticipation.__table__
    pk = tuple_(table.c.creditor_id, table.c.debtor_id, table.c.turn_id)
    columns = [
        CreditorParticipation.creditor_id,
        CreditorParticipation.debtor_id,
        CreditorParticipation.turn_id,
    ]

    def __init__(self):
        super().__init__()
        self.sharding_realm = current_app.config["SHARDING_REALM"]

    @property
    def blocks_per_query(self) -> int:
        return current_app.config[
            "APP_CREDITOR_PARTICIPATIONS_SCAN_BLOCKS_PER_QUERY"
        ]

    @property
    def target_beat_duration(self) -> int:
        return current_app.config[
            "APP_CREDITOR_PARTICIPATIONS_SCAN_BEAT_MILLISECS"
        ]

    @atomic
    def process_rows(self, rows):
        assert current_app.config["DELETE_PARENT_SHARD_RECORDS"]
        self._delete_parent_shard_records(rows, datetime.now(tz=timezone.utc))

    def _delete_parent_shard_records(self, rows, current_ts):
        c = self.table.c
        c_creditor_id = c.creditor_id
        c_debtor_id = c.debtor_id
        c_turn_id = c.turn_id

        def belongs_to_parent_shard(row) -> bool:
            creditor_id = row[c_creditor_id]
            return (
                not self.sharding_realm.match(creditor_id)
                and self.sharding_realm.match(creditor_id, match_parent=True)
            )

        pks_to_delete = [
            (row[c_creditor_id], row[c_debtor_id], row[c_turn_id])
            for row in rows
            if belongs_to_parent_shard(row)
        ]
        if pks_to_delete:
            to_delete = (
                CreditorParticipation.query
                .filter(self.pk.in_(pks_to_delete))
                .with_for_update(skip_locked=True)
                .options(load_only(CreditorParticipation.creditor_id))
                .all()
            )

            for record in to_delete:
                db.session.delete(record)

            db.session.commit()
