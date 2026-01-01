from typing import TypeVar, Callable
from datetime import datetime, timedelta, timezone
from swpt_pythonlib.scan_table import TableScanner
from flask import current_app
from sqlalchemy.orm import load_only
from swpt_trade.extensions import db
from swpt_trade.models import RecentlyNeededCollector

T = TypeVar("T")
atomic: Callable[[T], T] = db.atomic


class RecentlyNeededCollectorsScanner(TableScanner):
    table = RecentlyNeededCollector.__table__
    pk = table.c.debtor_id
    columns = [
        RecentlyNeededCollector.debtor_id,
        RecentlyNeededCollector.needed_at,
    ]

    def __init__(self):
        super().__init__()
        self.sharding_realm = current_app.config["SHARDING_REALM"]
        self.locator_claim_expiry_period = timedelta(
            days=current_app.config["APP_LOCATOR_CLAIM_EXPIRY_DAYS"]
        )

    @property
    def blocks_per_query(self) -> int:
        return current_app.config[
            "APP_RECENTLY_NEEDED_COLLECTORS_SCAN_BLOCKS_PER_QUERY"
        ]

    @property
    def target_beat_duration(self) -> int:
        return current_app.config[
            "APP_RECENTLY_NEEDED_COLLECTORS_SCAN_BEAT_MILLISECS"
        ]

    @atomic
    def process_rows(self, rows):
        current_ts = datetime.now(tz=timezone.utc)

        if current_app.config["DELETE_PARENT_SHARD_RECORDS"]:
            self._delete_parent_shard_records(rows, current_ts)

        self._delete_stale_records(rows, current_ts)

    def _delete_parent_shard_records(self, rows, current_ts):
        c = self.table.c
        c_debtor_id = c.debtor_id

        def belongs_to_parent_shard(row) -> bool:
            debtor_id = row[c_debtor_id]
            return (
                not self.sharding_realm.match(debtor_id)
                and self.sharding_realm.match(debtor_id, match_parent=True)
            )

        pks_to_delete = [
            row[c_debtor_id] for row in rows if belongs_to_parent_shard(row)
        ]
        if pks_to_delete:
            to_delete = (
                RecentlyNeededCollector.query
                .filter(self.pk.in_(pks_to_delete))
                .with_for_update(skip_locked=True)
                .options(load_only(RecentlyNeededCollector.debtor_id))
                .all()
            )

            for record in to_delete:
                db.session.delete(record)

            db.session.commit()

    def _delete_stale_records(self, rows, current_ts):
        c = self.table.c
        c_debtor_id = c.debtor_id
        c_needed_at = c.needed_at
        cutoff_ts = current_ts - self.locator_claim_expiry_period

        def is_stale(row) -> bool:
            return row[c_needed_at] < cutoff_ts

        pks_to_delete = [
            row[c_debtor_id] for row in rows if is_stale(row)
        ]
        if pks_to_delete:
            to_delete = (
                RecentlyNeededCollector.query
                .filter(
                    self.pk.in_(pks_to_delete),
                    RecentlyNeededCollector.needed_at < cutoff_ts,
                )
                .with_for_update(skip_locked=True)
                .options(load_only(RecentlyNeededCollector.debtor_id))
                .all()
            )

            for record in to_delete:
                db.session.delete(record)

            db.session.commit()
