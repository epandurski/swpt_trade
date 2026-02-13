from datetime import datetime, timedelta, timezone
from swpt_pythonlib.scan_table import TableScanner
from flask import current_app
from sqlalchemy.sql.expression import tuple_
from sqlalchemy.orm import load_only
from swpt_trade.extensions import db
from swpt_trade.models import (
    DebtorInfoDocument,
    SET_INDEXSCAN_OFF,
    SET_INDEXSCAN_ON,
)


class DebtorInfoDocumentsScanner(TableScanner):
    table = DebtorInfoDocument.__table__
    pk = tuple_(DebtorInfoDocument.debtor_info_locator)
    columns = [
        DebtorInfoDocument.debtor_info_locator,
        DebtorInfoDocument.fetched_at,
    ]

    def __init__(self):
        super().__init__()
        self.sharding_realm = current_app.config["SHARDING_REALM"]
        self.expiry_period = timedelta(
            days=current_app.config["APP_LOCATOR_CLAIM_EXPIRY_DAYS"]
        )

    @property
    def blocks_per_query(self) -> int:
        return current_app.config[
            "APP_DEBTOR_INFO_DOCUMENTS_SCAN_BLOCKS_PER_QUERY"
        ]

    @property
    def target_beat_duration(self) -> int:
        return current_app.config[
            "APP_DEBTOR_INFO_DOCUMENTS_SCAN_BEAT_MILLISECS"
        ]

    def process_rows(self, rows):
        current_ts = datetime.now(tz=timezone.utc)

        if current_app.config["DELETE_PARENT_SHARD_RECORDS"]:
            self._delete_parent_shard_documents(rows, current_ts)

        self._delete_stale_documents(rows, current_ts)
        db.session.close()

    def _delete_parent_shard_documents(self, rows, current_ts):
        c = self.table.c
        c_debtor_info_locator = c.debtor_info_locator

        def belongs_to_parent_shard(row) -> bool:
            s = row[c_debtor_info_locator]
            return (
                not self.sharding_realm.match_str(s)
                and self.sharding_realm.match_str(s, match_parent=True)
            )

        pks_to_delete = [
            (row[c_debtor_info_locator],)
            for row in rows
            if belongs_to_parent_shard(row)
        ]
        if pks_to_delete:
            db.session.execute(SET_INDEXSCAN_OFF)
            chosen = DebtorInfoDocument.choose_rows(pks_to_delete)
            to_delete = (
                DebtorInfoDocument.query
                .join(chosen, self.pk == tuple_(*chosen.c))
                .with_for_update(skip_locked=True)
                .options(load_only(DebtorInfoDocument.debtor_info_locator))
                .all()
            )
            db.session.execute(SET_INDEXSCAN_ON)

            for document in to_delete:
                db.session.delete(document)

            db.session.commit()

    def _delete_stale_documents(self, rows, current_ts):
        c = self.table.c
        c_debtor_info_locator = c.debtor_info_locator
        c_fetched_at = c.fetched_at
        cutoff_ts = current_ts - self.expiry_period

        def is_stale(row) -> bool:
            return row[c_fetched_at] < cutoff_ts

        pks_to_delete = [
            (row[c_debtor_info_locator],)
            for row in rows
            if is_stale(row)
        ]
        if pks_to_delete:
            db.session.execute(SET_INDEXSCAN_OFF)
            chosen = DebtorInfoDocument.choose_rows(pks_to_delete)
            to_delete = (
                DebtorInfoDocument.query
                .join(chosen, self.pk == tuple_(*chosen.c))
                .filter(DebtorInfoDocument.fetched_at < cutoff_ts)
                .with_for_update(skip_locked=True)
                .options(load_only(DebtorInfoDocument.debtor_info_locator))
                .all()
            )
            db.session.execute(SET_INDEXSCAN_ON)

            for document in to_delete:
                db.session.delete(document)

            db.session.commit()
