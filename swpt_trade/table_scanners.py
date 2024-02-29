from typing import TypeVar, Callable
from datetime import datetime, timedelta, timezone
from swpt_pythonlib.scan_table import TableScanner
from flask import current_app
from sqlalchemy.sql.expression import tuple_
from swpt_trade.extensions import db
from swpt_trade.models import (
    DebtorInfoDocument,
    DebtorLocatorClaim,
)

T = TypeVar("T")
atomic: Callable[[T], T] = db.atomic


class DebtorInfoDocumentScanner(TableScanner):
    """Garbage collects stale debtor info documents."""

    table = DebtorInfoDocument.__table__
    pk = tuple_(DebtorInfoDocument.debtor_info_locator)

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

    @atomic
    def process_rows(self, rows):
        current_ts = datetime.now(tz=timezone.utc)

        if current_app.config["DELETE_PARENT_SHARD_RECORDS"]:
            self._delete_parent_shard_documents(rows, current_ts)

        self._delete_stale_documents(rows, current_ts)

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
            to_delete = (
                DebtorInfoDocument.query.filter(self.pk.in_(pks_to_delete))
                .with_for_update(skip_locked=True)
                .all()
            )

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
            (row[c_debtor_info_locator],) for row in rows if is_stale(row)
        ]
        if pks_to_delete:
            to_delete = (
                DebtorInfoDocument.query.filter(self.pk.in_(pks_to_delete))
                .with_for_update(skip_locked=True)
                .all()
            )

            for document in to_delete:
                db.session.delete(document)

            db.session.commit()


class DebtorLocatorClaimScanner(TableScanner):
    """Garbage collects stale debtor locator claims."""

    table = DebtorLocatorClaim.__table__
    pk = tuple_(DebtorLocatorClaim.debtor_id)

    def __init__(self):
        super().__init__()
        self.sharding_realm = current_app.config["SHARDING_REALM"]
        self.debtor_info_expiry_period = timedelta(
            days=current_app.config["APP_DEBTOR_INFO_EXPIRY_DAYS"]
        )
        self.locator_claim_expiry_period = timedelta(
            days=current_app.config["APP_LOCATOR_CLAIM_EXPIRY_DAYS"]
        )

    @property
    def blocks_per_query(self) -> int:
        return current_app.config[
            "APP_DEBTOR_LOCATOR_CLAIMS_SCAN_BLOCKS_PER_QUERY"
        ]

    @property
    def target_beat_duration(self) -> int:
        return current_app.config[
            "APP_DEBTOR_LOCATOR_CLAIMS_SCAN_BEAT_MILLISECS"
        ]

    @atomic
    def process_rows(self, rows):
        current_ts = datetime.now(tz=timezone.utc)

        if current_app.config["DELETE_PARENT_SHARD_RECORDS"]:
            self._delete_parent_shard_claims(rows, current_ts)

        self._delete_stale_claims(rows, current_ts)

    def _delete_parent_shard_claims(self, rows, current_ts):
        c = self.table.c
        c_debtor_id = c.debtor_id

        def belongs_to_parent_shard(row) -> bool:
            debtor_id = row[c_debtor_id]
            return (
                not self.sharding_realm.match(debtor_id)
                and self.sharding_realm.match(debtor_id, match_parent=True)
            )

        pks_to_delete = [
            (row[c_debtor_id],) for row in rows if belongs_to_parent_shard(row)
        ]
        if pks_to_delete:
            to_delete = (
                DebtorLocatorClaim.query.filter(self.pk.in_(pks_to_delete))
                .with_for_update(skip_locked=True)
                .all()
            )

            for claim in to_delete:
                db.session.delete(claim)

            db.session.commit()

    def _delete_stale_claims(self, rows, current_ts):
        c = self.table.c
        c_debtor_id = c.debtor_id
        c_latest_locator_fetch_at = c.latest_locator_fetch_at
        c_latest_discovery_fetch_at = c.latest_discovery_fetch_at
        first_cutoff_ts = current_ts - self.locator_claim_expiry_period
        second_cutoff_ts = current_ts - self.debtor_info_expiry_period

        def is_stale(row) -> bool:
            latest_locator_fetch_at = row[c_latest_locator_fetch_at]
            return (
                latest_locator_fetch_at is not None
                and latest_locator_fetch_at < first_cutoff_ts
            ) or (
                latest_locator_fetch_at is None
                and row[c_latest_discovery_fetch_at] < second_cutoff_ts
            )

        pks_to_delete = [
            (row[c_debtor_id],) for row in rows if is_stale(row)
        ]
        if pks_to_delete:
            to_delete = (
                DebtorLocatorClaim.query.filter(self.pk.in_(pks_to_delete))
                .with_for_update(skip_locked=True)
                .all()
            )

            for claim in to_delete:
                db.session.delete(claim)

            db.session.commit()
