from datetime import datetime, timedelta, timezone
from swpt_pythonlib.scan_table import TableScanner
from flask import current_app
from sqlalchemy.sql.expression import tuple_, and_, or_, null
from sqlalchemy.orm import load_only
from swpt_trade.extensions import db
from swpt_trade.models import (
    DebtorLocatorClaim,
    SET_HASHJOIN_OFF,
    SET_MERGEJOIN_OFF,
)


class DebtorLocatorClaimsScanner(TableScanner):
    table = DebtorLocatorClaim.__table__
    pk = tuple_(DebtorLocatorClaim.debtor_id)
    columns = [
        DebtorLocatorClaim.debtor_id,
        DebtorLocatorClaim.latest_locator_fetch_at,
        DebtorLocatorClaim.latest_discovery_fetch_at,
    ]

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

    def process_rows(self, rows):
        current_ts = datetime.now(tz=timezone.utc)

        if current_app.config["DELETE_PARENT_SHARD_RECORDS"]:
            self._delete_parent_shard_claims(rows, current_ts)

        self._delete_stale_claims(rows, current_ts)
        db.session.close()

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
            (row[c_debtor_id],)
            for row in rows
            if belongs_to_parent_shard(row)
        ]
        if pks_to_delete:
            db.session.execute(SET_MERGEJOIN_OFF)
            db.session.execute(SET_HASHJOIN_OFF)
            chosen = DebtorLocatorClaim.choose_rows(pks_to_delete)
            to_delete = (
                DebtorLocatorClaim.query
                .join(chosen, self.pk == tuple_(*chosen.c))
                .with_for_update(skip_locked=True)
                .options(load_only(DebtorLocatorClaim.debtor_id))
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
            (row[c_debtor_id],)
            for row in rows
            if is_stale(row)
        ]
        if pks_to_delete:
            db.session.execute(SET_MERGEJOIN_OFF)
            db.session.execute(SET_HASHJOIN_OFF)
            chosen = DebtorLocatorClaim.choose_rows(pks_to_delete)
            locator_fetch_at = DebtorLocatorClaim.latest_locator_fetch_at
            discovery_fetch_at = DebtorLocatorClaim.latest_discovery_fetch_at
            to_delete = (
                DebtorLocatorClaim.query
                .join(chosen, self.pk == tuple_(*chosen.c))
                .filter(
                    or_(
                        and_(
                            locator_fetch_at != null(),
                            locator_fetch_at < first_cutoff_ts,
                        ),
                        and_(
                            locator_fetch_at == null(),
                            discovery_fetch_at < second_cutoff_ts,
                        ),
                    )
                )
                .with_for_update(skip_locked=True)
                .options(load_only(DebtorLocatorClaim.debtor_id))
                .all()
            )

            for claim in to_delete:
                db.session.delete(claim)

            db.session.commit()
