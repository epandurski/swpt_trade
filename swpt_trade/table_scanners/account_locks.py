from datetime import datetime, timedelta, timezone
from swpt_pythonlib.scan_table import TableScanner
from flask import current_app
from sqlalchemy.orm import load_only
from sqlalchemy.sql.expression import tuple_, and_, or_, null
from swpt_trade.extensions import db
from swpt_trade.models import AccountLock, SET_INDEXSCAN_OFF, SET_INDEXSCAN_ON


class AccountLocksScanner(TableScanner):
    table = AccountLock.__table__
    pk = tuple_(AccountLock.creditor_id, AccountLock.debtor_id)
    columns = [
        AccountLock.creditor_id,
        AccountLock.debtor_id,
        AccountLock.initiated_at,
        AccountLock.released_at,
    ]

    def __init__(self):
        super().__init__()
        self.sharding_realm = current_app.config["SHARDING_REALM"]
        self.lock_max_interval = timedelta(
            days=current_app.config["APP_ACCOUNT_LOCK_MAX_DAYS"]
        )
        self.released_lock_max_interval = timedelta(
            days=current_app.config["APP_RELEASED_ACCOUNT_LOCK_MAX_DAYS"]
        )

    @property
    def blocks_per_query(self) -> int:
        return current_app.config["APP_ACCOUNT_LOCKS_SCAN_BLOCKS_PER_QUERY"]

    @property
    def target_beat_duration(self) -> int:
        return current_app.config["APP_ACCOUNT_LOCKS_SCAN_BEAT_MILLISECS"]

    def process_rows(self, rows):
        current_ts = datetime.now(tz=timezone.utc)

        if current_app.config["DELETE_PARENT_SHARD_RECORDS"]:
            self._delete_parent_shard_records(rows, current_ts)

        self._delete_stale_records(rows, current_ts)
        db.session.close()

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
            db.session.execute(SET_INDEXSCAN_OFF)
            chosen = AccountLock.choose_rows(pks_to_delete)
            to_delete = (
                AccountLock.query
                .join(chosen, self.pk == tuple_(*chosen.c))
                .with_for_update(skip_locked=True)
                .options(load_only(AccountLock.creditor_id))
                .all()
            )
            db.session.execute(SET_INDEXSCAN_ON)

            for account_lock in to_delete:
                db.session.delete(account_lock)

            db.session.commit()

    def _delete_stale_records(self, rows, current_ts):
        c = self.table.c
        c_creditor_id = c.creditor_id
        c_debtor_id = c.debtor_id
        c_initiated_at = c.initiated_at
        c_released_at = c.released_at
        cutoff_ts = current_ts - self.lock_max_interval
        released_cutoff_ts = current_ts - self.released_lock_max_interval

        def is_stale(row) -> bool:
            return (
                row[c_initiated_at] < cutoff_ts
                or (
                    row[c_released_at] is not None
                    and row[c_released_at] < released_cutoff_ts
                )
            )

        pks_to_delete = [
            (row[c_creditor_id], row[c_debtor_id])
            for row in rows
            if is_stale(row)
        ]
        if pks_to_delete:
            db.session.execute(SET_INDEXSCAN_OFF)
            chosen = AccountLock.choose_rows(pks_to_delete)
            to_delete = (
                AccountLock.query
                .join(chosen, self.pk == tuple_(*chosen.c))
                .filter(
                    or_(
                        AccountLock.initiated_at < cutoff_ts,
                        and_(
                            AccountLock.released_at != null(),
                            AccountLock.released_at < released_cutoff_ts,
                        )
                    )
                )
                .with_for_update(skip_locked=True)
                .options(load_only(AccountLock.creditor_id))
                .all()
            )
            db.session.execute(SET_INDEXSCAN_ON)

            for record in to_delete:
                db.session.delete(record)

            db.session.commit()
