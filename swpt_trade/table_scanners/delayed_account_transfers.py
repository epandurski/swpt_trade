import logging
from datetime import datetime, timezone, timedelta
from swpt_pythonlib.scan_table import TableScanner
from flask import current_app
from sqlalchemy.orm import load_only
from sqlalchemy.sql.expression import tuple_
from swpt_trade.extensions import db
from swpt_trade.models import (
    DelayedAccountTransfer,
    SET_HASHJOIN_OFF,
    SET_MERGEJOIN_OFF,
)


class DelayedAccountTransfersScanner(TableScanner):
    table = DelayedAccountTransfer.__table__
    pk = tuple_(
        DelayedAccountTransfer.turn_id,
        DelayedAccountTransfer.message_id,
    )
    columns = [
        DelayedAccountTransfer.turn_id,
        DelayedAccountTransfer.message_id,
        DelayedAccountTransfer.ts,
    ]

    def __init__(self):
        super().__init__()
        cfg = current_app.config
        self.expiry_period = timedelta(
            days=cfg["APP_DELAYED_ACCOUNT_TRANSFERS_EXPIRY_DAYS"]
        )

    @property
    def blocks_per_query(self) -> int:
        return current_app.config[
            "APP_DELAYED_ACCOUNT_TRANSFERS_SCAN_BLOCKS_PER_QUERY"
        ]

    @property
    def target_beat_duration(self) -> int:
        return current_app.config[
            "APP_DELAYED_ACCOUNT_TRANSFERS_SCAN_BEAT_MILLISECS"
        ]

    def process_rows(self, rows):
        current_ts = datetime.now(tz=timezone.utc)
        self._delete_stale_records(rows, current_ts)
        db.session.close()

    def _delete_stale_records(self, rows, current_ts):
        c = self.table.c
        c_turn_id = c.turn_id
        c_message_id = c.message_id
        c_ts = c.ts
        cutoff_ts = current_ts - self.expiry_period

        def is_stale(row) -> bool:
            return row[c_ts] < cutoff_ts

        pks_to_delete = [
            (row[c_turn_id], row[c_message_id])
            for row in rows
            if is_stale(row)
        ]
        if pks_to_delete:
            db.session.execute(SET_MERGEJOIN_OFF)
            db.session.execute(SET_HASHJOIN_OFF)
            logger = logging.getLogger(__name__)
            chosen = DelayedAccountTransfer.choose_rows(pks_to_delete)
            to_delete = (
                DelayedAccountTransfer.query
                .join(chosen, self.pk == tuple_(*chosen.c))
                .with_for_update(skip_locked=True)
                .options(load_only(DelayedAccountTransfer.turn_id))
                .all()
            )

            for record in to_delete:
                logger.warning(
                    "Deleting staled delayed account transfer"
                    " (turn_id=%d, debtor_id=%d, ts=%s).",
                    record.turn_id,
                    record.debtor_id,
                    record.ts,
                )
                db.session.delete(record)

            db.session.commit()
