import time
from datetime import datetime, timezone, timedelta
from flask import current_app
from swpt_pythonlib.scan_table import TableScanner
from swpt_trade.models import DISCARD_PLANS
from swpt_trade.extensions import db

PLANS_DISCARD_INTERVAL = timedelta(seconds=10.0)


class PlansDiscardingTableScanner(TableScanner):
    def __init__(self):
        super().__init__()
        self.latest_plans_discard_ts = datetime.now(tz=timezone.utc)

    def _process_rows_done(self):
        db.session.expunge_all()
        current_ts = datetime.now(tz=timezone.utc)
        if (
                current_ts - self.latest_plans_discard_ts
                >= PLANS_DISCARD_INTERVAL
        ):  # pragma: no cover
            # Discard possibly outdated execution plans.
            db.session.execute(DISCARD_PLANS)
            db.session.commit()
            db.session.close()
            self.latest_plans_discard_ts = current_ts


class ParentRecordsCleaner(PlansDiscardingTableScanner):
    """A scanner which only task is to delete parent shard records."""

    def run(self, engine, completion_goal, quit_early):
        if current_app.config["DELETE_PARENT_SHARD_RECORDS"]:
            return super().run(engine, completion_goal, quit_early)

        elif not quit_early:  # pragma: no cover
            while True:
                time.sleep(5)
