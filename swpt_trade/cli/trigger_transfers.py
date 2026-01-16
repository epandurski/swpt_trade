import logging
import time
import click
import signal
import sys
import random
from typing import Any
from datetime import timedelta
from flask import current_app
from flask.cli import with_appcontext
from swpt_pythonlib.multiproc_utils import (
    HANDLED_SIGNALS,
    spawn_worker_processes,
    try_unblock_signals,
)
from .common import swpt_trade


@swpt_trade.command("trigger_transfers")
@with_appcontext
@click.option(
    "-p",
    "--processes",
    type=int,
    help=(
        "The number of worker processes."
        " If not specified, the value of the TRIGGER_TRANSFERS_PROCESSES"
        " environment variable will be used, defaulting to 1 if empty."
    ),
)
@click.option(
    "-w",
    "--wait",
    type=float,
    help=(
        "Poll the database for scheduled requests every FLOAT seconds."
        " If not specified, 1/12th of the value of the"
        " TRANSFERS_HEALTHY_MAX_COMMIT_DELAY environment variable will be"
        " used, defaulting to 10 minutes if it is empty."
    ),
)
@click.option(
    "--quit-early",
    is_flag=True,
    default=False,
    help="Exit after some time (mainly useful during testing).",
)
def trigger_transfers(
    processes: int,
    wait: float,
    quit_early: bool,
) -> None:
    """Run processes that trigger rescheduled transfer attempts.
    """
    from swpt_trade.process_transfers import process_rescheduled_transfers

    cfg = current_app.config
    default_wait: timedelta = cfg["TRANSFERS_HEALTHY_MAX_COMMIT_DELAY"] / 12
    logger = logging.getLogger(__name__)
    logger.info("Started triggering transfer attempts.")

    def _trigger(wait: float) -> None:  # pragma: no cover
        from swpt_trade import create_app

        app = create_app()
        stopped = False

        def stop(signum: Any = None, frame: Any = None) -> None:
            nonlocal stopped
            stopped = True

        for sig in HANDLED_SIGNALS:
            signal.signal(sig, stop)
        try_unblock_signals()

        with app.app_context():
            time.sleep(wait * random.random())
            while not stopped:
                started_at = time.time()
                try:
                    count = process_rescheduled_transfers()
                except Exception:
                    logger.exception(
                        "Caught error while triggering transfer attempts."
                    )
                    sys.exit(1)

                if count > 0:
                    logger.info(
                        "%i transfer attempts have been triggered.", count
                    )
                else:
                    logger.debug("0 transfer attempts have been triggered.")

                if quit_early:
                    break
                time.sleep(max(0.0, wait + started_at - time.time()))

    spawn_worker_processes(
        processes=(
            processes
            if processes is not None
            else cfg["TRIGGER_TRANSFERS_PROCESSES"]
        ),
        target=_trigger,
        wait=(
            wait
            if wait is not None
            else default_wait.total_seconds()
        ),
    )
    sys.exit(1)
