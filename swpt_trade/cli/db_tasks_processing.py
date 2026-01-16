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
from swpt_trade import sync_collectors
from .common import swpt_trade


@swpt_trade.command("handle_pristine_collectors")
@with_appcontext
@click.option(
    "-t", "--threads", type=int, help="The number of worker threads."
)
@click.option(
    "-w",
    "--wait",
    type=float,
    help=(
        "The minimal number of seconds between"
        " the queries to obtain pristine collector accounts."
    ),
)
@click.option(
    "--quit-early",
    is_flag=True,
    default=False,
    help="Exit after some time (mainly useful during testing).",
)
def handle_pristine_collectors(threads, wait, quit_early):
    """Run a process which handles pristine collector accounts.

    If --threads is not specified, the value of the configuration
    variable HANDLE_PRISTINE_COLLECTORS_THREADS is taken. If it is
    not set, the default number of threads is 1.

    If --wait is not specified, the default is 600 seconds.
    """

    logger = logging.getLogger(__name__)
    logger.info("Started pristine collector accounts processor.")
    time.sleep(wait * random.random())
    sync_collectors.handle_pristine_collectors(threads, wait, quit_early)


@swpt_trade.command("replay_delayed_account_transfers")
@with_appcontext
@click.option(
    "-w",
    "--wait",
    type=float,
    help=(
        "Poll the worker's database every FLOAT seconds for delayed"
        " account transfers ready to be processed. If not specified,"
        " 1/12th of the value of the TRANSFERS_HEALTHY_MAX_COMMIT_DELAY"
        " environment variable will be used, defaulting to 10 minutes if it"
        " is empty."
    ),
)
@click.option(
    "--quit-early",
    is_flag=True,
    default=False,
    help="Exit after some time (mainly useful during testing).",
)
def replay_delayed_account_transfers(wait, quit_early):
    """Run a process which polls the worker's database for delayed
    account transfers ready to be processed.
    """
    from swpt_trade.process_transfers import process_delayed_account_transfers

    cfg = current_app.config
    wait_interval: timedelta = cfg["TRANSFERS_HEALTHY_MAX_COMMIT_DELAY"] / 12
    wait_seconds = (
        wait
        if wait is not None
        else wait_interval.total_seconds()
    )
    logger = logging.getLogger(__name__)
    logger.info("Started replaying delayed account transfers.")

    while True:
        logger.info(
            "Looking for delayed account transfers ready to be processed."
        )
        started_at = time.time()
        n = process_delayed_account_transfers()
        logger.info("Replayed %d account transfer messages.", n)

        if quit_early:
            break
        if wait_seconds > 0.0:  # pragma: no cover
            time.sleep(max(0.0, wait_seconds + started_at - time.time()))


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


@swpt_trade.command("apply_collector_changes")
@with_appcontext
@click.option(
    "-w",
    "--wait",
    type=float,
    help=(
        "The minimal number of seconds between"
        " the queries to obtain collector changes."
    ),
)
@click.option(
    "--quit-early",
    is_flag=True,
    default=False,
    help="Exit after some time (mainly useful during testing).",
)
def apply_collector_changes(wait, quit_early):
    """Run a process which applies pending collector changes.

    If --wait is not specified, the default is 120 seconds.
    """

    wait = (
        wait
        if wait is not None
        else current_app.config["APP_HANDLE_PRISTINE_COLLECTORS_WAIT"] / 5
    )
    logger = logging.getLogger(__name__)
    logger.info("Started collector changes processor.")
    time.sleep(wait * random.random())
    iteration_counter = 0

    while not (quit_early and iteration_counter > 0):
        iteration_counter += 1
        started_at = time.time()
        sync_collectors.process_collector_status_changes()
        sync_collectors.create_needed_collector_accounts()
        time.sleep(max(0.0, wait + started_at - time.time()))
