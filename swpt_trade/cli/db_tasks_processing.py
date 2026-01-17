import logging
import time
import click
import random
from datetime import timedelta
from flask import current_app
from flask.cli import with_appcontext
from swpt_trade import sync_collectors
from .common import swpt_trade


@swpt_trade.command("process_dispatchings")
@with_appcontext
@click.option(
    "-w",
    "--wait",
    type=float,
    help=(
        "Poll the worker's database every FLOAT seconds for collector"
        " accounts that are ready to initiate transfers. If not specified,"
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
def process_dispatchings(wait, quit_early):
    """Run a process which polls the worker's database for collector
    accounts that are ready to initiate transfers. This process is
    also responsible for replaying delayed account transfers.
    """
    from swpt_trade import process_transfers as pt

    cfg = current_app.config
    wait_interval: timedelta = cfg["TRANSFERS_HEALTHY_MAX_COMMIT_DELAY"] / 12
    wait_seconds = (
        wait
        if wait is not None
        else wait_interval.total_seconds()
    )
    logger = logging.getLogger(__name__)
    logger.info("Started updating dispatchings.")

    while True:
        logger.info(
            "Looking for collector accounts ready to initiate transfers."
        )
        started_at = time.time()
        pt.signal_dispatching_statuses_ready_to_send()
        pt.update_dispatching_statuses_with_everything_sent()
        pt.signal_dispatching_statuses_ready_to_dispatch()
        pt.delete_dispatching_statuses_with_everything_dispatched()

        logger.info(
            "Looking for delayed account transfers ready to be replayed."
        )
        number_replayed = pt.process_delayed_account_transfers()
        if number_replayed > 0:
            logger.info(
                "Replayed %d account transfer messages.", number_replayed
            )

        if quit_early:
            break
        if wait_seconds > 0.0:  # pragma: no cover
            time.sleep(max(0.0, wait_seconds + started_at - time.time()))


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
        else current_app.config["APP_APPLY_COLLECTOR_CHANGES_WAIT"]
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
