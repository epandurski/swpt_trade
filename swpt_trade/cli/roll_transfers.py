import logging
import time
import click
from datetime import timedelta
from flask import current_app
from flask.cli import with_appcontext
from .common import swpt_trade


@swpt_trade.command("roll_transfers")
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
def roll_transfers(wait, quit_early):
    """Run a process which polls the worker's database for collector
    accounts that are ready to initiate transfers.
    """
    from swpt_trade import run_transfers

    cfg = current_app.config
    wait_interval: timedelta = cfg["TRANSFERS_HEALTHY_MAX_COMMIT_DELAY"] / 12
    wait_seconds = (
        wait
        if wait is not None
        else wait_interval.total_seconds()
    )
    logger = logging.getLogger(__name__)
    logger.info("Started rolling transfers.")

    while True:
        logger.info(
            "Looking for collector accounts ready to initiate transfers."
        )
        started_at = time.time()
        run_transfers.signal_dispatching_statuses_ready_to_send()
        run_transfers.update_dispatching_statuses_with_everything_sent()
        run_transfers.signal_dispatching_statuses_ready_to_dispatch()
        run_transfers.delete_dispatching_statuses_with_everything_dispatched()

        if quit_early:
            break
        if wait_seconds > 0.0:  # pragma: no cover
            time.sleep(max(0.0, wait_seconds + started_at - time.time()))


@swpt_trade.command("roll_delayed_account_transfers")
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
def roll_delayed_account_transfers(wait, quit_early):
    """Run a process which polls the worker's database for delayed
    account transfers ready to be processed.
    """
    from swpt_trade import run_transfers

    cfg = current_app.config
    wait_interval: timedelta = cfg["TRANSFERS_HEALTHY_MAX_COMMIT_DELAY"] / 12
    wait_seconds = (
        wait
        if wait is not None
        else wait_interval.total_seconds()
    )
    logger = logging.getLogger(__name__)
    logger.info("Started rolling delayed account transfers.")

    while True:
        logger.info(
            "Looking for delayed account transfers ready to be processed."
        )
        started_at = time.time()
        run_transfers.process_delayed_account_transfers()

        if quit_early:
            break
        if wait_seconds > 0.0:  # pragma: no cover
            time.sleep(max(0.0, wait_seconds + started_at - time.time()))
