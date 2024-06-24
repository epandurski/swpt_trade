import logging
import time
import click
import signal
import sys
from typing import Any
from datetime import timedelta
from flask import current_app
from flask.cli import with_appcontext
from swpt_pythonlib.utils import ShardingRealm
from swpt_pythonlib.multiproc_utils import (
    ThreadPoolProcessor,
    HANDLED_SIGNALS,
    spawn_worker_processes,
    try_unblock_signals,
)
from swpt_trade.utils import u16_to_i16
from swpt_trade import procedures
from swpt_trade.run_transfers import process_rescheduled_transfers
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

    If --wait is not specified, the value of the configuration
    variable HANDLE_PRISTINE_COLLECTORS_PERIOD is taken. If it is
    not set, the default number of seconds is 60.
    """

    # TODO: Consider allowing load-sharing between multiple processes
    #       or containers. This may also be true for the other
    #       "process_*" CLI commands. A possible way to do this is to
    #       separate the `args collection` in multiple buckets,
    #       assigning a dedicated process/container for each bucket.
    #       Note that this would makes sense only if the load is
    #       CPU-bound, which is unlikely, especially if we
    #       re-implement the logic in stored procedures.

    threads = threads or current_app.config[
        "HANDLE_PRISTINE_COLLECTORS_THREADS"
    ]
    wait = (
        wait
        if wait is not None
        else current_app.config["HANDLE_PRISTINE_COLLECTORS_PERIOD"]
    )
    max_count = current_app.config["APP_HANDLE_PRISTINE_COLLECTORS_MAX_COUNT"]
    max_postponement = timedelta(
        days=current_app.config["APP_EXTREME_MESSAGE_DELAY_DAYS"]
    )
    sharding_realm: ShardingRealm = current_app.config["SHARDING_REALM"]
    hash_prefix = u16_to_i16(sharding_realm.realm >> 16)
    hash_mask = u16_to_i16(sharding_realm.realm_mask >> 16)

    def get_args_collection():
        return procedures.get_pristine_collectors(
            hash_mask=hash_mask,
            hash_prefix=hash_prefix,
            max_count=max_count,
        )

    def handle_pristine_collector(debtor_id, collector_id):
        assert sharding_realm.match(collector_id)
        procedures.configure_worker_account(
            debtor_id=debtor_id,
            collector_id=collector_id,
            max_postponement=max_postponement,
        )
        procedures.mark_requested_collector(
            debtor_id=debtor_id, collector_id=collector_id
        )

    logger = logging.getLogger(__name__)
    logger.info("Started pristine collector accounts processor.")

    ThreadPoolProcessor(
        threads,
        get_args_collection=get_args_collection,
        process_func=handle_pristine_collector,
        wait_seconds=wait,
        max_count=max_count,
    ).run(quit_early=quit_early)


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
        " If not specified, the value of the TRIGGER_TRANSFERS_PERIOD"
        " environment variable will be used, defaulting to 5 seconds"
        " if empty."
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
            else current_app.config["TRIGGER_TRANSFERS_PROCESSES"]
        ),
        target=_trigger,
        wait=(
            wait
            if wait is not None
            else current_app.config["TRIGGER_TRANSFERS_PERIOD"]
        ),
    )
    sys.exit(1)
