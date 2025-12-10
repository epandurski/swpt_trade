import logging
import time
import signal
import sys
import random
import click
from typing import Any
from flask import current_app
from flask.cli import with_appcontext
from swpt_pythonlib.multiproc_utils import (
    HANDLED_SIGNALS,
    spawn_worker_processes,
    try_unblock_signals,
)
from swpt_trade.fetch_debtor_infos import process_debtor_info_fetches
from .common import swpt_trade


@swpt_trade.command("fetch_debtor_infos")
@with_appcontext
@click.option(
    "-p",
    "--processes",
    type=int,
    help=(
        "The number of worker processes."
        " If not specified, the value of the HTTP_FETCH_PROCESSES environment"
        " variable will be used, defaulting to 1 if empty."
    ),
)
@click.option(
    "-c",
    "--connections",
    type=int,
    help=(
        "The maximum number of HTTP connections that each worker process"
        " will initiate simultaneously. If not specified, the value of"
        " the HTTP_FETCH_CONNECTIONS environment variable will be used,"
        " defaulting to 100 if empty."
    ),
)
@click.option(
    "-t",
    "--timeout",
    type=float,
    help=(
        "The number of seconds to wait for a response, before the HTTP"
        " request is cancelled. If not specified, the value of the "
        " HTTP_FETCH_TIMEOUT environment variable will be used, defaulting"
        " to 10 seconds if empty."
    ),
)
@click.option(
    "-w",
    "--wait",
    type=float,
    help=(
        "Poll the database for scheduled requests every FLOAT seconds."
        " If not specified, the value of the HTTP_FETCH_PERIOD environment"
        " variable will be used, defaulting to 5 seconds if empty."
    ),
)
@click.option(
    "--quit-early",
    is_flag=True,
    default=False,
    help="Exit after some time (mainly useful during testing).",
)
def fetch_debtor_infos(
    processes: int,
    connections: int,
    timeout: float,
    wait: float,
    quit_early: bool,
) -> None:
    """Run processes that perform scheduled HTTP requests to fetch
    debtor info documents.

    """
    logger = logging.getLogger(__name__)
    logger.info("Started fetching debtor info documents.")

    def _fetch(
            connections: int,
            timeout: float,
            wait: float,
    ) -> None:  # pragma: no cover
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
                    count = process_debtor_info_fetches(connections, timeout)
                except Exception:
                    logger.exception(
                        "Caught error while fetching debtor info documents."
                    )
                    sys.exit(1)

                if count > 0:
                    logger.info(
                        "%i debtor info documents have been fetched.", count
                    )
                else:
                    logger.debug("0 debtor info documents have been fetched.")

                if quit_early:
                    break
                time.sleep(max(0.0, wait + started_at - time.time()))

    spawn_worker_processes(
        processes=(
            processes
            if processes is not None
            else current_app.config["HTTP_FETCH_PROCESSES"]
        ),
        target=_fetch,
        connections=(
            connections
            if connections is not None
            else current_app.config["HTTP_FETCH_CONNECTIONS"]
        ),
        timeout=(
            timeout
            if timeout is not None
            else current_app.config["HTTP_FETCH_TIMEOUT"]
        ),
        wait=(
            wait
            if wait is not None
            else current_app.config["HTTP_FETCH_PERIOD"]
        ),
    )
    sys.exit(1)
