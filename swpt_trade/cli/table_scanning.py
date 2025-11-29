import logging
import click
import time
import threading
from datetime import timedelta
from flask import current_app
from swpt_trade.extensions import db
from .common import swpt_trade


@click.option("-d", "--days", type=float, help="The number of days.")
@click.option(
    "--quit-early",
    is_flag=True,
    default=False,
    help="Exit after some time (mainly useful during testing).",
)
def scan_debtor_info_documents(days, quit_early):
    """Start a process that garbage collects stale debtor info documents.

    The specified number of days determines the intended duration of a
    single pass through the debtor info documents table. If the number
    of days is not specified, the default is 7 days.
    """
    from swpt_trade.table_scanners import DebtorInfoDocumentsScanner

    logger = logging.getLogger(__name__)
    logger.info("Started debtor info documents scanner.")
    days = days or current_app.config["APP_DEBTOR_INFO_DOCUMENTS_SCAN_DAYS"]
    assert days > 0.0
    scanner = DebtorInfoDocumentsScanner()
    scanner.run(db.engine, timedelta(days=days), quit_early=quit_early)


@click.option("-d", "--days", type=float, help="The number of days.")
@click.option(
    "--quit-early",
    is_flag=True,
    default=False,
    help="Exit after some time (mainly useful during testing).",
)
def scan_debtor_locator_claims(days, quit_early):
    """Start a process that garbage collects stale debtor locator claims.

    The specified number of days determines the intended duration of a
    single pass through the debtor locator claims table. If the number
    of days is not specified, the default is 1 day.
    """
    from swpt_trade.table_scanners import DebtorLocatorClaimsScanner

    logger = logging.getLogger(__name__)
    logger.info("Started debtor locator claims scanner.")
    days = days or current_app.config["APP_DEBTOR_LOCATOR_CLAIMS_SCAN_DAYS"]
    assert days > 0.0
    scanner = DebtorLocatorClaimsScanner()
    scanner.run(db.engine, timedelta(days=days), quit_early=quit_early)


@click.option("-d", "--days", type=float, help="The number of days.")
@click.option(
    "--quit-early",
    is_flag=True,
    default=False,
    help="Exit after some time (mainly useful during testing).",
)
def scan_trading_policies(days, quit_early):
    """Start a process that garbage collects useless trading policies.

    The specified number of days determines the intended duration of a
    single pass through the trading policies table. If the number of
    days is not specified, the default is 7 days.
    """
    from swpt_trade.table_scanners import TradingPoliciesScanner

    logger = logging.getLogger(__name__)
    logger.info("Started trading policies scanner.")
    days = days or current_app.config["APP_TRADING_POLICIES_SCAN_DAYS"]
    assert days > 0.0
    scanner = TradingPoliciesScanner()
    scanner.run(db.engine, timedelta(days=days), quit_early=quit_early)


@click.option("-d", "--days", type=float, help="The number of days.")
@click.option(
    "--quit-early",
    is_flag=True,
    default=False,
    help="Exit after some time (mainly useful during testing).",
)
def scan_worker_accounts(days, quit_early):
    """Start a process that garbage collects worker accounts.

    The specified number of days determines the intended duration of a
    single pass through the worker accounts table. If the number of
    days is not specified, the default is 7 days.
    """
    from swpt_trade.table_scanners import WorkerAccountsScanner

    logger = logging.getLogger(__name__)
    logger.info("Started worker accounts scanner.")
    days = days or current_app.config["APP_WORKER_ACCOUNTS_SCAN_DAYS"]
    assert days > 0.0
    scanner = WorkerAccountsScanner()
    scanner.run(db.engine, timedelta(days=days), quit_early=quit_early)


@click.option("-d", "--days", type=float, help="The number of days.")
@click.option(
    "--quit-early",
    is_flag=True,
    default=False,
    help="Exit after some time (mainly useful during testing).",
)
def scan_recently_needed_collectors(days, quit_early):
    """Start a process that garbage collects recently needed collector
    records.

    The specified number of days determines the intended duration of a
    single pass through the debtor locator claims table. If the number
    of days is not specified, the default is 7 days.
    """
    from swpt_trade.table_scanners import RecentlyNeededCollectorsScanner

    logger = logging.getLogger(__name__)
    logger.info("Started recently needed collector records scanner.")
    days = days or current_app.config[
        "APP_RECENTLY_NEEDED_COLLECTORS_SCAN_DAYS"
    ]
    assert days > 0.0
    scanner = RecentlyNeededCollectorsScanner()
    scanner.run(db.engine, timedelta(days=days), quit_early=quit_early)


@click.option("-d", "--days", type=float, help="The number of days.")
@click.option(
    "--quit-early",
    is_flag=True,
    default=False,
    help="Exit after some time (mainly useful during testing).",
)
def scan_account_locks(days, quit_early):
    """Start a process that garbage collects account lock records.

    The specified number of days determines the intended duration of a
    single pass through the account locks table. If the number of days
    is not specified, the default is 7 days.
    """
    from swpt_trade.table_scanners import AccountLocksScanner

    logger = logging.getLogger(__name__)
    logger.info("Started account lock records scanner.")
    days = days or current_app.config["APP_ACCOUNT_LOCKS_SCAN_DAYS"]
    assert days > 0.0
    scanner = AccountLocksScanner()
    scanner.run(db.engine, timedelta(days=days), quit_early=quit_early)


@click.option("-d", "--days", type=float, help="The number of days.")
@click.option(
    "--quit-early",
    is_flag=True,
    default=False,
    help="Exit after some time (mainly useful during testing).",
)
def scan_creditor_participations(days, quit_early):
    """Start a process that garbage collects creditor participation
    records.

    The specified number of days determines the intended duration of a
    single pass through the creditor participations table. If the
    number of days is not specified, the default is 7 days.
    """
    from swpt_trade.table_scanners import CreditorParticipationsScanner

    logger = logging.getLogger(__name__)
    logger.info("Started creditor participation records scanner.")
    days = days or current_app.config["APP_CREDITOR_PARTICIPATIONS_SCAN_DAYS"]
    assert days > 0.0
    scanner = CreditorParticipationsScanner()
    scanner.run(db.engine, timedelta(days=days), quit_early=quit_early)


@click.option("-d", "--days", type=float, help="The number of days.")
@click.option(
    "--quit-early",
    is_flag=True,
    default=False,
    help="Exit after some time (mainly useful during testing).",
)
def scan_dispatching_statuses(days, quit_early):
    """Start a process that garbage collects dispatching status records.

    The specified number of days determines the intended duration of a
    single pass through the dispatching status table. If the number of
    days is not specified, the default is 7 days.
    """
    from swpt_trade.table_scanners import DispatchingStatusesScanner

    logger = logging.getLogger(__name__)
    logger.info("Started dispatching status records scanner.")
    days = days or current_app.config["APP_DISPATCHING_STATUSES_SCAN_DAYS"]
    assert days > 0.0
    scanner = DispatchingStatusesScanner()
    scanner.run(db.engine, timedelta(days=days), quit_early=quit_early)


@click.option("-d", "--days", type=float, help="The number of days.")
@click.option(
    "--quit-early",
    is_flag=True,
    default=False,
    help="Exit after some time (mainly useful during testing).",
)
def scan_worker_collectings(days, quit_early):
    """Start a process that garbage collects worker collecting records.

    The specified number of days determines the intended duration of a
    single pass through the worker collecting table. If the number of
    days is not specified, the default is 3 days.
    """
    from swpt_trade.table_scanners import WorkerCollectingsScanner

    logger = logging.getLogger(__name__)
    logger.info("Started worker collecting records scanner.")
    days = days or current_app.config["APP_WORKER_COLLECTINGS_SCAN_DAYS"]
    assert days > 0.0
    scanner = WorkerCollectingsScanner()
    scanner.run(db.engine, timedelta(days=days), quit_early=quit_early)


@click.option("-d", "--days", type=float, help="The number of days.")
@click.option(
    "--quit-early",
    is_flag=True,
    default=False,
    help="Exit after some time (mainly useful during testing).",
)
def scan_worker_sendings(days, quit_early):
    """Start a process that garbage collects worker sending records.

    The specified number of days determines the intended duration of a
    single pass through the worker sending table. If the number of
    days is not specified, the value default is 1 day.
    """
    from swpt_trade.table_scanners import WorkerSendingsScanner

    logger = logging.getLogger(__name__)
    logger.info("Started worker sending records scanner.")
    days = days or current_app.config["APP_WORKER_SENDINGS_SCAN_DAYS"]
    assert days > 0.0
    scanner = WorkerSendingsScanner()
    scanner.run(db.engine, timedelta(days=days), quit_early=quit_early)


@click.option("-d", "--days", type=float, help="The number of days.")
@click.option(
    "--quit-early",
    is_flag=True,
    default=False,
    help="Exit after some time (mainly useful during testing).",
)
def scan_worker_receivings(days, quit_early):
    """Start a process that garbage collects worker receiving records.

    The specified number of days determines the intended duration of a
    single pass through the worker receiving table. If the number of
    days is not specified, the value default is 1 day.
    """
    from swpt_trade.table_scanners import WorkerReceivingsScanner

    logger = logging.getLogger(__name__)
    logger.info("Started worker receiving records scanner.")
    days = days or current_app.config["APP_WORKER_RECEIVINGS_SCAN_DAYS"]
    assert days > 0.0
    scanner = WorkerReceivingsScanner()
    scanner.run(db.engine, timedelta(days=days), quit_early=quit_early)


@click.option("-d", "--days", type=float, help="The number of days.")
@click.option(
    "--quit-early",
    is_flag=True,
    default=False,
    help="Exit after some time (mainly useful during testing).",
)
def scan_worker_dispatchings(days, quit_early):
    """Start a process that garbage collects worker dispatching records.

    The specified number of days determines the intended duration of a
    single pass through the worker dispatching table. If the number of
    days is not specified, the default is 7 days.
    """
    from swpt_trade.table_scanners import WorkerDispatchingsScanner

    logger = logging.getLogger(__name__)
    logger.info("Started worker dispatching records scanner.")
    days = days or current_app.config["APP_WORKER_DISPATCHINGS_SCAN_DAYS"]
    assert days > 0.0
    scanner = WorkerDispatchingsScanner()
    scanner.run(db.engine, timedelta(days=days), quit_early=quit_early)


@click.option("-d", "--days", type=float, help="The number of days.")
@click.option(
    "--quit-early",
    is_flag=True,
    default=False,
    help="Exit after some time (mainly useful during testing).",
)
def scan_transfer_attempts(days, quit_early):
    """Start a process that garbage collects transfer attempt records.

    The specified number of days determines the intended duration of a
    single pass through the transfer attempt table. If the number of
    days is not specified, the default is 7 days.
    """
    from swpt_trade.table_scanners import TransferAttemptsScanner

    logger = logging.getLogger(__name__)
    logger.info("Started worker transfer attempts scanner.")
    days = days or current_app.config["APP_TRANSFER_ATTEMPTS_SCAN_DAYS"]
    assert days > 0.0
    scanner = TransferAttemptsScanner()
    scanner.run(db.engine, timedelta(days=days), quit_early=quit_early)


_all_scans = [
    scan_debtor_info_documents,
    scan_debtor_locator_claims,
    scan_trading_policies,
    scan_worker_accounts,
    scan_recently_needed_collectors,
    scan_account_locks,
    scan_creditor_participations,
    scan_dispatching_statuses,
    scan_worker_collectings,
    scan_worker_sendings,
    scan_worker_receivings,
    scan_worker_dispatchings,
    scan_transfer_attempts,
]
for scan in _all_scans:
    swpt_trade.command(scan.__name__)(scan)


@swpt_trade.command("scan_all")
def scan_all():  # pragma: no cover
    app = current_app._get_current_object()

    def wrap(f):
        def wrapper_func(*args, **kwargs):
            logger = logging.getLogger(__name__)
            ctx = app.app_context()
            ctx.push()
            while True:
                try:
                    f(*args, **kwargs)
                except Exception:
                    logger.exception("Caught error in %s.", f.__name__)
                else:
                    logger.error("Unexpected return from %s.", f.__name__)
                time.sleep(60.0)
        return wrapper_func

    for t in [
        threading.Thread(
            target=wrap(scan),
            args=(None, False),
            daemon=True,
        )
        for scan in _all_scans
    ]:
        t.start()

    while True:
        time.sleep(5.0)
