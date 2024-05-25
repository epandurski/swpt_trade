import logging
import click
from datetime import timedelta
from flask import current_app
from flask.cli import with_appcontext
from swpt_trade.extensions import db
from .common import swpt_trade


@swpt_trade.command("scan_debtor_info_documents")
@with_appcontext
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
    of days is not specified, the value of the environment variable
    APP_DEBTOR_INFO_DOCUMENTS_SCAN_DAYS is taken. If it is not set,
    the default number of days is 7.
    """
    from swpt_trade.table_scanners import DebtorInfoDocumentScanner

    logger = logging.getLogger(__name__)
    logger.info("Started debtor info documents scanner.")
    days = days or current_app.config["APP_DEBTOR_INFO_DOCUMENTS_SCAN_DAYS"]
    assert days > 0.0
    scanner = DebtorInfoDocumentScanner()
    scanner.run(db.engine, timedelta(days=days), quit_early=quit_early)


@swpt_trade.command("scan_debtor_locator_claims")
@with_appcontext
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
    of days is not specified, the value of the environment variable
    APP_DEBTOR_LOCATOR_CLAIMS_SCAN_DAYS is taken. If it is not set,
    the default number of days is 1.
    """
    from swpt_trade.table_scanners import DebtorLocatorClaimScanner

    logger = logging.getLogger(__name__)
    logger.info("Started debtor locator claims scanner.")
    days = days or current_app.config["APP_DEBTOR_LOCATOR_CLAIMS_SCAN_DAYS"]
    assert days > 0.0
    scanner = DebtorLocatorClaimScanner()
    scanner.run(db.engine, timedelta(days=days), quit_early=quit_early)


@swpt_trade.command("scan_trading_policies")
@with_appcontext
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
    days is not specified, the value of the environment variable
    APP_TRADING_POLICIES_SCAN_DAYS is taken. If it is not set, the
    default number of days is 7.
    """
    from swpt_trade.table_scanners import TradingPoliciesScanner

    logger = logging.getLogger(__name__)
    logger.info("Started trading policies scanner.")
    days = days or current_app.config["APP_TRADING_POLICIES_SCAN_DAYS"]
    assert days > 0.0
    scanner = TradingPoliciesScanner()
    scanner.run(db.engine, timedelta(days=days), quit_early=quit_early)


@swpt_trade.command("scan_worker_accounts")
@with_appcontext
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
    days is not specified, the value of the environment variable
    APP_WORKER_ACCOUNTS_SCAN_DAYS is taken. If it is not set, the
    default number of days is 7.
    """
    from swpt_trade.table_scanners import WorkerAccountsScanner

    logger = logging.getLogger(__name__)
    logger.info("Started worker accounts scanner.")
    days = days or current_app.config["APP_WORKER_ACCOUNTS_SCAN_DAYS"]
    assert days > 0.0
    scanner = WorkerAccountsScanner()
    scanner.run(db.engine, timedelta(days=days), quit_early=quit_early)


@swpt_trade.command("scan_needed_worker_accounts")
@with_appcontext
@click.option("-d", "--days", type=float, help="The number of days.")
@click.option(
    "--quit-early",
    is_flag=True,
    default=False,
    help="Exit after some time (mainly useful during testing).",
)
def scan_needed_worker_accounts(days, quit_early):
    """Start a process that garbage collects needed worker accounts.

    The specified number of days determines the intended duration of a
    single pass through the needed worker accounts table. If the
    number of days is not specified, the value of the environment
    variable APP_NEEDED_WORKER_ACCOUNTS_SCAN_DAYS is taken. If it is
    not set, the default number of days is 7.
    """
    from swpt_trade.table_scanners import NeededWorkerAccountsScanner

    logger = logging.getLogger(__name__)
    logger.info("Started needed worker accounts scanner.")
    days = days or current_app.config["APP_NEEDED_WORKER_ACCOUNTS_SCAN_DAYS"]
    assert days > 0.0
    scanner = NeededWorkerAccountsScanner()
    scanner.run(db.engine, timedelta(days=days), quit_early=quit_early)


@swpt_trade.command("scan_interest_rate_changes")
@with_appcontext
@click.option("-d", "--days", type=float, help="The number of days.")
@click.option(
    "--quit-early",
    is_flag=True,
    default=False,
    help="Exit after some time (mainly useful during testing).",
)
def scan_interest_rate_changes(days, quit_early):
    """Start a process that garbage collects stale interest rate changes.

    The specified number of days determines the intended duration of a
    single pass through the debtor info documents table. If the number
    of days is not specified, the value of the environment variable
    APP_INTEREST_RATE_CHANGES_SCAN_DAYS is taken. If it is not set,
    the default number of days is 7.
    """
    from swpt_trade.table_scanners import InterestRateChangeScanner

    logger = logging.getLogger(__name__)
    logger.info("Started interest rate changes scanner.")
    days = days or current_app.config["APP_INTEREST_RATE_CHANGES_SCAN_DAYS"]
    assert days > 0.0
    scanner = InterestRateChangeScanner()
    scanner.run(db.engine, timedelta(days=days), quit_early=quit_early)


@swpt_trade.command("scan_recently_needed_collectors")
@with_appcontext
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
    of days is not specified, the value of the environment variable
    APP_RECENTLY_NEEDED_COLLECTORS_SCAN_DAYS is taken. If it is not
    set, the default number of days is 7.
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


@swpt_trade.command("scan_account_locks")
@with_appcontext
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
    is not specified, the value of the environment variable
    APP_ACCOUNT_LOCKS_SCAN_DAYS is taken. If it is not set, the
    default number of days is 7.
    """
    from swpt_trade.table_scanners import AccountLocksScanner

    logger = logging.getLogger(__name__)
    logger.info("Started account lock records scanner.")
    days = days or current_app.config["APP_ACCOUNT_LOCKS_SCAN_DAYS"]
    assert days > 0.0
    scanner = AccountLocksScanner()
    scanner.run(db.engine, timedelta(days=days), quit_early=quit_early)