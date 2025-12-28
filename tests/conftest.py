import pytest
import sqlalchemy
import flask_migrate
from datetime import datetime, timezone, timedelta
from swpt_trade import create_app
from swpt_trade.extensions import db

config_dict = {
    "TESTING": True,
    "MIN_COLLECTOR_ID": 4294967296,
    "MAX_COLLECTOR_ID": 4294968296,
    "OWNER_CREDITOR_ID": 12345,
    "TURN_PHASE1_DURATION": "0",
    "TURN_PHASE2_DURATION": "0",
    "OFFERS_POURING_DURATION": timedelta(minutes=5),
    "TRANSFERS_COLLECTOR_LIMIT": 1,
    "APP_ENABLE_CORS": True,
    "APP_ENABLE_INESSENTIAL_WEBAPIS": True,
    "APP_DEBTOR_INFO_FETCH_BURST_COUNT": 1,
    "APP_RESCHEDULED_TRANSFERS_BURST_COUNT": 1,
    "APP_ACCOUNT_LOCK_MAX_DAYS": 365,
    "APP_RELEASED_ACCOUNT_LOCK_MAX_DAYS": 30,
    "APP_VERIFY_SHARD_YIELD_PER": 1,
    "APP_SURPLUS_BLOCKING_DELAY_DAYS": 14.0,
    "SOLVER_CLIENT_POOL_SIZE": 0,
}


@pytest.fixture(scope="module")
def app():
    """Get a Flask application object."""

    app = create_app(config_dict)
    with app.app_context():
        flask_migrate.upgrade()
        yield app


@pytest.fixture(scope="function")
def db_session(app):
    """Get a Flask-SQLAlchmey session, with an automatic cleanup."""

    yield db.session

    # Cleanup:
    db.session.remove()
    for cmd in [
        "TRUNCATE TABLE configure_account_signal",
        "TRUNCATE TABLE prepare_transfer_signal",
        "TRUNCATE TABLE finalize_transfer_signal",
        "TRUNCATE TABLE fetch_debtor_info_signal",
        "TRUNCATE TABLE store_document_signal",
        "TRUNCATE TABLE discover_debtor_signal",
        "TRUNCATE TABLE confirm_debtor_signal",
        "TRUNCATE TABLE candidate_offer_signal",
        "TRUNCATE TABLE needed_collector_signal",
        "TRUNCATE TABLE revise_account_lock_signal",
        "TRUNCATE TABLE trigger_transfer_signal",
        "TRUNCATE TABLE account_id_request_signal",
        "TRUNCATE TABLE account_id_response_signal",
        "TRUNCATE TABLE start_sending_signal",
        "TRUNCATE TABLE start_dispatching_signal",
        "TRUNCATE TABLE calculate_surplus_signal",
        "TRUNCATE TABLE replayed_account_transfer_signal",
        "TRUNCATE TABLE delayed_account_transfer",
        "TRUNCATE TABLE debtor_info_document",
        "TRUNCATE TABLE debtor_locator_claim",
        "TRUNCATE TABLE debtor_info_fetch",
        "TRUNCATE TABLE trading_policy",
        "TRUNCATE TABLE worker_account",
        "TRUNCATE TABLE needed_worker_account",
        "TRUNCATE TABLE account_lock",
        "TRUNCATE TABLE recently_needed_collector",
        "TRUNCATE TABLE usable_collector",
        "TRUNCATE TABLE interest_rate_change",
        "TRUNCATE TABLE collector_status_change",
        "TRUNCATE TABLE needed_collector_account",
        "DELETE FROM worker_turn",
        "TRUNCATE TABLE creditor_participation",
        "TRUNCATE TABLE dispatching_status",
        "TRUNCATE TABLE worker_collecting",
        "TRUNCATE TABLE worker_sending",
        "TRUNCATE TABLE worker_receiving",
        "TRUNCATE TABLE worker_dispatching",
        "TRUNCATE TABLE transfer_attempt",
        "TRUNCATE TABLE worker_hoarded_currency",
    ]:
        db.session.execute(sqlalchemy.text(cmd))

    for cmd in [
        "TRUNCATE TABLE collector_account",
        "TRUNCATE TABLE turn",
        "TRUNCATE TABLE debtor_info",
        "TRUNCATE TABLE confirmed_debtor",
        "TRUNCATE TABLE currency_info",
        "TRUNCATE TABLE buy_offer",
        "TRUNCATE TABLE sell_offer",
        "TRUNCATE TABLE creditor_taking",
        "TRUNCATE TABLE collector_collecting",
        "TRUNCATE TABLE collector_sending",
        "TRUNCATE TABLE collector_receiving",
        "TRUNCATE TABLE collector_dispatching",
        "TRUNCATE TABLE creditor_giving",
        "TRUNCATE TABLE overloaded_currency",
        "TRUNCATE TABLE hoarded_currency",
    ]:
        db.session.execute(
            sqlalchemy.text(cmd),
            bind_arguments={"bind": db.engines["solver"]},
        )

    db.session.commit()


@pytest.fixture(scope="function")
def current_ts():
    return datetime.now(tz=timezone.utc)


@pytest.fixture()
def restore_sharding_realm(app):
    orig_sharding_realm = app.config["SHARDING_REALM"]
    orig_delete_parent_recs = app.config["DELETE_PARENT_SHARD_RECORDS"]
    yield
    app.config["DELETE_PARENT_SHARD_RECORDS"] = orig_delete_parent_recs
    app.config["SHARDING_REALM"] = orig_sharding_realm
