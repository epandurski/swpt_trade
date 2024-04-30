__version__ = "0.1.0"

import logging
import json
import sys
import os
import os.path
from json import dumps
from typing import List
from flask_cors import CORS
from ast import literal_eval
from swpt_pythonlib.utils import u64_to_i64, ShardingRealm
from .utils import parse_timedelta


def _engine_options(s: str) -> dict:
    try:
        options = json.loads(s)
        options["json_serializer"] = lambda obj: dumps(
            obj, ensure_ascii=False, allow_nan=False, separators=(",", ":")
        )
        return options
    except ValueError:  # pragma: no cover
        raise ValueError(f"Invalid JSON configuration value: {s}")


def _parse_creditor_id(s: str) -> int:
    try:
        n = literal_eval(s.strip())
        if (
            not isinstance(n, int) or n < (-1 << 63) or n >= (1 << 64)
        ):  # pragma: no cover
            raise ValueError
    except Exception:  # pragma: no cover
        raise ValueError(f"Invalid creditor/debtor ID: {s}")

    if n < 0:  # pragma: no cover
        return n
    return u64_to_i64(n)


_parse_debtor_id = _parse_creditor_id


def _excepthook(exc_type, exc_value, traceback):  # pragma: nocover
    logging.error(
        "Uncaught exception occured", exc_info=(exc_type, exc_value, traceback)
    )


def _remove_handlers(logger):
    for h in logger.handlers:
        logger.removeHandler(h)  # pragma: nocover


def _add_console_hander(logger, format: str):
    handler = logging.StreamHandler(sys.stdout)
    fmt = "%(asctime)s:%(levelname)s:%(name)s:%(message)s"

    if format == "text":
        handler.setFormatter(
            logging.Formatter(fmt, datefmt="%Y-%m-%d %H:%M:%S%z")
        )
    elif format == "json":  # pragma: nocover
        from pythonjsonlogger import jsonlogger

        handler.setFormatter(
            jsonlogger.JsonFormatter(fmt, datefmt="%Y-%m-%dT%H:%M:%S%z")
        )
    else:  # pragma: nocover
        raise RuntimeError(f"invalid log format: {format}")

    handler.addFilter(_filter_pika_connection_reset_errors)
    logger.addHandler(handler)


def _configure_root_logger(format: str) -> logging.Logger:
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.WARNING)
    _remove_handlers(root_logger)
    _add_console_hander(root_logger, format)

    return root_logger


def _filter_pika_connection_reset_errors(
    record: logging.LogRecord,
) -> bool:  # pragma: nocover
    # NOTE: Currently, when one of Pika's connections to the RabbitMQ
    # server has not been used for some time, it will be closed by the
    # server. We successfully recover form these situations, but pika
    # logs a bunch of annoying errors. Here we filter out those
    # errors.

    message = record.getMessage()
    is_pika_connection_reset_error = record.levelno == logging.ERROR and (
        (
            record.name == "pika.adapters.utils.io_services_utils"
            and message.startswith(
                "_AsyncBaseTransport._produce() failed, aborting connection: "
                "error=ConnectionResetError(104, 'Connection reset by peer'); "
            )
        )
        or (
            record.name == "pika.adapters.base_connection"
            and message.startswith(
                'connection_lost: StreamLostError: ("Stream connection lost:'
                " ConnectionResetError(104, 'Connection reset by peer')\",)"
            )
        )
        or (
            record.name == "pika.adapters.blocking_connection"
            and message.startswith(
                "Unexpected connection close detected: StreamLostError:"
                ' ("Stream connection lost: ConnectionResetError(104,'
                " 'Connection reset by peer')\",)"
            )
        )
    )

    return not is_pika_connection_reset_error


def configure_logging(
    level: str, format: str, associated_loggers: List[str]
) -> None:
    root_logger = _configure_root_logger(format)

    # Set the log level for this app's logger.
    app_logger = logging.getLogger(__name__)
    app_logger.setLevel(level.upper())
    app_logger_level = app_logger.getEffectiveLevel()

    # Make sure that all loggers that are associated to this app have
    # their log levels set to the specified level as well.
    for qualname in associated_loggers:
        logging.getLogger(qualname).setLevel(app_logger_level)

    # Make sure that the root logger's log level (that is: the log
    # level for all third party libraires) is not lower than the
    # specified level.
    if app_logger_level > root_logger.getEffectiveLevel():
        root_logger.setLevel(app_logger_level)  # pragma: no cover

    # Delete all gunicorn's log handlers (they are not needed in a
    # docker container because everything goes to the stdout anyway),
    # and make sure that the gunicorn logger's log level is not lower
    # than the specified level.
    gunicorn_logger = logging.getLogger("gunicorn.error")
    gunicorn_logger.propagate = True
    _remove_handlers(gunicorn_logger)
    if app_logger_level > gunicorn_logger.getEffectiveLevel():
        gunicorn_logger.setLevel(app_logger_level)  # pragma: no cover


class MetaEnvReader(type):
    def __init__(cls, name, bases, dct):
        """MetaEnvReader class initializer.

        This function will get called when a new class which utilizes
        this metaclass is defined, as opposed to when an instance is
        initialized. This function overrides the default configuration
        from environment variables.

        """

        super().__init__(name, bases, dct)
        NoneType = type(None)
        annotations = dct.get("__annotations__", {})
        falsy_values = {"false", "off", "no", ""}
        for key, value in os.environ.items():
            if hasattr(cls, key):
                target_type = annotations.get(key) or type(getattr(cls, key))
                if target_type is NoneType:  # pragma: no cover
                    target_type = str

                if target_type is bool:
                    value = value.lower() not in falsy_values
                else:
                    value = target_type(value)

                setattr(cls, key, value)


class Configuration(metaclass=MetaEnvReader):
    MIN_COLLECTOR_ID: _parse_creditor_id = None
    MAX_COLLECTOR_ID: _parse_creditor_id = None

    TURN_PERIOD = "1d"
    TURN_PERIOD_OFFSET = "0"
    TURN_CHECK_INTERVAL = "1m"
    TURN_PHASE1_DURATION = "10m"
    TURN_PHASE2_DURATION = "1h"
    TURN_MAX_COMMIT_PERIOD = "30d"

    BASE_DEBTOR_INFO_LOCATOR: str = None
    BASE_DEBTOR_ID: _parse_debtor_id = None
    MAX_DISTANCE_TO_BASE = 10
    MIN_TRADE_AMOUNT = 1000

    SOLVER_POSTGRES_URL = ""
    SOLVER_CLIENT_POOL_SIZE: int = None

    # The default value for `WORKER_POSTGRES_URL` is a random valid
    # connection string. This allows solver processes (they do not use
    # a worker database) to do not specify any value.
    WORKER_POSTGRES_URL = "postgresql+psycopg://localhost/postgres"

    SQLALCHEMY_ENGINE_OPTIONS: _engine_options = _engine_options(
        '{"pool_size": 0}'
    )
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SQLALCHEMY_ECHO = False

    PROTOCOL_BROKER_URL = "amqp://guest:guest@localhost:5672"
    PROTOCOL_BROKER_QUEUE = "swpt_trade"
    PROTOCOL_BROKER_QUEUE_ROUTING_KEY = "#"
    PROTOCOL_BROKER_PROCESSES = 1
    PROTOCOL_BROKER_THREADS = 1
    PROTOCOL_BROKER_PREFETCH_SIZE = 0
    PROTOCOL_BROKER_PREFETCH_COUNT = 1

    CHORES_BROKER_URL = "amqp://guest:guest@localhost:5672"
    CHORES_BROKER_QUEUE = "swpt_trade_chores"
    CHORES_BROKER_PROCESSES = 1
    CHORES_BROKER_THREADS = 1
    CHORES_BROKER_PREFETCH_SIZE = 0
    CHORES_BROKER_PREFETCH_COUNT = 1

    PROCESS_PRISTINE_COLLECTORS_THREADS = 1

    FLUSH_PROCESSES = 1
    FLUSH_PERIOD = 2.0

    FETCH_PROCESSES = 1
    FETCH_PERIOD = 2.0
    FETCH_CONNECTIONS = 100
    FETCH_TIMEOUT = 10.0

    DELETE_PARENT_SHARD_RECORDS = False

    API_TITLE = "Trade API"
    API_VERSION = "v1"
    OPENAPI_VERSION = "3.0.2"
    OPENAPI_URL_PREFIX = "/trade/.docs"
    OPENAPI_REDOC_PATH = ""
    OPENAPI_REDOC_URL = (
        "https://cdn.jsdelivr.net/npm/redoc@next/bundles/redoc.standalone.js"
    )
    OPENAPI_SWAGGER_UI_PATH = "swagger-ui"
    OPENAPI_SWAGGER_UI_URL = (
        None  # or 'https://cdn.jsdelivr.net/npm/swagger-ui-dist/'
    )

    APP_ENABLE_CORS = False
    APP_VERIFY_SSL_CERTS = True
    APP_MIN_DEMURRAGE_RATE = -50.0
    APP_MIN_TRANSFER_NOTE_MAX_BYTES = 100
    APP_ROLL_WORKER_TURNS_WAIT = 60.0
    APP_PROCESS_PRISTINE_COLLECTORS_WAIT = 60.0
    APP_PROCESS_PRISTINE_COLLECTORS_MAX_COUNT = 100000
    APP_LOCATOR_CLAIM_EXPIRY_DAYS = 45.0
    APP_DEBTOR_INFO_EXPIRY_DAYS = 7.0
    APP_EXTREME_MESSAGE_DELAY_DAYS = 7.0
    APP_MAX_HEARTBEAT_DELAY_DAYS = 365.0
    APP_DEBTOR_INFO_DOCUMENTS_SCAN_DAYS = 7.0
    APP_DEBTOR_INFO_DOCUMENTS_SCAN_BLOCKS_PER_QUERY = 40
    APP_DEBTOR_INFO_DOCUMENTS_SCAN_BEAT_MILLISECS = 100
    APP_DEBTOR_LOCATOR_CLAIMS_SCAN_DAYS = 1.0
    APP_DEBTOR_LOCATOR_CLAIMS_SCAN_BLOCKS_PER_QUERY = 40
    APP_DEBTOR_LOCATOR_CLAIMS_SCAN_BEAT_MILLISECS = 100
    APP_TRADING_POLICIES_SCAN_DAYS = 7.0
    APP_TRADING_POLICIES_SCAN_BLOCKS_PER_QUERY = 40
    APP_TRADING_POLICIES_SCAN_BEAT_MILLISECS = 100
    APP_WORKER_ACCOUNTS_SCAN_DAYS = 7.0
    APP_WORKER_ACCOUNTS_SCAN_BLOCKS_PER_QUERY = 40
    APP_WORKER_ACCOUNTS_SCAN_BEAT_MILLISECS = 100
    APP_INTEREST_RATE_CHANGES_SCAN_DAYS = 7
    APP_INTEREST_RATE_CHANGES_SCAN_BLOCKS_PER_QUERY = 40
    APP_INTEREST_RATE_CHANGES_SCAN_BEAT_MILLISECS = 100
    APP_NEEDED_WORKER_ACCOUNTS_SCAN_DAYS = 7.0
    APP_NEEDED_WORKER_ACCOUNTS_SCAN_BLOCKS_PER_QUERY = 40
    APP_NEEDED_WORKER_ACCOUNTS_SCAN_BEAT_MILLISECS = 100
    APP_RECENTLY_NEEDED_COLLECTORS_SCAN_DAYS = 7.0
    APP_RECENTLY_NEEDED_COLLECTORS_SCAN_BLOCKS_PER_QUERY = 40
    APP_RECENTLY_NEEDED_COLLECTORS_SCAN_BEAT_MILLISECS = 100
    APP_FLUSH_CONFIGURE_ACCOUNTS_BURST_COUNT = 10000
    APP_FLUSH_PREPARE_TRANSFERS_BURST_COUNT = 10000
    APP_FLUSH_FINALIZE_TRANSFERS_BURST_COUNT = 10000
    APP_FLUSH_FETCH_DEBTOR_INFO_BURST_COUNT = 10000
    APP_FLUSH_STORE_DOCUMENT_BURST_COUNT = 10000
    APP_FLUSH_DISCOVER_DEBTOR_BURST_COUNT = 10000
    APP_FLUSH_CONFIRM_DEBTOR_BURST_COUNT = 10000
    APP_FLUSH_ACTIVATE_COLLECTOR_BURST_COUNT = 10000
    APP_FLUSH_CANDIDATE_OFFER_BURST_COUNT = 10000
    APP_FLUSH_NEEDED_COLLECTOR_BURST_COUNT = 10000
    APP_DEBTOR_INFO_FETCH_BURST_COUNT = 2000
    APP_SUPERUSER_SUBJECT_REGEX = "^creditors-superuser$"
    APP_SUPERVISOR_SUBJECT_REGEX = "^creditors-supervisor$"
    APP_CREDITOR_SUBJECT_REGEX = "^creditors:([0-9]+)$"


def _check_config_sanity(c):  # pragma: nocover
    if (c["SHARDING_REALM"].realm_mask & 0x0000ffff) != 0:
        raise RuntimeError(
            "The configured SHARDING_REALM indicates that there are"
            " too many shards."
        )

    if c["APP_LOCATOR_CLAIM_EXPIRY_DAYS"] < 30.0:
        raise RuntimeError(
            "The configured value for APP_LOCATOR_CLAIM_EXPIRY_DAYS is"
            " too small. Choose a more appropriate value."
        )

    if c["APP_DEBTOR_INFO_EXPIRY_DAYS"] < 1.0:
        raise RuntimeError(
            "The configured value for APP_DEBTOR_INFO_EXPIRY_DAYS is too"
            " small. Choose a more appropriate value."
        )

    if (
        c["APP_LOCATOR_CLAIM_EXPIRY_DAYS"]
        < 5 * c["APP_DEBTOR_INFO_EXPIRY_DAYS"]
    ):
        raise RuntimeError(
            "The configured value for APP_LOCATOR_CLAIM_EXPIRY_DAYS is"
            " too small compared to the configured value for"
            " APP_DEBTOR_INFO_EXPIRY_DAYS. Choose more appropriate"
            " configuration values."
        )


def create_app(config_dict={}):
    from werkzeug.middleware.proxy_fix import ProxyFix
    from flask import Flask
    from swpt_pythonlib.utils import Int64Converter
    from .extensions import db, migrate, api, publisher, chores_publisher
    from .routes import admin_api, specs
    from .cli import swpt_trade
    from . import models  # noqa

    app = Flask(__name__)
    app.wsgi_app = ProxyFix(app.wsgi_app, x_port=1)
    app.url_map.converters["i64"] = Int64Converter
    app.config.from_object(Configuration)
    app.config.from_mapping(config_dict)
    app.config["API_SPEC_OPTIONS"] = specs.API_SPEC_OPTIONS
    app.config["SQLALCHEMY_DATABASE_URI"] = app.config["WORKER_POSTGRES_URL"]
    app.config["TURN_MAX_COMMIT_INTERVAL"] = parse_timedelta(
        app.config["TURN_MAX_COMMIT_PERIOD"]
    )

    solver_engine_options = app.config["SQLALCHEMY_ENGINE_OPTIONS"].copy()
    solver_client_pool_size = app.config["SOLVER_CLIENT_POOL_SIZE"]
    if solver_client_pool_size is not None:
        solver_engine_options["pool_size"] = solver_client_pool_size

    app.config["SQLALCHEMY_BINDS"] = {
        "solver": {
            "url": app.config["SOLVER_POSTGRES_URL"],
            **solver_engine_options,
        },
    }
    app.config["SHARDING_REALM"] = ShardingRealm(
        Configuration.PROTOCOL_BROKER_QUEUE_ROUTING_KEY
    )
    if app.config["APP_ENABLE_CORS"]:
        CORS(
            app,
            max_age=24 * 60 * 60,
            vary_header=False,
            expose_headers=["Location"],
        )
    db.init_app(app)
    migrate.init_app(app, db)
    publisher.init_app(app)
    chores_publisher.init_app(app)
    api.init_app(app)
    api.register_blueprint(admin_api)
    app.cli.add_command(swpt_trade)
    _check_config_sanity(app.config)

    return app


configure_logging(
    level=os.environ.get("APP_LOG_LEVEL", "warning"),
    format=os.environ.get("APP_LOG_FORMAT", "text"),
    associated_loggers=os.environ.get("APP_ASSOCIATED_LOGGERS", "").split(),
)
sys.excepthook = _excepthook
