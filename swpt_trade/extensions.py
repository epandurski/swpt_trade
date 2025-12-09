import warnings
from sqlalchemy.exc import SAWarning
from sqlalchemy.engine import Engine
from sqlalchemy import event
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from swpt_pythonlib.flask_signalbus import (
    SignalBusMixin,
    AtomicProceduresMixin,
)
from swpt_pythonlib import rabbitmq
from flask_smorest import Api


TO_COORDINATORS_EXCHANGE = "to_coordinators"
TO_DEBTORS_EXCHANGE = "to_debtors"
TO_CREDITORS_EXCHANGE = "to_creditors"
ACCOUNTS_IN_EXCHANGE = "accounts_in"
CREDITORS_OUT_EXCHANGE = "creditors_out"
CREDITORS_IN_EXCHANGE = "creditors_in"
DEBTORS_OUT_EXCHANGE = "debtors_out"
DEBTORS_IN_EXCHANGE = "debtors_in"
TO_TRADE_EXCHANGE = "to_trade"


warnings.filterwarnings(
    "ignore",
    r"Reset agent is not active.  This should not occur unless there was"
    r" already a connectivity error in progress",
    SAWarning,
)


class CustomAlchemy(AtomicProceduresMixin, SignalBusMixin, SQLAlchemy):
    pass


@event.listens_for(Engine, "connect")
def set_postgres_parametes(dbapi_connection, connection_record):
    cursor = dbapi_connection.cursor()
    cursor.execute("SET SESSION enable_seqscan = off")
    cursor.close()
    dbapi_connection.commit()


db = CustomAlchemy()
migrate = Migrate()
publisher = rabbitmq.Publisher(url_config_key="PROTOCOL_BROKER_URL")
api = Api()
