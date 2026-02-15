"""create pktypes

Revision ID: e056ba322b0b
Revises: 0693d4e041bb
Create Date: 2026-02-12 15:57:19.037951

"""
from alembic import op
import sqlalchemy as sa
from datetime import datetime
from sqlalchemy.inspection import inspect


# revision identifiers, used by Alembic.
revision = 'e056ba322b0b'
down_revision = '0693d4e041bb'
branch_labels = None
depends_on = None


def _pg_type(column_type):
    if column_type.python_type == datetime:
        if column_type.timezone:
            return "TIMESTAMP WITH TIME ZONE"
        else:
            return "TIMESTAMP"

    return str(column_type)


def _pktype_name(model):
    return f"{model.__table__.name}_pktype"


def create_pktype(model):
    mapper = inspect(model)
    type_declaration = ','.join(
        f"{c.key} {_pg_type(c.type)}" for c in mapper.primary_key
    )
    op.execute(
        f"CREATE TYPE {_pktype_name(model)} AS ({type_declaration})"
    )


def drop_pktype(model):
    op.execute(f"DROP TYPE IF EXISTS {_pktype_name(model)}")


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()





def upgrade_():
    from swpt_trade import models

    create_pktype(models.DebtorInfoDocument)
    create_pktype(models.DebtorInfoFetch)
    create_pktype(models.DebtorLocatorClaim)
    create_pktype(models.TradingPolicy)
    create_pktype(models.NeededWorkerAccount)
    create_pktype(models.InterestRateChange)
    create_pktype(models.CollectorStatusChange)
    create_pktype(models.NeededCollectorAccount)
    create_pktype(models.WorkerAccount)
    create_pktype(models.RecentlyNeededCollector)
    create_pktype(models.AccountLock)
    create_pktype(models.CreditorParticipation)
    create_pktype(models.DispatchingStatus)
    create_pktype(models.WorkerCollecting)
    create_pktype(models.WorkerSending)
    create_pktype(models.WorkerReceiving)
    create_pktype(models.WorkerDispatching)
    create_pktype(models.TransferAttempt)
    create_pktype(models.DelayedAccountTransfer)
    create_pktype(models.ConfigureAccountSignal)
    create_pktype(models.PrepareTransferSignal)
    create_pktype(models.FinalizeTransferSignal)
    create_pktype(models.FetchDebtorInfoSignal)
    create_pktype(models.StoreDocumentSignal)
    create_pktype(models.DiscoverDebtorSignal)
    create_pktype(models.ConfirmDebtorSignal)
    create_pktype(models.CandidateOfferSignal)
    create_pktype(models.NeededCollectorSignal)
    create_pktype(models.ReviseAccountLockSignal)
    create_pktype(models.TriggerTransferSignal)
    create_pktype(models.AccountIdRequestSignal)
    create_pktype(models.AccountIdResponseSignal)
    create_pktype(models.StartSendingSignal)
    create_pktype(models.StartDispatchingSignal)
    create_pktype(models.CalculateSurplusSignal)
    create_pktype(models.ReplayedAccountTransferSignal)


def downgrade_():
    from swpt_trade import models

    drop_pktype(models.DebtorInfoDocument)
    drop_pktype(models.DebtorInfoFetch)
    drop_pktype(models.DebtorLocatorClaim)
    drop_pktype(models.TradingPolicy)
    drop_pktype(models.NeededWorkerAccount)
    drop_pktype(models.InterestRateChange)
    drop_pktype(models.CollectorStatusChange)
    drop_pktype(models.NeededCollectorAccount)
    drop_pktype(models.WorkerAccount)
    drop_pktype(models.RecentlyNeededCollector)
    drop_pktype(models.AccountLock)
    drop_pktype(models.CreditorParticipation)
    drop_pktype(models.DispatchingStatus)
    drop_pktype(models.WorkerCollecting)
    drop_pktype(models.WorkerSending)
    drop_pktype(models.WorkerReceiving)
    drop_pktype(models.WorkerDispatching)
    drop_pktype(models.TransferAttempt)
    drop_pktype(models.DelayedAccountTransfer)
    drop_pktype(models.ConfigureAccountSignal)
    drop_pktype(models.PrepareTransferSignal)
    drop_pktype(models.FinalizeTransferSignal)
    drop_pktype(models.FetchDebtorInfoSignal)
    drop_pktype(models.StoreDocumentSignal)
    drop_pktype(models.DiscoverDebtorSignal)
    drop_pktype(models.ConfirmDebtorSignal)
    drop_pktype(models.CandidateOfferSignal)
    drop_pktype(models.NeededCollectorSignal)
    drop_pktype(models.ReviseAccountLockSignal)
    drop_pktype(models.TriggerTransferSignal)
    drop_pktype(models.AccountIdRequestSignal)
    drop_pktype(models.AccountIdResponseSignal)
    drop_pktype(models.StartSendingSignal)
    drop_pktype(models.StartDispatchingSignal)
    drop_pktype(models.CalculateSurplusSignal)
    drop_pktype(models.ReplayedAccountTransferSignal)


def upgrade_solver():
    from swpt_trade import models

    create_pktype(models.CollectorAccount)
    create_pktype(models.HoardedCurrency)


def downgrade_solver():
    from swpt_trade import models

    drop_pktype(models.CollectorAccount)
    drop_pktype(models.HoardedCurrency)
