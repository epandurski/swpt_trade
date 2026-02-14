"""storage params

Revision ID: 13c9124c25ec
Revises: c5c10689ac67
Create Date: 2026-01-12 14:02:46.588973

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '13c9124c25ec'
down_revision = 'c5c10689ac67'
branch_labels = None
depends_on = None


def set_storage_params(table, **kwargs):
    storage_params = ', '.join(
        f"{param} = {str(value).lower()}" for param, value in kwargs.items()
    )
    op.execute(f"ALTER TABLE {table} SET ({storage_params})")


def reset_storage_params(table, param_names):
    op.execute(f"ALTER TABLE {table} RESET ({', '.join(param_names)})")


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()





def upgrade_():
    # Tables containing non-updatable records:
    set_storage_params(
        'recently_needed_collector',
        fillfactor=100,
        autovacuum_vacuum_scale_factor=0.2,
        autovacuum_vacuum_insert_scale_factor=0.2,
    )
    set_storage_params(
        'interest_rate_change',
        fillfactor=100,
        autovacuum_vacuum_threshold=10000,
        autovacuum_vacuum_scale_factor=0.001,
        autovacuum_vacuum_insert_threshold=10000,
        autovacuum_vacuum_insert_scale_factor=0.001,
    )
    set_storage_params(
        'usable_collector',
        fillfactor=100,
        autovacuum_vacuum_scale_factor=0.2,
        autovacuum_vacuum_insert_scale_factor=0.2,
    )
    set_storage_params(
        'worker_hoarded_currency',
        fillfactor=100,
        autovacuum_vacuum_scale_factor=0.2,
        autovacuum_vacuum_insert_scale_factor=0.2,
    )
    set_storage_params(
        'creditor_participation',
        fillfactor=100,
        autovacuum_vacuum_scale_factor=0.2,
        autovacuum_vacuum_insert_scale_factor=0.2,
    )
    set_storage_params(
        'delayed_account_transfer',
        fillfactor=100,
        autovacuum_vacuum_scale_factor=0.2,
        autovacuum_vacuum_insert_scale_factor=0.2,
    )

    # Tables containing updatable records:
    set_storage_params(
        'worker_turn',
        fillfactor=100,
        autovacuum_vacuum_scale_factor=0.2,
        autovacuum_vacuum_insert_threshold=100,
        autovacuum_vacuum_insert_scale_factor=0.0,
    )
    set_storage_params(
        'debtor_info_fetch',
        fillfactor=80,
        autovacuum_vacuum_scale_factor=0.08,
        autovacuum_vacuum_insert_scale_factor=0.2,
    )
    set_storage_params(
        'transfer_attempt',
        fillfactor=80,
        autovacuum_vacuum_scale_factor=0.08,
        autovacuum_vacuum_insert_scale_factor=0.2,
    )
    set_storage_params(
        'debtor_info_document',
        fillfactor=85,
        autovacuum_vacuum_scale_factor=0.06,
        autovacuum_vacuum_insert_scale_factor=0.2,
    )
    set_storage_params(
        'debtor_locator_claim',
        fillfactor=85,
        autovacuum_vacuum_scale_factor=0.06,
        autovacuum_vacuum_insert_scale_factor=0.2,
    )
    set_storage_params(
        'trading_policy',
        fillfactor=80,
        autovacuum_vacuum_scale_factor=0.08,
        autovacuum_vacuum_insert_scale_factor=0.2,
    )
    set_storage_params(
        'worker_account',
        fillfactor=80,
        autovacuum_vacuum_scale_factor=0.08,
        autovacuum_vacuum_insert_scale_factor=0.2,
    )
    set_storage_params(
        'needed_worker_account',
        fillfactor=95,
        autovacuum_vacuum_scale_factor=0.02,
        autovacuum_vacuum_insert_scale_factor=0.02,
    )
    set_storage_params(
        'account_lock',
        fillfactor=80,
        autovacuum_vacuum_scale_factor=0.08,
        autovacuum_vacuum_insert_scale_factor=0.2,
    )

    # Tables related to dispatching:
    set_storage_params(
        'dispatching_status',
        fillfactor=100,
        autovacuum_vacuum_scale_factor=0.2,
        autovacuum_vacuum_insert_scale_factor=0.2,
    )
    set_storage_params(
        'worker_collecting',
        fillfactor=80,
        autovacuum_vacuum_scale_factor=0.2,
        autovacuum_vacuum_insert_scale_factor=0.2,
    )
    set_storage_params(
        'worker_sending',
        fillfactor=100,
        autovacuum_vacuum_scale_factor=0.2,
        autovacuum_vacuum_insert_scale_factor=0.2,
    )
    set_storage_params(
        'worker_receiving',
        fillfactor=80,
        autovacuum_vacuum_scale_factor=0.2,
        autovacuum_vacuum_insert_scale_factor=0.2,
    )
    set_storage_params(
        'worker_dispatching',
        fillfactor=100,
        autovacuum_vacuum_scale_factor=0.2,
        autovacuum_vacuum_insert_scale_factor=0.2,
    )

    # Buffer tables:
    set_storage_params(
        'needed_collector_account',
        fillfactor=100,
        autovacuum_vacuum_insert_threshold=-1,
    )
    set_storage_params(
        'collector_status_change',
        fillfactor=100,
        autovacuum_vacuum_insert_threshold=-1,
    )

    # Signals:
    set_storage_params(
        'configure_account_signal',
        fillfactor=100,
        autovacuum_vacuum_insert_threshold=-1,
    )
    set_storage_params(
        'prepare_transfer_signal',
        fillfactor=100,
        autovacuum_vacuum_insert_threshold=-1,
    )
    set_storage_params(
        'finalize_transfer_signal',
        fillfactor=100,
        autovacuum_vacuum_insert_threshold=-1,
    )
    set_storage_params(
        'fetch_debtor_info_signal',
        fillfactor=100,
        autovacuum_vacuum_insert_threshold=-1,
    )
    set_storage_params(
        'store_document_signal',
        fillfactor=100,
        autovacuum_vacuum_insert_threshold=-1,
    )
    set_storage_params(
        'discover_debtor_signal',
        fillfactor=100,
        autovacuum_vacuum_insert_threshold=-1,
    )
    set_storage_params(
        'confirm_debtor_signal',
        fillfactor=100,
        autovacuum_vacuum_insert_threshold=-1,
    )
    set_storage_params(
        'candidate_offer_signal',
        fillfactor=100,
        autovacuum_vacuum_insert_threshold=-1,
    )
    set_storage_params(
        'needed_collector_signal',
        fillfactor=100,
        autovacuum_vacuum_insert_threshold=-1,
    )
    set_storage_params(
        'revise_account_lock_signal',
        fillfactor=100,
        autovacuum_vacuum_insert_threshold=-1,
    )
    set_storage_params(
        'trigger_transfer_signal',
        fillfactor=100,
        autovacuum_vacuum_insert_threshold=-1,
    )
    set_storage_params(
        'account_id_request_signal',
        fillfactor=100,
        autovacuum_vacuum_insert_threshold=-1,
    )
    set_storage_params(
        'account_id_response_signal',
        fillfactor=100,
        autovacuum_vacuum_insert_threshold=-1,
    )
    set_storage_params(
        'start_sending_signal',
        fillfactor=100,
        autovacuum_vacuum_insert_threshold=-1,
    )
    set_storage_params(
        'start_dispatching_signal',
        fillfactor=100,
        autovacuum_vacuum_insert_threshold=-1,
    )
    set_storage_params(
        'calculate_surplus_signal',
        fillfactor=100,
        autovacuum_vacuum_insert_threshold=-1,
    )
    set_storage_params(
        'replayed_account_transfer_signal',
        fillfactor=100,
        autovacuum_vacuum_insert_threshold=-1,
    )


def downgrade_():
    # Tables containing non-updatable records:
    reset_storage_params(
        'recently_needed_collector',
        [
            'fillfactor',
            'autovacuum_vacuum_scale_factor',
            'autovacuum_vacuum_insert_scale_factor',
        ]
    )
    reset_storage_params(
        'interest_rate_change',
        [
            'fillfactor',
            'autovacuum_vacuum_threshold',
            'autovacuum_vacuum_scale_factor',
            'autovacuum_vacuum_insert_threshold'
            'autovacuum_vacuum_insert_scale_factor',
        ]
    )
    reset_storage_params(
        'usable_collector',
        [
            'fillfactor',
            'autovacuum_vacuum_scale_factor',
            'autovacuum_vacuum_insert_scale_factor',
        ]
    )
    reset_storage_params(
        'worker_hoarded_currency',
        [
            'fillfactor',
            'autovacuum_vacuum_scale_factor',
            'autovacuum_vacuum_insert_scale_factor',
        ]
    )
    reset_storage_params(
        'creditor_participation',
        [
            'fillfactor',
            'autovacuum_vacuum_scale_factor',
            'autovacuum_vacuum_insert_scale_factor',
        ]
    )
    reset_storage_params(
        'delayed_account_transfer',
        [
            'fillfactor',
            'autovacuum_vacuum_scale_factor',
            'autovacuum_vacuum_insert_scale_factor',
        ]
    )

    # Tables containing updatable records:
    reset_storage_params(
        'worker_turn',
        [
            'fillfactor',
            'autovacuum_vacuum_scale_factor',
            'autovacuum_vacuum_insert_threshold',
            'autovacuum_vacuum_insert_scale_factor',
        ]
    )
    reset_storage_params(
        'debtor_info_fetch',
        [
            'fillfactor',
            'autovacuum_vacuum_scale_factor',
            'autovacuum_vacuum_insert_scale_factor',
        ]
    )
    reset_storage_params(
        'transfer_attempt',
        [
            'fillfactor',
            'autovacuum_vacuum_scale_factor',
            'autovacuum_vacuum_insert_scale_factor',
        ]
    )
    reset_storage_params(
        'debtor_info_document',
        [
            'fillfactor',
            'autovacuum_vacuum_scale_factor',
            'autovacuum_vacuum_insert_scale_factor',
        ]
    )
    reset_storage_params(
        'debtor_locator_claim',
        [
            'fillfactor',
            'autovacuum_vacuum_scale_factor',
            'autovacuum_vacuum_insert_scale_factor',
        ]
    )
    reset_storage_params(
        'trading_policy',
        [
            'fillfactor',
            'autovacuum_vacuum_scale_factor',
            'autovacuum_vacuum_insert_scale_factor',
        ]
    )
    reset_storage_params(
        'worker_account',
        [
            'fillfactor',
            'autovacuum_vacuum_scale_factor',
            'autovacuum_vacuum_insert_scale_factor',
        ]
    )
    reset_storage_params(
        'needed_worker_account',
        [
            'fillfactor',
            'autovacuum_vacuum_scale_factor',
            'autovacuum_vacuum_insert_scale_factor',
        ]
    )
    reset_storage_params(
        'account_lock',
        [
            'fillfactor',
            'autovacuum_vacuum_scale_factor',
            'autovacuum_vacuum_insert_scale_factor',
        ]
    )

    # Tables related to dispatching:
    reset_storage_params(
        'dispatching_status',
        [
            'fillfactor',
            'autovacuum_vacuum_scale_factor',
            'autovacuum_vacuum_insert_scale_factor',
        ]
    )
    reset_storage_params(
        'worker_collecting',
        [
            'fillfactor',
            'autovacuum_vacuum_scale_factor',
            'autovacuum_vacuum_insert_scale_factor',
        ]
    )
    reset_storage_params(
        'worker_sending',
        [
            'fillfactor',
            'autovacuum_vacuum_scale_factor',
            'autovacuum_vacuum_insert_scale_factor',
        ]
    )
    reset_storage_params(
        'worker_receiving',
        [
            'fillfactor',
            'autovacuum_vacuum_scale_factor',
            'autovacuum_vacuum_insert_scale_factor',
        ]
    )
    reset_storage_params(
        'worker_dispatching',
        [
            'fillfactor',
            'autovacuum_vacuum_scale_factor',
            'autovacuum_vacuum_insert_scale_factor',
        ]
    )

    # Buffer tables:
    reset_storage_params(
        'needed_collector_account',
        [
            'fillfactor',
            'autovacuum_vacuum_insert_threshold',
        ]
    )
    reset_storage_params(
        'collector_status_change',
        [
            'fillfactor',
            'autovacuum_vacuum_insert_threshold',
        ]
    )

    # Signals:
    reset_storage_params(
        'configure_account_signal',
        [
            'fillfactor',
            'autovacuum_vacuum_insert_threshold',
        ]
    )
    reset_storage_params(
        'prepare_transfer_signal',
        [
            'fillfactor',
            'autovacuum_vacuum_insert_threshold',
        ]
    )
    reset_storage_params(
        'finalize_transfer_signal',
        [
            'fillfactor',
            'autovacuum_vacuum_insert_threshold',
        ]
    )
    reset_storage_params(
        'fetch_debtor_info_signal',
        [
            'fillfactor',
            'autovacuum_vacuum_insert_threshold',
        ]
    )
    reset_storage_params(
        'store_document_signal',
        [
            'fillfactor',
            'autovacuum_vacuum_insert_threshold',
        ]
    )
    reset_storage_params(
        'discover_debtor_signal',
        [
            'fillfactor',
            'autovacuum_vacuum_insert_threshold',
        ]
    )
    reset_storage_params(
        'confirm_debtor_signal',
        [
            'fillfactor',
            'autovacuum_vacuum_insert_threshold',
        ]
    )
    reset_storage_params(
        'candidate_offer_signal',
        [
            'fillfactor',
            'autovacuum_vacuum_insert_threshold',
        ]
    )
    reset_storage_params(
        'needed_collector_signal',
        [
            'fillfactor',
            'autovacuum_vacuum_insert_threshold',
        ]
    )
    reset_storage_params(
        'revise_account_lock_signal',
        [
            'fillfactor',
            'autovacuum_vacuum_insert_threshold',
        ]
    )
    reset_storage_params(
        'trigger_transfer_signal',
        [
            'fillfactor',
            'autovacuum_vacuum_insert_threshold',
        ]
    )
    reset_storage_params(
        'account_id_request_signal',
        [
            'fillfactor',
            'autovacuum_vacuum_insert_threshold',
        ]
    )
    reset_storage_params(
        'account_id_response_signal',
        [
            'fillfactor',
            'autovacuum_vacuum_insert_threshold',
        ]
    )
    reset_storage_params(
        'start_sending_signal',
        [
            'fillfactor',
            'autovacuum_vacuum_insert_threshold',
        ]
    )
    reset_storage_params(
        'start_dispatching_signal',
        [
            'fillfactor',
            'autovacuum_vacuum_insert_threshold',
        ]
    )
    reset_storage_params(
        'calculate_surplus_signal',
        [
            'fillfactor',
            'autovacuum_vacuum_insert_threshold',
        ]
    )
    reset_storage_params(
        'replayed_account_transfer_signal',
        [
            'fillfactor',
            'autovacuum_vacuum_insert_threshold',
        ]
    )


def upgrade_solver():
    # Tables containing updatable records:
    set_storage_params(
        'turn',
        fillfactor=100,
        autovacuum_vacuum_scale_factor=0.2,
        autovacuum_vacuum_insert_threshold=100,
        autovacuum_vacuum_insert_scale_factor=0.0,
    )
    set_storage_params(
        'collector_account',
        fillfactor=100,
        autovacuum_vacuum_threshold=10000,
        autovacuum_vacuum_scale_factor=0.001,
        autovacuum_vacuum_insert_threshold=10000,
        autovacuum_vacuum_insert_scale_factor=0.001,
    )


def downgrade_solver():
    # Tables containing updatable records:
    reset_storage_params(
        'turn',
        [
            'fillfactor',
            'autovacuum_vacuum_scale_factor',
            'autovacuum_vacuum_insert_threshold',
            'autovacuum_vacuum_insert_scale_factor',
        ]
    )
    reset_storage_params(
        'collector_account',
        [
            'fillfactor',
            'autovacuum_vacuum_threshold',
            'autovacuum_vacuum_scale_factor',
            'autovacuum_vacuum_insert_threshold'
            'autovacuum_vacuum_insert_scale_factor',
        ]
    )
