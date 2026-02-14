"""fix analyze threshold

Revision ID: a7e7aaf91f84
Revises: e056ba322b0b
Create Date: 2026-02-14 14:01:15.808910

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'a7e7aaf91f84'
down_revision = 'e056ba322b0b'
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
    # Buffer tables:
    reset_storage_params(
        'needed_collector_account',
        [
            'autovacuum_analyze_threshold',
        ]
    )
    reset_storage_params(
        'collector_status_change',
        [
            'autovacuum_analyze_threshold',
        ]
    )

    # Signals:
    reset_storage_params(
        'configure_account_signal',
        [
            'autovacuum_analyze_threshold',
        ]
    )
    reset_storage_params(
        'prepare_transfer_signal',
        [
            'autovacuum_analyze_threshold',
        ]
    )
    reset_storage_params(
        'finalize_transfer_signal',
        [
            'autovacuum_analyze_threshold',
        ]
    )
    reset_storage_params(
        'fetch_debtor_info_signal',
        [
            'autovacuum_analyze_threshold',
        ]
    )
    reset_storage_params(
        'store_document_signal',
        [
            'autovacuum_analyze_threshold',
        ]
    )
    reset_storage_params(
        'discover_debtor_signal',
        [
            'autovacuum_analyze_threshold',
        ]
    )
    reset_storage_params(
        'confirm_debtor_signal',
        [
            'autovacuum_analyze_threshold',
        ]
    )
    reset_storage_params(
        'candidate_offer_signal',
        [
            'autovacuum_analyze_threshold',
        ]
    )
    reset_storage_params(
        'needed_collector_signal',
        [
            'autovacuum_analyze_threshold',
        ]
    )
    reset_storage_params(
        'revise_account_lock_signal',
        [
            'autovacuum_analyze_threshold',
        ]
    )
    reset_storage_params(
        'trigger_transfer_signal',
        [
            'autovacuum_analyze_threshold',
        ]
    )
    reset_storage_params(
        'account_id_request_signal',
        [
            'autovacuum_analyze_threshold',
        ]
    )
    reset_storage_params(
        'account_id_response_signal',
        [
            'autovacuum_analyze_threshold',
        ]
    )
    reset_storage_params(
        'start_sending_signal',
        [
            'autovacuum_analyze_threshold',
        ]
    )
    reset_storage_params(
        'start_dispatching_signal',
        [
            'autovacuum_analyze_threshold',
        ]
    )
    reset_storage_params(
        'calculate_surplus_signal',
        [
            'autovacuum_analyze_threshold',
        ]
    )
    reset_storage_params(
        'replayed_account_transfer_signal',
        [
            'autovacuum_analyze_threshold',
        ]
    )


def downgrade_():
    # Buffer tables:
    set_storage_params(
        'needed_collector_account',
        autovacuum_analyze_threshold=2000000000,
    )
    set_storage_params(
        'collector_status_change',
        autovacuum_analyze_threshold=2000000000,
    )

    # Signals:
    set_storage_params(
        'configure_account_signal',
        autovacuum_analyze_threshold=2000000000,
    )
    set_storage_params(
        'prepare_transfer_signal',
        autovacuum_analyze_threshold=2000000000,
    )
    set_storage_params(
        'finalize_transfer_signal',
        autovacuum_analyze_threshold=2000000000,
    )
    set_storage_params(
        'fetch_debtor_info_signal',
        autovacuum_analyze_threshold=2000000000,
    )
    set_storage_params(
        'store_document_signal',
        autovacuum_analyze_threshold=2000000000,
    )
    set_storage_params(
        'discover_debtor_signal',
        autovacuum_analyze_threshold=2000000000,
    )
    set_storage_params(
        'confirm_debtor_signal',
        autovacuum_analyze_threshold=2000000000,
    )
    set_storage_params(
        'candidate_offer_signal',
        autovacuum_analyze_threshold=2000000000,
    )
    set_storage_params(
        'needed_collector_signal',
        autovacuum_analyze_threshold=2000000000,
    )
    set_storage_params(
        'revise_account_lock_signal',
        autovacuum_analyze_threshold=2000000000,
    )
    set_storage_params(
        'trigger_transfer_signal',
        autovacuum_analyze_threshold=2000000000,
    )
    set_storage_params(
        'account_id_request_signal',
        autovacuum_analyze_threshold=2000000000,
    )
    set_storage_params(
        'account_id_response_signal',
        autovacuum_analyze_threshold=2000000000,
    )
    set_storage_params(
        'start_sending_signal',
        autovacuum_analyze_threshold=2000000000,
    )
    set_storage_params(
        'start_dispatching_signal',
        autovacuum_analyze_threshold=2000000000,
    )
    set_storage_params(
        'calculate_surplus_signal',
        autovacuum_analyze_threshold=2000000000,
    )
    set_storage_params(
        'replayed_account_transfer_signal',
        autovacuum_analyze_threshold=2000000000,
    )


def upgrade_solver():
    pass


def downgrade_solver():
    pass

