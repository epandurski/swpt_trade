"""empty message

Revision ID: bfcaabe3e2b5
Revises: 01a7c27aad49
Create Date: 2024-04-27 20:29:46.876892

"""
from alembic import op
from sqlalchemy.schema import Sequence, CreateSequence, DropSequence
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'bfcaabe3e2b5'
down_revision = '01a7c27aad49'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()





def upgrade_():
    op.execute(CreateSequence(Sequence('coordinator_request_id_seq')))
    
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('account_lock',
    sa.Column('creditor_id', sa.BigInteger(), nullable=False),
    sa.Column('debtor_id', sa.BigInteger(), nullable=False),
    sa.Column('turn_id', sa.Integer(), nullable=False),
    sa.Column('collector_id', sa.BigInteger(), nullable=False),
    sa.Column('has_been_released', sa.BOOLEAN(), nullable=False),
    sa.Column('initiated_at', sa.TIMESTAMP(timezone=True), nullable=False),
    sa.Column('coordinator_request_id', sa.BigInteger(), server_default=sa.text("nextval('coordinator_request_id_seq')"), nullable=False),
    sa.Column('transfer_id', sa.BigInteger(), nullable=True),
    sa.Column('amount', sa.BigInteger(), nullable=True, comment='The amount that is guaranteed to be available up until the transfer deadline has been reached. This is calculated by reducing the `locked_amount` in accordance with the stated `demurrage_rate`.'),
    sa.Column('deadline', sa.TIMESTAMP(timezone=True), nullable=True),
    sa.Column('finalized_at', sa.TIMESTAMP(timezone=True), nullable=True),
    sa.Column('status_code', sa.String(), nullable=True),
    sa.Column('account_creation_date', sa.DATE(), nullable=True),
    sa.Column('account_last_transfer_number', sa.BigInteger(), nullable=True),
    sa.CheckConstraint('account_creation_date IS NULL AND account_last_transfer_number IS NULL OR account_creation_date IS NOT NULL AND account_last_transfer_number IS NOT NULL'),
    sa.CheckConstraint('amount >= 0'),
    sa.CheckConstraint('transfer_id IS NULL AND amount IS NULL AND deadline IS NULL AND finalized_at IS NULL OR transfer_id IS NOT NULL AND amount IS NOT NULL AND deadline IS NOT NULL AND (finalized_at IS NOT NULL OR status_code IS NULL)'),
    sa.ForeignKeyConstraint(['turn_id'], ['worker_turn.turn_id'], ),
    sa.PrimaryKeyConstraint('creditor_id', 'debtor_id'),
    comment='Represents an attempt to arrange the participation of a given account in a given trading turn. Normally, this includes sending a `PrepareTransfer` SMP message.'
    )
    with op.batch_alter_table('account_lock', schema=None) as batch_op:
        batch_op.create_index('idx_lock_account_turn_id', ['turn_id'], unique=False)

    # ### end Alembic commands ###


def downgrade_():
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table('account_lock', schema=None) as batch_op:
        batch_op.drop_index('idx_lock_account_turn_id')

    op.drop_table('account_lock')
    op.execute(DropSequence(Sequence('coordinator_request_id_seq')))
    # ### end Alembic commands ###


def upgrade_solver():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_solver():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###

