"""empty message

Revision ID: 22f3142acc31
Revises: 01a7c27aad49
Create Date: 2024-04-30 17:58:27.243782

"""
from alembic import op
from sqlalchemy.schema import Sequence, CreateSequence, DropSequence
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '22f3142acc31'
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
    op.create_table('interest_rate_change',
    sa.Column('creditor_id', sa.BigInteger(), nullable=False),
    sa.Column('debtor_id', sa.BigInteger(), nullable=False),
    sa.Column('change_ts', sa.TIMESTAMP(timezone=True), nullable=False),
    sa.Column('interest_rate', sa.REAL(), nullable=False),
    sa.CheckConstraint('interest_rate >= -100.0'),
    )
    # Create a "covering" index instead of a "normal" index.
    op.execute('CREATE UNIQUE INDEX idx_interest_rate_change_pk ON interest_rate_change (creditor_id, debtor_id, change_ts) INCLUDE (interest_rate)')
    op.execute('ALTER TABLE interest_rate_change ADD CONSTRAINT interest_rate_change_pkey PRIMARY KEY USING INDEX idx_interest_rate_change_pk')

    op.create_table('account_lock',
    sa.Column('creditor_id', sa.BigInteger(), nullable=False),
    sa.Column('debtor_id', sa.BigInteger(), nullable=False),
    sa.Column('turn_id', sa.Integer(), nullable=False),
    sa.Column('collector_id', sa.BigInteger(), nullable=False),
    sa.Column('has_been_released', sa.BOOLEAN(), nullable=False),
    sa.Column('initiated_at', sa.TIMESTAMP(timezone=True), nullable=False, comment='The timestamp of the sent `PrepareTransfer` SMP message.'),
    sa.Column('coordinator_request_id', sa.BigInteger(), server_default=sa.text("nextval('coordinator_request_id_seq')"), nullable=False),
    sa.Column('transfer_id', sa.BigInteger(), nullable=True),
    sa.Column('amount', sa.BigInteger(), nullable=True, comment='The amount that is guaranteed to be available up until the `collection_deadline` has been reached. This is calculated by reducing the `locked_amount` in accordance with the stated `demurrage_rate`.'),
    sa.Column('finalized_at', sa.TIMESTAMP(timezone=True), nullable=True),
    sa.Column('status_code', sa.String(), nullable=True),
    sa.Column('account_creation_date', sa.DATE(), nullable=True),
    sa.Column('account_last_transfer_number', sa.BigInteger(), nullable=True),
    sa.CheckConstraint('account_creation_date IS NULL AND account_last_transfer_number IS NULL OR account_creation_date IS NOT NULL AND account_last_transfer_number IS NOT NULL'),
    sa.CheckConstraint('amount >= 0'),
    sa.CheckConstraint('transfer_id IS NULL AND amount IS NULL AND finalized_at IS NULL OR transfer_id IS NOT NULL AND amount IS NOT NULL AND (finalized_at IS NOT NULL OR status_code IS NULL)'),
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
    op.drop_table('interest_rate_change')
    # ### end Alembic commands ###

    op.execute(DropSequence(Sequence('coordinator_request_id_seq')))


def upgrade_solver():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_solver():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###

