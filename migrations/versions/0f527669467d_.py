"""empty message

Revision ID: 0f527669467d
Revises: 5aafeb2c295f
Create Date: 2024-05-29 18:54:28.056307

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '0f527669467d'
down_revision = '5aafeb2c295f'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()





def upgrade_():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('creditor_participation',
    sa.Column('creditor_id', sa.BigInteger(), nullable=False),
    sa.Column('debtor_id', sa.BigInteger(), nullable=False),
    sa.Column('turn_id', sa.Integer(), nullable=False),
    sa.Column('amount', sa.BigInteger(), nullable=False, comment='Can be positive or negative, but can not be zero or one. A positive number indicates that this amount should be given to the creditor. A negative number indicates that this amount should be taken from the creditor.'),
    sa.Column('collector_id', sa.BigInteger(), nullable=False),
    sa.CheckConstraint('amount < 0 OR amount > 1'),
    sa.PrimaryKeyConstraint('creditor_id', 'debtor_id', 'turn_id'),
    comment='Indicates that the given amount must be given or taken to/from the given creditor as part of the given trading turn. During the phase 3 of each turn, "worker" servers will move the records from the "creditor_giving" and and "creditor_taking" solver tables to this table.'
    )
    op.create_table('dispatching_status',
    sa.Column('collector_id', sa.BigInteger(), nullable=False),
    sa.Column('turn_id', sa.Integer(), nullable=False),
    sa.Column('debtor_id', sa.BigInteger(), nullable=False),
    sa.Column('inserted_at', sa.TIMESTAMP(timezone=True), nullable=False),
    sa.Column('amount_to_collect', sa.BigInteger(), nullable=False, comment='The sum of all amounts from the corresponding records in the "worker_collecting" table, at the moment the "dispatching_status" record has been created.'),
    sa.Column('total_collected_amount', sa.BigInteger(), nullable=True, comment='A non-NULL value indicates that no more transfers for corresponding records in the "worker_collecting" table will be collected.'),
    sa.Column('amount_to_send', sa.BigInteger(), nullable=False, comment='The sum of all amounts from the corresponding records in the "worker_sending" table, at the moment the "dispatching_status" record has been created.'),
    sa.Column('started_sending', sa.BOOLEAN(), nullable=False),
    sa.Column('all_sent', sa.BOOLEAN(), nullable=False),
    sa.Column('amount_to_receive', sa.BigInteger(), nullable=False, comment='The sum of all expected amounts from the corresponding records in the "worker_receiving" table, at the moment the "dispatching_status" record has been created.'),
    sa.Column('number_to_receive', sa.Integer(), nullable=False, comment='The number of corresponding records in the "worker_receiving" table, at the moment the "dispatching_status" record has been created.'),
    sa.Column('total_received_amount', sa.BigInteger(), nullable=True, comment='A non-NULL value indicates that no more transfers for corresponding records in the "worker_receiving" table will be received.'),
    sa.Column('all_received', sa.BOOLEAN(), nullable=False),
    sa.Column('amount_to_dispatch', sa.BigInteger(), nullable=False, comment='The sum of all amounts from the corresponding records in the "worker_dispatching" table, at the moment the "dispatching_status" record has been created.'),
    sa.Column('started_dispatching', sa.BOOLEAN(), nullable=False),
    sa.CheckConstraint('started_sending = (total_collected_amount IS NOT NULL)'),
    sa.CheckConstraint('all_sent = false OR started_sending = true'),
    sa.CheckConstraint('all_received = false OR total_received_amount IS NOT NULL'),
    sa.CheckConstraint('started_dispatching = (all_sent = true AND total_received_amount IS NOT NULL)'),
    sa.CheckConstraint('amount_to_collect >= 0'),
    sa.CheckConstraint('amount_to_dispatch >= 0'),
    sa.CheckConstraint('amount_to_send <= amount_to_collect'),
    sa.CheckConstraint('amount_to_send >= 0'),
    sa.CheckConstraint('amount_to_receive >= 0'),
    sa.CheckConstraint('number_to_receive >= 0'),
    sa.CheckConstraint('total_collected_amount <= amount_to_collect'),
    sa.CheckConstraint('total_collected_amount >= 0'),
    sa.CheckConstraint('total_received_amount >= 0'),
    comment='Represents the status of the process of collecting, sending, receiving, and dispatching for a given collector account, during a given trading turn.'
    )
    # Create a "covering" index instead of a "normal" index.
    op.execute('CREATE UNIQUE INDEX idx_dispatching_status_pk ON dispatching_status (collector_id, turn_id, debtor_id) INCLUDE (started_sending, all_sent, started_dispatching)')
    op.execute('ALTER TABLE dispatching_status ADD CONSTRAINT dispatching_status_pkey PRIMARY KEY USING INDEX idx_dispatching_status_pk')

    op.create_table('worker_collecting',
    sa.Column('collector_id', sa.BigInteger(), nullable=False),
    sa.Column('turn_id', sa.Integer(), nullable=False),
    sa.Column('debtor_id', sa.BigInteger(), nullable=False),
    sa.Column('creditor_id', sa.BigInteger(), nullable=False),
    sa.Column('amount', sa.BigInteger(), nullable=False),
    sa.Column('collected', sa.BOOLEAN(), nullable=False),
    sa.Column('purge_after', sa.TIMESTAMP(timezone=True), nullable=False),
    sa.CheckConstraint('amount > 0'),
    sa.CheckConstraint('collector_id != creditor_id'),
    comment='Indicates that the given amount will be withdrawn (collected) from the given creditor\'s account, as part of the given trading turn, and will be transferred to the given collector. During the phase 3 of each turn, "worker" servers will move the records from the "collector_collecting" solver table to this table.'
    )
    # Create a "covering" index instead of a "normal" index.
    op.execute('CREATE UNIQUE INDEX idx_worker_collecting_pk ON worker_collecting (collector_id, turn_id, debtor_id, creditor_id) INCLUDE (amount)')
    op.execute('ALTER TABLE worker_collecting ADD CONSTRAINT worker_collecting_pkey PRIMARY KEY USING INDEX idx_worker_collecting_pk')

    with op.batch_alter_table('worker_collecting', schema=None) as batch_op:
        batch_op.create_index('idx_worker_collecting_not_collected', ['collector_id', 'turn_id', 'debtor_id', 'creditor_id'], unique=False, postgresql_where=sa.text('collected = false'))

    op.create_table('worker_dispatching',
    sa.Column('collector_id', sa.BigInteger(), nullable=False),
    sa.Column('turn_id', sa.Integer(), nullable=False),
    sa.Column('debtor_id', sa.BigInteger(), nullable=False),
    sa.Column('creditor_id', sa.BigInteger(), nullable=False),
    sa.Column('amount', sa.BigInteger(), nullable=False),
    sa.Column('purge_after', sa.TIMESTAMP(timezone=True), nullable=False),
    sa.CheckConstraint('amount > 1'),
    sa.CheckConstraint('collector_id != creditor_id'),
    comment='Indicates that the given amount must be deposited (dispatched) to the given customer account, as part of the given trading turn. During the phase 3 of each turn, "worker" servers will move the records from the "collector_dispatching" solver table to this table.'
    )
    # Create a "covering" index instead of a "normal" index.
    op.execute('CREATE UNIQUE INDEX idx_worker_dispatching_pk ON worker_dispatching (collector_id, turn_id, debtor_id, creditor_id) INCLUDE (amount)')
    op.execute('ALTER TABLE worker_dispatching ADD CONSTRAINT worker_dispatching_pkey PRIMARY KEY USING INDEX idx_worker_dispatching_pk')

    op.create_table('worker_receiving',
    sa.Column('to_collector_id', sa.BigInteger(), nullable=False),
    sa.Column('turn_id', sa.Integer(), nullable=False),
    sa.Column('debtor_id', sa.BigInteger(), nullable=False),
    sa.Column('from_collector_id', sa.BigInteger(), nullable=False),
    sa.Column('expected_amount', sa.BigInteger(), nullable=False),
    sa.Column('received_amount', sa.BigInteger(), nullable=False, comment='The received amount will be equal to the expected amount minus the accumulated negative interest (that is: when the interest rate is negative).'),
    sa.Column('purge_after', sa.TIMESTAMP(timezone=True), nullable=False),
    sa.CheckConstraint('expected_amount > 1'),
    sa.CheckConstraint('received_amount >= 0'),
    sa.CheckConstraint('from_collector_id != to_collector_id'),
    comment='Indicates that some amount will be transferred (received) from another collector account, as part of the given trading turn. During the phase 3 of each turn, "worker" servers will move the records from the "collector_receiving" solver table to this table.'
    )
    # Create a "covering" index instead of a "normal" index.
    op.execute('CREATE UNIQUE INDEX idx_worker_receiving_pk ON worker_receiving (to_collector_id, turn_id, debtor_id, from_collector_id) INCLUDE (received_amount)')
    op.execute('ALTER TABLE worker_receiving ADD CONSTRAINT worker_receiving_pkey PRIMARY KEY USING INDEX idx_worker_receiving_pk')

    with op.batch_alter_table('worker_receiving', schema=None) as batch_op:
        batch_op.create_index('idx_worker_receiving_not_received', ['to_collector_id', 'turn_id', 'debtor_id', 'from_collector_id'], unique=False, postgresql_where=sa.text('received_amount = 0'))

    op.create_table('worker_sending',
    sa.Column('from_collector_id', sa.BigInteger(), nullable=False),
    sa.Column('turn_id', sa.Integer(), nullable=False),
    sa.Column('debtor_id', sa.BigInteger(), nullable=False),
    sa.Column('to_collector_id', sa.BigInteger(), nullable=False),
    sa.Column('amount', sa.BigInteger(), nullable=False),
    sa.Column('purge_after', sa.TIMESTAMP(timezone=True), nullable=False),
    sa.CheckConstraint('amount > 1'),
    sa.CheckConstraint('from_collector_id != to_collector_id'),
    comment='Indicates that the given amount must be transferred (sent) to another collector account, as part of the given trading turn. During the phase 3 of each turn, "worker" servers will move the records from the "collector_sending" solver table to this table.'
    )
    # Create a "covering" index instead of a "normal" index.
    op.execute('CREATE UNIQUE INDEX idx_worker_sending_pk ON worker_sending (from_collector_id, turn_id, debtor_id, to_collector_id) INCLUDE (amount)')
    op.execute('ALTER TABLE worker_sending ADD CONSTRAINT worker_sending_pkey PRIMARY KEY USING INDEX idx_worker_sending_pk')
    # ### end Alembic commands ###


def downgrade_():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('worker_sending')
    with op.batch_alter_table('worker_receiving', schema=None) as batch_op:
        batch_op.drop_index('idx_worker_receiving_not_received', postgresql_where=sa.text('received_amount = 0'))

    op.drop_table('worker_receiving')
    op.drop_table('worker_dispatching')
    with op.batch_alter_table('worker_collecting', schema=None) as batch_op:
        batch_op.drop_index('idx_worker_collecting_not_collected', postgresql_where=sa.text('collected = false'))

    op.drop_table('worker_collecting')
    op.drop_table('dispatching_status')
    op.drop_table('creditor_participation')
    # ### end Alembic commands ###


def upgrade_solver():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_solver():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###

