"""empty message

Revision ID: 549f5c0df20e
Revises: 0f527669467d
Create Date: 2024-06-01 15:13:29.023240

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '549f5c0df20e'
down_revision = '0f527669467d'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()





def upgrade_():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('revise_account_lock_signal',
    sa.Column('signal_id', sa.BigInteger(), autoincrement=True, nullable=False),
    sa.Column('creditor_id', sa.BigInteger(), nullable=False),
    sa.Column('debtor_id', sa.BigInteger(), nullable=False),
    sa.Column('turn_id', sa.Integer(), nullable=False),
    sa.Column('inserted_at', sa.TIMESTAMP(timezone=True), nullable=False),
    sa.PrimaryKeyConstraint('signal_id')
    )
    # ### end Alembic commands ###


def downgrade_():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('revise_account_lock_signal')
    # ### end Alembic commands ###


def upgrade_solver():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_solver():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###

