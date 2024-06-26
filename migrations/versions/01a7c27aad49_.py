"""empty message

Revision ID: 01a7c27aad49
Revises: fa9f1737672b
Create Date: 2024-04-19 17:28:25.676128

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '01a7c27aad49'
down_revision = 'fa9f1737672b'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()





def upgrade_():
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table('worker_turn', schema=None) as batch_op:
        batch_op.create_index('idx_worker_turn_phase', ['phase'], unique=False, postgresql_where=sa.text('phase < 3'))

    # ### end Alembic commands ###


def downgrade_():
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table('worker_turn', schema=None) as batch_op:
        batch_op.drop_index('idx_worker_turn_phase', postgresql_where=sa.text('phase < 3'))

    # ### end Alembic commands ###


def upgrade_solver():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_solver():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###

