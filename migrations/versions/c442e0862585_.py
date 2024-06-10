"""empty message

Revision ID: c442e0862585
Revises: b3f985d02ae1
Create Date: 2024-06-10 16:46:20.165558

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'c442e0862585'
down_revision = 'b3f985d02ae1'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()





def upgrade_():
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table('trading_policy', schema=None) as batch_op:
        batch_op.add_column(sa.Column('account_id_is_obsolete', sa.BOOLEAN(), nullable=True))

    op.execute("UPDATE trading_policy SET account_id_is_obsolete = FALSE")

    with op.batch_alter_table('trading_policy', schema=None) as batch_op:
        batch_op.alter_column('account_id_is_obsolete', nullable=False)

    # ### end Alembic commands ###


def downgrade_():
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table('trading_policy', schema=None) as batch_op:
        batch_op.drop_column('account_id_is_obsolete')

    # ### end Alembic commands ###


def upgrade_solver():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_solver():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###

