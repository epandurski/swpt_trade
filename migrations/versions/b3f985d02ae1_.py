"""empty message

Revision ID: b3f985d02ae1
Revises: 549f5c0df20e
Create Date: 2024-06-04 19:23:47.056758

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'b3f985d02ae1'
down_revision = '549f5c0df20e'
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()





def upgrade_():
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table('account_lock', schema=None) as batch_op:
        batch_op.add_column(sa.Column('has_been_revised', sa.BOOLEAN(), nullable=True))

    op.execute("UPDATE account_lock SET has_been_revised = FALSE")

    with op.batch_alter_table('account_lock', schema=None) as batch_op:
        batch_op.alter_column('has_been_revised', nullable=False)

    # ### end Alembic commands ###


def downgrade_():
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table('account_lock', schema=None) as batch_op:
        batch_op.drop_column('has_been_revised')

    # ### end Alembic commands ###


def upgrade_solver():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###


def downgrade_solver():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###

