"""empty message

Revision ID: fa9f1737672b
Revises: 
Create Date: 2024-04-15 17:14:28.938724

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'fa9f1737672b'
down_revision = None
branch_labels = None
depends_on = None


def upgrade(engine_name):
    globals()["upgrade_%s" % engine_name]()


def downgrade(engine_name):
    globals()["downgrade_%s" % engine_name]()





def upgrade_():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('activate_collector_signal',
    sa.Column('signal_id', sa.BigInteger(), autoincrement=True, nullable=False),
    sa.Column('debtor_id', sa.BigInteger(), nullable=False),
    sa.Column('account_id', sa.String(), nullable=False),
    sa.Column('creditor_id', sa.BigInteger(), nullable=False),
    sa.Column('inserted_at', sa.TIMESTAMP(timezone=True), nullable=False),
    sa.PrimaryKeyConstraint('signal_id')
    )
    op.create_table('active_collector',
    sa.Column('debtor_id', sa.BigInteger(), nullable=False),
    sa.Column('collector_id', sa.BigInteger(), nullable=False),
    sa.Column('account_id', sa.String(), nullable=False),
    sa.CheckConstraint("account_id != ''"),
    sa.PrimaryKeyConstraint('debtor_id', 'collector_id'),
    comment='Represents an active Swaptacular account which can be used to collect and dispatch transfers. Each "Worker" servers will maintain its own copy of this table (that is: no rows-sharding) by periodically copying the relevant records from the solver\'s "collector_account" table. "Worker" servers will use this local copy so as to avoid querying the central database too often.'
    )
    op.create_table('candidate_offer_signal',
    sa.Column('turn_id', sa.SmallInteger(), nullable=False),
    sa.Column('debtor_id', sa.BigInteger(), nullable=False),
    sa.Column('creditor_id', sa.BigInteger(), nullable=False),
    sa.Column('amount', sa.BigInteger(), nullable=False),
    sa.Column('account_creation_date', sa.DATE(), nullable=False),
    sa.Column('last_transfer_number', sa.BigInteger(), nullable=False),
    sa.Column('inserted_at', sa.TIMESTAMP(timezone=True), nullable=False),
    sa.PrimaryKeyConstraint('turn_id', 'debtor_id', 'creditor_id')
    )
    op.create_table('configure_account_signal',
    sa.Column('creditor_id', sa.BigInteger(), nullable=False),
    sa.Column('debtor_id', sa.BigInteger(), nullable=False),
    sa.Column('ts', sa.TIMESTAMP(timezone=True), nullable=False),
    sa.Column('seqnum', sa.Integer(), nullable=False),
    sa.Column('negligible_amount', sa.REAL(), nullable=False),
    sa.Column('config_data', sa.String(), nullable=False),
    sa.Column('config_flags', sa.Integer(), nullable=False),
    sa.Column('inserted_at', sa.TIMESTAMP(timezone=True), nullable=False),
    sa.PrimaryKeyConstraint('creditor_id', 'debtor_id', 'ts', 'seqnum')
    )
    op.create_table('confirm_debtor_signal',
    sa.Column('signal_id', sa.BigInteger(), autoincrement=True, nullable=False),
    sa.Column('debtor_id', sa.BigInteger(), nullable=False),
    sa.Column('debtor_info_locator', sa.String(), nullable=False),
    sa.Column('inserted_at', sa.TIMESTAMP(timezone=True), nullable=False),
    sa.PrimaryKeyConstraint('signal_id')
    )
    op.create_table('debtor_info_document',
    sa.Column('debtor_info_locator', sa.String(), nullable=False),
    sa.Column('debtor_id', sa.BigInteger(), nullable=False),
    sa.Column('peg_debtor_info_locator', sa.String(), nullable=True),
    sa.Column('peg_debtor_id', sa.BigInteger(), nullable=True),
    sa.Column('peg_exchange_rate', sa.FLOAT(), nullable=True),
    sa.Column('will_not_change_until', sa.TIMESTAMP(timezone=True), nullable=True),
    sa.Column('fetched_at', sa.TIMESTAMP(timezone=True), nullable=False),
    sa.CheckConstraint('peg_debtor_info_locator IS NULL AND peg_debtor_id IS NULL AND peg_exchange_rate IS NULL OR peg_debtor_info_locator IS NOT NULL AND peg_debtor_id IS NOT NULL AND peg_exchange_rate IS NOT NULL'),
    sa.CheckConstraint('peg_exchange_rate >= 0.0'),
    sa.PrimaryKeyConstraint('debtor_info_locator'),
    comment="Represents relevant trading information about a given currency (aka debtor), that have been parsed from the debtor's debtor info document, obtained via HTTP request."
    )
    op.create_table('debtor_info_fetch',
    sa.Column('iri', sa.String(), nullable=False),
    sa.Column('debtor_id', sa.BigInteger(), nullable=False),
    sa.Column('is_locator_fetch', sa.BOOLEAN(), nullable=False),
    sa.Column('is_discovery_fetch', sa.BOOLEAN(), nullable=False),
    sa.Column('ignore_cache', sa.BOOLEAN(), nullable=False),
    sa.Column('recursion_level', sa.SmallInteger(), nullable=False),
    sa.Column('inserted_at', sa.TIMESTAMP(timezone=True), nullable=False),
    sa.Column('attempts_count', sa.SmallInteger(), nullable=False),
    sa.Column('latest_attempt_at', sa.TIMESTAMP(timezone=True), nullable=True),
    sa.Column('latest_attempt_errorcode', sa.SmallInteger(), nullable=True),
    sa.Column('next_attempt_at', sa.TIMESTAMP(timezone=True), nullable=False),
    sa.CheckConstraint('attempts_count = 0 AND latest_attempt_at IS NULL AND latest_attempt_errorcode IS NULL OR attempts_count > 0 AND latest_attempt_at IS NOT NULL'),
    sa.CheckConstraint('recursion_level >= 0'),
    sa.PrimaryKeyConstraint('iri', 'debtor_id'),
    comment="Represents a scheduled HTTP request (HTTP fetch) to obtain relevant trading information about a given currency (aka debtor). There are two non-mutually exclusive request types: 1) a locator fetch, which wants to obtain the latest version of the debtor's debtor info document, from the official debtor info locator; 2) a discovery fetch, which wants to obtain a particular (possibly obsolete) version of the debtor's debtor info document, not necessarily from the official debtor info locator."
    )
    with op.batch_alter_table('debtor_info_fetch', schema=None) as batch_op:
        batch_op.create_index('idx_debtor_info_fetch_next_attempt_at', ['next_attempt_at'], unique=False)

    op.create_table('debtor_locator_claim',
    sa.Column('debtor_id', sa.BigInteger(), autoincrement=False, nullable=False),
    sa.Column('debtor_info_locator', sa.String(), nullable=True),
    sa.Column('latest_locator_fetch_at', sa.TIMESTAMP(timezone=True), nullable=True),
    sa.Column('latest_discovery_fetch_at', sa.TIMESTAMP(timezone=True), nullable=False),
    sa.Column('forced_locator_refetch_at', sa.TIMESTAMP(timezone=True), nullable=True),
    sa.CheckConstraint('debtor_info_locator IS NULL AND latest_locator_fetch_at IS NULL OR debtor_info_locator IS NOT NULL AND latest_locator_fetch_at IS NOT NULL'),
    sa.PrimaryKeyConstraint('debtor_id'),
    comment='Represents a reliable claim made by a given debtor, declaring what the official debtor info locator for the given debtor is.'
    )
    with op.batch_alter_table('debtor_locator_claim', schema=None) as batch_op:
        batch_op.create_index('idx_debtor_locator_claim_latest_locator_fetch_at', ['latest_locator_fetch_at'], unique=False, postgresql_where=sa.text('latest_locator_fetch_at IS NOT NULL'))

    op.create_table('discover_debtor_signal',
    sa.Column('signal_id', sa.BigInteger(), autoincrement=True, nullable=False),
    sa.Column('debtor_id', sa.BigInteger(), nullable=False),
    sa.Column('force_locator_refetch', sa.Boolean(), nullable=False),
    sa.Column('iri', sa.String(), nullable=False),
    sa.Column('inserted_at', sa.TIMESTAMP(timezone=True), nullable=False),
    sa.PrimaryKeyConstraint('signal_id')
    )
    op.create_table('fetch_debtor_info_signal',
    sa.Column('signal_id', sa.BigInteger(), autoincrement=True, nullable=False),
    sa.Column('iri', sa.String(), nullable=False),
    sa.Column('debtor_id', sa.BigInteger(), nullable=False),
    sa.Column('is_locator_fetch', sa.BOOLEAN(), nullable=False),
    sa.Column('is_discovery_fetch', sa.BOOLEAN(), nullable=False),
    sa.Column('ignore_cache', sa.BOOLEAN(), nullable=False),
    sa.Column('recursion_level', sa.SmallInteger(), nullable=False),
    sa.Column('inserted_at', sa.TIMESTAMP(timezone=True), nullable=False),
    sa.PrimaryKeyConstraint('signal_id')
    )
    op.create_table('finalize_transfer_signal',
    sa.Column('creditor_id', sa.BigInteger(), nullable=False),
    sa.Column('signal_id', sa.BigInteger(), autoincrement=True, nullable=False),
    sa.Column('debtor_id', sa.BigInteger(), nullable=False),
    sa.Column('transfer_id', sa.BigInteger(), nullable=False),
    sa.Column('coordinator_id', sa.BigInteger(), nullable=False),
    sa.Column('coordinator_request_id', sa.BigInteger(), nullable=False),
    sa.Column('committed_amount', sa.BigInteger(), nullable=False),
    sa.Column('transfer_note_format', sa.String(), nullable=False),
    sa.Column('transfer_note', sa.String(), nullable=False),
    sa.Column('inserted_at', sa.TIMESTAMP(timezone=True), nullable=False),
    sa.PrimaryKeyConstraint('creditor_id', 'signal_id')
    )
    op.create_table('needed_collector_signal',
    sa.Column('signal_id', sa.BigInteger(), autoincrement=True, nullable=False),
    sa.Column('debtor_id', sa.BigInteger(), nullable=False),
    sa.Column('inserted_at', sa.TIMESTAMP(timezone=True), nullable=False),
    sa.PrimaryKeyConstraint('signal_id')
    )
    op.create_table('needed_worker_account',
    sa.Column('creditor_id', sa.BigInteger(), nullable=False),
    sa.Column('debtor_id', sa.BigInteger(), nullable=False),
    sa.Column('configured_at', sa.TIMESTAMP(timezone=True), nullable=False),
    sa.PrimaryKeyConstraint('creditor_id', 'debtor_id'),
    comment='Represents the fact that a "worker" server has requested the configuration (aka creation) of a Swaptacular account, which will be used to collect and dispatch transfers.'
    )
    op.create_table('prepare_transfer_signal',
    sa.Column('creditor_id', sa.BigInteger(), nullable=False),
    sa.Column('coordinator_request_id', sa.BigInteger(), nullable=False),
    sa.Column('debtor_id', sa.BigInteger(), nullable=False),
    sa.Column('recipient', sa.String(), nullable=False),
    sa.Column('locked_amount', sa.BigInteger(), nullable=False),
    sa.Column('min_interest_rate', sa.Float(), nullable=False),
    sa.Column('max_commit_delay', sa.Integer(), nullable=False),
    sa.Column('inserted_at', sa.TIMESTAMP(timezone=True), nullable=False),
    sa.PrimaryKeyConstraint('creditor_id', 'coordinator_request_id')
    )
    op.create_table('recently_needed_collector',
    sa.Column('debtor_id', sa.BigInteger(), autoincrement=False, nullable=False),
    sa.Column('needed_at', sa.TIMESTAMP(timezone=True), nullable=False),
    sa.CheckConstraint('debtor_id != 0'),
    sa.PrimaryKeyConstraint('debtor_id'),
    comment='Indicates that the creation of a collector account for the currency with the given debtor ID has been recently requested. This information is used to prevent "worker" servers from making repetitive queries to the central database.'
    )
    op.create_table('store_document_signal',
    sa.Column('signal_id', sa.BigInteger(), autoincrement=True, nullable=False),
    sa.Column('debtor_info_locator', sa.String(), nullable=False),
    sa.Column('debtor_id', sa.BigInteger(), nullable=False),
    sa.Column('peg_debtor_info_locator', sa.String(), nullable=True),
    sa.Column('peg_debtor_id', sa.BigInteger(), nullable=True),
    sa.Column('peg_exchange_rate', sa.FLOAT(), nullable=True),
    sa.Column('will_not_change_until', sa.TIMESTAMP(timezone=True), nullable=True),
    sa.Column('inserted_at', sa.TIMESTAMP(timezone=True), nullable=False),
    sa.PrimaryKeyConstraint('signal_id')
    )
    op.create_table('trading_policy',
    sa.Column('creditor_id', sa.BigInteger(), nullable=False),
    sa.Column('debtor_id', sa.BigInteger(), nullable=False),
    sa.Column('latest_ledger_update_id', sa.BigInteger(), nullable=False),
    sa.Column('latest_ledger_update_ts', sa.TIMESTAMP(timezone=True), nullable=False),
    sa.Column('account_id', sa.String(), nullable=False),
    sa.Column('creation_date', sa.DATE(), nullable=False),
    sa.Column('principal', sa.BigInteger(), nullable=False),
    sa.Column('last_transfer_number', sa.BigInteger(), nullable=False),
    sa.Column('latest_policy_update_id', sa.BigInteger(), nullable=False),
    sa.Column('latest_policy_update_ts', sa.TIMESTAMP(timezone=True), nullable=False),
    sa.Column('policy_name', sa.String(), nullable=True),
    sa.Column('min_principal', sa.BigInteger(), nullable=False),
    sa.Column('max_principal', sa.BigInteger(), nullable=False),
    sa.Column('peg_debtor_id', sa.BigInteger(), nullable=True),
    sa.Column('peg_exchange_rate', sa.FLOAT(), nullable=True),
    sa.Column('latest_flags_update_id', sa.BigInteger(), nullable=False),
    sa.Column('latest_flags_update_ts', sa.TIMESTAMP(timezone=True), nullable=False),
    sa.Column('config_flags', sa.Integer(), nullable=False),
    sa.CheckConstraint('last_transfer_number >= 0'),
    sa.CheckConstraint('latest_flags_update_id >= 0'),
    sa.CheckConstraint('latest_ledger_update_id >= 0'),
    sa.CheckConstraint('latest_policy_update_id >= 0'),
    sa.CheckConstraint('peg_debtor_id IS NULL AND peg_exchange_rate IS NULL OR peg_debtor_id IS NOT NULL AND peg_exchange_rate IS NOT NULL'),
    sa.CheckConstraint('peg_exchange_rate >= 0.0'),
    sa.PrimaryKeyConstraint('creditor_id', 'debtor_id'),
    comment="Represents important information about a given customer account. This includes things like: the account's ID, the available amount, the customer's trading policy etc."
    )
    op.create_table('worker_account',
    sa.Column('creditor_id', sa.BigInteger(), nullable=False),
    sa.Column('debtor_id', sa.BigInteger(), nullable=False),
    sa.Column('creation_date', sa.DATE(), nullable=False),
    sa.Column('last_change_ts', sa.TIMESTAMP(timezone=True), nullable=False),
    sa.Column('last_change_seqnum', sa.Integer(), nullable=False),
    sa.Column('principal', sa.BigInteger(), nullable=False),
    sa.Column('interest', sa.FLOAT(), nullable=False),
    sa.Column('interest_rate', sa.REAL(), nullable=False),
    sa.Column('last_interest_rate_change_ts', sa.TIMESTAMP(timezone=True), nullable=False),
    sa.Column('config_flags', sa.Integer(), nullable=False),
    sa.Column('account_id', sa.String(), nullable=False),
    sa.Column('debtor_info_iri', sa.String(), nullable=True),
    sa.Column('last_transfer_number', sa.BigInteger(), nullable=False),
    sa.Column('last_transfer_committed_at', sa.TIMESTAMP(timezone=True), nullable=False),
    sa.Column('demurrage_rate', sa.FLOAT(), nullable=False),
    sa.Column('commit_period', sa.Integer(), nullable=False),
    sa.Column('transfer_note_max_bytes', sa.Integer(), nullable=False),
    sa.Column('last_heartbeat_ts', sa.TIMESTAMP(timezone=True), nullable=False),
    sa.CheckConstraint('commit_period >= 0'),
    sa.CheckConstraint('demurrage_rate >= -100.0 AND demurrage_rate <= 0.0'),
    sa.CheckConstraint('interest_rate >= -100.0'),
    sa.CheckConstraint('last_transfer_number >= 0'),
    sa.CheckConstraint('transfer_note_max_bytes >= 0'),
    sa.PrimaryKeyConstraint('creditor_id', 'debtor_id'),
    comment='Represents an existing Swaptacular account, managed by a  "worker" server. The account is used to collect and dispatch transfers.'
    )
    op.create_table('worker_turn',
    sa.Column('turn_id', sa.Integer(), autoincrement=False, nullable=False),
    sa.Column('started_at', sa.TIMESTAMP(timezone=True), nullable=False),
    sa.Column('base_debtor_info_locator', sa.String(), nullable=False),
    sa.Column('base_debtor_id', sa.BigInteger(), nullable=False),
    sa.Column('max_distance_to_base', sa.SmallInteger(), nullable=False),
    sa.Column('min_trade_amount', sa.BigInteger(), nullable=False),
    sa.Column('phase', sa.SmallInteger(), nullable=False),
    sa.Column('phase_deadline', sa.TIMESTAMP(timezone=True), nullable=True),
    sa.Column('collection_started_at', sa.TIMESTAMP(timezone=True), nullable=True),
    sa.Column('collection_deadline', sa.TIMESTAMP(timezone=True), nullable=True),
    sa.Column('worker_turn_subphase', sa.SmallInteger(), nullable=False, comment='The worker may divide the processing of each phase to one or more sub-phases. The initial sub-phase is always `0`, and the final sub-phase is always `10`. Sequential sub-phases do not need to be (and normally will not be) represented by sequential numbers. This gives the freedom to add sub-phases if necessary.'),
    sa.CheckConstraint('base_debtor_id != 0'),
    sa.CheckConstraint('max_distance_to_base > 0'),
    sa.CheckConstraint('min_trade_amount > 0'),
    sa.CheckConstraint('phase < 2 OR collection_deadline IS NOT NULL'),
    sa.CheckConstraint('phase < 3 OR collection_started_at IS NOT NULL'),
    sa.CheckConstraint('phase > 0 AND phase <= 3'),
    sa.CheckConstraint('phase > 2 OR phase_deadline IS NOT NULL'),
    sa.CheckConstraint('worker_turn_subphase >= 0 AND worker_turn_subphase <= 10'),
    sa.PrimaryKeyConstraint('turn_id'),
    comment='Represents a circular trading round in which a "worker" server participates. "Worker" servers will watch for new and changed rows in the solver\'s `turn` table, and will copy them off.'
    )
    with op.batch_alter_table('worker_turn', schema=None) as batch_op:
        batch_op.create_index('idx_worker_turn_subphase', ['worker_turn_subphase'], unique=False, postgresql_where=sa.text('worker_turn_subphase < 10'))

    # ### end Alembic commands ###


def downgrade_():
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table('worker_turn', schema=None) as batch_op:
        batch_op.drop_index('idx_worker_turn_subphase', postgresql_where=sa.text('worker_turn_subphase < 10'))

    op.drop_table('worker_turn')
    op.drop_table('worker_account')
    op.drop_table('trading_policy')
    op.drop_table('store_document_signal')
    op.drop_table('recently_needed_collector')
    op.drop_table('prepare_transfer_signal')
    op.drop_table('needed_worker_account')
    op.drop_table('needed_collector_signal')
    op.drop_table('finalize_transfer_signal')
    op.drop_table('fetch_debtor_info_signal')
    op.drop_table('discover_debtor_signal')
    with op.batch_alter_table('debtor_locator_claim', schema=None) as batch_op:
        batch_op.drop_index('idx_debtor_locator_claim_latest_locator_fetch_at', postgresql_where=sa.text('latest_locator_fetch_at IS NOT NULL'))

    op.drop_table('debtor_locator_claim')
    with op.batch_alter_table('debtor_info_fetch', schema=None) as batch_op:
        batch_op.drop_index('idx_debtor_info_fetch_next_attempt_at')

    op.drop_table('debtor_info_fetch')
    op.drop_table('debtor_info_document')
    op.drop_table('confirm_debtor_signal')
    op.drop_table('configure_account_signal')
    op.drop_table('candidate_offer_signal')
    op.drop_table('active_collector')
    op.drop_table('activate_collector_signal')
    # ### end Alembic commands ###


def upgrade_solver():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('buy_offer',
    sa.Column('turn_id', sa.Integer(), nullable=False),
    sa.Column('creditor_id', sa.BigInteger(), nullable=False),
    sa.Column('debtor_id', sa.BigInteger(), nullable=False),
    sa.Column('amount', sa.BigInteger(), nullable=False),
    sa.CheckConstraint('amount > 0'),
    sa.PrimaryKeyConstraint('turn_id', 'creditor_id', 'debtor_id'),
    comment='Represents a buy offer, participating in a given trading turn. "Worker" servers are responsible for populating this table during the phase 2 of each turn. The "solver" server will read from this table, and will delete the records before advancing to phase 3 of the turn.'
    )
    op.create_table('collector_account',
    sa.Column('debtor_id', sa.BigInteger(), nullable=False),
    sa.Column('collector_id', sa.BigInteger(), nullable=False),
    sa.Column('collector_hash', sa.SmallInteger(), nullable=False),
    sa.Column('account_id', sa.String(), nullable=False),
    sa.Column('status', sa.SmallInteger(), nullable=False, comment="Collector account's status: 0) pristine; 1) account creation has been requested; 2) the account has been created, and an account ID has been assigned to it; 3) disabled."),
    sa.Column('latest_status_change_at', sa.TIMESTAMP(timezone=True), nullable=False),
    sa.CheckConstraint('status >= 0 AND status <= 3'),
    comment='Represents a planned or existing Swaptacular account, which should be used to collect and dispatch transfers. "Worker" servers will watch for new (pristine) records inserted in this table, and will try to create and use all the accounts catalogued in this table.'
    )
    # Create a "covering" index instead of a "normal" index.
    op.execute('CREATE UNIQUE INDEX idx_collector_account_pk ON collector_account (debtor_id, collector_id) INCLUDE (status)')
    op.execute('ALTER TABLE collector_account ADD CONSTRAINT collector_account_pkey PRIMARY KEY USING INDEX idx_collector_account_pk')

    with op.batch_alter_table('collector_account', schema=None) as batch_op:
        batch_op.create_index('idx_collector_account_creation_request', ['status'], unique=False, postgresql_where=sa.text('status = 0'))

    op.create_table('collector_collecting',
    sa.Column('turn_id', sa.Integer(), nullable=False),
    sa.Column('debtor_id', sa.BigInteger(), nullable=False),
    sa.Column('creditor_id', sa.BigInteger(), nullable=False),
    sa.Column('amount', sa.BigInteger(), nullable=False),
    sa.Column('collector_id', sa.BigInteger(), nullable=False),
    sa.Column('collector_hash', sa.SmallInteger(), nullable=False),
    sa.CheckConstraint('amount > 0'),
    sa.PrimaryKeyConstraint('turn_id', 'debtor_id', 'creditor_id'),
    comment='Informs the "worker" server responsible for the given collector, that the given amount will be withdrawn (collected) from the given customer account, as part of the given trading turn. During the phase 3 of each turn, "Worker" servers should make their own copy of the records in this table, and then delete the original records.'
    )
    op.create_table('collector_dispatching',
    sa.Column('turn_id', sa.Integer(), nullable=False),
    sa.Column('debtor_id', sa.BigInteger(), nullable=False),
    sa.Column('creditor_id', sa.BigInteger(), nullable=False),
    sa.Column('amount', sa.BigInteger(), nullable=False),
    sa.Column('collector_id', sa.BigInteger(), nullable=False),
    sa.Column('collector_hash', sa.SmallInteger(), nullable=False),
    sa.CheckConstraint('amount > 0'),
    sa.PrimaryKeyConstraint('turn_id', 'debtor_id', 'creditor_id'),
    comment='Informs the "worker" server responsible for the given collector, that the given amount must be deposited (dispatched) to the given customer account, as part of the given trading turn. During the phase 3 of each turn, "Worker" servers should make their own copy of the records in this table, and then delete the original records.'
    )
    op.create_table('collector_receiving',
    sa.Column('turn_id', sa.Integer(), nullable=False),
    sa.Column('debtor_id', sa.BigInteger(), nullable=False),
    sa.Column('to_collector_id', sa.BigInteger(), nullable=False),
    sa.Column('from_collector_id', sa.BigInteger(), nullable=False),
    sa.Column('to_collector_hash', sa.SmallInteger(), nullable=False),
    sa.Column('amount', sa.BigInteger(), nullable=False),
    sa.CheckConstraint('amount > 0'),
    sa.CheckConstraint('from_collector_id != to_collector_id'),
    sa.PrimaryKeyConstraint('turn_id', 'debtor_id', 'to_collector_id', 'from_collector_id'),
    comment='Informs the "worker" server responsible for the given "to collector" account, that the given amount will be transferred (received) from another collector account, as part of the given trading turn. During the phase 3 of each turn, "Worker" servers should make their own copy of the records in this table, and then delete the original records.'
    )
    op.create_table('collector_sending',
    sa.Column('turn_id', sa.Integer(), nullable=False),
    sa.Column('debtor_id', sa.BigInteger(), nullable=False),
    sa.Column('from_collector_id', sa.BigInteger(), nullable=False),
    sa.Column('to_collector_id', sa.BigInteger(), nullable=False),
    sa.Column('from_collector_hash', sa.SmallInteger(), nullable=False),
    sa.Column('amount', sa.BigInteger(), nullable=False),
    sa.CheckConstraint('amount > 0'),
    sa.CheckConstraint('from_collector_id != to_collector_id'),
    sa.PrimaryKeyConstraint('turn_id', 'debtor_id', 'from_collector_id', 'to_collector_id'),
    comment='Informs the "worker" server responsible for the given "from collector" account, that the given amount must be transferred (sent) to another collector account, as part of the given trading turn. During the phase 3 of each turn, "Worker" servers should make their own copy of the records in this table, and then delete the original records.'
    )
    op.create_table('confirmed_debtor',
    sa.Column('turn_id', sa.Integer(), nullable=False),
    sa.Column('debtor_id', sa.BigInteger(), nullable=False),
    sa.Column('debtor_info_locator', sa.String(), nullable=False),
    sa.PrimaryKeyConstraint('turn_id', 'debtor_id'),
    comment='Represents the fact that a given currency (aka debtor) is verified (confirmed), so that this currency can be traded during the given trading turn. "Worker" servers are responsible for populating this table during the phase 1 of each turn. The "solver" server will read from this table, and will delete the records before advancing to phase 2 of the turn.'
    )
    op.create_table('creditor_giving',
    sa.Column('turn_id', sa.Integer(), nullable=False),
    sa.Column('creditor_id', sa.BigInteger(), nullable=False),
    sa.Column('debtor_id', sa.BigInteger(), nullable=False),
    sa.Column('creditor_hash', sa.SmallInteger(), nullable=False),
    sa.Column('amount', sa.BigInteger(), nullable=False),
    sa.Column('collector_id', sa.BigInteger(), nullable=False),
    sa.CheckConstraint('amount > 0'),
    sa.PrimaryKeyConstraint('turn_id', 'creditor_id', 'debtor_id'),
    comment='Informs the "worker" server responsible for the given customer account, that the given amount will be deposited (given) to this account, as part of the given trading turn. During the phase 3 of each turn, "Worker" servers should make their own copy of the records in this table, and then delete the original records.'
    )
    op.create_table('creditor_taking',
    sa.Column('turn_id', sa.Integer(), nullable=False),
    sa.Column('creditor_id', sa.BigInteger(), nullable=False),
    sa.Column('debtor_id', sa.BigInteger(), nullable=False),
    sa.Column('creditor_hash', sa.SmallInteger(), nullable=False),
    sa.Column('amount', sa.BigInteger(), nullable=False),
    sa.Column('collector_id', sa.BigInteger(), nullable=False),
    sa.CheckConstraint('amount > 0'),
    sa.PrimaryKeyConstraint('turn_id', 'creditor_id', 'debtor_id'),
    comment='Informs the "worker" server responsible for the given customer account, that the given amount must be withdrawn (taken) from the account, as part of the given trading turn. During the phase 3 of each turn, "Worker" servers should make their own copy of the records in this table, and then delete the original records.'
    )
    op.create_table('currency_info',
    sa.Column('turn_id', sa.Integer(), nullable=False),
    sa.Column('debtor_info_locator', sa.String(), nullable=False),
    sa.Column('debtor_id', sa.BigInteger(), nullable=False),
    sa.Column('peg_debtor_info_locator', sa.String(), nullable=True),
    sa.Column('peg_debtor_id', sa.BigInteger(), nullable=True),
    sa.Column('peg_exchange_rate', sa.FLOAT(), nullable=True),
    sa.Column('is_confirmed', sa.BOOLEAN(), nullable=False),
    sa.PrimaryKeyConstraint('turn_id', 'debtor_info_locator'),
    comment='Represents relevant information about a given currency (aka debtor), so that the currency can participate in a given trading turn. The "solver" server will populate this table before the start of phase 2 of each turn, and will delete the records before advancing to phase 3. "Worker" servers will read from this table, so as to generate relevant buy and sell offers.'
    )
    with op.batch_alter_table('currency_info', schema=None) as batch_op:
        batch_op.create_index('idx_currency_info_confirmed_debtor_id', ['turn_id', 'debtor_id'], unique=True, postgresql_where=sa.text('is_confirmed'))

    op.create_table('debtor_info',
    sa.Column('turn_id', sa.Integer(), nullable=False),
    sa.Column('debtor_info_locator', sa.String(), nullable=False),
    sa.Column('debtor_id', sa.BigInteger(), nullable=False),
    sa.Column('peg_debtor_info_locator', sa.String(), nullable=True),
    sa.Column('peg_debtor_id', sa.BigInteger(), nullable=True),
    sa.Column('peg_exchange_rate', sa.FLOAT(), nullable=True),
    sa.PrimaryKeyConstraint('turn_id', 'debtor_info_locator'),
    comment='Represents relevant information about a given currency (aka debtor), so that the currency can participate in a given trading turn. "Worker" servers are responsible for populating this table during the phase 1 of each turn. The "solver" server will read from this table, and will delete the records before advancing to phase 2 of the turn.'
    )
    op.create_table('sell_offer',
    sa.Column('turn_id', sa.Integer(), nullable=False),
    sa.Column('creditor_id', sa.BigInteger(), nullable=False),
    sa.Column('debtor_id', sa.BigInteger(), nullable=False),
    sa.Column('amount', sa.BigInteger(), nullable=False),
    sa.Column('collector_id', sa.BigInteger(), nullable=False),
    sa.CheckConstraint('amount > 0'),
    sa.PrimaryKeyConstraint('turn_id', 'creditor_id', 'debtor_id'),
    comment='Represents a sell offer, participating in a given trading turn. "Worker" servers are responsible for populating this table during the phase 2 of each turn. The "solver" server will read from this table, and will delete the records before advancing to phase 3 of the turn.'
    )
    op.create_table('turn',
    sa.Column('turn_id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('started_at', sa.TIMESTAMP(timezone=True), nullable=False),
    sa.Column('base_debtor_info_locator', sa.String(), nullable=False),
    sa.Column('base_debtor_id', sa.BigInteger(), nullable=False),
    sa.Column('max_distance_to_base', sa.SmallInteger(), nullable=False),
    sa.Column('min_trade_amount', sa.BigInteger(), nullable=False),
    sa.Column('phase', sa.SmallInteger(), nullable=False, comment="Turn's phase: 1) gathering currencies info; 2) gathering buy and sell offers; 3) giving and taking; 4) done."),
    sa.Column('phase_deadline', sa.TIMESTAMP(timezone=True), nullable=True),
    sa.Column('collection_started_at', sa.TIMESTAMP(timezone=True), nullable=True),
    sa.Column('collection_deadline', sa.TIMESTAMP(timezone=True), nullable=True),
    sa.CheckConstraint('base_debtor_id != 0'),
    sa.CheckConstraint('max_distance_to_base > 0'),
    sa.CheckConstraint('min_trade_amount > 0'),
    sa.CheckConstraint('phase < 2 OR collection_deadline IS NOT NULL'),
    sa.CheckConstraint('phase < 3 OR collection_started_at IS NOT NULL'),
    sa.CheckConstraint('phase > 0 AND phase <= 4'),
    sa.CheckConstraint('phase > 2 OR phase_deadline IS NOT NULL'),
    sa.PrimaryKeyConstraint('turn_id'),
    comment='Represents a circular trading round, created and managed by the "solver" server. "Worker" servers will watch for changes in this table, so as to participate in the different phases of each trading round.'
    )
    with op.batch_alter_table('turn', schema=None) as batch_op:
        batch_op.create_index('idx_turn_phase', ['phase'], unique=False, postgresql_where=sa.text('phase < 4'))
        batch_op.create_index('idx_turn_started_at', ['started_at'], unique=False)

    # ### end Alembic commands ###


def downgrade_solver():
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table('turn', schema=None) as batch_op:
        batch_op.drop_index('idx_turn_started_at')
        batch_op.drop_index('idx_turn_phase', postgresql_where=sa.text('phase < 4'))

    op.drop_table('turn')
    op.drop_table('sell_offer')
    op.drop_table('debtor_info')
    with op.batch_alter_table('currency_info', schema=None) as batch_op:
        batch_op.drop_index('idx_currency_info_confirmed_debtor_id', postgresql_where=sa.text('is_confirmed'))

    op.drop_table('currency_info')
    op.drop_table('creditor_taking')
    op.drop_table('creditor_giving')
    op.drop_table('confirmed_debtor')
    op.drop_table('collector_sending')
    op.drop_table('collector_receiving')
    op.drop_table('collector_dispatching')
    op.drop_table('collector_collecting')
    with op.batch_alter_table('collector_account', schema=None) as batch_op:
        batch_op.drop_index('idx_collector_account_creation_request', postgresql_where=sa.text('status = 0'))

    op.drop_table('collector_account')
    op.drop_table('buy_offer')
    # ### end Alembic commands ###

