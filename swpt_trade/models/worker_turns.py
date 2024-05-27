from __future__ import annotations
from datetime import date
from .common import get_now_utc
from sqlalchemy.sql.expression import null, or_, and_
from swpt_trade.extensions import db


class WorkerTurn(db.Model):
    turn_id = db.Column(db.Integer, primary_key=True, autoincrement=False)
    started_at = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
    base_debtor_info_locator = db.Column(db.String, nullable=False)
    base_debtor_id = db.Column(db.BigInteger, nullable=False)
    max_distance_to_base = db.Column(db.SmallInteger, nullable=False)
    min_trade_amount = db.Column(db.BigInteger, nullable=False)
    phase = db.Column(db.SmallInteger, nullable=False)
    phase_deadline = db.Column(db.TIMESTAMP(timezone=True))
    collection_started_at = db.Column(db.TIMESTAMP(timezone=True))
    collection_deadline = db.Column(db.TIMESTAMP(timezone=True))
    worker_turn_subphase = db.Column(
        db.SmallInteger,
        nullable=False,
        default=0,
        comment=(
            "The worker may divide the processing of each phase to one or more"
            " sub-phases. The initial sub-phase is always `0`, and the final"
            " sub-phase is always `10`. Sequential sub-phases do not need to"
            " be (and normally will not be) represented by sequential"
            " numbers. This gives the freedom to add sub-phases if necessary."
        ),
    )
    __table_args__ = (
        db.CheckConstraint(base_debtor_id != 0),
        db.CheckConstraint(max_distance_to_base > 0),
        db.CheckConstraint(min_trade_amount > 0),
        db.CheckConstraint(and_(phase > 0, phase <= 3)),
        db.CheckConstraint(or_(phase > 2, phase_deadline != null())),
        db.CheckConstraint(or_(phase < 2, collection_deadline != null())),
        db.CheckConstraint(or_(phase < 3, collection_started_at != null())),
        db.CheckConstraint(
            and_(worker_turn_subphase >= 0, worker_turn_subphase <= 10)
        ),
        db.Index(
            "idx_worker_turn_phase",
            phase,
            postgresql_where=phase < 3,
        ),
        db.Index(
            "idx_worker_turn_subphase",
            worker_turn_subphase,
            postgresql_where=worker_turn_subphase < 10,
        ),
        {
            "comment": (
                'Represents a circular trading round in which a "worker"'
                ' server participates. "Worker" servers will watch for'
                " new and changed rows in the solver's `turn` table, and will"
                ' copy them off.'
            ),
        },
    )


class RecentlyNeededCollector(db.Model):
    debtor_id = db.Column(db.BigInteger, primary_key=True, autoincrement=False)
    needed_at = db.Column(
        db.TIMESTAMP(timezone=True), nullable=False, default=get_now_utc
    )
    __table_args__ = (
        db.CheckConstraint(debtor_id != 0),
        {
            "comment": (
                'Indicates that the creation of a collector account for the'
                ' currency with the given debtor ID has been recently'
                ' requested. This information is used to prevent "worker"'
                ' servers from making repetitive queries to the central'
                " database."
            ),
        },
    )


class ActiveCollector(db.Model):
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    collector_id = db.Column(db.BigInteger, primary_key=True)
    account_id = db.Column(db.String, nullable=False)
    __table_args__ = (
        db.CheckConstraint(account_id != ""),
        {
            "comment": (
                'Represents an active Swaptacular account which can be'
                ' used to collect and dispatch transfers. Each "Worker"'
                ' servers will maintain its own copy of this table (that is:'
                ' no rows-sharding) by periodically copying the relevant'
                ' records from the solver\'s "collector_account" table.'
                ' "Worker" servers will use this local copy so as to avoid'
                ' querying the central database too often.'
            ),
        },
    )


# This sequence is used to generate `coordinator_request_id`s for the
# issued `PrepareTransfer` SMP messages.
#
# NOTE:
# `op.execute(CreateSequence(Sequence('coordinator_request_id_seq')))`
# should be manually added to the generated migration file.
cr_seq = db.Sequence(
    "coordinator_request_id_seq", metadata=db.Model.metadata
)


class AccountLock(db.Model):
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    turn_id = db.Column(db.Integer, nullable=False)
    collector_id = db.Column(db.BigInteger, nullable=False)
    initiated_at = db.Column(
        db.TIMESTAMP(timezone=True),
        nullable=False,
        default=get_now_utc,
        comment="The timestamp of the sent `PrepareTransfer` SMP message.",
    )
    coordinator_request_id = db.Column(
        db.BigInteger, nullable=False, server_default=cr_seq.next_value()
    )
    amount = db.Column(
        db.BigInteger,
        comment=(
            "Can be negative or zero (the trader wants to sell), or positive"
            " (the trader wants to buy). When selling, and the `transfer_id`"
            " column is being set to a non-NULL value, the amount will be"
            " re-calculated to be equal to the locked amount reduced in"
            " accordance with the effective demurrage rate. Also, when"
            " selling, and the `finalized_at` column is being set to a"
            " non-NULL value, the amount will be re-set to be equal to the"
            " committed amount with a negative sign."
        ),
        nullable=False,
    )
    transfer_id = db.Column(db.BigInteger)
    finalized_at = db.Column(
        db.TIMESTAMP(timezone=True),
        comment="The timestamp of the sent `FinalizeTransfer` SMP message.",
    )
    released_at = db.Column(db.TIMESTAMP(timezone=True))
    account_creation_date = db.Column(db.DATE)
    account_last_transfer_number = db.Column(db.BigInteger)
    __mapper_args__ = {"eager_defaults": True}
    __table_args__ = (
        db.CheckConstraint(or_(finalized_at == null(), transfer_id != null())),
        db.CheckConstraint(
            or_(
                released_at == null(),
                and_(
                    finalized_at != null(),
                    account_creation_date != null(),
                    account_last_transfer_number != null(),
                ),
            )
        ),
        db.ForeignKeyConstraint(["turn_id"], ["worker_turn.turn_id"]),
        db.Index("idx_lock_account_turn_id", turn_id),
        db.Index(
            "idx_lock_account_coordinator_request_id",
            coordinator_request_id,
            unique=True,
        ),
        {
            "comment": (
                "Represents an attempt to arrange the participation of a"
                " given account in a given trading turn. Normally, this"
                " includes sending a `PrepareTransfer` SMP message."
            ),
        },
    )

    worker_turn = db.relationship("WorkerTurn")

    @property
    def is_self_lock(self):
        return self.creditor_id == self.collector_id

    def is_in_force(self, acd: date, altn: int) -> bool:
        return not (
            self.released_at is not None
            and (
                self.account_creation_date < acd
                or (
                    self.account_creation_date == acd
                    and self.account_last_transfer_number <= altn
                )
            )
        )


class CreditorParticipation(db.Model):
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    turn_id = db.Column(db.Integer, primary_key=True)
    amount = db.Column(
        db.BigInteger,
        nullable=False,
        comment=(
            "Can be positive or negative, but can not be zero. A positive"
            " number indicates that this amount should be given to the"
            " creditor. A negative number indicates that this amount should"
            " be taken from the creditor."
        ),
    )
    collector_id = db.Column(db.BigInteger, nullable=False)
    __table_args__ = (
        db.CheckConstraint(amount != 0),
        {
            "comment": (
                'Indicates that the given amount must be given or taken'
                ' to/from the given creditor as part of the given trading'
                ' turn. During the phase 3 of each turn, "worker" servers'
                ' will move the records from the "creditor_giving" and'
                ' and "creditor_taking" solver tables to this table.'
            ),
        },
    )


class TransfersTrigger(db.Model):
    collector_id = db.Column(db.BigInteger, primary_key=True)
    turn_id = db.Column(db.Integer, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    amount_to_collect = db.Column(
        db.BigInteger,
        nullable=False,
        comment=(
            'The sum of all amounts from the corresponding records in the'
            ' "worker_collecting" table.'
        ),
    )
    amount_to_send = db.Column(
        db.BigInteger,
        nullable=False,
        comment=(
            'The sum of all amounts from the corresponding records in the'
            ' "worker_sending" table.'
        ),
    )
    triggered_sending = db.Column(db.BOOLEAN, nullable=False, default=False)
    all_sent = db.Column(db.BOOLEAN, nullable=False, default=False)
    total_received_amount = db.Column(
        db.BigInteger,
        comment=(
            "A non-NULL value (including zero) indicates that all transfers"
            ' for the corresponding records in the "worker_receiving" table'
            ' have been received.'
        ),
    )
    __table_args__ = (
        db.CheckConstraint(amount_to_collect >= 0),
        db.CheckConstraint(amount_to_send >= 0),
        db.CheckConstraint(amount_to_send <= amount_to_collect),
        db.CheckConstraint(total_received_amount >= 0),
        {
            "comment": (
                'Represents the status of the process of collecting, sending,'
                ' and receiving for a given collector account during a given'
                ' trading turn.'
            ),
        },
    )

    @property
    def all_received(self) -> bool:
        return self.total_received_amount is not None

    @property
    def available_amount(self) -> int:
        return (
            + self.amount_to_collect
            - self.amount_to_send
            + (self.total_received_amount or 0)
        )


class WorkerCollecting(db.Model):
    collector_id = db.Column(db.BigInteger, primary_key=True)
    turn_id = db.Column(db.Integer, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    amount = db.Column(db.BigInteger, nullable=False)
    __table_args__ = (
        db.CheckConstraint(amount > 0),
        {
            "comment": (
                'Indicates that the given amount will be withdrawn (collected)'
                " from the given creditor's account, as part of the given"
                ' trading turn, and will be transferred to the given'
                ' collector. During the phase 3 of each turn, "worker" servers'
                ' will move the records from the "collector_collecting" solver'
                ' table to this table.'
            ),
        },
        # NOTE: Normally, there should be a foreign key constraint
        # connecting each row in this table to a row in the
        # "transfers_trigger" table. For performance reasons, however,
        # this foreign key is not declared.
    )


class WorkerSending(db.Model):
    from_collector_id = db.Column(db.BigInteger, primary_key=True)
    turn_id = db.Column(db.Integer, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    to_collector_id = db.Column(db.BigInteger, primary_key=True)
    amount = db.Column(db.BigInteger, nullable=False)
    __table_args__ = (
        db.CheckConstraint(amount > 0),
        {
            "comment": (
                'Indicates that the given amount must be transferred (sent)'
                ' to another collector account, as part of the given trading'
                ' turn. During the phase 3 of each turn, "worker" servers will'
                ' move the records from the "collector_sending" solver table'
                ' to this table.'
            ),
        },
    )


class WorkerReceiving(db.Model):
    to_collector_id = db.Column(db.BigInteger, primary_key=True)
    turn_id = db.Column(db.Integer, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    from_collector_id = db.Column(db.BigInteger, primary_key=True)
    expected_amount = db.Column(db.BigInteger, nullable=False)
    received_amount = db.Column(
        db.BigInteger,
        nullable=False,
        default=0,
        comment=(
            'The received amount will be equal to the expected amount'
            ' minus the accumulated negative interest (that is: when'
            ' the interest rate is negative).'
        ),
        # NOTE: When the expected amount is `1`, after applying the
        # possibly negative interest rate, and rounding down, the
        # received amount would have to be zero, which is impossible.
        # Therefore, we can expect to receive only amounts greater
        # than `1`.
    )
    __table_args__ = (
        db.CheckConstraint(expected_amount > 1),
        db.CheckConstraint(received_amount >= 0),
        db.Index(
            "idx_worker_receiving_not_received",
            to_collector_id,
            turn_id,
            debtor_id,
            from_collector_id,
            postgresql_where=received_amount == 0,
        ),
        {
            "comment": (
                'Indicates that some amount will be transferred (received)'
                ' from another collector account, as part of the given trading'
                ' turn. During the phase 3 of each turn, "worker" servers will'
                ' move the records from the "collector_receiving" solver table'
                ' to this table.'
            ),
        },
        # NOTE: Normally, there should be a foreign key constraint
        # connecting each row in this table to a row in the
        # "transfers_trigger" table. For performance reasons, however,
        # this foreign key is not declared.
    )


class WorkerDispatching(db.Model):
    collector_id = db.Column(db.BigInteger, primary_key=True)
    turn_id = db.Column(db.Integer, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    amount = db.Column(db.BigInteger, nullable=False)
    __table_args__ = (
        db.CheckConstraint(amount > 0),
        {
            "comment": (
                'Indicates that the given amount must be deposited'
                ' (dispatched) to the given customer account, as part of the'
                ' given trading turn. During the phase 3 of each turn,'
                ' "worker" servers will move the records from the'
                ' "collector_dispatching" solver table to this table.'
            ),
        },
    )
