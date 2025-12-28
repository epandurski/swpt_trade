from __future__ import annotations
from datetime import date, timedelta
from .common import get_now_utc, MAX_INT16, MAX_INT32, MIN_INT64, MAX_INT64
from sqlalchemy.dialects import postgresql as pg
from sqlalchemy.sql.expression import null, true, false, or_, and_
from swpt_trade.utils import TransferNote
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
    max_locked_amount = db.Column(
        db.BigInteger,
        nullable=False,
        default=MAX_INT64,
        comment="An upper limit for the actual locked amount.",
    )
    transfer_id = db.Column(db.BigInteger)
    finalized_at = db.Column(
        db.TIMESTAMP(timezone=True),
        comment="The timestamp of the sent `FinalizeTransfer` SMP message.",
    )
    released_at = db.Column(db.TIMESTAMP(timezone=True))
    account_creation_date = db.Column(db.DATE)
    account_last_transfer_number = db.Column(db.BigInteger)
    has_been_revised = db.Column(db.BOOLEAN, nullable=False, default=False)
    __mapper_args__ = {"eager_defaults": True}
    __table_args__ = (
        db.CheckConstraint(max_locked_amount >= 0),
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

    @property
    def is_self_lock(self):
        """Return `True` if this account lock is a preparation to make
        a transfer from one account to the same account. This can
        happen when a collector account ties to participate in a
        trading turn.
        """
        return self.creditor_id == self.collector_id

    def is_in_force(self, acd: date, altn: int) -> bool:
        """Determine whether the account lock is in effect, by
        checking if the lock has been released, and if it has been,
        comparing the passed `acd` and `altn` parameters to the
        `self.account_creation_date` and
        `self.account_last_transfer_number` attributes.
        """
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
            "Can be positive or negative, but can not be zero or one. A"
            " positive number indicates that this amount should be given to"
            " the creditor. A negative number indicates that this amount"
            " should be taken from the creditor."
        ),
        # NOTE: When the amount is `1`, after applying the possibly
        # negative interest rate, and rounding down, the transferred
        # amount would have to be zero, which is impossible to be
        # transferred. Therefore, we can try to give only amounts
        # greater than `1`.
    )
    collector_id = db.Column(db.BigInteger, nullable=False)
    __table_args__ = (
        db.CheckConstraint(or_(amount < 0, amount > 1)),
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


class DispatchingStatus(db.Model):
    # NOTE: The `started_sending`, `all_sent`, `started_dispatching`,
    # and `awaiting_signal_flag` columns are not be part of the
    # primary key, but it probably is a good idea to include them in
    # the primary key index to allow index-only scans. Because
    # SQLAlchemy does not support this yet (2024-01-19), the migration
    # file should be edited so as not to create a "normal" index, but
    # create a "covering" index instead.

    collector_id = db.Column(db.BigInteger, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    turn_id = db.Column(db.Integer, primary_key=True)
    inserted_at = db.Column(
        db.TIMESTAMP(timezone=True), nullable=False, default=get_now_utc
    )
    amount_to_collect = db.Column(
        db.BigInteger,
        nullable=False,
        comment=(
            'The sum of all amounts from the corresponding records in the'
            ' "worker_collecting" table, at the moment the'
            ' "dispatching_status" record has been created.'
        ),
    )
    total_collected_amount = db.Column(
        db.BigInteger,
        comment=(
            "A non-NULL value indicates that no more transfers for"
            ' corresponding records in the "worker_collecting" table will'
            ' be collected.'
        ),
    )
    amount_to_send = db.Column(
        db.BigInteger,
        nullable=False,
        comment=(
            'The sum of all amounts from the corresponding records in the'
            ' "worker_sending" table, at the moment the "dispatching_status"'
            ' record has been created.'
        ),
    )
    started_sending = db.Column(db.BOOLEAN, nullable=False, default=False)
    all_sent = db.Column(db.BOOLEAN, nullable=False, default=False)
    amount_to_receive = db.Column(
        db.BigInteger,
        nullable=False,
        comment=(
            'The sum of all expected amounts from the corresponding records'
            ' in the "worker_receiving" table, at the moment the'
            ' "dispatching_status" record has been created.'
        ),
    )
    number_to_receive = db.Column(
        db.Integer,
        nullable=False,
        comment=(
            'The number of corresponding records in the "worker_receiving"'
            ' table, at the moment the "dispatching_status" record has'
            ' been created.'
        ),
    )
    total_received_amount = db.Column(
        db.BigInteger,
        comment=(
            "A non-NULL value indicates that no more transfers for"
            ' corresponding records in the "worker_receiving" table will'
            ' be received.'
        ),
    )
    all_received = db.Column(db.BOOLEAN, nullable=False, default=False)
    amount_to_dispatch = db.Column(
        db.BigInteger,
        nullable=False,
        comment=(
            'The sum of all amounts from the corresponding records in the'
            ' "worker_dispatching" table, at the moment the'
            ' "dispatching_status" record has been created.'
        ),
    )
    started_dispatching = db.Column(db.BOOLEAN, nullable=False, default=False)
    awaiting_signal_flag = db.Column(db.BOOLEAN, nullable=False, default=False)
    __table_args__ = (
        db.CheckConstraint(amount_to_collect >= 0),
        db.CheckConstraint(total_collected_amount >= 0),
        db.CheckConstraint(total_collected_amount <= amount_to_collect),
        db.CheckConstraint(amount_to_send >= 0),
        db.CheckConstraint(amount_to_send <= amount_to_collect),
        db.CheckConstraint(amount_to_receive >= 0),
        db.CheckConstraint(number_to_receive >= 0),
        db.CheckConstraint(total_received_amount >= 0),
        db.CheckConstraint(amount_to_dispatch >= 0),
        db.CheckConstraint(
            started_sending == (total_collected_amount != null())
        ),
        db.CheckConstraint(
            or_(all_sent == false(), started_sending == true())
        ),
        db.CheckConstraint(
            or_(all_received == false(), total_received_amount != null())
        ),
        db.CheckConstraint(
            started_dispatching == and_(
                all_sent == true(), total_received_amount != null()
            )
        ),
        {
            "comment": (
                'Represents the status of the process of collecting, sending,'
                ' receiving, and dispatching for a given collector account,'
                ' during a given trading turn.'
            ),
        },
    )

    @property
    def finished_collecting(self) -> bool:
        return self.total_collected_amount is not None

    @property
    def all_collected(self) -> bool:
        return self.total_collected_amount == self.amount_to_collect

    @property
    def collected_amount(self) -> int:
        return (self.total_collected_amount or 0)

    @property
    def missing_collected_amount(self) -> int:
        """Amount that we expected to collect but we did not.
        """
        amt = self.amount_to_collect - self.collected_amount
        assert 0 <= amt <= self.amount_to_collect
        return amt

    @property
    def available_amount_to_send(self) -> int:
        """Amount that we will be sending.

        When the collected amount is smaller than expected, the
        missing amount will be hoarded instead of being sent.
        """
        amt = max(self.amount_to_send - self.missing_collected_amount, 0)
        assert 0 <= amt <= self.amount_to_send
        return amt

    @property
    def hoarded_collected_amount(self) -> int:
        """Amount that we were supposed to send but we will not.
        """
        amt = self.amount_to_send - self.available_amount_to_send
        assert 0 <= amt <= self.amount_to_send
        return amt

    @property
    def finished_receiving(self) -> bool:
        return self.total_received_amount is not None

    @property
    def received_amount(self) -> int:
        return (self.total_received_amount or 0)

    @property
    def missing_received_amount(self) -> int:
        """Amount that we expected to receive but we did not.
        """
        amt = max(self.amount_to_receive - self.received_amount, 0)
        assert 0 <= amt <= self.amount_to_receive
        return amt

    @property
    def available_amount_to_dispatch(self) -> int:
        """Amount that we will be dispatching.
        """
        amt = max(
            (
                + self.amount_to_dispatch
                - self.missing_collected_amount
                + self.hoarded_collected_amount
                - self.missing_received_amount
            ),
            0,
        )
        assert 0 <= amt <= self.amount_to_dispatch
        return amt


class WorkerCollecting(db.Model):
    # NOTE: The `amount` column is not be part of the primary key, but
    # it probably is a good idea to include it in the primary key
    # index to allow index-only scans. Because SQLAlchemy does not
    # support this yet (2024-01-19), the migration file should be
    # edited so as not to create a "normal" index, but create a
    # "covering" index instead.

    collector_id = db.Column(db.BigInteger, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    turn_id = db.Column(db.Integer, primary_key=True)
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    amount = db.Column(db.BigInteger, nullable=False)
    collected = db.Column(db.BOOLEAN, nullable=False, default=False)
    purge_after = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
    __table_args__ = (
        db.CheckConstraint(amount > 0),
        db.CheckConstraint(collector_id != creditor_id),
        db.Index(
            "idx_worker_collecting_not_collected",
            collector_id,
            debtor_id,
            turn_id,
            creditor_id,
            postgresql_where=collected == false(),
        ),
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
    )


class WorkerSending(db.Model):
    # NOTE: The `amount` column is not be part of the primary key, but
    # it probably is a good idea to include it in the primary key
    # index to allow index-only scans. Because SQLAlchemy does not
    # support this yet (2024-01-19), the migration file should be
    # edited so as not to create a "normal" index, but create a
    # "covering" index instead.

    from_collector_id = db.Column(db.BigInteger, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    turn_id = db.Column(db.Integer, primary_key=True)
    to_collector_id = db.Column(db.BigInteger, primary_key=True)
    amount = db.Column(db.BigInteger, nullable=False)
    purge_after = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
    __table_args__ = (
        db.CheckConstraint(amount > 1),
        db.CheckConstraint(from_collector_id != to_collector_id),
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
    # NOTE: The `received_amount` column is not be part of the primary
    # key, but it probably is a good idea to include it in the primary
    # key index to allow index-only scans. Because SQLAlchemy does not
    # support this yet (2024-01-19), the migration file should be
    # edited so as not to create a "normal" index, but create a
    # "covering" index instead.

    to_collector_id = db.Column(db.BigInteger, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    turn_id = db.Column(db.Integer, primary_key=True)
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
    )
    purge_after = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
    __table_args__ = (
        db.CheckConstraint(expected_amount > 1),
        db.CheckConstraint(received_amount >= 0),
        db.CheckConstraint(from_collector_id != to_collector_id),
        db.Index(
            "idx_worker_receiving_not_received",
            to_collector_id,
            debtor_id,
            turn_id,
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
    )


class WorkerDispatching(db.Model):
    # NOTE: The `amount` column is not be part of the primary key, but
    # it probably is a good idea to include it in the primary key
    # index to allow index-only scans. Because SQLAlchemy does not
    # support this yet (2024-01-19), the migration file should be
    # edited so as not to create a "normal" index, but create a
    # "covering" index instead.

    collector_id = db.Column(db.BigInteger, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    turn_id = db.Column(db.Integer, primary_key=True)
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    amount = db.Column(db.BigInteger, nullable=False)
    purge_after = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
    __table_args__ = (
        db.CheckConstraint(amount > 1),
        db.CheckConstraint(collector_id != creditor_id),
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


class TransferAttempt(db.Model):
    UNSPECIFIED_FAILURE = 0
    TIMEOUT = 1
    NEWER_INTEREST_RATE = 2
    RECIPIENT_IS_UNREACHABLE = 3
    INSUFFICIENT_AVAILABLE_AMOUNT = 4
    KIND_SENDING = 0
    KIND_DISPATCHING = 1
    KIND_MOVING = 2
    TRANSFER_NOTE_KINDS: dict[int, TransferNote.Kind] = {
        KIND_SENDING: TransferNote.Kind.SENDING,
        KIND_DISPATCHING: TransferNote.Kind.DISPATCHING,
        KIND_MOVING: TransferNote.Kind.MOVING,
    }

    collector_id = db.Column(
        db.BigInteger,
        primary_key=True,
        comment="This is the creditor ID of the sender.",
    )
    turn_id = db.Column(db.Integer, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    creditor_id = db.Column(
        db.BigInteger,
        primary_key=True,
        comment="This is the creditor ID of the recipient.",
    )
    transfer_kind = db.Column(
        db.SmallInteger,
        primary_key=True,
        comment=(
            "Will be 0 when the collector is sending some amount to"
            " another collector, 1 when the collector is dispatching"
            " some amount to a buyer, and 2 when the surplus amount is"
            " being moved."
        ),
    )
    nominal_amount = db.Column(db.FLOAT, nullable=False)
    collection_started_at = db.Column(
        db.TIMESTAMP(timezone=True),
        nullable=False,
    )
    inserted_at = db.Column(
        db.TIMESTAMP(timezone=True),
        nullable=False,
        default=get_now_utc,
    )
    recipient = db.Column(db.String, nullable=False, default="")
    recipient_version = db.Column(
        db.BigInteger,
        nullable=False,
        default=MIN_INT64,
        comment=(
            "In very rare cases the recipient may be change. This column"
            " allows us to tell which recipient value is newer."
        ),
    )
    rescheduled_for = db.Column(db.TIMESTAMP(timezone=True))
    attempted_at = db.Column(
        db.TIMESTAMP(timezone=True),
        comment="The timestamp of the sent `PrepareTransfer` SMP message.",
    )
    coordinator_request_id = db.Column(db.BigInteger)
    final_interest_rate_ts = db.Column(db.TIMESTAMP(timezone=True))
    amount = db.Column(db.BigInteger)
    transfer_id = db.Column(db.BigInteger)
    finalized_at = db.Column(
        db.TIMESTAMP(timezone=True),
        comment="The timestamp of the sent `FinalizeTransfer` SMP message.",
    )
    failure_code = db.Column(
        db.SmallInteger,
        comment=(
            "Failure codes:"
            " 0) An unspecified failure;"
            " 1) TIMEOUT;"
            " 2) NEWER_INTEREST_RATE;"
            " 3) RECIPIENT_IS_UNREACHABLE;"
            " 4) INSUFFICIENT_AVAILABLE_AMOUNT."
        ),
    )
    backoff_counter = db.Column(db.SmallInteger, nullable=False, default=0)
    fatal_error = db.Column(db.String)
    __table_args__ = (
        db.CheckConstraint(and_(transfer_kind >= 0, transfer_kind <= 2)),
        db.CheckConstraint(nominal_amount >= 1.0),
        db.CheckConstraint(amount > 0),
        db.CheckConstraint(backoff_counter >= 0),
        db.CheckConstraint(
            or_(
                and_(
                    # A transfer attempt has not been made yet.
                    attempted_at == null(),
                    coordinator_request_id == null(),
                    final_interest_rate_ts == null(),
                    amount == null(),
                ),
                and_(
                    # The description of the latest transfer attempt:
                    attempted_at != null(),
                    coordinator_request_id != null(),
                    final_interest_rate_ts != null(),
                    amount != null(),
                    recipient != "",
                ),
            )
        ),
        db.CheckConstraint(
            # A transfer can not become successfully prepared before
            # it has been attempted.
            or_(transfer_id == null(), attempted_at != null())
        ),
        db.CheckConstraint(
            # Successfully prepared transfers are finalized instantly.
            or_(
                and_(transfer_id == null(), finalized_at == null()),
                and_(transfer_id != null(), finalized_at != null()),
            )
        ),
        db.CheckConstraint(
            # A transfer can not fail before it has been attempted.
            or_(failure_code == null(), attempted_at != null())
        ),
        db.CheckConstraint(
            # Can not reschedule a transfer unless if it has not been
            # attempted yet, or the latest attempt has failed.
            or_(
                rescheduled_for == null(),
                attempted_at == null(),
                failure_code != null(),
            )
        ),
        db.CheckConstraint(
            # Transfers ending with a fatal error can not be rescheduled.
            or_(
                fatal_error == null(),
                and_(
                    failure_code == UNSPECIFIED_FAILURE,
                    rescheduled_for == null(),
                ),
            )
        ),
        db.Index(
            "idx_transfer_coordinator_request_id",
            coordinator_request_id,
            postgresql_where=coordinator_request_id != null(),
            unique=True,
        ),
        db.Index(
            "idx_transfer_rescheduled_for",
            rescheduled_for,
            postgresql_where=rescheduled_for != null(),
        ),
        db.Index(
            "idx_transfer_moving_transfer_kind",
            collector_id,
            debtor_id,
            postgresql_where=transfer_kind == KIND_MOVING,
        ),
        {
            "comment": (
                "Represents a past or future attempt to transfer some amount"
                " form a given collector's account to another account, as a"
                " part of a given trading turn. More than one attempt may be"
                " made if the first attempt has failed."
            ),
        },
    )

    @property
    def unknown_recipient(self) -> bool:
        return (
            self.recipient == ""
            or self.failure_code == self.RECIPIENT_IS_UNREACHABLE
        )

    @property
    def can_be_triggered(self) -> bool:
        return (
            not self.rescheduled_for
            and self.recipient != ""
            and (
                not self.attempted_at
                or (
                    # The transfer has been attempted, and has failed,
                    # but not fatally.
                    self.failure_code is not None
                    and self.fatal_error is None
                )
            )
        )

    @property
    def transfer_note_kind(self) -> TransferNote.Kind:
        return self.TRANSFER_NOTE_KINDS[self.transfer_kind]

    def calc_backoff_seconds(self, min_backoff_seconds: float) -> int:
        min_backoff_seconds = max(0, int(min_backoff_seconds))
        n = min(self.backoff_counter, 31)
        return min(min_backoff_seconds * (2 ** n), MAX_INT32)

    def increment_backoff_counter(self) -> None:
        n = self.backoff_counter

        if n < 3:
            # NOTE: The fist back-off is bigger than the next
            # back-offs. This ensures than the exponential factor
            # greatly exceeds the linear factor since the very
            # beginning.
            #
            # For example: When a lot of transfers begin to fail with
            # "TIMEOUT"s, due to a very slow network, the failed
            # transfers will be attempted again with an 8-times bigger
            # `max_commit_delay`s (2 ** 3 == 8). Thus, twice more
            # transfer attempts (the failed, plus the repeated
            # attempts) will be given 9-times more time to complete (1
            # + 8 == 9) compared to the time given to the failed
            # attempts alone.
            self.backoff_counter = 3

        elif n < MAX_INT16:
            self.backoff_counter = n + 1

    def reschedule_failed_attempt(
            self,
            failure_code: int,
            min_backoff_seconds: float,
    ) -> None:
        assert self.attempted_at
        assert self.rescheduled_for is None
        self.rescheduled_for = self.attempted_at + timedelta(
            seconds=self.calc_backoff_seconds(min_backoff_seconds)
        )
        self.failure_code = failure_code
        self.increment_backoff_counter()


class DelayedAccountTransfer(db.Model):
    turn_id = db.Column(db.Integer, primary_key=True)
    message_id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    creditor_id = db.Column(db.BigInteger, nullable=False)
    debtor_id = db.Column(db.BigInteger, nullable=False)
    creation_date = db.Column(db.DATE, nullable=False)
    transfer_number = db.Column(db.BigInteger, nullable=False)
    coordinator_type = db.Column(db.String(30), nullable=False)
    committed_at = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
    acquired_amount = db.Column(db.BigInteger, nullable=False)
    transfer_note_format = db.Column(pg.TEXT, nullable=False)
    transfer_note = db.Column(pg.TEXT, nullable=False)
    principal = db.Column(db.BigInteger, nullable=False)
    previous_transfer_number = db.Column(db.BigInteger, nullable=False)
    sender = db.Column(db.String(100), nullable=False)
    recipient = db.Column(db.String(100), nullable=False)
    ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
    __table_args__ = (
        {
            "comment": (
                'Represents an `AccountTransfer` SMP message which has been'
                ' triggered by the actions of another worker, and has been'
                ' received by this worker, before it is ready to process it.'
                ' Such messages will be saved in this table, in order to be'
                ' "replayed" once this worker is ready to process them.'
            ),
        },
    )
