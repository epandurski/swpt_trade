import logging
import random
import math
from typing import TypeVar, Callable, Tuple, Optional
from dataclasses import dataclass
from datetime import datetime, date, timezone, timedelta
from sqlalchemy import select, insert, delete, update
from sqlalchemy.sql.expression import (
    and_,
    tuple_,
    null,
    true,
    false,
    func,
    literal,
    text,
    cast,
)
from sqlalchemy.orm import exc, load_only
from swpt_trade.utils import (
    TransferNote,
    calc_demurrage,
    contain_principal_overflow,
)
from swpt_trade.extensions import db
from swpt_trade.models import (
    DATE0,
    MAX_INT32,
    MIN_INT64,
    MAX_INT64,
    T_INFINITY,
    AGENT_TRANSFER_NOTE_FORMAT,
    cr_seq,
    WorkerTurn,
    AccountLock,
    UsableCollector,
    PrepareTransferSignal,
    FinalizeTransferSignal,
    TriggerTransferSignal,
    CreditorParticipation,
    DispatchingStatus,
    WorkerCollecting,
    WorkerSending,
    WorkerReceiving,
    WorkerDispatching,
    TransferAttempt,
    TradingPolicy,
    WorkerAccount,
    InterestRateChange,
    AccountIdRequestSignal,
    AccountIdResponseSignal,
    DelayedAccountTransfer,
)

T = TypeVar("T")
atomic: Callable[[T], T] = db.atomic
TD_TOLERABLE_ROUNDING_ERROR = timedelta(seconds=2)
TD_CLOCK_PRECISION_SAFETY_MARGIN = timedelta(minutes=10)

TRANSFER_ATTEMPT_PK = tuple_(
    TransferAttempt.collector_id,
    TransferAttempt.turn_id,
    TransferAttempt.debtor_id,
    TransferAttempt.creditor_id,
    TransferAttempt.transfer_kind,
)
ACCOUNT_LOCK_LOAD_ONLY_PK = load_only(
    AccountLock.creditor_id,
    AccountLock.debtor_id,
)
ACCOUNT_LOCK_LOAD_ONLY_ESSENTIALS = load_only(
    AccountLock.transfer_id,
    AccountLock.collector_id,
    AccountLock.coordinator_request_id,
    AccountLock.amount,
    AccountLock.released_at,
)
ACCOUNT_LOCK_LOAD_ONLY_PREPARED_TRANSFER_DATA = load_only(
    AccountLock.collector_id,
    AccountLock.turn_id,
    AccountLock.initiated_at,
    AccountLock.amount,
    AccountLock.finalized_at,
    AccountLock.transfer_id,
    AccountLock.released_at,
)
ACCOUNT_LOCK_LOAD_ONLY_CANDIDATE_OFFER_DATA = load_only(
    AccountLock.account_last_transfer_number,
    AccountLock.account_creation_date,
    AccountLock.released_at,
    AccountLock.collector_id,
)
WORKER_TURN_LOAD_ONLY_ESSENTIALS = load_only(
    WorkerTurn.phase,
    WorkerTurn.worker_turn_subphase,
    WorkerTurn.collection_started_at,
    WorkerTurn.collection_deadline,
    WorkerTurn.min_trade_amount,
)
WORKER_ACCOUNT_LOAD_ONLY_ESSENTIALS = load_only(
    WorkerAccount.surplus_last_transfer_number,
    WorkerAccount.surplus_spent_amount,
    WorkerAccount.creation_date,
)
DISPATCHING_STATUS_LOAD_ONLY_ESSENTIALS = load_only(
    DispatchingStatus.amount_to_send,
    DispatchingStatus.amount_to_collect,
    DispatchingStatus.total_collected_amount,
    DispatchingStatus.amount_to_dispatch,
    DispatchingStatus.amount_to_receive,
    DispatchingStatus.total_received_amount,
)
TRANSFER_ATTEMPT_LOAD_ONLY_ESSENTIALS = load_only(
    TransferAttempt.transfer_id,
    TransferAttempt.attempted_at,
    TransferAttempt.rescheduled_for,
    TransferAttempt.finalized_at,
    TransferAttempt.amount,
    TransferAttempt.backoff_counter,
    TransferAttempt.failure_code,
)

# Transfer status codes:
SC_OK = "OK"
SC_TIMEOUT = "TIMEOUT"
SC_NEWER_INTEREST_RATE = "NEWER_INTEREST_RATE"
SC_RECIPIENT_IS_UNREACHABLE = "RECIPIENT_IS_UNREACHABLE"
SC_INSUFFICIENT_AVAILABLE_AMOUNT = "INSUFFICIENT_AVAILABLE_AMOUNT"


@dataclass
class DemurrageInfo:
    """Information about the demurrage on a collector account.

    The `rate` field represents the lowest interest rate on the
    collector account for the duration of the trading turn, but not
    exceeding zero.

    The `final_interest_rate_ts` field represents the onset moment of
    the last known interest rate on the collector account. A `None`
    indicates that we know that we are missing some interest rate
    changes, that could be important.
    """
    rate: float
    final_interest_rate_ts: Optional[datetime]


@dataclass
class TransferParams:
    amount: int
    final_interest_rate_ts: Optional[datetime]
    max_commit_delay: int


@atomic
def process_candidate_offer_signal(
        *,
        demurrage_rate: float,
        turn_id: int,
        debtor_id: int,
        creditor_id: int,
        amount: int,
        account_creation_date: date,
        last_transfer_number: int,
):
    current_ts = datetime.now(tz=timezone.utc)

    # TODO: Each candidate offer obtaining a FOR SHARE lock here
    # *might* be a performance problem. When multiple transactions
    # simultaneously hold locks to a single row, PostgreSQL maintains
    # a list of transaction IDs in the "pg_multixact subdirectory",
    # which can grow significantly in size. If this turns out to be a
    # problem in practice, we may be able to get by without obtaining
    # a FOR SHARE lock here. For example, we may simply check the
    # current time instead.
    worker_turn = (
        WorkerTurn.query
        .filter_by(
            turn_id=turn_id,
            phase=2,
            worker_turn_subphase=5,
        )
        .options(WORKER_TURN_LOAD_ONLY_ESSENTIALS)
        .with_for_update(read=True, skip_locked=True)
        .one_or_none()
    )
    if not worker_turn:
        return

    account_lock = (
        AccountLock.query
        .filter_by(
            creditor_id=creditor_id,
            debtor_id=debtor_id,
        )
        .options(ACCOUNT_LOCK_LOAD_ONLY_CANDIDATE_OFFER_DATA)
        .with_for_update()
        .one_or_none()
    )
    if account_lock and account_lock.is_in_force(
            account_creation_date, last_transfer_number
    ):
        return

    active_collectors = db.session.execute(
        select(
            UsableCollector.collector_id,
            UsableCollector.account_id,
        )
        .where(
            UsableCollector.debtor_id == debtor_id,
            UsableCollector.disabled_at == null(),
        )
    ).all()

    try:
        collector = random.choice(active_collectors)
    except IndexError:
        return

    if amount > 0:
        # a buy offer
        min_locked_amount = 0
        max_locked_amount = 0
    else:
        # a sell offer
        assert amount < 0
        assert demurrage_rate > -100.0
        assert worker_turn.min_trade_amount > 0
        worst_possible_demurrage = calc_demurrage(
            demurrage_rate, worker_turn.collection_deadline - current_ts
        )
        min_locked_amount = contain_principal_overflow(
            math.ceil(worker_turn.min_trade_amount / worst_possible_demurrage)
        )
        max_locked_amount = contain_principal_overflow(
            math.ceil((-amount) / worst_possible_demurrage)
        )

    if max_locked_amount < min_locked_amount:
        return  # pragma: no cover

    coordinator_request_id = db.session.scalar(cr_seq)

    if account_lock:
        account_lock.turn_id = turn_id
        account_lock.coordinator_request_id = coordinator_request_id
        account_lock.collector_id = collector.collector_id
        account_lock.initiated_at = current_ts
        account_lock.amount = amount
        account_lock.max_locked_amount = max_locked_amount
        account_lock.transfer_id = None
        account_lock.finalized_at = None
        account_lock.released_at = None
        account_lock.account_creation_date = None
        account_lock.account_last_transfer_number = None
        account_lock.has_been_revised = False
    else:
        with db.retry_on_integrity_error():
            account_lock = AccountLock(
                creditor_id=creditor_id,
                debtor_id=debtor_id,
                turn_id=turn_id,
                coordinator_request_id=coordinator_request_id,
                collector_id=collector.collector_id,
                initiated_at=current_ts,
                max_locked_amount=max_locked_amount,
                amount=amount,
            )
            db.session.add(account_lock)

    if account_lock.is_self_lock:
        # NOTE: In this case, a collector account is both the sender
        # and the recipient. This can happen if we want to trade
        # surpluses accumulated on collector accounts. Obviously, it
        # does not make sense to attempt to prepare such a transfer.
        # Instead, we pretend that the transfer has been successfully
        # prepared.
        account_lock.transfer_id = 0  # a made-up transfer ID
        account_lock.amount = amount  # successfully locked the whole amount
    else:
        db.session.add(
            PrepareTransferSignal(
                creditor_id=creditor_id,
                coordinator_request_id=coordinator_request_id,
                debtor_id=debtor_id,
                recipient=collector.account_id,
                min_locked_amount=min_locked_amount,
                max_locked_amount=max_locked_amount,
                final_interest_rate_ts=T_INFINITY,
                max_commit_delay=MAX_INT32,
                inserted_at=current_ts,
            )
        )


@atomic
def dismiss_prepared_transfer(
        *,
        debtor_id: int,
        creditor_id: int,
        transfer_id: int,
        coordinator_id: int,
        coordinator_request_id: int,
        current_ts: Optional[datetime] = None,
):
    if current_ts is None:
        current_ts = datetime.now(tz=timezone.utc)

    db.session.add(
        FinalizeTransferSignal(
            debtor_id=debtor_id,
            creditor_id=creditor_id,
            transfer_id=transfer_id,
            coordinator_id=coordinator_id,
            coordinator_request_id=coordinator_request_id,
            committed_amount=0,
            transfer_note_format="",
            transfer_note="",
            inserted_at=current_ts,
        )
    )


@atomic
def put_rejected_transfer_through_account_locks(
        *,
        coordinator_id: int,
        coordinator_request_id: int,
        status_code: str,
        debtor_id: int,
        creditor_id: int,
) -> bool:
    """Return `True` if a corresponding account lock has been found.
    """
    lock = (
        AccountLock.query
        .filter_by(
            creditor_id=coordinator_id,
            coordinator_request_id=coordinator_request_id,
        )
        .options(ACCOUNT_LOCK_LOAD_ONLY_ESSENTIALS)
        .one_or_none()
    )
    if lock is None:
        return False

    if (
            lock.released_at is None
            and lock.transfer_id is None
            and lock.debtor_id == debtor_id
            and lock.creditor_id == creditor_id
    ):
        # The current status is "initiated". Delete the account lock.
        db.session.delete(lock)

    return True


@atomic
def put_prepared_transfer_through_account_locks(
        *,
        debtor_id: int,
        creditor_id: int,
        transfer_id: int,
        coordinator_id: int,
        coordinator_request_id: int,
        locked_amount: int,
        demurrage_rate: float,
        deadline: datetime,
        min_demurrage_rate: float,
) -> bool:
    """Return `True` if a corresponding account lock has been found.
    """
    query = (
        db.session.query(AccountLock, WorkerTurn)
        .join(WorkerTurn, WorkerTurn.turn_id == AccountLock.turn_id)
        .filter(
            AccountLock.creditor_id == coordinator_id,
            AccountLock.coordinator_request_id == coordinator_request_id,
        )
        .options(
            ACCOUNT_LOCK_LOAD_ONLY_PREPARED_TRANSFER_DATA,
            WORKER_TURN_LOAD_ONLY_ESSENTIALS,
        )
    )
    try:
        lock, worker_turn = query.one()
    except exc.NoResultFound:
        return False

    if lock.debtor_id == debtor_id and lock.creditor_id == creditor_id:
        if lock.released_at is None and lock.transfer_id is None:
            # The current status is "initiated".
            assert lock.finalized_at is None

            min_deadline = worker_turn.collection_deadline or T_INFINITY

            if deadline < min_deadline or demurrage_rate < min_demurrage_rate:
                # Dismiss the transfer directly when either the
                # deadline or the demurrage rate is not appropriate.
                db.session.delete(lock)

            else:
                # Change the current status to "prepared".
                lock.transfer_id = transfer_id

                if lock.amount <= 0:
                    # When selling, re-calculate the amount so that it
                    # is equal to the locked amount reduced in
                    # accordance with the effective demurrage rate,
                    # but not exceeding the original amount which is
                    # for sale.
                    worst_possible_demurrage = calc_demurrage(
                        demurrage_rate, min_deadline - lock.initiated_at
                    )
                    lock.amount = max(
                        lock.amount,
                        - math.floor(locked_amount * worst_possible_demurrage),
                    )

                return True

        elif lock.released_at is None and lock.finalized_at is None:
            # The current status is "prepared".
            assert lock.transfer_id is not None

            if lock.transfer_id == transfer_id:
                return True

        else:
            # The current status is "settled".
            assert lock.transfer_id is not None
            assert lock.finalized_at

            if lock.transfer_id == transfer_id and lock.amount < 0:
                db.session.add(
                    FinalizeTransferSignal(
                        creditor_id=creditor_id,
                        debtor_id=debtor_id,
                        transfer_id=transfer_id,
                        coordinator_id=coordinator_id,
                        coordinator_request_id=coordinator_request_id,
                        committed_amount=(-lock.amount),
                        transfer_note_format=AGENT_TRANSFER_NOTE_FORMAT,
                        transfer_note=str(
                            TransferNote(
                                lock.turn_id,
                                TransferNote.Kind.COLLECTING,
                                lock.collector_id,
                                creditor_id,
                            )
                        ),
                    )
                )
                return True

    dismiss_prepared_transfer(
        debtor_id=debtor_id,
        creditor_id=creditor_id,
        transfer_id=transfer_id,
        coordinator_id=coordinator_id,
        coordinator_request_id=coordinator_request_id,
    )
    return True


@atomic
def process_revise_account_lock_signal(
        *,
        creditor_id: int,
        debtor_id: int,
        turn_id: int,
):
    current_ts = datetime.now(tz=timezone.utc)

    participation = (
        CreditorParticipation.query
        .filter_by(
            creditor_id=creditor_id,
            debtor_id=debtor_id,
            turn_id=turn_id,
        )
        .with_for_update()
        .one_or_none()
    )
    lock = (
        AccountLock.query
        .filter_by(
            creditor_id=creditor_id,
            debtor_id=debtor_id,
            turn_id=turn_id,
            finalized_at=null(),
            has_been_revised=False,
        )
        .options(ACCOUNT_LOCK_LOAD_ONLY_ESSENTIALS)
        .with_for_update()
        .one_or_none()
    )
    if lock:
        def dismiss():
            if not (lock.transfer_id is None or lock.is_self_lock):
                dismiss_prepared_transfer(
                    debtor_id=debtor_id,
                    creditor_id=creditor_id,
                    transfer_id=lock.transfer_id,
                    coordinator_id=creditor_id,
                    coordinator_request_id=lock.coordinator_request_id,
                    current_ts=current_ts,
                )

        def commit(committed_amount: int):
            assert participation
            assert lock.transfer_id is not None
            assert lock.collector_id != creditor_id
            assert lock.collector_id == participation.collector_id
            db.session.add(
                FinalizeTransferSignal(
                    creditor_id=creditor_id,
                    debtor_id=debtor_id,
                    transfer_id=lock.transfer_id,
                    coordinator_id=creditor_id,
                    coordinator_request_id=lock.coordinator_request_id,
                    committed_amount=committed_amount,
                    transfer_note_format=AGENT_TRANSFER_NOTE_FORMAT,
                    transfer_note=str(
                        TransferNote(
                            turn_id,
                            TransferNote.Kind.COLLECTING,
                            lock.collector_id,
                            creditor_id,
                        )
                    ),
                    inserted_at=current_ts,
                )
            )

        if participation:
            amount = participation.amount
            assert lock.transfer_id is not None
            assert (
                0 < abs(amount) <= abs(lock.amount) < abs(amount + lock.amount)
            )

            if creditor_id == participation.collector_id:
                # The sender and the recipient is the same collector
                # account. In this case, transferring the amount is
                # neither possible nor needed. Instead, we simply
                # release the account lock.
                dismiss()
                acd, altn = _register_collector_trade(
                    debtor_id,
                    creditor_id,
                    amount,
                )
                lock.released_at = lock.finalized_at = current_ts
                lock.account_creation_date = acd
                lock.account_last_transfer_number = altn

            elif amount < 0:
                # The account is the sender. The amount must be taken
                # from the account, and transferred to the collector.
                commit(-amount)
                lock.finalized_at = current_ts

            else:
                # The account is the receiver. We expect the amount to
                # be transferred to the account at a later stage. In
                # order to guarantee that the account will not be
                # deleted in the meantime, we should not dismiss the
                # prepared transfer yet.
                assert amount > 1

            lock.amount = amount
            lock.has_been_revised = True  # ensure idempotency

        else:
            dismiss()
            db.session.delete(lock)

    if participation:
        db.session.delete(participation)


@atomic
def release_seller_account_lock(
        creditor_id: int,
        debtor_id: int,
        turn_id: int,
        acquired_amount: int,
        account_creation_date: date,
        account_transfer_number: int,
        is_collector_trade: bool,
) -> None:
    assert acquired_amount < 0
    current_ts = datetime.now(tz=timezone.utc)

    lock = (
        AccountLock.query
        .filter_by(
            creditor_id=creditor_id,
            debtor_id=debtor_id,
            turn_id=turn_id,
        )
        .filter(
            AccountLock.amount == acquired_amount,
            AccountLock.finalized_at != null(),
            AccountLock.released_at == null(),
        )
        .options(ACCOUNT_LOCK_LOAD_ONLY_PK)
        .with_for_update()
        .one_or_none()
    )
    if lock:
        if is_collector_trade:
            account_creation_date, account_transfer_number = (
                _register_collector_trade(
                    debtor_id,
                    creditor_id,
                    acquired_amount,
                )
            )
        lock.released_at = current_ts
        lock.account_creation_date = account_creation_date
        lock.account_last_transfer_number = account_transfer_number


@atomic
def update_worker_collecting_record(
        *,
        collector_id: int,
        turn_id: int,
        debtor_id: int,
        creditor_id: int,
        acquired_amount: int,
        transfer_number: int,
        creation_date: date,
        coordinator_type: str,
        sender: str,
        recipient: str,
        transfer_note_format: str,
        transfer_note: str,
        committed_at: datetime,
        principal: int,
        previous_transfer_number: int,
        ts: datetime,
) -> None:
    assert acquired_amount > 0

    wt = (
        WorkerTurn.query
        .filter_by(turn_id=turn_id)
        .options(WORKER_TURN_LOAD_ONLY_ESSENTIALS)
        .one_or_none()
    )
    if wt and (wt.phase > 3 or wt.phase == 3 and wt.worker_turn_subphase >= 5):
        db.session.execute(
            update(WorkerCollecting)
            .execution_options(synchronize_session=False)
            .where(
                WorkerCollecting.collector_id == collector_id,
                WorkerCollecting.debtor_id == debtor_id,
                WorkerCollecting.turn_id == turn_id,
                WorkerCollecting.creditor_id == creditor_id,
                WorkerCollecting.amount == acquired_amount,
                WorkerCollecting.collected == false(),
            )
            .values(collected=True)
        )
    else:
        db.session.add(
            DelayedAccountTransfer(
                turn_id=turn_id,
                debtor_id=debtor_id,
                creditor_id=collector_id,
                transfer_number=transfer_number,
                creation_date=creation_date,
                coordinator_type=coordinator_type,
                sender=sender,
                recipient=recipient,
                acquired_amount=acquired_amount,
                transfer_note_format=transfer_note_format,
                transfer_note=transfer_note,
                committed_at=committed_at,
                principal=principal,
                previous_transfer_number=previous_transfer_number,
                ts=ts,
            )
        )


@atomic
def delete_worker_sending_record(
        from_collector_id: int,
        turn_id: int,
        debtor_id: int,
        to_collector_id: int,
) -> None:
    db.session.execute(
        delete(WorkerSending)
        .execution_options(synchronize_session=False)
        .where(
            WorkerSending.from_collector_id == from_collector_id,
            WorkerSending.debtor_id == debtor_id,
            WorkerSending.turn_id == turn_id,
            WorkerSending.to_collector_id == to_collector_id,
        )
    )


@atomic
def update_worker_receiving_record(
        *,
        to_collector_id: int,
        turn_id: int,
        debtor_id: int,
        from_collector_id: int,
        acquired_amount: int,
        creditor_id: int,
        transfer_number: int,
        creation_date: date,
        coordinator_type: str,
        sender: str,
        recipient: str,
        transfer_note_format: str,
        transfer_note: str,
        committed_at: datetime,
        principal: int,
        previous_transfer_number: int,
        ts: datetime,
) -> None:
    assert acquired_amount > 0

    wt = (
        WorkerTurn.query
        .filter_by(turn_id=turn_id)
        .options(WORKER_TURN_LOAD_ONLY_ESSENTIALS)
        .one_or_none()
    )
    if wt and (wt.phase > 3 or wt.phase == 3 and wt.worker_turn_subphase >= 5):
        db.session.execute(
            update(WorkerReceiving)
            .execution_options(synchronize_session=False)
            .where(
                WorkerReceiving.to_collector_id == to_collector_id,
                WorkerReceiving.debtor_id == debtor_id,
                WorkerReceiving.turn_id == turn_id,
                WorkerReceiving.from_collector_id == from_collector_id,
                WorkerReceiving.received_amount == 0,
            )
            .values(received_amount=acquired_amount)
        )
    else:
        db.session.add(
            DelayedAccountTransfer(
                turn_id=turn_id,
                debtor_id=debtor_id,
                creditor_id=creditor_id,
                transfer_number=transfer_number,
                creation_date=creation_date,
                coordinator_type=coordinator_type,
                sender=sender,
                recipient=recipient,
                acquired_amount=acquired_amount,
                transfer_note_format=transfer_note_format,
                transfer_note=transfer_note,
                committed_at=committed_at,
                principal=principal,
                previous_transfer_number=previous_transfer_number,
                ts=ts,
            )
        )


@atomic
def delete_worker_dispatching_record(
        collector_id: int,
        turn_id: int,
        debtor_id: int,
        creditor_id: int,
) -> None:
    db.session.execute(
        delete(WorkerDispatching)
        .execution_options(synchronize_session=False)
        .where(
            WorkerDispatching.collector_id == collector_id,
            WorkerDispatching.debtor_id == debtor_id,
            WorkerDispatching.turn_id == turn_id,
            WorkerDispatching.creditor_id == creditor_id,
        )
    )


@atomic
def release_buyer_account_lock(
        creditor_id: int,
        debtor_id: int,
        turn_id: int,
        acquired_amount: int,
        account_creation_date: date,
        account_transfer_number: int,
        is_collector_trade: bool,
) -> None:
    assert acquired_amount > 0
    current_ts = datetime.now(tz=timezone.utc)

    lock = (
        AccountLock.query
        .filter_by(
            creditor_id=creditor_id,
            debtor_id=debtor_id,
            turn_id=turn_id,
        )
        .filter(
            AccountLock.amount > 0,
            AccountLock.transfer_id != null(),
            AccountLock.finalized_at == null(),
            AccountLock.released_at == null(),
        )
        .options(ACCOUNT_LOCK_LOAD_ONLY_ESSENTIALS)
        .with_for_update()
        .one_or_none()
    )
    if lock:
        if is_collector_trade:
            account_creation_date, account_transfer_number = (
                _register_collector_trade(
                    debtor_id,
                    creditor_id,
                    acquired_amount,
                )
            )
        lock.released_at = lock.finalized_at = current_ts
        lock.account_creation_date = account_creation_date
        lock.account_last_transfer_number = account_transfer_number

        if not lock.is_self_lock:
            dismiss_prepared_transfer(
                debtor_id=debtor_id,
                creditor_id=creditor_id,
                transfer_id=lock.transfer_id,
                coordinator_id=creditor_id,
                coordinator_request_id=lock.coordinator_request_id,
                current_ts=current_ts,
            )


def _register_collector_trade(
        debtor_id: int,
        collector_id: int,
        amount: int,
) -> Tuple[date, int]:
    # This function will be called when a collector account
    # participates in a trading turn. The `amount` will be negative
    # when the collector account is the seller, and will be positive
    # when the collector account is the buyer.

    worker_account = (
        WorkerAccount.query
        .filter_by(debtor_id=debtor_id, creditor_id=collector_id)
        .options(WORKER_ACCOUNT_LOAD_ONLY_ESSENTIALS)
        .with_for_update()
        .one_or_none()
    )
    if worker_account:
        worker_account.surplus_last_transfer_number += 1

        # When selling -- increase the spent surplus amount. When
        # buying -- do nothing, because the buying offers coming from
        # worker accounts are always unlimited.
        if amount < 0:
            worker_account.surplus_spent_amount -= amount

        return (
            worker_account.creation_date,
            worker_account.surplus_last_transfer_number,
        )
    else:
        logger = logging.getLogger(__name__)
        logger.warning(
            "A collector trade has been registered, but the corresponding"
            " WorkerAccount(creditor_id=%d, debtor_id=%d) does not exist.",
            collector_id,
            debtor_id,
        )
        return DATE0, 0


@atomic
def process_account_id_request_signal(
        collector_id: int,
        turn_id: int,
        debtor_id: int,
        creditor_id: int,
        transfer_kind: int,
) -> bool:
    row = (
        (
            found := db.session.execute(
                select(
                    TradingPolicy.account_id,
                    TradingPolicy.latest_ledger_update_id,
                )
                .where(
                    TradingPolicy.creditor_id == creditor_id,
                    TradingPolicy.debtor_id == debtor_id,
                )
            )
            .one_or_none()
        )
        or (
            found := db.session.execute(
                select(
                    WorkerAccount.account_id,
                    WorkerAccount.creation_date - DATE0,
                )
                .where(
                    WorkerAccount.creditor_id == creditor_id,
                    WorkerAccount.debtor_id == debtor_id,
                    WorkerAccount.account_id != "",
                )
            )
            .one_or_none()
        )
        or ("", MIN_INT64 + 1)
    )

    db.session.add(
        AccountIdResponseSignal(
            collector_id=collector_id,
            turn_id=turn_id,
            debtor_id=debtor_id,
            creditor_id=creditor_id,
            transfer_kind=transfer_kind,
            account_id=row[0],
            account_id_version=row[1],
        )
    )
    return bool(found)


@atomic
def process_account_id_response_signal(
        collector_id: int,
        turn_id: int,
        debtor_id: int,
        creditor_id: int,
        transfer_kind: int,
        account_id: str,
        account_id_version: int,
        transfers_healthy_max_commit_delay: timedelta,
        transfers_amount_cut: float,
) -> None:
    attempt = (
        TransferAttempt.query.
        filter_by(
            collector_id=collector_id,
            turn_id=turn_id,
            debtor_id=debtor_id,
            creditor_id=creditor_id,
            transfer_kind=transfer_kind,
        )
        .with_for_update()
        .one_or_none()
    )
    if (
            attempt
            and attempt.unknown_recipient
            and attempt.recipient_version < account_id_version
    ):
        # When the account ID can not be obtained, we can safely
        # forget about surplus-moving transfers that have no chance to
        # succeed. This can happen when, for example, the owner's
        # account has not been configured correctly.
        if (
                account_id == ""
                and transfer_kind == TransferAttempt.KIND_MOVING
                and not (attempt.finalized_at and attempt.failure_code is None)
        ):  # pragma: no cover
            db.session.delete(attempt)
            return

        attempt.recipient_version = account_id_version

        if attempt.recipient != account_id:
            attempt.recipient = account_id

            if attempt.failure_code is not None:
                # When the recipient changes, the failure code becomes
                # misleading.
                attempt.failure_code = attempt.UNSPECIFIED_FAILURE

            _trigger_transfer_if_possible(
                attempt,
                transfers_healthy_max_commit_delay,
                transfers_amount_cut,
            )


@atomic
def process_trigger_transfer_signal(
        collector_id: int,
        turn_id: int,
        debtor_id: int,
        creditor_id: int,
        transfer_kind: int,
        transfers_healthy_max_commit_delay: timedelta,
        transfers_amount_cut: float,
) -> None:
    attempt = (
        TransferAttempt.query.
        filter_by(
            collector_id=collector_id,
            turn_id=turn_id,
            debtor_id=debtor_id,
            creditor_id=creditor_id,
            transfer_kind=transfer_kind,
        )
        .with_for_update()
        .one_or_none()
    )
    if attempt:
        _trigger_transfer_if_possible(
            attempt,
            transfers_healthy_max_commit_delay,
            transfers_amount_cut,
        )


def _trigger_transfer_if_possible(
        attempt: TransferAttempt,
        transfers_healthy_max_commit_delay: timedelta,
        transfers_amount_cut: float,
) -> None:
    if not attempt.can_be_triggered:
        return

    assert attempt.rescheduled_for is None
    assert attempt.fatal_error is None
    assert attempt.recipient != ""

    current_ts = datetime.now(tz=timezone.utc)
    old_failure_code = attempt.failure_code
    old_interest_rate_ts = attempt.final_interest_rate_ts

    params = _calc_transfer_params(
        attempt,
        transfers_healthy_max_commit_delay,
        transfers_amount_cut,
        current_ts,
    )
    new_coordinator_request_id = db.session.scalar(cr_seq)
    new_interest_rate_ts = params.final_interest_rate_ts
    new_amount = params.amount

    if new_amount == 0:
        # Attempting to make a transfer will never make sense.
        # Rescheduling more attempts is futile. Normally, this should
        # not happen.
        return  # pragma: no cover

    if (
        new_interest_rate_ts is None
        or (
            old_failure_code == attempt.NEWER_INTEREST_RATE
            and old_interest_rate_ts
            and old_interest_rate_ts >= new_interest_rate_ts
        )
    ):
        # Attempting to make a transfer right now does not make sense.
        # Instead, we reschedule the attempt for a later time.
        attempt.rescheduled_for = (
            current_ts + transfers_healthy_max_commit_delay
        )
        return

    attempt.attempted_at = current_ts
    attempt.coordinator_request_id = new_coordinator_request_id
    attempt.final_interest_rate_ts = new_interest_rate_ts
    attempt.amount = new_amount
    attempt.transfer_id = None
    attempt.finalized_at = None

    if old_failure_code == attempt.RECIPIENT_IS_UNREACHABLE:
        # In this case, we know beforehand that the transfer
        # will fail with a "RECIPIENT_IS_UNREACHABLE" error.
        # Therefore, we make a request to obtain the new
        # account ID of the creditor, and directly reschedule
        # the transfer for a later time.
        db.session.add(
            AccountIdRequestSignal(
                collector_id=attempt.collector_id,
                turn_id=attempt.turn_id,
                debtor_id=attempt.debtor_id,
                creditor_id=attempt.creditor_id,
                transfer_kind=attempt.transfer_kind,
            )
        )
        attempt.reschedule_failed_attempt(
            old_failure_code,
            transfers_healthy_max_commit_delay.total_seconds(),
        )
        return

    db.session.add(
        PrepareTransferSignal(
            creditor_id=attempt.collector_id,
            coordinator_request_id=new_coordinator_request_id,
            debtor_id=attempt.debtor_id,
            recipient=attempt.recipient,
            min_locked_amount=0,
            max_locked_amount=0,
            final_interest_rate_ts=new_interest_rate_ts,
            max_commit_delay=params.max_commit_delay,
            inserted_at=current_ts,
        )
    )
    attempt.failure_code = None


def _calc_transfer_params(
        attempt: TransferAttempt,
        transfers_healthy_max_commit_delay: timedelta,
        transfers_amount_cut: float,
        current_ts: datetime,
) -> TransferParams:
    max_commit_delay = attempt.calc_backoff_seconds(
        transfers_healthy_max_commit_delay.total_seconds()
    )
    if attempt.transfer_kind != TransferAttempt.KIND_DISPATCHING:
        # We seize the "cut" portion of the transfer only when
        # dispatching to the buyers' accounts. For all other transfer
        # kinds, we reduce the amount imperceptibly, just to be on the
        # safe side of the rounding errors.
        transfers_amount_cut = 1e-10  # pragma: no cover

    assert 0 <= max_commit_delay <= MAX_INT32
    past_demmurage_period = current_ts - attempt.collection_started_at
    future_demmurage_period = timedelta(seconds=max_commit_delay)
    demurrage_period = (
        TD_CLOCK_PRECISION_SAFETY_MARGIN
        + past_demmurage_period
        + future_demmurage_period
    )
    demurrage_info = _get_demurrage_info(attempt)
    demurrage = calc_demurrage(demurrage_info.rate, demurrage_period)
    nominal_amount = attempt.nominal_amount

    assert 1.0 <= nominal_amount
    assert 0.0 <= demurrage <= 1.0
    assert 1e-10 <= transfers_amount_cut <= 0.1
    try:
        amount = contain_principal_overflow(
            math.floor(
                nominal_amount
                * demurrage
                * (1.0 - transfers_amount_cut)
            )
        )
    except OverflowError:  # pragma: no cover
        # This can happen if `nominal_amount` is +Infinity
        amount = MAX_INT64

    assert 0 <= amount <= MAX_INT64
    assert amount <= nominal_amount

    return TransferParams(
        amount,
        demurrage_info.final_interest_rate_ts,
        max_commit_delay,
    )


def _get_demurrage_info(attempt: TransferAttempt) -> DemurrageInfo:
    collector_id = attempt.collector_id
    debtor_id = attempt.debtor_id
    collection_started_at = attempt.collection_started_at

    try:
        collector_info = (
            db.session.execute(
                select(
                    WorkerAccount.interest_rate,
                    WorkerAccount.last_interest_rate_change_ts,
                )
                .where(
                    WorkerAccount.creditor_id == collector_id,
                    WorkerAccount.debtor_id == debtor_id,
                )
            )
            .one()
        )
    except exc.NoResultFound:
        # Normally, this should never happen. But if it did happen,
        # returning an incorrect demurrage rate here (0.0) is the
        # least of our problems.
        return DemurrageInfo(0.0, None)

    interest_rate_changes = (
        db.session.execute(
            select(
                InterestRateChange.interest_rate,
                InterestRateChange.change_ts,
            )
            .where(
                InterestRateChange.creditor_id == collector_id,
                InterestRateChange.debtor_id == debtor_id,
            )
            .order_by(InterestRateChange.change_ts.desc())
        )
        .all()
    )

    min_interest_rate = collector_info.interest_rate
    last_change_ts = collector_info.last_interest_rate_change_ts

    for interest_rate, change_ts in interest_rate_changes:
        if interest_rate < min_interest_rate:
            min_interest_rate = interest_rate

        if change_ts > last_change_ts:
            last_change_ts = change_ts  # pragma: no cover

        if change_ts < collection_started_at:
            break

    return DemurrageInfo(
        min(min_interest_rate, 0.0),
        last_change_ts + TD_TOLERABLE_ROUNDING_ERROR,
    )


@atomic
def put_rejected_transfer_through_transfer_attempts(
        *,
        coordinator_id: int,
        coordinator_request_id: int,
        status_code: str,
        debtor_id: int,
        creditor_id: int,
        transfers_healthy_max_commit_delay: timedelta,
) -> Optional[str]:
    """Return `True` if a corresponding transfer attempt has been found.
    """
    critical_error = ""
    attempt = (
        TransferAttempt.query.
        filter_by(
            collector_id=coordinator_id,
            coordinator_request_id=coordinator_request_id,
        )
        .filter(TransferAttempt.coordinator_request_id != null())
        .options(TRANSFER_ATTEMPT_LOAD_ONLY_ESSENTIALS)
        .with_for_update()
        .one_or_none()
    )
    if attempt is None:
        return None

    if attempt.transfer_id is None and attempt.failure_code is None:
        assert attempt.attempted_at
        assert attempt.rescheduled_for is None

        critical_error = _reschedule_failed_attempt(
            attempt, status_code, transfers_healthy_max_commit_delay
        )
        assert attempt.failure_code is not None

    return critical_error


@atomic
def put_prepared_transfer_through_transfer_attempts(
        *,
        debtor_id: int,
        creditor_id: int,
        transfer_id: int,
        coordinator_id: int,
        coordinator_request_id: int,
) -> bool:
    """Return `True` if a corresponding transfer attempt has been found.
    """
    current_ts = datetime.now(tz=timezone.utc)

    attempt = (
        TransferAttempt.query.
        filter_by(
            collector_id=coordinator_id,
            coordinator_request_id=coordinator_request_id,
        )
        .filter(TransferAttempt.coordinator_request_id != null())
        .options(TRANSFER_ATTEMPT_LOAD_ONLY_ESSENTIALS)
        .with_for_update()
        .one_or_none()
    )
    if attempt is None:
        return False

    if (
            attempt.debtor_id == debtor_id
            and attempt.collector_id == creditor_id
            and attempt.failure_code is None
    ):
        assert attempt.attempted_at
        assert attempt.amount > 0

        if attempt.transfer_id is None:
            # The current status is "initiated". Change it to "settled".
            assert attempt.finalized_at is None
            attempt.transfer_id = transfer_id
            attempt.finalized_at = current_ts

        # The current status is "settled".
        assert attempt.finalized_at
        if attempt.transfer_id == transfer_id:
            db.session.add(
                FinalizeTransferSignal(
                    creditor_id=creditor_id,
                    debtor_id=debtor_id,
                    transfer_id=transfer_id,
                    coordinator_id=coordinator_id,
                    coordinator_request_id=coordinator_request_id,
                    committed_amount=attempt.amount,
                    transfer_note_format=AGENT_TRANSFER_NOTE_FORMAT,
                    transfer_note=str(
                        TransferNote(
                            attempt.turn_id,
                            attempt.transfer_note_kind,
                            attempt.collector_id,
                            attempt.creditor_id,
                        )
                    ),
                    inserted_at=current_ts,
                )
            )
            return True

    dismiss_prepared_transfer(
        debtor_id=debtor_id,
        creditor_id=creditor_id,
        transfer_id=transfer_id,
        coordinator_id=coordinator_id,
        coordinator_request_id=coordinator_request_id,
    )
    return True


@atomic
def put_finalized_transfer_through_transfer_attempts(
        *,
        debtor_id: int,
        creditor_id: int,
        transfer_id: int,
        coordinator_id: int,
        coordinator_request_id: int,
        committed_amount: int,
        status_code: str,
        transfers_healthy_max_commit_delay: timedelta,
) -> str:
    critical_error = ""
    attempt = (
        TransferAttempt.query.
        filter_by(
            collector_id=coordinator_id,
            coordinator_request_id=coordinator_request_id,
            debtor_id=debtor_id,
            transfer_id=transfer_id,
            failure_code=null(),
        )
        .filter(
            TransferAttempt.finalized_at != null(),
            TransferAttempt.coordinator_request_id != null(),
        )
        .options(TRANSFER_ATTEMPT_LOAD_ONLY_ESSENTIALS)
        .with_for_update()
        .one_or_none()
    )
    if attempt and attempt.collector_id == creditor_id:
        assert attempt.attempted_at
        assert attempt.rescheduled_for is None

        if status_code == SC_OK and committed_amount == attempt.amount:
            db.session.delete(attempt)

        else:
            if committed_amount != 0:
                status_code = "UNEXPECTED_COMMITTED_AMOUNT"  # pragma: no cover

            critical_error = _reschedule_failed_attempt(
                attempt, status_code, transfers_healthy_max_commit_delay
            )
            assert attempt.failure_code is not None

    return critical_error


def _reschedule_failed_attempt(
        attempt: TransferAttempt,
        status_code: str,
        transfers_healthy_max_commit_delay: timedelta,
) -> str:
    assert attempt.attempted_at
    assert attempt.rescheduled_for is None

    critical_error = ""
    current_ts = datetime.now(tz=timezone.utc)
    min_backoff_seconds = transfers_healthy_max_commit_delay.total_seconds()

    if status_code == SC_NEWER_INTEREST_RATE:
        attempt.failure_code = attempt.NEWER_INTEREST_RATE
        attempt.rescheduled_for = (
            current_ts + transfers_healthy_max_commit_delay
        )

    elif status_code == SC_TIMEOUT:
        attempt.reschedule_failed_attempt(
            attempt.TIMEOUT, min_backoff_seconds
        )

    elif status_code == SC_RECIPIENT_IS_UNREACHABLE:
        db.session.add(
            AccountIdRequestSignal(
                collector_id=attempt.collector_id,
                turn_id=attempt.turn_id,
                debtor_id=attempt.debtor_id,
                creditor_id=attempt.creditor_id,
                transfer_kind=attempt.transfer_kind,
            )
        )
        attempt.reschedule_failed_attempt(
            attempt.RECIPIENT_IS_UNREACHABLE, min_backoff_seconds
        )

    elif status_code == SC_INSUFFICIENT_AVAILABLE_AMOUNT:
        attempt.reschedule_failed_attempt(
            attempt.INSUFFICIENT_AVAILABLE_AMOUNT, min_backoff_seconds
        )
        critical_error = status_code

    else:
        attempt.failure_code = attempt.UNSPECIFIED_FAILURE
        attempt.fatal_error = status_code
        critical_error = status_code

    return critical_error


@atomic
def process_rescheduled_transfers_batch(
        transfer_attempt_pks: list,
        current_ts: datetime,
) -> int:
    to_trigger = db.session.execute(
        select(
            TransferAttempt.collector_id,
            TransferAttempt.turn_id,
            TransferAttempt.debtor_id,
            TransferAttempt.creditor_id,
            TransferAttempt.transfer_kind,
        )
        .select_from(TransferAttempt)
        .where(
            TRANSFER_ATTEMPT_PK.in_(transfer_attempt_pks),

            # The reason we use this complex expression here (instead
            # of the simple `TransferAttempt.rescheduled_for <=
            # current_ts`), is to ensure that the planner will not
            # decide to use the index on the `rescheduled_for` column
            # for the prepared generic plan.
            func.least(TransferAttempt.rescheduled_for, T_INFINITY)
            <= current_ts,
        )
        .with_for_update(skip_locked=True)
    ).all()

    if to_trigger:
        db.session.bulk_insert_mappings(
            TriggerTransferSignal,
            [
                dict(
                    collector_id=attempt.collector_id,
                    turn_id=attempt.turn_id,
                    debtor_id=attempt.debtor_id,
                    creditor_id=attempt.creditor_id,
                    transfer_kind=attempt.transfer_kind,
                    inserted_at=current_ts,
                )
                for attempt in to_trigger
            ]
        )
        db.session.execute(
            update(TransferAttempt)
            .execution_options(synchronize_session=False)
            .where(TRANSFER_ATTEMPT_PK.in_(to_trigger))
            .values(rescheduled_for=None)
        )

    return len(to_trigger)


@atomic
def process_start_sending_signal(
        *,
        collector_id: int,
        turn_id: int,
        debtor_id: int,
) -> None:
    current_ts = datetime.now(tz=timezone.utc)
    query = (
        db.session.query(DispatchingStatus, WorkerTurn)
        .outerjoin(WorkerTurn, WorkerTurn.turn_id == DispatchingStatus.turn_id)
        .filter(
            DispatchingStatus.collector_id == collector_id,
            DispatchingStatus.debtor_id == debtor_id,
            DispatchingStatus.turn_id == turn_id,
            DispatchingStatus.started_sending == false(),
            DispatchingStatus.awaiting_signal_flag == true(),
        )
        .options(
            DISPATCHING_STATUS_LOAD_ONLY_ESSENTIALS,
            WORKER_TURN_LOAD_ONLY_ESSENTIALS,
        )
        .with_for_update(of=DispatchingStatus)
    )
    try:
        dispatching_status, worker_turn = query.one()
    except exc.NoResultFound:
        return

    assert dispatching_status
    assert worker_turn
    assert worker_turn.collection_started_at

    worker_collectings_predicate = and_(
        WorkerCollecting.collector_id == collector_id,
        WorkerCollecting.debtor_id == debtor_id,
        WorkerCollecting.turn_id == turn_id,
    )
    dispatching_status.total_collected_amount = contain_principal_overflow(
        int(
            db.session.execute(
                select(
                    func.coalesce(
                        func.sum(WorkerCollecting.amount),
                        text("0::numeric"),
                    )
                )
                .select_from(WorkerCollecting)
                .where(worker_collectings_predicate)
            )
            .scalar_one()
        )
    )
    dispatching_status.started_sending = True
    dispatching_status.awaiting_signal_flag = False

    db.session.execute(
        delete(WorkerCollecting)
        .execution_options(synchronize_session=False)
        .where(worker_collectings_predicate)
    )

    if dispatching_status.amount_to_send > 0:
        nominal_amount_expression = (
            cast(WorkerSending.amount, db.FLOAT)
            * (
                dispatching_status.available_amount_to_send
                / dispatching_status.amount_to_send
            )
        )
        worker_sending_predicate = and_(
            WorkerSending.from_collector_id == collector_id,
            WorkerSending.debtor_id == debtor_id,
            WorkerSending.turn_id == turn_id,
            nominal_amount_expression >= 1.0,
        )

        db.session.execute(
            insert(TransferAttempt)
            .execution_options(synchronize_session=False)
            .from_select(
                [
                    "collector_id",
                    "debtor_id",
                    "turn_id",
                    "creditor_id",
                    "transfer_kind",
                    "nominal_amount",
                    "collection_started_at",
                    "inserted_at",
                    "recipient",
                    "recipient_version",
                    "backoff_counter",
                ],
                select(
                    WorkerSending.from_collector_id,
                    WorkerSending.debtor_id,
                    WorkerSending.turn_id,
                    WorkerSending.to_collector_id,
                    TransferAttempt.KIND_SENDING,
                    nominal_amount_expression,
                    literal(worker_turn.collection_started_at),
                    literal(current_ts),
                    literal(""),
                    literal(MIN_INT64, db.BigInteger),
                    literal(0, db.SmallInteger),
                )
                .where(worker_sending_predicate)
            )
        )
        db.session.execute(
            insert(AccountIdRequestSignal)
            .execution_options(synchronize_session=False)
            .from_select(
                [
                    "collector_id",
                    "debtor_id",
                    "turn_id",
                    "creditor_id",
                    "transfer_kind",
                ],
                select(
                    WorkerSending.from_collector_id,
                    WorkerSending.debtor_id,
                    WorkerSending.turn_id,
                    WorkerSending.to_collector_id,
                    TransferAttempt.KIND_SENDING,
                )
                .where(worker_sending_predicate)
            )
        )


@atomic
def process_start_dispatching_signal(
        *,
        collector_id: int,
        turn_id: int,
        debtor_id: int,
) -> None:
    current_ts = datetime.now(tz=timezone.utc)
    query = (
        db.session.query(DispatchingStatus, WorkerTurn)
        .outerjoin(WorkerTurn, WorkerTurn.turn_id == DispatchingStatus.turn_id)
        .filter(
            DispatchingStatus.collector_id == collector_id,
            DispatchingStatus.debtor_id == debtor_id,
            DispatchingStatus.turn_id == turn_id,
            DispatchingStatus.all_sent == true(),
            DispatchingStatus.started_dispatching == false(),
            DispatchingStatus.awaiting_signal_flag == true(),
        )
        .options(
            DISPATCHING_STATUS_LOAD_ONLY_ESSENTIALS,
            WORKER_TURN_LOAD_ONLY_ESSENTIALS,
        )
        .with_for_update(of=DispatchingStatus)
    )
    try:
        dispatching_status, worker_turn = query.one()
    except exc.NoResultFound:
        return

    assert dispatching_status
    assert worker_turn
    assert worker_turn.collection_started_at

    worker_receivings_predicate = and_(
        WorkerReceiving.to_collector_id == collector_id,
        WorkerReceiving.debtor_id == debtor_id,
        WorkerReceiving.turn_id == turn_id,
    )
    dispatching_status.total_received_amount = contain_principal_overflow(
        int(
            db.session.execute(
                select(
                    func.coalesce(
                        func.sum(WorkerReceiving.received_amount),
                        text("0::numeric"),
                    )
                )
                .select_from(WorkerReceiving)
                .where(worker_receivings_predicate)
            )
            .scalar_one()
        )
    )
    dispatching_status.started_dispatching = True
    dispatching_status.awaiting_signal_flag = False

    db.session.execute(
        delete(WorkerReceiving)
        .execution_options(synchronize_session=False)
        .where(worker_receivings_predicate)
    )

    if dispatching_status.amount_to_dispatch > 0:
        nominal_amount_expression = (
            cast(WorkerDispatching.amount, db.FLOAT)
            * (
                dispatching_status.available_amount_to_dispatch
                / dispatching_status.amount_to_dispatch
            )
        )
        worker_dispatching_predicate = and_(
            WorkerDispatching.collector_id == collector_id,
            WorkerDispatching.debtor_id == debtor_id,
            WorkerDispatching.turn_id == turn_id,
            nominal_amount_expression >= 1.0,
        )
        db.session.execute(
            insert(TransferAttempt)
            .execution_options(synchronize_session=False)
            .from_select(
                [
                    "collector_id",
                    "debtor_id",
                    "turn_id",
                    "creditor_id",
                    "transfer_kind",
                    "nominal_amount",
                    "collection_started_at",
                    "inserted_at",
                    "recipient",
                    "recipient_version",
                    "backoff_counter",
                ],
                select(
                    WorkerDispatching.collector_id,
                    WorkerDispatching.debtor_id,
                    WorkerDispatching.turn_id,
                    WorkerDispatching.creditor_id,
                    TransferAttempt.KIND_DISPATCHING,
                    nominal_amount_expression,
                    literal(worker_turn.collection_started_at),
                    literal(current_ts),
                    literal(""),
                    literal(MIN_INT64, db.BigInteger),
                    literal(0, db.SmallInteger),
                )
                .where(worker_dispatching_predicate)
            )
        )
        db.session.execute(
            insert(AccountIdRequestSignal)
            .execution_options(synchronize_session=False)
            .from_select(
                [
                    "collector_id",
                    "debtor_id",
                    "turn_id",
                    "creditor_id",
                    "transfer_kind",
                ],
                select(
                    WorkerDispatching.collector_id,
                    WorkerDispatching.debtor_id,
                    WorkerDispatching.turn_id,
                    WorkerDispatching.creditor_id,
                    TransferAttempt.KIND_DISPATCHING,
                )
                .where(worker_dispatching_predicate)
            )
        )
