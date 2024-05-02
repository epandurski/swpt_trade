import math
from typing import TypeVar, Callable, Optional
from datetime import date, datetime, timezone, timedelta
from swpt_pythonlib.utils import Seqnum
from sqlalchemy.orm import load_only
from swpt_trade.extensions import db
from swpt_trade.models import (
    NeededWorkerAccount,
    InterestRateChange,
    WorkerAccount,
    ConfigureAccountSignal,
    ActivateCollectorSignal,
    DiscoverDebtorSignal,
    HUGE_NEGLIGIBLE_AMOUNT,
    DEFAULT_CONFIG_FLAGS,
)

T = TypeVar("T")
atomic: Callable[[T], T] = db.atomic

EPS = 1e-5


@atomic
def process_account_update_signal(
        *,
        debtor_id: int,
        creditor_id: int,
        creation_date: date,
        last_change_ts: datetime,
        last_change_seqnum: int,
        principal: int,
        interest: float,
        interest_rate: float,
        demurrage_rate: float,
        commit_period: int,
        last_interest_rate_change_ts: datetime,
        transfer_note_max_bytes: int,
        negligible_amount: float,
        config_flags: int,
        account_id: str,
        debtor_info_iri: Optional[str],
        last_transfer_number: int,
        last_transfer_committed_at: datetime,
        ts: datetime,
        ttl: int,
        is_legible_for_trade: bool = True,
        interest_rate_history_period: timedelta = timedelta(days=100000),
) -> None:
    current_ts = datetime.now(tz=timezone.utc)

    is_needed_account = (
        db.session.query(
            NeededWorkerAccount.query
            .filter_by(creditor_id=creditor_id, debtor_id=debtor_id)
            .with_for_update(read=True)
            .exists()
        )
        .scalar()
    )
    if is_needed_account:
        # We should not miss any changes in the interest rate. For
        # this reason, interest rates in old messages, and even in
        # messages with expired TTLs should be archived.
        if store_interest_rate_change(
            creditor_id=creditor_id,
            debtor_id=debtor_id,
            change_ts=last_interest_rate_change_ts,
            interest_rate=interest_rate,
        ):
            compact_interest_rate_changes(
                creditor_id=creditor_id,
                debtor_id=debtor_id,
                cutoff_ts=current_ts - interest_rate_history_period,
                max_number_of_changes=interest_rate_history_period.days + 30,
            )

    if (current_ts - ts).total_seconds() > ttl:
        return  # expired TTL

    if not is_needed_account:
        _discard_unneeded_account(
            creditor_id, debtor_id, config_flags, negligible_amount
        )

    data = (
        WorkerAccount.query
        .filter_by(creditor_id=creditor_id, debtor_id=debtor_id)
        .with_for_update()
        .one_or_none()
    )
    if data is None:
        if not is_needed_account:
            # NOTE: Normally, this should never happen. Creating
            # `WorkerAccount` records for unneeded accounts is a
            # potential DoS attack vector.
            return

        with db.retry_on_integrity_error():
            db.session.add(
                WorkerAccount(
                    creditor_id=creditor_id,
                    debtor_id=debtor_id,
                    creation_date=creation_date,
                    last_change_ts=last_change_ts,
                    last_change_seqnum=last_change_seqnum,
                    principal=principal,
                    interest=interest,
                    interest_rate=interest_rate,
                    last_interest_rate_change_ts=last_interest_rate_change_ts,
                    config_flags=config_flags,
                    account_id=account_id,
                    debtor_info_iri=debtor_info_iri,
                    last_transfer_number=last_transfer_number,
                    last_transfer_committed_at=last_transfer_committed_at,
                    demurrage_rate=demurrage_rate,
                    commit_period=commit_period,
                    transfer_note_max_bytes=transfer_note_max_bytes,
                    last_heartbeat_ts=min(ts, current_ts),
                )
            )
        must_activate_collector = account_id != ""
        has_new_debtor_info_iri = True

    else:
        if ts > data.last_heartbeat_ts:
            data.last_heartbeat_ts = min(ts, current_ts)

        prev_event = (
            data.creation_date,
            data.last_change_ts,
            Seqnum(data.last_change_seqnum),
        )
        this_event = (
            creation_date, last_change_ts, Seqnum(last_change_seqnum)
        )
        if this_event <= prev_event:
            return  # old message

        must_activate_collector = account_id != "" and data.account_id == ""
        has_new_debtor_info_iri = debtor_info_iri != data.debtor_info_iri

        data.account_id = data.account_id or account_id
        data.creation_date = creation_date
        data.last_change_ts = last_change_ts
        data.last_change_seqnum = last_change_seqnum
        data.principal = principal
        data.interest = interest
        data.interest_rate = interest_rate
        data.demurrage_rate = demurrage_rate
        data.commit_period = commit_period
        data.last_interest_rate_change_ts = last_interest_rate_change_ts
        data.config_flags = config_flags
        data.transfer_note_max_bytes = transfer_note_max_bytes
        data.debtor_info_iri = debtor_info_iri
        data.last_transfer_number = last_transfer_number
        data.last_transfer_committed_at = last_transfer_committed_at

    if must_activate_collector:
        db.session.add(
            ActivateCollectorSignal(
                debtor_id=debtor_id,
                creditor_id=creditor_id,
                account_id=account_id,
            )
        )

    if (
            is_needed_account
            and is_legible_for_trade
            and account_id
            and debtor_info_iri
    ):
        db.session.add(
            DiscoverDebtorSignal(
                debtor_id=debtor_id,
                iri=debtor_info_iri,
                force_locator_refetch=has_new_debtor_info_iri,
            )
        )


@atomic
def store_interest_rate_change(
        *,
        creditor_id: int,
        debtor_id: int,
        change_ts: datetime,
        interest_rate: float,
) -> bool:
    should_be_added = not (
        db.session.query(
            InterestRateChange.query
            .filter_by(
                creditor_id=creditor_id,
                debtor_id=debtor_id,
                change_ts=change_ts,
            )
            .exists()
        )
        .scalar()
    )
    if should_be_added:
        with db.retry_on_integrity_error():
            db.session.add(
                InterestRateChange(
                    creditor_id=creditor_id,
                    debtor_id=debtor_id,
                    change_ts=change_ts,
                    interest_rate=interest_rate,
                )
            )

    return should_be_added


@atomic
def compact_interest_rate_changes(
        *,
        creditor_id: int,
        debtor_id: int,
        cutoff_ts: datetime,
        max_number_of_changes: int,
) -> None:
    """Remove redundant `InterestRateChange` rows.

    Because we are only interested in the lowest of the recent
    interest rates. Here we remove all rows that are either not recent
    enough, or have been superseded by a newer row which sets a lower
    (or the same) interest rate.
    """
    changes = (
        InterestRateChange.query
        .filter_by(creditor_id=creditor_id, debtor_id=debtor_id)
        .order_by(InterestRateChange.change_ts.desc())
        .all()
    )
    lowest_interest_rate: float = math.inf

    for n, change in enumerate(changes):
        interest_rate = change.interest_rate

        if interest_rate < lowest_interest_rate and n < max_number_of_changes:
            lowest_interest_rate = interest_rate
        else:
            db.session.delete(change)

        if change.change_ts < cutoff_ts:
            # This means that all the remaining changes should be deleted.
            lowest_interest_rate = -100.0


@atomic
def process_account_purge_signal(
        *,
        debtor_id: int,
        creditor_id: int,
        creation_date: date,
) -> bool:
    is_needed_account = (
        db.session.query(
            NeededWorkerAccount.query
            .filter_by(creditor_id=creditor_id, debtor_id=debtor_id)
            .with_for_update(read=True)
            .exists()
        )
        .scalar()
    )
    worker_account = (
        WorkerAccount.query.filter_by(
            creditor_id=creditor_id, debtor_id=debtor_id
        )
        .filter(WorkerAccount.creation_date <= creation_date)
        .with_for_update()
        .options(load_only(WorkerAccount.creation_date))
        .one_or_none()
    )
    if worker_account:
        db.session.delete(worker_account)

    return is_needed_account


def _discard_unneeded_account(
    creditor_id: int,
    debtor_id: int,
    config_flags: int,
    negligible_amount: float,
) -> None:
    scheduled_for_deletion_flag = (
        WorkerAccount.CONFIG_SCHEDULED_FOR_DELETION_FLAG
    )
    safely_huge_amount = (1 - EPS) * HUGE_NEGLIGIBLE_AMOUNT
    is_already_discarded = (
        config_flags & scheduled_for_deletion_flag
        and negligible_amount >= safely_huge_amount
    )

    if not is_already_discarded:
        db.session.add(
            ConfigureAccountSignal(
                creditor_id=creditor_id,
                debtor_id=debtor_id,
                ts=datetime.now(tz=timezone.utc),
                seqnum=0,
                negligible_amount=HUGE_NEGLIGIBLE_AMOUNT,
                config_flags=DEFAULT_CONFIG_FLAGS
                | scheduled_for_deletion_flag,
            )
        )
