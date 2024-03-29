from typing import TypeVar, Callable, Optional
from datetime import date, datetime, timezone
from swpt_pythonlib.utils import Seqnum
from sqlalchemy.orm import load_only
from swpt_trade.extensions import db
from swpt_trade.models import (
    NeededWorkerAccount,
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
) -> None:
    current_ts = datetime.now(tz=timezone.utc)
    if (current_ts - ts).total_seconds() > ttl:
        return

    is_needed_account = (
        db.session.query(
            NeededWorkerAccount.query
            .filter_by(creditor_id=creditor_id, debtor_id=debtor_id)
            .with_for_update(read=True)
            .exists()
        )
        .scalar()
    )
    if not is_needed_account:
        _discard_orphaned_account(
            creditor_id, debtor_id, config_flags, negligible_amount
        )

    data = (
        WorkerAccount.query
        .filter_by(creditor_id=creditor_id, debtor_id=debtor_id)
        .with_for_update()
        .one_or_none()
    )
    if data is None:
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
            return

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
        # TODO: Consider deleting related records in other tables as well.
        db.session.delete(worker_account)

    return is_needed_account


def _discard_orphaned_account(
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
