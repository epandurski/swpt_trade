import math
from typing import TypeVar, Callable, Optional
from datetime import date, datetime, timezone, timedelta
from swpt_pythonlib.utils import Seqnum
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import exc, load_only, Load
from swpt_trade.extensions import db
from swpt_trade.models import (
    HUGE_NEGLIGIBLE_AMOUNT,
    DEFAULT_CONFIG_FLAGS,
    WORST_NODE_CLOCKS_MISMATCH,
    KNOWN_SURPLUS_AMOUNT_PREDICATE,
    WORKER_ACCOUNT_TABLES_JOIN_PREDICATE,
    NeededWorkerAccount,
    WorkerAccount,
    InterestRateChange,
    RecentlyNeededCollector,
    ConfigureAccountSignal,
    DiscoverDebtorSignal,
    CollectorStatusChange,
    NeededCollectorAccount,
)
from swpt_trade.utils import (
    generate_collector_account_pkeys,
    contain_principal_overflow,
    calc_demurrage,
    calc_balance_at,
)


T = TypeVar("T")
atomic: Callable[[T], T] = db.atomic

EPS = 1e-5
WORKER_ACCOUNT_LOAD_OPTIONS = Load(WorkerAccount).load_only(
    WorkerAccount.principal,
    WorkerAccount.interest,
    WorkerAccount.interest_rate,
    WorkerAccount.demurrage_rate,
    WorkerAccount.last_change_ts,
    WorkerAccount.last_heartbeat_ts,
    WorkerAccount.surplus_amount,
    WorkerAccount.surplus_ts,
    WorkerAccount.surplus_spent_amount,
    WorkerAccount.surplus_last_transfer_number,
)


@atomic
def configure_worker_account(
        *,
        collector_id: int,
        debtor_id: int,
        max_postponement: timedelta,
) -> None:
    def has_worker_account():
        return (
            db.session.query(
                WorkerAccount.query
                .filter_by(creditor_id=collector_id, debtor_id=debtor_id)
                .exists()
            )
            .scalar()
        )

    current_ts = datetime.now(tz=timezone.utc)
    needed_worker_account = (
        NeededWorkerAccount.query
        .filter_by(creditor_id=collector_id, debtor_id=debtor_id)
        .one_or_none()
    )
    if needed_worker_account is None:
        with db.retry_on_integrity_error():
            db.session.add(
                NeededWorkerAccount(
                    creditor_id=collector_id,
                    debtor_id=debtor_id,
                    configured_at=current_ts,
                )
            )
        must_configure_account = True
    elif (
            needed_worker_account.configured_at + max_postponement < current_ts
            and not has_worker_account()
    ):
        # It's been a while since the last `ConfigureAccount` message
        # was sent for this collector account, and yet there is no
        # account created. The only reasonable thing that we can do in
        # this case, is to send another `ConfigureAccount` message for
        # the account, hoping that this will fix the problem.
        needed_worker_account.configured_at = current_ts
        must_configure_account = True
    else:
        must_configure_account = False

    if must_configure_account:
        db.session.add(
            ConfigureAccountSignal(
                creditor_id=collector_id,
                debtor_id=debtor_id,
                ts=current_ts,
                seqnum=0,
                negligible_amount=HUGE_NEGLIGIBLE_AMOUNT,
                config_flags=DEFAULT_CONFIG_FLAGS,
            )
        )
        db.session.add(
            CollectorStatusChange(
                collector_id=collector_id,
                debtor_id=debtor_id,
                from_status=0,
                to_status=1,
            )
        )


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
            .exists()
        )
        .scalar()
    )
    if is_needed_account:
        # NOTE: We should not miss any changes in the interest rate.
        # For this reason, interest rates in old messages, and even in
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
                max_number_of_changes=(
                    interest_rate_history_period.days // 3 + 30
                ),
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
            CollectorStatusChange(
                collector_id=creditor_id,
                debtor_id=debtor_id,
                from_status=1,
                to_status=2,
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
            .exists()
        )
        .scalar()
    )
    worker_account = (
        WorkerAccount.query
        .filter_by(creditor_id=creditor_id, debtor_id=debtor_id)
        .filter(WorkerAccount.creation_date <= creation_date)
        .with_for_update()
        .options(load_only(WorkerAccount.creation_date))
        .one_or_none()
    )
    if worker_account:
        db.session.delete(worker_account)

    return is_needed_account


@atomic
def process_calculate_surplus_signal(
        *,
        collector_id: int,
        debtor_id: int,
) -> None:
    query = (
        db.session.query(WorkerAccount, NeededWorkerAccount)
        .join(NeededWorkerAccount, WORKER_ACCOUNT_TABLES_JOIN_PREDICATE)
        .filter(
            WorkerAccount.creditor_id == collector_id,
            WorkerAccount.debtor_id == debtor_id,
            KNOWN_SURPLUS_AMOUNT_PREDICATE,
        )
        .options(WORKER_ACCOUNT_LOAD_OPTIONS)
        .with_for_update(of=WorkerAccount)
    )
    try:
        worker_account, needed_worker_account = query.one()
    except exc.NoResultFound:
        return

    if (
            worker_account.surplus_ts
            < needed_worker_account.collection_disabled_since
    ):
        demurrage_rate = worker_account.demurrage_rate

        # Because the time intervals that we calculate depend on
        # timestamps generated on two different nodes, the intervals
        # can not be known for certain with a very good precision.
        # Therefore, if the interest rate becomes too low or too high,
        # our calculations may become very inaccurate. Here we
        # estimate the worst possible "too low" relative error.
        safety_cushion = calc_demurrage(
            demurrage_rate, WORST_NODE_CLOCKS_MISMATCH
        )

        # Here we ensure that the "too high" error can not become
        # greater than the "too low" error. For example, if the
        # demurrage rate (that is: the lowest possible interest rate)
        # is -50%, we set the highest interest rate for our
        # calculation to +100%, which will result in the same worst
        # possible relative error.
        max_interest_rate = (
            (10000.0 / (100.0 + demurrage_rate)) - 100.0
            if demurrage_rate > -99.9999
            else 100000000.0
        )

        worker_account.surplus_amount = max(
            0,
            contain_principal_overflow(
                math.floor(
                    calc_balance_at(
                        principal=worker_account.principal,
                        interest=worker_account.interest,
                        interest_rate=min(
                            worker_account.interest_rate,
                            max_interest_rate,
                        ),
                        last_change_ts=worker_account.last_change_ts,
                        at=worker_account.last_heartbeat_ts,
                    ) * safety_cushion
                )
                - 1
                - needed_worker_account.blocked_amount
            ),
        )
        worker_account.surplus_ts = worker_account.last_heartbeat_ts
        worker_account.surplus_spent_amount = 0
        worker_account.surplus_last_transfer_number += 1

        assert (
            worker_account.surplus_ts
            >= needed_worker_account.collection_disabled_since
        )
        db.session.add(
            CollectorStatusChange(
                collector_id=collector_id,
                debtor_id=debtor_id,
                from_status=3,
                to_status=2,
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

    Because we are only interested in the lowest among the most recent
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
def register_needed_colector(
        *,
        debtor_id: int,
        needed_at: datetime,
        min_collector_id: int,
        max_collector_id: int,
        number_of_accounts: int,
) -> None:
    # NOTE: When there are more than one "worker" servers, it is quite
    # likely that more than one `NeededCollectorSignal` will be
    # received for a given debtor ID because every "worker" server may
    # send such a signal. Here we try to avoid making repetitive
    # queries to the central database.
    has_been_recently_needed = (
        db.session.query(
            RecentlyNeededCollector.query
            .filter_by(debtor_id=debtor_id)
            .exists()
        )
        .scalar()
    )
    if not has_been_recently_needed:
        with db.retry_on_integrity_error():
            db.session.add(
                RecentlyNeededCollector(
                    debtor_id=debtor_id,
                    needed_at=needed_at,
                )
            )
        db.session.execute(
            postgresql.insert(NeededCollectorAccount)
            .execution_options(synchronize_session=False)
            .on_conflict_do_nothing(
                index_elements=[
                    NeededCollectorAccount.debtor_id,
                    NeededCollectorAccount.collector_id,
                ]
            ),
            [
                {"debtor_id": x, "collector_id": y}
                for x, y in generate_collector_account_pkeys(
                        number_of_collector_account_pkeys=number_of_accounts,
                        debtor_id=debtor_id,
                        min_collector_id=min_collector_id,
                        max_collector_id=max_collector_id,
                        existing_collector_ids=set(),
                )
            ]
        )


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
