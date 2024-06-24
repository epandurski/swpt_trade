from typing import TypeVar, Callable, Sequence, List, Iterable, Tuple
from random import Random
from datetime import datetime, timezone, timedelta
from sqlalchemy import select, insert, text
from sqlalchemy.sql.expression import null, and_
from sqlalchemy.orm import load_only
from swpt_trade.utils import can_start_new_turn
from swpt_trade.extensions import db
from swpt_trade.models import (
    TS0,
    Turn,
    DebtorInfo,
    CollectorAccount,
    ConfirmedDebtor,
    CurrencyInfo,
    CollectorDispatching,
    CollectorReceiving,
    CollectorSending,
    CollectorCollecting,
    CreditorGiving,
    CreditorTaking,
)


T = TypeVar("T")
atomic: Callable[[T], T] = db.atomic


@atomic
def start_new_turn_if_possible(
        *,
        turn_period: timedelta,
        turn_period_offset: timedelta,
        phase1_duration: timedelta,
        base_debtor_info_locator: str,
        base_debtor_id: int,
        max_distance_to_base: int,
        min_trade_amount: int,
) -> Sequence[Turn]:
    current_ts = datetime.now(tz=timezone.utc)
    db.session.execute(
        text("LOCK TABLE turn IN SHARE ROW EXCLUSIVE MODE"),
        bind_arguments={"bind": db.engines["solver"]},
    )
    unfinished_turns = Turn.query.filter(Turn.phase < text("4")).all()
    if not unfinished_turns:
        latest_turn = (
            Turn.query
            .order_by(Turn.started_at.desc())
            .limit(1)
            .one_or_none()
        )
        if can_start_new_turn(
                turn_period=turn_period,
                turn_period_offset=turn_period_offset,
                latest_turn_started_at=(
                    latest_turn.started_at if latest_turn else TS0
                ),
                current_ts=current_ts,
        ):
            new_turn = Turn(
                started_at=current_ts,
                base_debtor_info_locator=base_debtor_info_locator,
                base_debtor_id=base_debtor_id,
                max_distance_to_base=max_distance_to_base,
                min_trade_amount=min_trade_amount,
                phase_deadline=current_ts + phase1_duration,
            )
            db.session.add(new_turn)
            return [new_turn]

    return unfinished_turns


@atomic
def try_to_advance_turn_to_phase2(
        *,
        turn_id: int,
        phase2_duration: timedelta,
        max_commit_period: timedelta,
) -> None:
    current_ts = datetime.now(tz=timezone.utc)
    turn = (
        Turn.query.filter_by(turn_id=turn_id)
        .with_for_update()
        .one_or_none()
    )
    if turn and turn.phase == 1:
        active_debtor = (
            # These are debtors that are confirmed, for which there
            # are at least one active (status == 2) collector account.
            select(ConfirmedDebtor)
            .distinct()
            .join(
                CollectorAccount,
                and_(
                    CollectorAccount.debtor_id == ConfirmedDebtor.debtor_id,
                    CollectorAccount.status == 2,
                ),
            )
            .where(ConfirmedDebtor.turn_id == turn_id)
            .subquery(name="ad")
        )
        db.session.execute(
            insert(CurrencyInfo).from_select(
                [
                    "turn_id",
                    "debtor_info_locator",
                    "debtor_id",
                    "peg_debtor_info_locator",
                    "peg_debtor_id",
                    "peg_exchange_rate",
                    "is_confirmed",
                ],
                select(
                    DebtorInfo.turn_id,
                    DebtorInfo.debtor_info_locator,
                    DebtorInfo.debtor_id,
                    DebtorInfo.peg_debtor_info_locator,
                    DebtorInfo.peg_debtor_id,
                    DebtorInfo.peg_exchange_rate,
                    (
                        active_debtor.c.turn_id != null()
                    ).label("is_confirmed"),
                )
                .select_from(DebtorInfo)
                .join(
                    active_debtor,
                    and_(
                        active_debtor.c.turn_id == DebtorInfo.turn_id,
                        active_debtor.c.debtor_id == DebtorInfo.debtor_id,
                        active_debtor.c.debtor_info_locator
                        == DebtorInfo.debtor_info_locator,
                    ),
                    isouter=True,
                )
                .where(DebtorInfo.turn_id == turn_id),
            )
        )
        DebtorInfo.query.filter_by(turn_id=turn_id).delete(
            synchronize_session=False
        )
        ConfirmedDebtor.query.filter_by(turn_id=turn_id).delete(
            synchronize_session=False
        )

        turn.phase = 2
        turn.phase_deadline = current_ts + phase2_duration
        turn.collection_deadline = current_ts + max_commit_period


@atomic
def try_to_advance_turn_to_phase4(turn_id: int) -> None:
    turn = (
        Turn.query.filter_by(turn_id=turn_id)
        .with_for_update()
        .one_or_none()
    )
    if turn and turn.phase == 3:
        for table in [
                CollectorDispatching,
                CollectorReceiving,
                CollectorSending,
                CollectorCollecting,
                CreditorGiving,
                CreditorTaking,
        ]:
            has_pending_rows = bool(
                db.session.execute(
                    select(1)
                    .select_from(table)
                    .filter_by(turn_id=turn_id)
                    .limit(1)
                ).one_or_none()
            )
            if has_pending_rows:
                break
        else:
            # There are no pending rows.
            turn.phase = 4
            turn.phase_deadline = None


@atomic
def get_unfinished_turns() -> Sequence[Turn]:
    return (
        Turn.query
        .filter(Turn.phase < text("4"))
        .all()
    )


@atomic
def get_turns_by_ids(turn_ids: List[int]) -> Sequence[Turn]:
    return (
        Turn.query
        .filter(Turn.turn_id.in_(turn_ids))
        .all()
    )


@atomic
def get_pristine_collectors(
        *,
        hash_mask: int,
        hash_prefix: int,
        max_count: int = None,
) -> Sequence[Tuple[int, int]]:
    query = (
        db.session.query(
            CollectorAccount.debtor_id, CollectorAccount.collector_id
        )
        .filter(
            and_(
                CollectorAccount.status == text("0"),
                CollectorAccount.collector_hash.op("&")(hash_mask)
                == hash_prefix,
            )
        )
    )
    if max_count is not None:
        query = query.limit(max_count)

    return query.all()


@atomic
def mark_requested_collector(
        *,
        debtor_id: int,
        collector_id: int,
) -> bool:
    current_ts = datetime.now(tz=timezone.utc)
    updated_rows = (
        CollectorAccount.query
        .filter_by(debtor_id=debtor_id, collector_id=collector_id, status=0)
        .update(
            {
                CollectorAccount.status: 1,  # requested account creation
                CollectorAccount.latest_status_change_at: current_ts,
            },
            synchronize_session=False,
        )
    )
    assert updated_rows <= 1
    return updated_rows > 0


@atomic
def activate_collector(
        *,
        debtor_id: int,
        collector_id: int,
        account_id: str,
) -> bool:
    assert account_id

    current_ts = datetime.now(tz=timezone.utc)
    updated_rows = (
        CollectorAccount.query
        .filter_by(debtor_id=debtor_id, collector_id=collector_id)
        .filter(CollectorAccount.status <= 1)
        .update(
            {
                CollectorAccount.account_id: account_id,
                CollectorAccount.status: 2,  # assigned account ID
                CollectorAccount.latest_status_change_at: current_ts,
            },
            synchronize_session=False,
        )
    )
    assert updated_rows <= 1
    return updated_rows > 0


@atomic
def ensure_collector_accounts(
        *,
        debtor_id: int,
        min_collector_id: int,
        max_collector_id: int,
        number_of_accounts: int = 1,
) -> None:
    """Ensure that for the given `debtor_id`, there are at least
    `number_of_accounts` alive (status != 3) collector accounts.

    When the number of existing alive collector accounts is less than
    the given `number_of_accounts`, new collector accounts will be
    created until the given number is reached.

    The collector IDs for the created accounts will be picked from a
    *repeatable* pseudoranom sequence. Thus, when two or more
    processes happen to call this procedure simultaneously (that is: a
    race condition has occurred), all the processes will pick the same
    collector IDs, avoiding the creation of unneeded collector
    accounts.
    """
    accounts = (
        CollectorAccount.query
        .filter_by(debtor_id=debtor_id)
        .options(load_only(CollectorAccount.status))
        .all()
    )
    number_of_alive_accounts = sum(
        1 for account in accounts if account.status != 3
    )

    def collector_ids_iter() -> Iterable[int]:
        number_of_dead_accounts = len(accounts) - number_of_alive_accounts
        final_number_of_accounts = number_of_dead_accounts + number_of_accounts
        ids_total_count = 1 + max_collector_id - min_collector_id

        # NOTE: Because we rely on being able to efficiently pick
        # random IDs between `min_collector_id` and
        # `max_collector_id`, we can not utilize the range of
        # available IDs at 100%. Here we ensure a 1/4 safety margin.
        if final_number_of_accounts > ids_total_count * 3 // 4:
            raise RuntimeError(
                "The number of available collector IDs is not big enough."
            )

        rgen = Random(debtor_id)
        while True:
            yield rgen.randint(min_collector_id, max_collector_id)

    if number_of_alive_accounts < number_of_accounts:
        with db.retry_on_integrity_error():
            existing_ids = set(x.collector_id for x in accounts)

            for collector_id in collector_ids_iter():
                if collector_id not in existing_ids:
                    db.session.add(
                        CollectorAccount(
                            debtor_id=debtor_id, collector_id=collector_id
                        )
                    )
                    existing_ids.add(collector_id)
                    number_of_alive_accounts += 1
                    if number_of_alive_accounts == number_of_accounts:
                        break
