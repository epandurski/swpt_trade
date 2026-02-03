from typing import TypeVar, Callable, Sequence, List, Iterable
from datetime import datetime, timezone, timedelta
from sqlalchemy import select, insert, delete, text, func
from sqlalchemy.dialects import postgresql
from sqlalchemy.sql.expression import null, and_
from swpt_trade.utils import (
    can_start_new_turn,
    generate_collector_account_pkeys,
)
from swpt_trade.extensions import db
from swpt_trade.models import (
    TS0,
    SET_SEQSCAN_ON,
    SET_FORCE_CUSTOM_PLAN,
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
    OverloadedCurrency,
    MostBoughtCurrency,
)

ACTIVE_COLLECTOR_ACCOUNT_EXISTS = (
    select(1)
    .select_from(CollectorAccount)
    .where(
        CollectorAccount.debtor_id == ConfirmedDebtor.debtor_id,
        CollectorAccount.status == text("2"),
    )
    .exists()
    .correlate(ConfirmedDebtor)
)
ACTIVE_CONFIRMED_DEBTOR_SUBQUERY = (
    select(
        ConfirmedDebtor.turn_id,
        ConfirmedDebtor.debtor_id,
        ConfirmedDebtor.debtor_info_locator,
    )
    .select_from(ConfirmedDebtor)
    .where(ACTIVE_COLLECTOR_ACCOUNT_EXISTS)
    .subquery(name="acd")
)

INSERT_BATCH_SIZE = 5000
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
) -> bool:
    current_ts = datetime.now(tz=timezone.utc)
    turn = (
        Turn.query.filter_by(turn_id=turn_id)
        .with_for_update()
        .one_or_none()
    )
    if turn and turn.phase == 1:
        turn.phase = 2
        turn.phase_deadline = current_ts + phase2_duration
        turn.collection_deadline = current_ts + max_commit_period

        db.session.flush()
        db.session.execute(
            text("ANALYZE debtor_info"),
            bind_arguments={"bind": db.engines["solver"]},
        )
        db.session.execute(
            text("ANALYZE confirmed_debtor"),
            bind_arguments={"bind": db.engines["solver"]},
        )
        db.session.execute(
            SET_SEQSCAN_ON,
            bind_arguments={"bind": db.engines["solver"]},
        )
        db.session.execute(
            SET_FORCE_CUSTOM_PLAN,
            bind_arguments={"bind": db.engines["solver"]},
        )
        db.session.execute(
            insert(CurrencyInfo)
            .execution_options(synchronize_session=False)
            .from_select(
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
                        ACTIVE_CONFIRMED_DEBTOR_SUBQUERY.c.debtor_id != null()
                    ).label("is_confirmed"),
                )
                .select_from(DebtorInfo)
                .join(
                    ACTIVE_CONFIRMED_DEBTOR_SUBQUERY,
                    and_(
                        ACTIVE_CONFIRMED_DEBTOR_SUBQUERY.c.turn_id
                        == DebtorInfo.turn_id,
                        ACTIVE_CONFIRMED_DEBTOR_SUBQUERY.c.debtor_id
                        == DebtorInfo.debtor_id,
                        ACTIVE_CONFIRMED_DEBTOR_SUBQUERY.c.debtor_info_locator
                        == DebtorInfo.debtor_info_locator,
                    ),
                    isouter=True,
                )
                .where(DebtorInfo.turn_id == turn_id),
            )
        )
        db.session.execute(
            text("ANALYZE currency_info"),
            bind_arguments={"bind": db.engines["solver"]},
        )

        # NOTE: When reaching turn phase 2, all records for the given
        # turn from the `DebtorInfo` and `ConfirmedDebtor` tables will
        # be deleted. This however, does not guarantee that a worker
        # process will not continue to insert new rows for the given
        # turn in these tables. Therefore, in order to ensure that
        # such obsolete records will be deleted eventually, here we
        # delete all records for which the turn phase 2 has been
        # reached.
        _delete_phase2_turn_records_from_table(DebtorInfo)
        _delete_phase2_turn_records_from_table(ConfirmedDebtor)

        return True

    return False


def _delete_phase2_turn_records_from_table(table) -> None:
    min_turn_id = (
        db.session.execute(
            select(func.min(table.turn_id))
            .select_from(table)
        )
        .scalar_one()
    )
    if min_turn_id is not None:
        db.session.execute(
            delete(table)
            .execution_options(synchronize_session=False)
            .where(
                Turn.turn_id == table.turn_id,
                Turn.turn_id >= min_turn_id,
                Turn.phase >= 2,
            )
        )


@atomic
def try_to_advance_turn_to_phase4(turn_id: int) -> bool:
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
                    .where(table.turn_id == turn_id)
                    .limit(1)
                ).one_or_none()
            )
            if has_pending_rows:
                break
        else:
            # There are no pending rows.
            turn.phase = 4
            turn.phase_deadline = None
            return True

    return False


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
def insert_collector_accounts(pks: Iterable[tuple[int, int]]) -> None:
    db.session.execute(
        postgresql.insert(CollectorAccount)
        .execution_options(
            insertmanyvalues_page_size=INSERT_BATCH_SIZE,
            synchronize_session=False,
        )
        .on_conflict_do_nothing(
            index_elements=[
                CollectorAccount.debtor_id,
                CollectorAccount.collector_id,
            ]
        ),
        [
            {"debtor_id": debtor_id, "collector_id": collector_id}
            for debtor_id, collector_id in pks
        ],
    )


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
    accounts = db.session.execute(
        select(CollectorAccount.collector_id, CollectorAccount.status)
        .where(CollectorAccount.debtor_id == debtor_id)
    ).all()

    number_of_dead_accounts = sum(1 for x in accounts if x.status == 3)
    number_of_alive_accounts = len(accounts) - number_of_dead_accounts
    number_of_missing_accounts = number_of_accounts - number_of_alive_accounts

    collector_account_pkeys = generate_collector_account_pkeys(
        number_of_collector_account_pkeys=number_of_missing_accounts,
        debtor_id=debtor_id,
        min_collector_id=min_collector_id,
        max_collector_id=max_collector_id,
        existing_collector_ids=set(x.collector_id for x in accounts),
    )
    if collector_account_pkeys:
        insert_collector_accounts(collector_account_pkeys)


@atomic
def forget_overloaded_currency(turn_id: int, debtor_id: int) -> None:
    db.session.execute(
        delete(OverloadedCurrency)
        .execution_options(synchronize_session=False)
        .where(
            OverloadedCurrency.turn_id == turn_id,
            OverloadedCurrency.debtor_id == debtor_id,
        )
    )


@atomic
def get_most_bought_currencies():
    db.session.execute(
        SET_SEQSCAN_ON,
        bind_arguments={"bind": db.engines["solver"]},
    )
    return db.session.execute(
        select(
            MostBoughtCurrency.debtor_id,
            MostBoughtCurrency.debtor_info_locator,
            MostBoughtCurrency.buyers_average_count,
        )
        .order_by(
            MostBoughtCurrency.buyers_average_count.desc(),
            MostBoughtCurrency.debtor_id,
        )
    ).all()
