import math
from typing import TypeVar, Callable
from itertools import groupby
from datetime import datetime, timezone, timedelta
from sqlalchemy import select, insert, update, delete, func, union_all
from sqlalchemy.sql.expression import text, tuple_
from flask import current_app
from swpt_trade.extensions import db
from swpt_trade.models import (
    SET_SEQSCAN_ON,
    SET_SEQSCAN_OFF,
    SET_FORCE_CUSTOM_PLAN,
    SET_DEFAULT_PLAN_CACHE_MODE,
    CollectorAccount,
    Turn,
    CurrencyInfo,
    SellOffer,
    BuyOffer,
    CollectorDispatching,
    CollectorReceiving,
    CollectorSending,
    CollectorCollecting,
    CreditorGiving,
    CreditorTaking,
    OverloadedCurrency,
    HoardedCurrency,
    MostBoughtCurrency,
)
from swpt_trade import procedures
from swpt_trade.solver import Solver
from swpt_trade.utils import (
    batched,
    calc_hash,
    get_primary_collector_id,
    CurrencyBuyersCountHeap,
)

INSERT_BATCH_SIZE = 5000
DELETE_BATCH_SIZE = 5000
SELECT_BATCH_SIZE = 20000

COLLECTOR_ACCOUNT_PK = tuple_(
    CollectorAccount.debtor_id,
    CollectorAccount.collector_id,
)
CONFIRMED_CURRENCY_UNIQUE_INDEX = tuple_(
    CurrencyInfo.turn_id,
    CurrencyInfo.debtor_id,
)

T = TypeVar("T")
atomic: Callable[[T], T] = db.atomic


def try_to_advance_turn_to_phase3(turn: Turn) -> bool:
    turn_id = turn.turn_id
    solver = Solver(
        turn.base_debtor_info_locator,
        turn.base_debtor_id,
        turn.max_distance_to_base,
        turn.min_trade_amount,
    )
    _register_currencies(solver, turn_id)
    _register_collector_accounts(solver, turn_id)
    solver.analyze_currencies()

    _register_sell_offers(solver, turn_id)
    _register_buy_offers(solver, turn_id)
    solver.analyze_offers()

    if _try_to_commit_solver_results(solver, turn_id):
        # At this point, the trading turn has successfully advanced to
        # phase 3. The next few calls are not bound to this specific
        # trading turn, but should generally be run near the end of
        # each turn.
        _strengthen_overloaded_currencies()
        _disable_some_collector_accounts()
        _delete_stuck_collector_accounts()
        db.session.close()

        return True

    return False


def _register_currencies(solver: Solver, turn_id: int) -> None:
    with db.engines['solver'].connect() as conn:
        conn.execute(text("ANALYZE currency_info"))
        conn.execute(SET_SEQSCAN_ON)
        with conn.execution_options(yield_per=SELECT_BATCH_SIZE).execute(
                select(
                    CurrencyInfo.is_confirmed,
                    CurrencyInfo.debtor_info_locator,
                    CurrencyInfo.debtor_id,
                    CurrencyInfo.peg_debtor_info_locator,
                    CurrencyInfo.peg_debtor_id,
                    CurrencyInfo.peg_exchange_rate,
                )
                .where(CurrencyInfo.turn_id == turn_id)
        ) as result:
            for row in result:
                if row[3] is None or row[4] is None or row[5] is None:
                    solver.register_currency(row[0], row[1], row[2])
                else:
                    solver.register_currency(*row)


def _register_collector_accounts(solver: Solver, turn_id: int) -> None:
    with db.engines['solver'].connect() as conn:
        conn.execute(SET_SEQSCAN_ON)
        with conn.execution_options(yield_per=SELECT_BATCH_SIZE).execute(
                select(
                    CollectorAccount.collector_id,
                    CollectorAccount.debtor_id,
                )
                .where(CollectorAccount.status == 2)
        ) as result:
            for row in result:
                solver.register_collector_account(*row)


def _register_sell_offers(solver: Solver, turn_id: int) -> None:
    with db.engines['solver'].connect() as conn:
        conn.execute(text("ANALYZE sell_offer"))
        conn.execute(SET_SEQSCAN_ON)
        with conn.execution_options(yield_per=SELECT_BATCH_SIZE).execute(
                select(
                    SellOffer.creditor_id,
                    SellOffer.debtor_id,
                    SellOffer.amount,
                    SellOffer.collector_id,
                )
                .where(SellOffer.turn_id == turn_id)
        ) as result:
            for row in result:
                solver.register_sell_offer(*row)


def _register_buy_offers(solver: Solver, turn_id: int) -> None:
    with db.engines['solver'].connect() as conn:
        conn.execute(text("ANALYZE buy_offer"))
        conn.execute(SET_SEQSCAN_ON)
        with conn.execution_options(yield_per=SELECT_BATCH_SIZE).execute(
                select(
                    BuyOffer.creditor_id,
                    BuyOffer.debtor_id,
                    BuyOffer.amount,
                )
                .where(BuyOffer.turn_id == turn_id)
        ) as result:
            for row in result:
                solver.register_buy_offer(*row)


@atomic
def _try_to_commit_solver_results(solver: Solver, turn_id: int) -> bool:
    turn = (
        Turn.query.filter_by(turn_id=turn_id)
        .with_for_update()
        .one_or_none()
    )
    if turn and turn.phase == 2:
        turn.phase = 3
        turn.phase_deadline = None
        turn.collection_started_at = datetime.now(tz=timezone.utc)
        db.session.flush()

        _write_takings(solver, turn_id)
        _write_collector_transfers(solver, turn_id)
        _write_givings(solver, turn_id)

        work_mem = current_app.config["SOLVER_INCREASED_WORK_MEM"]
        db.session.execute(
            text(f"SET LOCAL work_mem = '{work_mem}'"),
            bind_arguments={"bind": db.engines["solver"]},
        )
        _collect_trade_statistics(turn_id)
        _saturate_hoarded_currencies(turn_id)
        db.session.execute(
            SET_SEQSCAN_ON,
            bind_arguments={"bind": db.engines["solver"]},
        )
        db.session.execute(
            SET_FORCE_CUSTOM_PLAN,
            bind_arguments={"bind": db.engines["solver"]},
        )
        _detect_overloaded_currencies(turn_id)

        # NOTE: When reaching turn phase 3, all records for the given
        # turn from the `CurrencyInfo`, `SellOffer`, `BuyOffer`, and
        # `HoardedCurrency` tables will be deleted. This however, does
        # not guarantee that a worker process will not continue to
        # insert new rows for the given turn in these tables.
        # Therefore, in order to ensure that such obsolete records
        # will be deleted eventually, here we delete all records for
        # which the turn phase 3 has been reached.
        _delete_phase3_turn_records_from_table(CurrencyInfo)
        _delete_phase3_turn_records_from_table(SellOffer)
        _delete_phase3_turn_records_from_table(BuyOffer)
        _delete_phase3_turn_records_from_table(HoardedCurrency)

        return True

    return False


def _delete_phase3_turn_records_from_table(table) -> None:
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
                Turn.phase >= 3,
            )
        )


def _write_takings(solver: Solver, turn_id: int) -> None:
    for account_changes in batched(solver.takings_iter(), INSERT_BATCH_SIZE):
        db.session.execute(
            insert(CreditorTaking)
            .execution_options(synchronize_session=False)
            .from_select(
                [
                    "turn_id",
                    "creditor_id",
                    "debtor_id",
                    "creditor_hash",
                    "amount",
                    "collector_id",
                ],
                CreditorTaking.rows_to_insert(
                    [
                        (
                            turn_id,
                            ac.creditor_id,
                            ac.debtor_id,
                            calc_hash(ac.creditor_id),
                            -ac.amount,
                            ac.collector_id,
                        )
                        for ac in account_changes
                    ]
                ),
            )
        )
        db.session.execute(
            insert(CollectorCollecting)
            .execution_options(synchronize_session=False)
            .from_select(
                [
                    "turn_id",
                    "debtor_id",
                    "creditor_id",
                    "amount",
                    "collector_id",
                    "collector_hash",
                ],
                CollectorCollecting.rows_to_insert(
                    [
                        (
                            turn_id,
                            ac.debtor_id,
                            ac.creditor_id,
                            -ac.amount,
                            ac.collector_id,
                            calc_hash(ac.collector_id),
                        )
                        for ac in account_changes
                    ]
                ),
            )
        )

    db.session.execute(
        text("ANALYZE creditor_taking"),
        bind_arguments={"bind": db.engines["solver"]},
    )
    db.session.execute(
        text("ANALYZE collector_collecting"),
        bind_arguments={"bind": db.engines["solver"]},
    )


def _write_collector_transfers(solver: Solver, turn_id: int) -> None:
    for collector_transfers in batched(
            solver.collector_transfers_iter(), INSERT_BATCH_SIZE
    ):
        db.session.execute(
            insert(CollectorSending)
            .execution_options(synchronize_session=False)
            .from_select(
                [
                    "turn_id",
                    "debtor_id",
                    "from_collector_id",
                    "to_collector_id",
                    "from_collector_hash",
                    "amount",
                ],
                CollectorSending.rows_to_insert(
                    [
                        (
                            turn_id,
                            ct.debtor_id,
                            ct.from_creditor_id,
                            ct.to_creditor_id,
                            calc_hash(ct.from_creditor_id),
                            ct.amount,
                        )
                        for ct in collector_transfers
                    ]
                ),
            )
        )
        db.session.execute(
            insert(CollectorReceiving)
            .execution_options(synchronize_session=False)
            .from_select(
                [
                    "turn_id",
                    "debtor_id",
                    "to_collector_id",
                    "from_collector_id",
                    "to_collector_hash",
                    "amount",
                ],
                CollectorReceiving.rows_to_insert(
                    [
                        (
                            turn_id,
                            ct.debtor_id,
                            ct.to_creditor_id,
                            ct.from_creditor_id,
                            calc_hash(ct.to_creditor_id),
                            ct.amount,
                        )
                        for ct in collector_transfers
                    ]
                ),
            )
        )

    db.session.execute(
        text("ANALYZE collector_sending"),
        bind_arguments={"bind": db.engines["solver"]},
    )
    db.session.execute(
        text("ANALYZE collector_receiving"),
        bind_arguments={"bind": db.engines["solver"]},
    )


def _write_givings(solver: Solver, turn_id: int) -> None:
    for account_changes in batched(solver.givings_iter(), INSERT_BATCH_SIZE):
        db.session.execute(
            insert(CollectorDispatching)
            .execution_options(synchronize_session=False)
            .from_select(
                [
                    "turn_id",
                    "debtor_id",
                    "creditor_id",
                    "amount",
                    "collector_id",
                    "collector_hash",
                ],
                CollectorDispatching.rows_to_insert(
                    [
                        (
                            turn_id,
                            ac.debtor_id,
                            ac.creditor_id,
                            ac.amount,
                            ac.collector_id,
                            calc_hash(ac.collector_id),
                        )
                        for ac in account_changes
                    ]
                ),
            )
        )
        db.session.execute(
            insert(CreditorGiving)
            .execution_options(synchronize_session=False)
            .from_select(
                [
                    "turn_id",
                    "creditor_id",
                    "debtor_id",
                    "creditor_hash",
                    "amount",
                    "collector_id",
                ],
                CreditorGiving.rows_to_insert(
                    [
                        (
                            turn_id,
                            ac.creditor_id,
                            ac.debtor_id,
                            calc_hash(ac.creditor_id),
                            ac.amount,
                            ac.collector_id,
                        )
                        for ac in account_changes
                    ]
                ),
            )
        )

    db.session.execute(
        text("ANALYZE collector_dispatching"),
        bind_arguments={"bind": db.engines["solver"]},
    )
    db.session.execute(
        text("ANALYZE creditor_giving"),
        bind_arguments={"bind": db.engines["solver"]},
    )


def _collect_trade_statistics(turn_id: int) -> None:
    cfg = current_app.config
    n_turns = cfg["APP_NUMBER_OF_TURNS_FOR_BUYERS_COUNT_STATS"]
    n_currencies = cfg["APP_NUMBER_OF_CURRENCIES_IN_BUYERS_COUNT_STATS"]
    heap = CurrencyBuyersCountHeap(max_length=max(n_currencies // n_turns, 1))

    db.session.execute(
        SET_SEQSCAN_ON,
        bind_arguments={"bind": db.engines["solver"]},
    )
    most_bought_currencies: dict[int, float] = {
        row.debtor_id: row.buyers_average_count
        for row in db.session.execute(
            select(
                MostBoughtCurrency.debtor_id,
                MostBoughtCurrency.buyers_average_count,
            )
        ).all()
    }
    updated: set[int] = set()

    def update(buyers_count: float, debtor_id: int):
        old_value = most_bought_currencies[debtor_id]
        new_value = (old_value * (n_turns - 1) + buyers_count) / n_turns
        most_bought_currencies[debtor_id] = new_value
        updated.add(debtor_id)

    min_count = (
        int(min(most_bought_currencies.values()) / 10.0)
        if len(most_bought_currencies) >= n_currencies
        else 0
    )
    cd = CollectorDispatching

    with db.session.execute(
            select(
                cd.debtor_id,
                func.count(cd.creditor_id).label("buyers_count"),
            )
            .select_from(cd)
            .where(cd.turn_id == turn_id)
            .group_by(cd.debtor_id)
            .having(func.count(cd.creditor_id) >= min_count)
            .execution_options(yield_per=SELECT_BATCH_SIZE)
    ) as result:
        for row in result:
            debtor_id = row.debtor_id
            buyers_count = float(row.buyers_count)
            if debtor_id in most_bought_currencies:
                update(buyers_count, debtor_id)
            else:
                heap.push(buyers_count, debtor_id)

    for debtor_id in most_bought_currencies:
        if debtor_id not in updated:
            update(0.0, debtor_id)

    heap.set_max_length(n_currencies)
    for debtor_id, average_buyers_count in most_bought_currencies.items():
        heap.push(average_buyers_count, debtor_id)

    db.session.execute(
        delete(MostBoughtCurrency)
        .execution_options(synchronize_session=False)
    )
    db.session.execute(
        SET_SEQSCAN_OFF,
        bind_arguments={"bind": db.engines["solver"]},
    )
    if heap:
        db.session.execute(
            SET_FORCE_CUSTOM_PLAN,
            bind_arguments={"bind": db.engines["solver"]},
        )
        chosen_currencies = HoardedCurrency.choose_rows(
            [(turn_id, item.debtor_id) for item in heap]
        )
        locators: dict[int, str] = {
            row.debtor_id: row.debtor_info_locator
            for row in db.session.execute(
                    select(
                        CurrencyInfo.debtor_id,
                        CurrencyInfo.debtor_info_locator,
                    )
                    .select_from(CurrencyInfo)
                    .join(
                        chosen_currencies,
                        CONFIRMED_CURRENCY_UNIQUE_INDEX
                        == tuple_(*chosen_currencies.c),
                    )
                    .where(CurrencyInfo.is_confirmed)
            ).all()
        }
        db.session.execute(
            SET_DEFAULT_PLAN_CACHE_MODE,
            bind_arguments={"bind": db.engines["solver"]},
        )
        db.session.execute(
            insert(MostBoughtCurrency)
            .execution_options(
                insertmanyvalues_page_size=INSERT_BATCH_SIZE,
                synchronize_session=False,
            ),
            [
                {
                    "debtor_id": item.debtor_id,
                    "buyers_average_count": item.buyers_count,
                    "debtor_info_locator": locators[item.debtor_id],
                }
                for item in heap if item.debtor_id in locators
            ],
        )


def _detect_overloaded_currencies(turn_id: int) -> None:
    max_transfers_count = current_app.config["TRANSFERS_COLLECTOR_LIMIT"]
    assert max_transfers_count > 0

    cd = CollectorDispatching
    dispatchings = (
        select(
            cd.debtor_id.label("debtor_id"),
            cd.collector_id.label("collector_id"),
            func.count(cd.creditor_id).label("transfers_count"),
        )
        .select_from(cd)
        .where(cd.turn_id == turn_id)
        .group_by(cd.debtor_id, cd.collector_id)
        .subquery(name="dispatchings")
    )
    dispatching_overloads = (
        select(
            dispatchings.c.debtor_id.label("debtor_id"),
            func.count(dispatchings.c.collector_id).label("collectors_count"),
        )
        .select_from(dispatchings)
        .group_by(dispatchings.c.debtor_id)
        .having(func.max(dispatchings.c.transfers_count) > max_transfers_count)
    )

    cc = CollectorCollecting
    collectings = (
        select(
            cc.debtor_id.label("debtor_id"),
            cc.collector_id.label("collector_id"),
            func.count(cc.creditor_id).label("transfers_count"),
        )
        .select_from(cc)
        .where(cc.turn_id == turn_id)
        .group_by(cc.debtor_id, cc.collector_id)
        .subquery(name="collectings")
    )
    collecting_overloads = (
        select(
            collectings.c.debtor_id.label("debtor_id"),
            func.count(collectings.c.collector_id).label("collectors_count"),
        )
        .select_from(collectings)
        .group_by(collectings.c.debtor_id)
        .having(func.max(collectings.c.transfers_count) > max_transfers_count)
    )

    overloads = (
        union_all(dispatching_overloads, collecting_overloads)
        .subquery(name="overloads")
    )
    db.session.execute(
        insert(OverloadedCurrency)
        .execution_options(synchronize_session=False)
        .from_select(
            [
                "turn_id",
                "debtor_id",
                "collectors_count",
            ],
            select(
                turn_id,
                overloads.c.debtor_id,
                func.max(overloads.c.collectors_count),
            )
            .select_from(overloads)
            .group_by(overloads.c.debtor_id)
        )
    )


def _strengthen_overloaded_currencies() -> None:
    """Double the number of existing collector accounts for all
    overloaded currencies.
    """

    cfg = current_app.config
    min_collector_id = cfg["MIN_COLLECTOR_ID"]
    max_collector_id = cfg["MAX_COLLECTOR_ID"]

    with db.engines['solver'].connect() as conn:
        conn.execute(SET_SEQSCAN_ON)
        with conn.execution_options(yield_per=SELECT_BATCH_SIZE).execute(
                select(
                    OverloadedCurrency.turn_id,
                    OverloadedCurrency.debtor_id,
                    OverloadedCurrency.collectors_count,
                )
        ) as result:
            # NOTE: We should be able to afford to sequentially
            # process all the overloaded currencies here, because an
            # overloaded currency should be a quite rare thing.
            for row in result:
                procedures.ensure_collector_accounts(
                    debtor_id=row.debtor_id,
                    min_collector_id=min_collector_id,
                    max_collector_id=max_collector_id,
                    number_of_accounts=2 * row.collectors_count,
                )
                procedures.forget_overloaded_currency(
                    turn_id=row.turn_id,
                    debtor_id=row.debtor_id,
                )


def _saturate_hoarded_currencies(turn_id: int) -> None:
    """Ensure hoarded currencies utilize the maximum number of
    creditor IDs.

    NOTE: Surplus amounts will accumulate on collectors with different
    creditor IDs (between MIN_COLLECTOR_ID and MAX_COLLECTOR_ID).
    Thus, a surplus selling offer could come from any creditor in this
    interval, and therefore, in order to reliably match buying and
    selling offers, surplus buying offers should also be registered
    from all possible creditor IDs in this interval.
    """

    cfg = current_app.config
    min_collector_id = cfg["MIN_COLLECTOR_ID"]
    max_collector_id = cfg["MAX_COLLECTOR_ID"]
    assert min_collector_id <= max_collector_id
    number_of_ids = 1 + max_collector_id - min_collector_id

    with db.engines['solver'].connect() as conn:
        conn.execute(SET_SEQSCAN_ON)
        with conn.execution_options(yield_per=SELECT_BATCH_SIZE).execute(
                select(HoardedCurrency.debtor_id)
                .join(
                    CollectorAccount,
                    CollectorAccount.debtor_id == HoardedCurrency.debtor_id,
                    isouter=True,
                )
                .where(HoardedCurrency.turn_id == turn_id)
                .group_by(HoardedCurrency.debtor_id)
                .having(
                    func.count(CollectorAccount.collector_id) < number_of_ids
                )
        ) as result:
            for row in result:
                procedures.insert_collector_accounts(
                    (row.debtor_id, c_id)
                    for c_id in range(min_collector_id, max_collector_id + 1)
                )


def _disable_some_collector_accounts() -> None:
    """Try to disable some of the active collector accounts (status ==
    2), so that the surplus amounts accumulated on them can be
    detected.

    NOTE: If possible, this function will not disable more than ~1/3
    of the active collector accounts for any given currency. Also,
    collector accounts that have not been active for at least
    `APP_COLLECTOR_ACTIVITY_MIN_DAYS` days, will not be disabled.
    """

    current_ts = datetime.now(tz=timezone.utc)
    cfg = current_app.config
    min_collector_id = cfg["MIN_COLLECTOR_ID"]
    max_collector_id = cfg["MAX_COLLECTOR_ID"]
    activity_cutoff_ts = current_ts - timedelta(
        days=cfg["APP_COLLECTOR_ACTIVITY_MIN_DAYS"]
    )
    waiting_to_be_disabled = []
    waiting_to_be_ensured_to_exist = set()

    @atomic
    def process_waiting():
        if waiting_to_be_disabled:
            db.session.execute(
                SET_FORCE_CUSTOM_PLAN,
                bind_arguments={"bind": db.engines["solver"]},
            )
            chosen = CollectorAccount.choose_rows(waiting_to_be_disabled)
            db.session.execute(
                update(CollectorAccount)
                .execution_options(synchronize_session=False)
                .where(
                    COLLECTOR_ACCOUNT_PK == tuple_(*chosen.c),
                    CollectorAccount.status == 2,
                )
                .values(
                    status=3,
                    latest_status_change_at=current_ts,
                )
            )
            db.session.execute(
                SET_DEFAULT_PLAN_CACHE_MODE,
                bind_arguments={"bind": db.engines["solver"]},
            )
            waiting_to_be_disabled.clear()

        if waiting_to_be_ensured_to_exist:
            procedures.insert_collector_accounts(
                waiting_to_be_ensured_to_exist
            )
            waiting_to_be_ensured_to_exist.clear()

    with db.engines['solver'].connect() as conn:
        conn.execute(SET_SEQSCAN_ON)
        with conn.execution_options(yield_per=SELECT_BATCH_SIZE).execute(
                select(
                    CollectorAccount.debtor_id,
                    CollectorAccount.collector_id,
                    CollectorAccount.status,
                    CollectorAccount.latest_status_change_at,
                )
                .where(CollectorAccount.status >= text("2"))
                .order_by(CollectorAccount.debtor_id)
        ) as result:
            for debtor_id, group in groupby(result, lambda r: r.debtor_id):
                primary_collector_id = None
                rows = list(group)
                rows.sort(
                    key=lambda x: (
                        x.status, x.latest_status_change_at, x.collector_id
                    )
                )
                number_of_disabled = sum(1 for r in rows if r.status != 2)
                max_number_to_disable = (
                    math.ceil(len(rows) / 3) - number_of_disabled
                )
                for n, row in enumerate(rows):
                    if (
                        n >= max_number_to_disable
                        or row.status != 2
                        or row.latest_status_change_at > activity_cutoff_ts
                    ):
                        break

                    collector_id = row.collector_id
                    waiting_to_be_disabled.append((debtor_id, collector_id))

                    # NOTE: Each disabled collector account will
                    # eventually be activated again. At the time of
                    # activation, an attempt may be made to transfer
                    # the calculated surplus amount to another
                    # collector account (the primary collector
                    # account). Here we ensure that the primary
                    # collector account exists. In some rare cases, it
                    # may happen that the creation of the primary
                    # collector account has failed, and the account
                    # has been deleted.
                    if primary_collector_id is None:
                        primary_collector_id = get_primary_collector_id(
                            debtor_id=debtor_id,
                            min_collector_id=min_collector_id,
                            max_collector_id=max_collector_id,
                        )
                    if collector_id != primary_collector_id:
                        waiting_to_be_ensured_to_exist.add(
                            (debtor_id, primary_collector_id)
                        )

                    if len(waiting_to_be_disabled) > INSERT_BATCH_SIZE:
                        process_waiting()  # pragma: no cover

            process_waiting()


def _delete_stuck_collector_accounts() -> None:
    """Delete collector accounts which are stuck at `status==1` for
    quite a long time.

    Collector accounts could become stuck at `status==1` if the issued
    `ConfigureAccount` SMP message has been lost (Which must never
    happen under normal circumstances.), or when an otherwise
    successfully created worker account has been, for some reason,
    removed (purged) from the accounting authority server.
    """

    current_ts = datetime.now(tz=timezone.utc)
    cutoff_ts = current_ts - timedelta(
        days=2 * current_app.config["APP_SURPLUS_BLOCKING_DELAY_DAYS"]
    )
    with db.engines['solver'].connect() as conn:
        conn.execute(SET_SEQSCAN_ON)
        with conn.execution_options(yield_per=SELECT_BATCH_SIZE).execute(
                select(
                    CollectorAccount.debtor_id,
                    CollectorAccount.collector_id,
                )
                .where(
                    CollectorAccount.status == text("1"),
                    CollectorAccount.latest_status_change_at < cutoff_ts,
                )
        ) as result:
            for rows in result.partitions(DELETE_BATCH_SIZE):
                db.session.execute(
                    SET_FORCE_CUSTOM_PLAN,
                    bind_arguments={"bind": db.engines["solver"]},
                )
                chosen = CollectorAccount.choose_rows(
                    [tuple(row) for row in rows]
                )
                pks_locked = [
                    tuple(row)
                    for row in db.session.execute(
                            select(
                                CollectorAccount.debtor_id,
                                CollectorAccount.collector_id,
                            )
                            .select_from(CollectorAccount)
                            .join(
                                chosen,
                                COLLECTOR_ACCOUNT_PK == tuple_(*chosen.c)
                            )
                            .with_for_update(skip_locked=True)
                    ).all()
                ]
                if pks_locked:
                    to_delete = CollectorAccount.choose_rows(pks_locked)
                    db.session.execute(
                        delete(CollectorAccount)
                        .execution_options(synchronize_session=False)
                        .where(COLLECTOR_ACCOUNT_PK == tuple_(*to_delete.c))
                    )
                db.session.commit()
