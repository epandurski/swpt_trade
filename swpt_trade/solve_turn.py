import math
from typing import TypeVar, Callable
from itertools import groupby
from datetime import datetime, timezone, timedelta
from sqlalchemy import select, insert, update, delete, func, union_all
from sqlalchemy.sql.expression import text, and_, tuple_
from flask import current_app
from swpt_trade.extensions import db
from swpt_trade.models import (
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
)
from swpt_trade import procedures
from swpt_trade.solver import Solver
from swpt_trade.utils import batched, calc_hash

INSERT_BATCH_SIZE = 50000
SELECT_BATCH_SIZE = 50000

COLLECTOR_ACCOUNT_PK = tuple_(
    CollectorAccount.debtor_id,
    CollectorAccount.collector_id,
)

T = TypeVar("T")
atomic: Callable[[T], T] = db.atomic


def try_to_advance_turn_to_phase3(turn: Turn) -> None:
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

    _try_to_commit_solver_results(solver, turn_id)
    _handle_overloaded_currencies()
    _disable_extra_collector_accounts()


def _register_currencies(solver: Solver, turn_id: int) -> None:
    with db.engines['solver'].connect() as conn:
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
def _try_to_commit_solver_results(solver: Solver, turn_id: int) -> None:
    turn = (
        Turn.query.filter_by(turn_id=turn_id)
        .with_for_update()
        .one_or_none()
    )
    if turn and turn.phase == 2:
        _write_takings(solver, turn_id)
        _write_collector_transfers(solver, turn_id)
        _write_givings(solver, turn_id)
        _detect_overloaded_currencies(turn_id)
        _handle_hoarded_currencies(turn_id)

        turn.phase = 3
        turn.phase_deadline = None
        turn.collection_started_at = datetime.now(tz=timezone.utc)

        # NOTE: When reaching turn phase 3, all records for the given
        # turn from the `CurrencyInfo`, `SellOffer`, `BuyOffer`, and
        # `HoardedCurrency` tables will be deleted. This however, does
        # not guarantee that a worker process will not continue to
        # insert new rows for the given turn in these tables.
        # Therefore, in order to ensure that such obsolete records
        # will be deleted eventually, here we delete all records for
        # which the turn phase 3 has been reached.
        db.session.execute(
            delete(CurrencyInfo)
            .where(
                and_(
                    Turn.turn_id == CurrencyInfo.turn_id,
                    Turn.phase >= 3,
                )
            )
        )
        db.session.execute(
            delete(SellOffer)
            .where(
                and_(
                    Turn.turn_id == SellOffer.turn_id,
                    Turn.phase >= 3,
                )
            )
        )
        db.session.execute(
            delete(BuyOffer)
            .where(
                and_(
                    Turn.turn_id == BuyOffer.turn_id,
                    Turn.phase >= 3,
                )
            )
        )
        db.session.execute(
            delete(HoardedCurrency)
            .where(
                and_(
                    Turn.turn_id == HoardedCurrency.turn_id,
                    Turn.phase >= 3,
                )
            )
        )


def _write_takings(solver: Solver, turn_id: int) -> None:
    for account_changes in batched(solver.takings_iter(), INSERT_BATCH_SIZE):
        db.session.execute(
            insert(CreditorTaking).execution_options(
                insertmanyvalues_page_size=INSERT_BATCH_SIZE
            ),
            [
                {
                    "turn_id": turn_id,
                    "creditor_id": ac.creditor_id,
                    "debtor_id": ac.debtor_id,
                    "creditor_hash": calc_hash(ac.creditor_id),
                    "amount": -ac.amount,
                    "collector_id": ac.collector_id,
                } for ac in account_changes
            ],
        )
        db.session.execute(
            insert(CollectorCollecting).execution_options(
                insertmanyvalues_page_size=INSERT_BATCH_SIZE
            ),
            [
                {
                    "turn_id": turn_id,
                    "debtor_id": ac.debtor_id,
                    "creditor_id": ac.creditor_id,
                    "amount": -ac.amount,
                    "collector_id": ac.collector_id,
                    "collector_hash": calc_hash(ac.collector_id),
                } for ac in account_changes
            ],
        )


def _write_collector_transfers(solver: Solver, turn_id: int) -> None:
    for collector_transfers in batched(
            solver.collector_transfers_iter(), INSERT_BATCH_SIZE
    ):
        db.session.execute(
            insert(CollectorSending).execution_options(
                insertmanyvalues_page_size=INSERT_BATCH_SIZE
            ),
            [
                {
                    "turn_id": turn_id,
                    "debtor_id": ct.debtor_id,
                    "from_collector_id": ct.from_creditor_id,
                    "to_collector_id": ct.to_creditor_id,
                    "from_collector_hash": calc_hash(ct.from_creditor_id),
                    "amount": ct.amount,
                } for ct in collector_transfers
            ],
        )
        db.session.execute(
            insert(CollectorReceiving).execution_options(
                insertmanyvalues_page_size=INSERT_BATCH_SIZE
            ),
            [
                {
                    "turn_id": turn_id,
                    "debtor_id": ct.debtor_id,
                    "to_collector_id": ct.to_creditor_id,
                    "from_collector_id": ct.from_creditor_id,
                    "to_collector_hash": calc_hash(ct.to_creditor_id),
                    "amount": ct.amount,
                } for ct in collector_transfers
            ],
        )


def _write_givings(solver: Solver, turn_id: int) -> None:
    for account_changes in batched(solver.givings_iter(), INSERT_BATCH_SIZE):
        db.session.execute(
            insert(CollectorDispatching).execution_options(
                insertmanyvalues_page_size=INSERT_BATCH_SIZE
            ),
            [
                {
                    "turn_id": turn_id,
                    "debtor_id": ac.debtor_id,
                    "creditor_id": ac.creditor_id,
                    "amount": ac.amount,
                    "collector_id": ac.collector_id,
                    "collector_hash": calc_hash(ac.collector_id),
                } for ac in account_changes
            ],
        )
        db.session.execute(
            insert(CreditorGiving).execution_options(
                insertmanyvalues_page_size=INSERT_BATCH_SIZE
            ),
            [
                {
                    "turn_id": turn_id,
                    "creditor_id": ac.creditor_id,
                    "debtor_id": ac.debtor_id,
                    "creditor_hash": calc_hash(ac.creditor_id),
                    "amount": ac.amount,
                    "collector_id": ac.collector_id,
                } for ac in account_changes
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
        insert(OverloadedCurrency).from_select(
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


def _handle_overloaded_currencies() -> None:
    cfg = current_app.config
    min_collector_id = cfg["MIN_COLLECTOR_ID"]
    max_collector_id = cfg["MAX_COLLECTOR_ID"]

    with db.engines['solver'].connect() as conn:
        with conn.execution_options(yield_per=SELECT_BATCH_SIZE).execute(
                select(
                    OverloadedCurrency.turn_id,
                    OverloadedCurrency.debtor_id,
                    OverloadedCurrency.collectors_count,
                )
        ) as result:
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


def _handle_hoarded_currencies(turn_id: int) -> None:
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


def _disable_extra_collector_accounts() -> None:
    """Try to disable some of the active collector accounts (status ==
    2), so that the surplus amounts accumulated on them can be
    detected.

    NOTE: If possible, this function will not disable more than ~1/3
    of the active collector accounts for any given currency. Also,
    collector accounts that have not been active for at least
    `APP_COLLECTOR_ACTIVITY_MIN_DAYS` days, will not be disabled.
    """

    current_ts = datetime.now(tz=timezone.utc)
    activity_cutoff_ts = current_ts - timedelta(
        days=current_app.config["APP_COLLECTOR_ACTIVITY_MIN_DAYS"]
    )
    waiting_to_be_disabled = []

    @atomic
    def disable_waiting():
        if waiting_to_be_disabled:
            db.session.execute(
                update(CollectorAccount)
                .where(
                    and_(
                        COLLECTOR_ACCOUNT_PK.in_(waiting_to_be_disabled),
                        CollectorAccount.status == 2,
                    )
                )
                .values(
                    status=3,
                    latest_status_change_at=current_ts,
                )
            )
            waiting_to_be_disabled.clear()

    with db.engines['solver'].connect() as conn:
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
                    waiting_to_be_disabled.append(
                        (debtor_id, row.collector_id)
                    )
                    if len(waiting_to_be_disabled) > 5000:  # pragma: no cover
                        disable_waiting()

            disable_waiting()
