import math
from typing import TypeVar, Callable
from datetime import datetime, timezone
from sqlalchemy import select, insert, delete, text, func, union_all
from sqlalchemy.sql.expression import and_
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
)
from swpt_trade import procedures
from swpt_trade.solver import Solver
from swpt_trade.utils import batched, calc_hash

INSERT_BATCH_SIZE = 50000
SELECT_BATCH_SIZE = 50000


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
        _ensure_enough_collector_accounts(turn_id)

        turn.phase = 3
        turn.phase_deadline = None
        turn.collection_started_at = datetime.now(tz=timezone.utc)

        # NOTE: When reaching turn phase 3, all records for the given
        # turn from the `CurrencyInfo`, `SellOffer`, and `BuyOffer`
        # tables will be deleted. This however, does not guarantee
        # that a worker process will not continue to insert new rows
        # for the given turn in these tables. Therefore, in order to
        # ensure that such obsolete records will be deleted
        # eventually, here we delete all records for which the turn
        # phase 3 has been reached.
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


def _ensure_enough_collector_accounts(turn_id: int) -> None:
    cfg = current_app.config
    max_count = cfg["TRANSFERS_COLLECTOR_LIMIT"]
    assert max_count >= 0

    dispatching_overshoots = (
        select(
            CollectorDispatching.debtor_id.label("debtor_id"),
            CollectorDispatching.collector_id.label("collector_id"),
            func.count(CollectorDispatching.creditor_id).label("count"),
        )
        .where(CollectorDispatching.turn_id == turn_id)
        .group_by(
            CollectorDispatching.debtor_id,
            CollectorDispatching.collector_id,
        )
        .having(
            func.count(CollectorDispatching.creditor_id) > max_count
        )
    )
    collecting_overshoots = (
        select(
            CollectorCollecting.debtor_id.label("debtor_id"),
            CollectorCollecting.collector_id.label("collector_id"),
            func.count(CollectorCollecting.creditor_id).label("count"),
        )
        .where(CollectorCollecting.turn_id == turn_id)
        .group_by(
            CollectorCollecting.debtor_id,
            CollectorCollecting.collector_id,
        )
        .having(
            func.count(CollectorCollecting.creditor_id) > max_count
        )
    )
    overshoots = (
        union_all(dispatching_overshoots, collecting_overshoots)
        .subquery(name="overshoots")
    )

    overshooted_currencies = db.session.execute(
        select(
            overshoots.c.debtor_id.label("debtor_id"),
            func.max(overshoots.c.count).label("transfers_count"),
        )
        .select_from(overshoots)
        .group_by(overshoots.c.debtor_id)
    ).all()

    for currency in overshooted_currencies:
        try:
            overshooting_factor = currency.transfers_count / max_count
        except ZeroDivisionError:
            # This happens only when `max_count == 0`. This case
            # is allowed only for testing purposes.
            overshooting_factor = 1.0

        # TODO: This can deadlock very badly! Instead, write a record
        # to a new table, and implement a CLI command that reads the
        # table and executes `ensure_collector_accounts` for each
        # record.
        procedures.ensure_collector_accounts(
            debtor_id=currency.debtor_id,
            min_collector_id=cfg["MIN_COLLECTOR_ID"],
            max_collector_id=cfg["MAX_COLLECTOR_ID"],
            number_of_doublings=math.ceil(math.log(overshooting_factor, 2.0)),
        )
