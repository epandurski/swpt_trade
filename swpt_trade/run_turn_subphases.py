import logging
import math
from typing import TypeVar, Callable
from datetime import datetime, timezone, timedelta
from itertools import groupby
from sqlalchemy import select, insert, update, delete, text, bindparam, Numeric
from sqlalchemy.orm import load_only
from sqlalchemy.exc import IntegrityError
from sqlalchemy.sql.expression import (
    null,
    false,
    case,
    cast,
    func,
    and_,
    not_,
    tuple_,
)
from sqlalchemy.sql.functions import coalesce
from flask import current_app
from swpt_pythonlib.utils import ShardingRealm
from swpt_trade.utils import (
    batched,
    u16_to_i16,
    calc_demurrage,
    contain_principal_overflow,
    DispatchingData,
)
from swpt_trade.extensions import db
from swpt_trade.solver import CandidateOfferAuxData, BidProcessor
from swpt_trade.models import (
    TS0,
    MAX_INT64,
    DebtorInfoDocument,
    DebtorLocatorClaim,
    DebtorInfo,
    ConfirmedDebtor,
    WorkerTurn,
    CurrencyInfo,
    TradingPolicy,
    WorkerAccount,
    NeededWorkerAccount,
    CandidateOfferSignal,
    NeededCollectorSignal,
    ReviseAccountLockSignal,
    CalculateSurplusSignal,
    CollectorAccount,
    HoardedCurrency,
    UsableCollector,
    AccountLock,
    SellOffer,
    BuyOffer,
    CreditorParticipation,
    DispatchingStatus,
    WorkerCollecting,
    WorkerSending,
    WorkerReceiving,
    WorkerDispatching,
    CreditorTaking,
    CreditorGiving,
    CollectorCollecting,
    CollectorSending,
    CollectorReceiving,
    CollectorDispatching,
    CollectorStatusChange,
    InterestRateChange,
)

NEEDED_WORKER_ACCOUNT_PK = tuple_(
    NeededWorkerAccount.creditor_id,
    NeededWorkerAccount.debtor_id,
)
KILL_BROKEN_ACCOUNTS_BATCH_SIZE = 1000
INSERT_BATCH_SIZE = 5000
UPDATE_BATCH_SIZE = 5000
DELETE_BATCH_SIZE = 5000
SELECT_BATCH_SIZE = 50000
BID_COUNTER_THRESHOLD = 100000
DELETION_FLAG = WorkerAccount.CONFIG_SCHEDULED_FOR_DELETION_FLAG
NUMERIC = Numeric(36, 0)
TD_DAY = timedelta(days=1)

T = TypeVar("T")
atomic: Callable[[T], T] = db.atomic


@atomic
def run_phase1_subphase0(turn_id: int) -> None:
    worker_turn = (
        WorkerTurn.query
        .filter_by(
            turn_id=turn_id,
            phase=1,
            worker_turn_subphase=0,
        )
        .with_for_update()
        .one_or_none()
    )
    if worker_turn:
        if worker_turn.phase_deadline > datetime.now(tz=timezone.utc):
            with (
                    db.engine.connect() as w_conn,
                    db.engines["solver"].connect() as s_conn,
            ):
                _populate_debtor_infos(w_conn, s_conn, turn_id)
                _populate_confirmed_debtors(w_conn, s_conn, turn_id)
                _populate_hoarded_currencies(w_conn, s_conn, turn_id)

        worker_turn.worker_turn_subphase = 10


def _populate_debtor_infos(w_conn, s_conn, turn_id):
    sharding_realm: ShardingRealm = current_app.config["SHARDING_REALM"]

    with w_conn.execution_options(yield_per=SELECT_BATCH_SIZE).execute(
            select(
                DebtorInfoDocument.debtor_info_locator,
                DebtorInfoDocument.debtor_id,
                DebtorInfoDocument.peg_debtor_info_locator,
                DebtorInfoDocument.peg_debtor_id,
                DebtorInfoDocument.peg_exchange_rate,
            )
    ) as result:
        for rows in result.partitions(INSERT_BATCH_SIZE):
            dicts_to_insert = [
                {
                    "turn_id": turn_id,
                    "debtor_info_locator": row.debtor_info_locator,
                    "debtor_id": row.debtor_id,
                    "peg_debtor_info_locator": row.peg_debtor_info_locator,
                    "peg_debtor_id": row.peg_debtor_id,
                    "peg_exchange_rate": row.peg_exchange_rate,
                }
                for row in rows
                if sharding_realm.match_str(row.debtor_info_locator)
            ]
            if dicts_to_insert:
                try:
                    s_conn.execute(
                        insert(DebtorInfo).execution_options(
                            insertmanyvalues_page_size=INSERT_BATCH_SIZE,
                            synchronize_session=False,
                        ),
                        dicts_to_insert,
                    )
                except IntegrityError:
                    logger = logging.getLogger(__name__)
                    logger.warning(
                        "An attempt has been made to insert an already"
                        " existing debtor info row for turn %d.",
                        turn_id,
                    )
                    s_conn.rollback()
                    break
        else:
            s_conn.commit()


def _populate_confirmed_debtors(w_conn, s_conn, turn_id):
    sharding_realm: ShardingRealm = current_app.config["SHARDING_REALM"]

    with w_conn.execution_options(yield_per=SELECT_BATCH_SIZE).execute(
            select(
                DebtorLocatorClaim.debtor_id,
                DebtorLocatorClaim.debtor_info_locator,
            )
            .where(DebtorLocatorClaim.debtor_info_locator != null())
    ) as result:
        for rows in result.partitions(INSERT_BATCH_SIZE):
            dicts_to_insert = [
                {
                    "turn_id": turn_id,
                    "debtor_info_locator": row.debtor_info_locator,
                    "debtor_id": row.debtor_id,
                }
                for row in rows
                if sharding_realm.match(row.debtor_id)
            ]
            if dicts_to_insert:
                try:
                    s_conn.execute(
                        insert(ConfirmedDebtor).execution_options(
                            insertmanyvalues_page_size=INSERT_BATCH_SIZE,
                            synchronize_session=False,
                        ),
                        dicts_to_insert,
                    )
                except IntegrityError:
                    logger = logging.getLogger(__name__)
                    logger.warning(
                        "An attempt has been made to insert an already"
                        " existing confirmed debtor row for turn %d.",
                        turn_id,
                    )
                    s_conn.rollback()
                    break
        else:
            s_conn.commit()


def _populate_hoarded_currencies(w_conn, s_conn, turn_id):
    cfg = current_app.config
    sharding_realm: ShardingRealm = cfg["SHARDING_REALM"]
    owner_creditor_id = cfg["OWNER_CREDITOR_ID"]

    if sharding_realm.match(owner_creditor_id):
        with w_conn.execution_options(yield_per=SELECT_BATCH_SIZE).execute(
                select(
                    TradingPolicy.debtor_id,
                    TradingPolicy.peg_debtor_id,
                    TradingPolicy.peg_exchange_rate,
                )
                .where(
                    TradingPolicy.creditor_id == owner_creditor_id,
                    TradingPolicy.account_id != "",
                    TradingPolicy.account_id_is_obsolete == false(),
                    TradingPolicy.config_flags.op("&")(DELETION_FLAG) == 0,
                    TradingPolicy.policy_name != null(),
                    TradingPolicy.principal < TradingPolicy.min_principal,
                    TradingPolicy.min_principal <= TradingPolicy.max_principal,
                )
        ) as result:
            for rows in result.partitions(INSERT_BATCH_SIZE):
                dicts_to_insert = [
                    {
                        "turn_id": turn_id,
                        "debtor_id": row.debtor_id,
                        "peg_debtor_id": row.peg_debtor_id,
                        "peg_exchange_rate": row.peg_exchange_rate,
                    }
                    for row in rows
                ]
                try:
                    s_conn.execute(
                        insert(HoardedCurrency).execution_options(
                            insertmanyvalues_page_size=INSERT_BATCH_SIZE,
                            synchronize_session=False,
                        ),
                        dicts_to_insert,
                    )
                except IntegrityError:
                    logger = logging.getLogger(__name__)
                    logger.warning(
                        "An attempt has been made to insert an already"
                        " existing hoarded currency row for turn %d.",
                        turn_id,
                    )
                    s_conn.rollback()
                    break
            else:
                s_conn.commit()


@atomic
def run_phase2_subphase0(turn_id: int) -> None:
    worker_turn = (
        WorkerTurn.query
        .filter_by(
            turn_id=turn_id,
            phase=2,
            worker_turn_subphase=0,
        )
        .with_for_update()
        .one_or_none()
    )
    if worker_turn:
        if worker_turn.phase_deadline > datetime.now(tz=timezone.utc):
            bp = BidProcessor(
                worker_turn.base_debtor_info_locator,
                worker_turn.base_debtor_id,
                worker_turn.max_distance_to_base,
                worker_turn.min_trade_amount,
            )
            _load_currencies(bp, turn_id)
            _generate_user_candidate_offers(bp, turn_id)
            _generate_owner_candidate_offers(
                bp,
                turn_id,
                worker_turn.collection_deadline,
            )
            _copy_usable_collectors(bp)
            _insert_needed_collector_signals(bp)

        worker_turn.worker_turn_subphase = 5


def _load_currencies(bp: BidProcessor, turn_id: int) -> None:
    with db.engines["solver"].connect() as s_conn:
        with s_conn.execution_options(yield_per=SELECT_BATCH_SIZE).execute(
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
                    bp.register_currency(row[0], row[1], row[2])
                else:
                    bp.register_currency(*row)


def _generate_user_candidate_offers(bp, turn_id):
    current_ts = datetime.now(tz=timezone.utc)
    sharding_realm: ShardingRealm = current_app.config["SHARDING_REALM"]
    bid_counter = 0

    def calc_user_bid_amount(row) -> int:
        if (
                row.is_scheduled_for_deletion
                or not row.has_account_id
                or not row.wants_to_trade
                or row.max_principal < row.min_principal
                or row.min_principal <= row.principal <= row.max_principal
        ):
            return 0

        if row.principal < row.min_principal:
            # Return a positive number (buy).
            return contain_principal_overflow(
                row.min_principal - row.principal
            )

        # Return a negative number (sell).
        assert row.principal > row.max_principal
        return contain_principal_overflow(
            row.max_principal - row.principal
        )

    with db.engine.connect() as w_conn:
        with w_conn.execution_options(yield_per=SELECT_BATCH_SIZE).execute(
                select(
                    TradingPolicy.creditor_id,
                    TradingPolicy.debtor_id,
                    TradingPolicy.creation_date,
                    TradingPolicy.principal,
                    TradingPolicy.last_transfer_number,
                    TradingPolicy.min_principal,
                    TradingPolicy.max_principal,
                    TradingPolicy.peg_debtor_id,
                    TradingPolicy.peg_exchange_rate,
                    and_(
                        TradingPolicy.account_id != "",
                        TradingPolicy.account_id_is_obsolete == false(),
                    ).label("has_account_id"),
                    (
                        TradingPolicy.config_flags.op("&")(DELETION_FLAG) != 0
                    ).label("is_scheduled_for_deletion"),
                    (
                        TradingPolicy.policy_name != null()
                    ).label("wants_to_trade"),
                )
                .order_by(TradingPolicy.creditor_id)
        ) as result:
            for creditor_id, rows in groupby(result, lambda r: r.creditor_id):
                if sharding_realm.match(creditor_id):
                    for row in rows:
                        assert row.creditor_id == creditor_id
                        peg_rate = row.peg_exchange_rate

                        bp.register_bid(
                            creditor_id,
                            row.debtor_id,
                            calc_user_bid_amount(row),
                            row.peg_debtor_id or 0,
                            math.nan if peg_rate is None else peg_rate,
                            CandidateOfferAuxData(
                                creation_date=row.creation_date,
                                last_transfer_number=row.last_transfer_number,
                            ),
                        )
                        bid_counter += 1

                    # Process the registered bids when they become too
                    # many, so that they can not use up the available
                    # memory.
                    if bid_counter >= BID_COUNTER_THRESHOLD:
                        _process_bids(bp, turn_id, current_ts)
                        bid_counter = 0

            _process_bids(bp, turn_id, current_ts)


def _generate_owner_candidate_offers(bp, turn_id, collection_deadline):
    current_ts = datetime.now(tz=timezone.utc)
    sharding_realm: ShardingRealm = current_app.config["SHARDING_REALM"]
    bid_counter = 0

    def calc_owner_bid_amount(row, hoarded) -> int:
        if row.is_scheduled_for_deletion or not row.has_account_id:
            return 0  # Do not trade.
        if hoarded:
            # Return a huge positive number (buy without limit).
            return MAX_INT64
        worse_surplus_demurrage = calc_demurrage(
            row.demurrage_rate,
            collection_deadline - min(row.surplus_ts, current_ts),
        )
        available_surplus_amount = max(
            0,
            + math.floor(row.surplus_amount * worse_surplus_demurrage)
            - row.surplus_spent_amount
        )
        # We must make sure that the amount locked during the trading
        # turn will never exceed the available surplus. To compensate
        # for the possible demurrage, the locked amount will be bigger
        # than the bid amount. Therefore, we must factor the possible
        # demurrage again, and also add some safety cushion.
        lock_correction_factor = 0.9999 * calc_demurrage(
            row.demurrage_rate, collection_deadline - current_ts
        )
        # Return a negative number (sell the available surplus).
        return contain_principal_overflow(
            - max(
                0,
                + math.floor(available_surplus_amount * lock_correction_factor)
                - 1
            )
        )

    with db.engines["solver"].connect() as s_conn:
        hoarded_currency_pegs = {
            row.debtor_id: (row.peg_debtor_id, row.peg_exchange_rate)
            for row in s_conn.execute(
                select(
                    HoardedCurrency.debtor_id,
                    HoardedCurrency.peg_debtor_id,
                    HoardedCurrency.peg_exchange_rate,
                )
                .where(HoardedCurrency.turn_id == turn_id)
            ).all()
        }

    with db.engine.connect() as w_conn:
        # NOTE: Disabled collector accounts (status == 3) must not try
        # to sell their surplus amounts, because this may interfere
        # with correctly determining the new surplus amounts.
        surplus_amount_expression = case(
            (NeededWorkerAccount.collection_disabled_since != null(), 0),
            else_=WorkerAccount.surplus_amount
        )
        with w_conn.execution_options(yield_per=SELECT_BATCH_SIZE).execute(
                select(
                    WorkerAccount.creditor_id,
                    WorkerAccount.debtor_id,
                    WorkerAccount.creation_date,
                    WorkerAccount.demurrage_rate,
                    surplus_amount_expression.label("surplus_amount"),
                    WorkerAccount.surplus_ts,
                    WorkerAccount.surplus_spent_amount,
                    (
                        WorkerAccount.surplus_last_transfer_number
                    ).label("last_transfer_number"),
                    (
                        WorkerAccount.account_id != ""
                    ).label("has_account_id"),
                    (
                        WorkerAccount.config_flags.op("&")(DELETION_FLAG) != 0
                    ).label("is_scheduled_for_deletion"),
                )
                .select_from(WorkerAccount)
                .join(
                    NeededWorkerAccount,
                    and_(
                        NeededWorkerAccount.creditor_id
                        == WorkerAccount.creditor_id,
                        NeededWorkerAccount.debtor_id
                        == WorkerAccount.debtor_id,
                    ),
                )
                .order_by(WorkerAccount.creditor_id)
        ) as result:
            for creditor_id, rows in groupby(result, lambda r: r.creditor_id):
                if sharding_realm.match(creditor_id):
                    for row in rows:
                        assert row.creditor_id == creditor_id
                        debtor_id = row.debtor_id
                        hoarded = hoarded_currency_pegs.get(debtor_id)
                        peg_debtor_id, peg_rate = (
                            # For hoarded currencies -- insist on
                            # using the peg provided by the owner of
                            # the creditors agent node. For other
                            # currencies -- accept the peg provided by
                            # the issuer of the currency.
                            hoarded or bp.get_tradable_currency_peg(debtor_id)
                        )
                        bp.register_bid(
                            creditor_id,
                            debtor_id,
                            calc_owner_bid_amount(row, hoarded),
                            peg_debtor_id or 0,
                            math.nan if peg_rate is None else peg_rate,
                            CandidateOfferAuxData(
                                creation_date=row.creation_date,
                                last_transfer_number=row.last_transfer_number,
                            ),
                        )
                        bid_counter += 1

                    # Process the registered bids when they become too
                    # many, so that they can not use up the available
                    # memory.
                    if bid_counter >= BID_COUNTER_THRESHOLD:
                        _process_bids(bp, turn_id, current_ts)
                        bid_counter = 0

            _process_bids(bp, turn_id, current_ts)


def _process_bids(bp: BidProcessor, turn_id: int, ts: datetime) -> None:
    for candidate_offers in batched(bp.analyze_bids(), INSERT_BATCH_SIZE):
        db.session.execute(
            insert(CandidateOfferSignal).execution_options(
                insertmanyvalues_page_size=INSERT_BATCH_SIZE,
                synchronize_session=False,
            ),
            [
                {
                    "turn_id": turn_id,
                    "amount": o.amount,
                    "debtor_id": o.debtor_id,
                    "creditor_id": o.creditor_id,
                    "account_creation_date": o.aux_data.creation_date,
                    "last_transfer_number": o.aux_data.last_transfer_number,
                    "inserted_at": ts,
                }
                for o in candidate_offers
            ],
        )


def _copy_usable_collectors(bp: BidProcessor) -> None:
    with db.engines["solver"].connect() as s_conn:
        UsableCollector.query.delete(synchronize_session=False)

        with s_conn.execution_options(yield_per=SELECT_BATCH_SIZE).execute(
                select(
                    CollectorAccount.debtor_id,
                    CollectorAccount.collector_id,
                    CollectorAccount.account_id,
                    CollectorAccount.status
                )
        ) as result:
            for rows in result.partitions(INSERT_BATCH_SIZE):
                dicts_to_insert = [
                    {
                        "debtor_id": row.debtor_id,
                        "collector_id": row.collector_id,
                        "account_id": row.account_id,
                        "disabled_at": (
                            None
                            if row.status == 2
                            else row.latest_status_change_at
                        ),
                    }
                    for row in rows if row.status >= 2
                ]
                if dicts_to_insert:
                    db.session.execute(
                        insert(UsableCollector).execution_options(
                            insertmanyvalues_page_size=INSERT_BATCH_SIZE,
                            synchronize_session=False,
                        ),
                        dicts_to_insert,
                    )

                for row in rows:
                    bp.remove_currency_to_be_confirmed(row.debtor_id)


def _insert_needed_collector_signals(bp: BidProcessor) -> None:
    current_ts = datetime.now(tz=timezone.utc)

    for debtor_ids in batched(
            bp.currencies_to_be_confirmed(), INSERT_BATCH_SIZE
    ):
        db.session.execute(
            insert(NeededCollectorSignal).execution_options(
                insertmanyvalues_page_size=INSERT_BATCH_SIZE,
                synchronize_session=False,
            ),
            [
                {
                    "debtor_id": debtor_id,
                    "inserted_at": current_ts,
                }
                for debtor_id in debtor_ids
            ],
        )


@atomic
def run_phase2_subphase5(turn_id: int) -> None:
    worker_turn = (
        WorkerTurn.query
        .filter_by(
            turn_id=turn_id,
            phase=2,
            worker_turn_subphase=5,
        )
        .with_for_update()
        .one_or_none()
    )
    if worker_turn:
        if worker_turn.phase_deadline > datetime.now(tz=timezone.utc):
            with (
                    db.engine.connect() as w_conn,
                    db.engines["solver"].connect() as s_conn,
            ):
                _populate_sell_offers(w_conn, s_conn, turn_id)
                _populate_buy_offers(w_conn, s_conn, turn_id)

        worker_turn.worker_turn_subphase = 10


def _populate_sell_offers(w_conn, s_conn, turn_id):
    sharding_realm: ShardingRealm = current_app.config["SHARDING_REALM"]

    with w_conn.execution_options(yield_per=SELECT_BATCH_SIZE).execute(
            select(
                AccountLock.creditor_id,
                AccountLock.debtor_id,
                AccountLock.amount,
                AccountLock.collector_id,
            )
            .where(
                AccountLock.turn_id == turn_id,
                AccountLock.released_at == null(),
                AccountLock.transfer_id != null(),
                AccountLock.finalized_at == null(),
                AccountLock.amount < 0,
            )
    ) as result:
        for rows in result.partitions(INSERT_BATCH_SIZE):
            dicts_to_insert = [
                {
                    "turn_id": turn_id,
                    "creditor_id": row.creditor_id,
                    "debtor_id": row.debtor_id,
                    "amount": -row.amount,
                    "collector_id": row.collector_id,
                }
                for row in rows
                if sharding_realm.match(row.creditor_id)
            ]
            if dicts_to_insert:
                try:
                    s_conn.execute(
                        insert(SellOffer).execution_options(
                            insertmanyvalues_page_size=INSERT_BATCH_SIZE,
                            synchronize_session=False,
                        ),
                        dicts_to_insert,
                    )
                except IntegrityError:
                    logger = logging.getLogger(__name__)
                    logger.warning(
                        "An attempt has been made to insert an already"
                        " existing sell offer row for turn %d.",
                        turn_id,
                    )
                    s_conn.rollback()
                    break
        else:
            s_conn.commit()


def _populate_buy_offers(w_conn, s_conn, turn_id):
    sharding_realm: ShardingRealm = current_app.config["SHARDING_REALM"]

    with w_conn.execution_options(yield_per=SELECT_BATCH_SIZE).execute(
            select(
                AccountLock.creditor_id,
                AccountLock.debtor_id,
                AccountLock.amount,
            )
            .where(
                AccountLock.turn_id == turn_id,
                AccountLock.released_at == null(),
                AccountLock.transfer_id != null(),
                AccountLock.finalized_at == null(),
                AccountLock.amount > 0,
            )
    ) as result:
        for rows in result.partitions(INSERT_BATCH_SIZE):
            dicts_to_insert = [
                {
                    "turn_id": turn_id,
                    "creditor_id": row.creditor_id,
                    "debtor_id": row.debtor_id,
                    "amount": row.amount,
                }
                for row in rows
                if sharding_realm.match(row.creditor_id)
            ]
            if dicts_to_insert:
                try:
                    s_conn.execute(
                        insert(BuyOffer).execution_options(
                            insertmanyvalues_page_size=INSERT_BATCH_SIZE,
                            synchronize_session=False,
                        ),
                        dicts_to_insert,
                    )
                except IntegrityError:
                    logger = logging.getLogger(__name__)
                    logger.warning(
                        "An attempt has been made to insert an already"
                        " existing buy offer row for turn %d.",
                        turn_id,
                    )
                    s_conn.rollback()
                    break
        else:
            s_conn.commit()


@atomic
def run_phase3_subphase0(turn_id: int) -> None:
    worker_turn = (
        WorkerTurn.query
        .filter_by(
            turn_id=turn_id,
            phase=3,
            worker_turn_subphase=0,
        )
        .with_for_update()
        .one_or_none()
    )
    if worker_turn:
        statuses = DispatchingData(worker_turn.turn_id)

        with db.engines["solver"].connect() as s_conn:
            _copy_creditor_takings(s_conn, worker_turn)
            _copy_creditor_givings(s_conn, worker_turn)
            _copy_collector_collectings(s_conn, worker_turn, statuses)
            _copy_collector_sendings(s_conn, worker_turn, statuses)
            _copy_collector_receivings(s_conn, worker_turn, statuses)
            _copy_collector_dispatchings(s_conn, worker_turn, statuses)
            _create_dispatching_statuses(worker_turn, statuses)
            _insert_revise_account_lock_signals(worker_turn)

        worker_turn.worker_turn_subphase = 5


def _copy_creditor_takings(s_conn, worker_turn):
    turn_id = worker_turn.turn_id
    sharding_realm: ShardingRealm = current_app.config["SHARDING_REALM"]
    hash_prefix = u16_to_i16(sharding_realm.realm >> 16)
    hash_mask = u16_to_i16(sharding_realm.realm_mask >> 16)

    with s_conn.execution_options(yield_per=SELECT_BATCH_SIZE).execute(
            select(
                CreditorTaking.turn_id,
                CreditorTaking.creditor_id,
                CreditorTaking.debtor_id,
                CreditorTaking.amount,
                CreditorTaking.collector_id,
            )
            .where(
                CreditorTaking.turn_id == turn_id,
                CreditorTaking.creditor_hash.op("&")(hash_mask) == hash_prefix,
            )
    ) as result:
        for rows in result.partitions(INSERT_BATCH_SIZE):
            dicts_to_insert = [
                {
                    "turn_id": turn_id,
                    "creditor_id": row.creditor_id,
                    "debtor_id": row.debtor_id,
                    "amount": (-row.amount),
                    "collector_id": row.collector_id,
                }
                for row in rows
            ]
            assert all(
                sharding_realm.match(r["creditor_id"])
                for r in dicts_to_insert
            )
            db.session.execute(
                insert(CreditorParticipation).execution_options(
                    insertmanyvalues_page_size=INSERT_BATCH_SIZE,
                    synchronize_session=False,
                ),
                dicts_to_insert,
            )


def _copy_creditor_givings(s_conn, worker_turn):
    turn_id = worker_turn.turn_id
    sharding_realm: ShardingRealm = current_app.config["SHARDING_REALM"]
    hash_prefix = u16_to_i16(sharding_realm.realm >> 16)
    hash_mask = u16_to_i16(sharding_realm.realm_mask >> 16)

    with s_conn.execution_options(yield_per=SELECT_BATCH_SIZE).execute(
            select(
                CreditorGiving.turn_id,
                CreditorGiving.creditor_id,
                CreditorGiving.debtor_id,
                CreditorGiving.amount,
                CreditorGiving.collector_id,
            )
            .where(
                CreditorGiving.turn_id == turn_id,
                CreditorGiving.creditor_hash.op("&")(hash_mask) == hash_prefix,
                CreditorGiving.amount > 1,
            )
    ) as result:
        for rows in result.partitions(INSERT_BATCH_SIZE):
            dicts_to_insert = [
                {
                    "turn_id": turn_id,
                    "creditor_id": row.creditor_id,
                    "debtor_id": row.debtor_id,
                    "amount": row.amount,
                    "collector_id": row.collector_id,
                }
                for row in rows
            ]
            assert all(
                sharding_realm.match(r["creditor_id"])
                for r in dicts_to_insert
            )
            db.session.execute(
                insert(CreditorParticipation).execution_options(
                    insertmanyvalues_page_size=INSERT_BATCH_SIZE,
                    synchronize_session=False,
                ),
                dicts_to_insert,
            )


def _copy_collector_collectings(s_conn, worker_turn, statuses):
    turn_id = worker_turn.turn_id
    cfg = current_app.config
    purge_after = (
        worker_turn.collection_deadline
        + timedelta(days=cfg["APP_WORKER_COLLECTING_SLACK_DAYS"])
    )
    sharding_realm: ShardingRealm = cfg["SHARDING_REALM"]
    hash_prefix = u16_to_i16(sharding_realm.realm >> 16)
    hash_mask = u16_to_i16(sharding_realm.realm_mask >> 16)

    with s_conn.execution_options(yield_per=SELECT_BATCH_SIZE).execute(
            select(
                CollectorCollecting.turn_id,
                CollectorCollecting.debtor_id,
                CollectorCollecting.creditor_id,
                CollectorCollecting.amount,
                CollectorCollecting.collector_id,
            )
            .where(
                CollectorCollecting.turn_id == turn_id,
                CollectorCollecting.collector_hash.op("&")(hash_mask)
                == hash_prefix,
                CollectorCollecting.creditor_id
                != CollectorCollecting.collector_id,
            )
    ) as result:
        for rows in result.partitions(INSERT_BATCH_SIZE):
            dicts_to_insert = [
                {
                    "collector_id": row.collector_id,
                    "debtor_id": row.debtor_id,
                    "turn_id": turn_id,
                    "creditor_id": row.creditor_id,
                    "amount": row.amount,
                    "collected": False,
                    "purge_after": purge_after,
                }
                for row in rows
            ]
            assert all(
                sharding_realm.match(r["collector_id"])
                for r in dicts_to_insert
            )
            db.session.execute(
                insert(WorkerCollecting).execution_options(
                    insertmanyvalues_page_size=INSERT_BATCH_SIZE,
                    synchronize_session=False,
                ),
                dicts_to_insert,
            )
            for d in dicts_to_insert:
                statuses.register_collecting(
                    d["collector_id"],
                    d["debtor_id"],
                    d["turn_id"],
                    d["amount"],
                )


def _copy_collector_sendings(s_conn, worker_turn, statuses):
    turn_id = worker_turn.turn_id
    cfg = current_app.config
    purge_after = (
        worker_turn.collection_deadline
        + timedelta(days=cfg["APP_WORKER_SENDING_SLACK_DAYS"])
    )
    sharding_realm: ShardingRealm = cfg["SHARDING_REALM"]
    hash_prefix = u16_to_i16(sharding_realm.realm >> 16)
    hash_mask = u16_to_i16(sharding_realm.realm_mask >> 16)

    with s_conn.execution_options(yield_per=SELECT_BATCH_SIZE).execute(
            select(
                CollectorSending.turn_id,
                CollectorSending.debtor_id,
                CollectorSending.from_collector_id,
                CollectorSending.to_collector_id,
                CollectorSending.amount,
            )
            .where(
                CollectorSending.turn_id == turn_id,
                CollectorSending.from_collector_hash.op("&")(hash_mask)
                == hash_prefix,
                CollectorSending.amount > 1,
            )
    ) as result:
        for rows in result.partitions(INSERT_BATCH_SIZE):
            dicts_to_insert = [
                {
                    "from_collector_id": row.from_collector_id,
                    "turn_id": turn_id,
                    "debtor_id": row.debtor_id,
                    "to_collector_id": row.to_collector_id,
                    "amount": row.amount,
                    "purge_after": purge_after,
                }
                for row in rows
            ]
            assert all(
                sharding_realm.match(r["from_collector_id"])
                for r in dicts_to_insert
            )
            db.session.execute(
                insert(WorkerSending).execution_options(
                    insertmanyvalues_page_size=INSERT_BATCH_SIZE,
                    synchronize_session=False,
                ),
                dicts_to_insert,
            )
            for d in dicts_to_insert:
                statuses.register_sending(
                    d["from_collector_id"],
                    d["debtor_id"],
                    d["turn_id"],
                    d["amount"],
                )


def _copy_collector_receivings(s_conn, worker_turn, statuses):
    turn_id = worker_turn.turn_id
    cfg = current_app.config
    purge_after = (
        worker_turn.collection_deadline
        + timedelta(days=cfg["APP_WORKER_SENDING_SLACK_DAYS"])
    )
    sharding_realm: ShardingRealm = cfg["SHARDING_REALM"]
    hash_prefix = u16_to_i16(sharding_realm.realm >> 16)
    hash_mask = u16_to_i16(sharding_realm.realm_mask >> 16)

    with s_conn.execution_options(yield_per=SELECT_BATCH_SIZE).execute(
            select(
                CollectorReceiving.turn_id,
                CollectorReceiving.debtor_id,
                CollectorReceiving.to_collector_id,
                CollectorReceiving.from_collector_id,
                CollectorReceiving.amount,
            )
            .where(
                CollectorReceiving.turn_id == turn_id,
                CollectorReceiving.to_collector_hash.op("&")(hash_mask)
                == hash_prefix,
                CollectorReceiving.amount > 1,
            )
    ) as result:
        for rows in result.partitions(INSERT_BATCH_SIZE):
            dicts_to_insert = [
                {
                    "to_collector_id": row.to_collector_id,
                    "turn_id": turn_id,
                    "debtor_id": row.debtor_id,
                    "from_collector_id": row.from_collector_id,
                    "expected_amount": row.amount,
                    "received_amount": 0,
                    "purge_after": purge_after,
                }
                for row in rows
            ]
            assert all(
                sharding_realm.match(r["to_collector_id"])
                for r in dicts_to_insert
            )
            db.session.execute(
                insert(WorkerReceiving).execution_options(
                    insertmanyvalues_page_size=INSERT_BATCH_SIZE,
                    synchronize_session=False,
                ),
                dicts_to_insert,
            )
            for d in dicts_to_insert:
                statuses.register_receiving(
                    d["to_collector_id"],
                    d["debtor_id"],
                    d["turn_id"],
                    d["expected_amount"],
                )


def _copy_collector_dispatchings(s_conn, worker_turn, statuses):
    turn_id = worker_turn.turn_id
    cfg = current_app.config
    purge_after = (
        worker_turn.collection_deadline
        + timedelta(days=cfg["APP_WORKER_DISPATCHING_SLACK_DAYS"])
    )
    sharding_realm: ShardingRealm = cfg["SHARDING_REALM"]
    hash_prefix = u16_to_i16(sharding_realm.realm >> 16)
    hash_mask = u16_to_i16(sharding_realm.realm_mask >> 16)

    with s_conn.execution_options(yield_per=SELECT_BATCH_SIZE).execute(
            select(
                CollectorDispatching.turn_id,
                CollectorDispatching.debtor_id,
                CollectorDispatching.creditor_id,
                CollectorDispatching.amount,
                CollectorDispatching.collector_id,
            )
            .where(
                CollectorDispatching.turn_id == turn_id,
                CollectorDispatching.collector_hash.op("&")(hash_mask)
                == hash_prefix,
                CollectorDispatching.amount > 1,
                CollectorDispatching.creditor_id
                != CollectorDispatching.collector_id,
            )
    ) as result:
        for rows in result.partitions(INSERT_BATCH_SIZE):
            dicts_to_insert = [
                {
                    "collector_id": row.collector_id,
                    "debtor_id": row.debtor_id,
                    "turn_id": turn_id,
                    "creditor_id": row.creditor_id,
                    "amount": row.amount,
                    "purge_after": purge_after,
                }
                for row in rows
            ]
            assert all(
                sharding_realm.match(r["collector_id"])
                for r in dicts_to_insert
            )
            db.session.execute(
                insert(WorkerDispatching).execution_options(
                    insertmanyvalues_page_size=INSERT_BATCH_SIZE,
                    synchronize_session=False,
                ),
                dicts_to_insert,
            )
            for d in dicts_to_insert:
                statuses.register_dispatching(
                    d["collector_id"],
                    d["debtor_id"],
                    d["turn_id"],
                    d["amount"],
                )


def _create_dispatching_statuses(worker_turn, statuses):
    for status_dicts in batched(statuses.statuses_iter(), INSERT_BATCH_SIZE):
        db.session.execute(
            insert(DispatchingStatus).execution_options(
                insertmanyvalues_page_size=INSERT_BATCH_SIZE,
                synchronize_session=False,
            ),
            status_dicts,
        )


def _insert_revise_account_lock_signals(worker_turn):
    turn_id = worker_turn.turn_id
    current_ts = datetime.now(tz=timezone.utc)
    sharding_realm: ShardingRealm = current_app.config["SHARDING_REALM"]

    with db.engine.connect() as w_conn:
        with w_conn.execution_options(yield_per=SELECT_BATCH_SIZE).execute(
                select(
                    AccountLock.creditor_id,
                    AccountLock.debtor_id,
                )
                .where(AccountLock.turn_id == turn_id)
        ) as result:
            for rows in result.partitions(INSERT_BATCH_SIZE):
                dicts_to_insert = [
                    {
                        "creditor_id": row.creditor_id,
                        "debtor_id": row.debtor_id,
                        "turn_id": turn_id,
                        "inserted_at": current_ts,
                    }
                    for row in rows
                    if sharding_realm.match(row.creditor_id)
                ]
                if dicts_to_insert:
                    db.session.execute(
                        insert(ReviseAccountLockSignal).execution_options(
                            insertmanyvalues_page_size=INSERT_BATCH_SIZE,
                            synchronize_session=False,
                        ),
                        dicts_to_insert,
                    )


@atomic
def run_phase3_subphase5(turn_id: int) -> None:
    worker_turn = (
        WorkerTurn.query
        .filter_by(
            turn_id=turn_id,
            phase=3,
            worker_turn_subphase=5,
        )
        .with_for_update()
        .one_or_none()
    )
    if worker_turn:
        turn_id = worker_turn.turn_id
        sharding_realm: ShardingRealm = current_app.config["SHARDING_REALM"]
        hash_prefix = u16_to_i16(sharding_realm.realm >> 16)
        hash_mask = u16_to_i16(sharding_realm.realm_mask >> 16)

        with db.engines["solver"].connect() as s_conn:
            s_conn.execute(
                delete(CreditorTaking)
                .execution_options(synchronize_session=False)
                .where(
                    CreditorTaking.turn_id == turn_id,
                    CreditorTaking.creditor_hash.op("&")(hash_mask)
                    == hash_prefix,
                )
            )
            s_conn.execute(
                delete(CreditorGiving)
                .execution_options(synchronize_session=False)
                .where(
                    CreditorGiving.turn_id == turn_id,
                    CreditorGiving.creditor_hash.op("&")(hash_mask)
                    == hash_prefix,
                )
            )
            s_conn.execute(
                delete(CollectorCollecting)
                .execution_options(synchronize_session=False)
                .where(
                    CollectorCollecting.turn_id == turn_id,
                    CollectorCollecting.collector_hash.op("&")(hash_mask)
                    == hash_prefix,
                )
            )
            s_conn.execute(
                delete(CollectorSending)
                .execution_options(synchronize_session=False)
                .where(
                    CollectorSending.turn_id == turn_id,
                    CollectorSending.from_collector_hash.op("&")(hash_mask)
                    == hash_prefix,
                )
            )
            s_conn.execute(
                delete(CollectorReceiving)
                .execution_options(synchronize_session=False)
                .where(
                    CollectorReceiving.turn_id == turn_id,
                    CollectorReceiving.to_collector_hash.op("&")(hash_mask)
                    == hash_prefix,
                )
            )
            s_conn.execute(
                delete(CollectorDispatching)
                .execution_options(synchronize_session=False)
                .where(
                    CollectorDispatching.turn_id == turn_id,
                    CollectorDispatching.collector_hash.op("&")(hash_mask)
                    == hash_prefix,
                )
            )
            s_conn.commit()

        _update_needed_worker_account_disabled_since()
        _update_needed_worker_account_blocked_amounts()
        _update_worker_account_surplus_amounts()
        _kill_broken_worker_accounts()

        worker_turn.worker_turn_subphase = 10


def _update_needed_worker_account_disabled_since() -> None:
    """Update the `collection_disabled_since` column of all worker
    accounts.

    When a collector account gets disabled on the solver server, this
    fact must become known on the worker server which is responsible
    for that collector (worker) account.
    """

    db.session.execute(
        update(NeededWorkerAccount)
        .execution_options(synchronize_session=False)
        .where(
            NeededWorkerAccount.creditor_id == UsableCollector.collector_id,
            NeededWorkerAccount.debtor_id == UsableCollector.debtor_id,
            NeededWorkerAccount.collection_disabled_since.is_distinct_from(
                UsableCollector.disabled_at
            ),
        )
        .values(collection_disabled_since=UsableCollector.disabled_at)
    )


def _update_needed_worker_account_blocked_amounts() -> None:
    """Calculate `blocked_amount` and `blocked_amount_ts` columns of
    all worker accounts.

    For some of the disabled collector accounts, there might be
    pending transfers whose status we do not know yet. For the purpose
    of determining the surplus amounts, we must always assume the
    worst possible case. This worst possible case is represented by
    the `NeededWorkerAccount.blocked_amount` column. It represents the
    sum of all theoretically possible future withdrawals from the
    worker account.
    """

    current_ts = datetime.now(tz=timezone.utc)
    sbd = timedelta(days=current_app.config["APP_SURPLUS_BLOCKING_DELAY_DAYS"])

    # Unreleased locks for surplus amounts which the collector wanted
    # to sell.
    locked_subq = (
        select(AccountLock.max_locked_amount)
        .where(
            AccountLock.creditor_id == NeededWorkerAccount.creditor_id,
            AccountLock.debtor_id == NeededWorkerAccount.debtor_id,
            AccountLock.released_at == null(),
        )
        .scalar_subquery()
        .correlate(NeededWorkerAccount)
    )

    # Pending sending to other collector accounts, and dispatching
    # to buyers.
    to_relay_subq = (
        select(
            func.sum(
                cast(DispatchingStatus.amount_to_send, NUMERIC)
                + cast(DispatchingStatus.amount_to_dispatch, NUMERIC)
            )
        )
        .where(
            DispatchingStatus.collector_id == NeededWorkerAccount.creditor_id,
            DispatchingStatus.debtor_id == NeededWorkerAccount.debtor_id,
        )
        .scalar_subquery()
        .correlate(NeededWorkerAccount)
    )

    nwa_row_filter = and_(
        # We must calculate the blocked amount only after the
        # collection has been disabled for a week or two. This gives
        # enough time for most pending transfers to be finalized, and
        # ensures that we are aware of all trading turns in which the
        # given worker account had participated.
        NeededWorkerAccount.collection_disabled_since < current_ts - sbd,

        # We should not try to calculate the blocked amount twice. To
        # decide if the calculated blocked amount is up-to-date, we
        # consult the `blocked_amount_ts` field.
        NeededWorkerAccount.collection_disabled_since
        > coalesce(NeededWorkerAccount.blocked_amount_ts, TS0) - sbd,
    )

    # NOTE: Setting `blocked_amount_ts` to `current_ts` guarantees
    # that if the `nwa_row_filter` predicate is True now, it will be
    # False after the update.
    nwa = NeededWorkerAccount.__table__
    nwa_update_statement = (
        update(nwa)
        .where(
            nwa.c.creditor_id == bindparam("b_creditor_id"),
            nwa.c.debtor_id == bindparam("b_debtor_id"),
            nwa.c.collection_disabled_since < current_ts - sbd,
            nwa.c.collection_disabled_since
            > coalesce(nwa.c.blocked_amount_ts, TS0) - sbd,
        )
        .values(
            blocked_amount=bindparam("b_blocked_amount"),
            blocked_amount_ts=current_ts,
        )
    )

    with db.engine.connect() as w_conn:
        with w_conn.execution_options(yield_per=SELECT_BATCH_SIZE).execute(
                select(
                    NeededWorkerAccount.creditor_id,
                    NeededWorkerAccount.debtor_id,
                    coalesce(locked_subq, text("0")).label("locked"),
                    coalesce(to_relay_subq, text("0")).label("to_relay"),
                )
                .select_from(NeededWorkerAccount)
                .where(nwa_row_filter)
        ) as result:
            for rows in result.partitions(UPDATE_BATCH_SIZE):
                dicts_to_update = [
                    {
                        "b_creditor_id": row.creditor_id,
                        "b_debtor_id": row.debtor_id,
                        "b_blocked_amount": contain_principal_overflow(
                            int(row.locked + row.to_relay)
                        ),
                    }
                    for row in rows
                ]
                db.session.execute(nwa_update_statement, dicts_to_update)


def _update_worker_account_surplus_amounts() -> None:
    sharding_realm: ShardingRealm = current_app.config["SHARDING_REALM"]

    with db.engine.connect() as w_conn:
        with w_conn.execution_options(yield_per=SELECT_BATCH_SIZE).execute(
                select(
                    NeededWorkerAccount.creditor_id,
                    NeededWorkerAccount.debtor_id,
                )
                .select_from(NeededWorkerAccount)
                .join(
                    WorkerAccount,
                    and_(
                        WorkerAccount.creditor_id
                        == NeededWorkerAccount.creditor_id,
                        WorkerAccount.debtor_id
                        == NeededWorkerAccount.debtor_id,
                    ),
                )
                .where(
                    NeededWorkerAccount.blocked_amount_ts
                    >= NeededWorkerAccount.collection_disabled_since,
                    WorkerAccount.last_change_ts
                    > NeededWorkerAccount.blocked_amount_ts + TD_DAY
                )
        ) as result:
            for rows in result.partitions(DELETE_BATCH_SIZE):
                for row in rows:
                    signal_dicts = []
                    accounts_not_from_this_shard = []
                    if sharding_realm.match(row.creditor_id):
                        signal_dicts.append(
                            {
                                "collector_id": row.creditor_id,
                                "debtor_id": row.debtor_id,
                            }
                        )
                    else:
                        accounts_not_from_this_shard.append(row)

                    if signal_dicts:
                        db.session.execute(
                            insert(CalculateSurplusSignal).execution_options(
                                insertmanyvalues_page_size=INSERT_BATCH_SIZE,
                                synchronize_session=False,
                            ),
                            signal_dicts,
                        )
                    if accounts_not_from_this_shard:
                        _kill_needed_worker_accounts_and_rate_stats(
                            accounts_not_from_this_shard
                        )


def _kill_broken_worker_accounts() -> None:
    """Garbage collects broken worker accounts.

    Accounting authority nodes to which this node were connected, but
    is no longer connected to, will not send heartbeat `AccountUpdate`
    SMP messages. Therefore, all `WorkerAccount` records related to
    such accounting authority nodes will be deleted some time after
    the disconnection. Their corresponding `NeededWorkerAccount` (and
    `InterestRateChange`) records however, will still remain in the
    database. This function detects and removes (garbage collects)
    such dysfunctional records from the database.
    """

    sharding_realm: ShardingRealm = current_app.config["SHARDING_REALM"]
    worker_account_subquery = (
        select(1)
        .select_from(WorkerAccount)
        .where(
            WorkerAccount.creditor_id == NeededWorkerAccount.creditor_id,
            WorkerAccount.debtor_id == NeededWorkerAccount.debtor_id,
        )
    ).exists()

    with db.engine.connect() as w_conn:
        with w_conn.execution_options(yield_per=SELECT_BATCH_SIZE).execute(
                select(
                    NeededWorkerAccount.creditor_id,
                    NeededWorkerAccount.debtor_id,
                )
                .select_from(NeededWorkerAccount)
                .where(
                    NeededWorkerAccount.blocked_amount_ts
                    >= NeededWorkerAccount.collection_disabled_since,
                    not_(worker_account_subquery),
                )
        ) as result:
            for rows in result.partitions(KILL_BROKEN_ACCOUNTS_BATCH_SIZE):
                _kill_needed_worker_accounts_and_rate_stats(rows)
                status_change_dicts = (
                    {
                        "collector_id": row.creditor_id,
                        "debtor_id": row.debtor_id,
                        "from_status": 3,
                        "to_status": 1,
                        "account_id": None,
                    }
                    for row in rows
                    if sharding_realm.match(row.creditor_id)
                )
                if status_change_dicts:
                    db.session.execute(
                        insert(CollectorStatusChange).execution_options(
                            insertmanyvalues_page_size=INSERT_BATCH_SIZE,
                            synchronize_session=False,
                        ),
                        status_change_dicts,
                    )


def _kill_needed_worker_accounts_and_rate_stats(primary_keys) -> None:
    db.session.execute(
        delete(NeededWorkerAccount)
        .execution_options(synchronize_session=False)
        .where(NEEDED_WORKER_ACCOUNT_PK.in_(primary_keys))
    )

    # NOTE: Here instead of directly executing an
    # "InterestRateChange" bulk delete statement, we first
    # *try* to obtain locks on the rows, and only then,
    # delete the locked rows. The reason for this is that
    # we can not be 100% certain that the bulk delete
    # statement would be able to obtain the needed locks
    # on all rows, and if it fails -- the whole work done
    # so far (the whole "phase 3, subphase 5" thing) could
    # be rolled back. The result of this choice is that,
    # theoretically, some "InterestRateChange" rows that
    # should be deleted, may remain in the database
    # forever. However, this can only happen for a worker
    # account that has not received a heartbeat for a huge
    # period of time (say a year), and exactly when the
    # account get removed, it receives a heartbeat, which
    # changes the interest rate. The probability for this
    # is practically zero, and the potential harm (an
    # "InterestRateChange" row which will have to be
    # removed manually at some point in the very distant
    # future) is minimal.
    interest_rate_changes_to_delete = (
        InterestRateChange.query
        .filter(
            tuple_(
                InterestRateChange.creditor_id,
                InterestRateChange.debtor_id,
            ).in_(primary_keys)
        )
        .options(load_only(InterestRateChange.change_ts))
        .with_for_update(skip_locked=True)
        .all()
    )
    for x in interest_rate_changes_to_delete:
        db.session.delete(x)

    db.session.flush()
    for x in interest_rate_changes_to_delete:
        db.session.expunge(x)
