from typing import TypeVar, Callable
from flask import current_app
from datetime import datetime, timezone
from sqlalchemy import select, insert, update, delete
from sqlalchemy.sql.expression import (
    tuple_,
    and_,
    or_,
    not_,
    true,
    false,
    text,
    null,
    func,
)
from swpt_pythonlib.utils import ShardingRealm
from swpt_trade.extensions import db
from swpt_trade.procedures import process_rescheduled_transfers_batch
from swpt_trade.models import (
    SET_SEQSCAN_ON,
    SET_INDEXSCAN_OFF,
    SET_FORCE_CUSTOM_PLAN,
    TransferAttempt,
    DispatchingStatus,
    WorkerCollecting,
    WorkerSending,
    WorkerReceiving,
    WorkerDispatching,
    StartSendingSignal,
    StartDispatchingSignal,
    ReplayedAccountTransferSignal,
    DelayedAccountTransfer,
    WorkerTurn,
)

DISPATCHING_STATUS_PK = tuple_(
    DispatchingStatus.collector_id,
    DispatchingStatus.debtor_id,
    DispatchingStatus.turn_id,
)
DELAYED_ACCOUNT_TRANSFER_PK = tuple_(
    DelayedAccountTransfer.turn_id,
    DelayedAccountTransfer.message_id,
)
PENDING_COLLECTINGS_EXIST = (
    (
        select(1)
        .select_from(WorkerCollecting)
        .where(
            WorkerCollecting.collector_id == DispatchingStatus.collector_id,
            WorkerCollecting.debtor_id == DispatchingStatus.debtor_id,
            WorkerCollecting.turn_id == DispatchingStatus.turn_id,
            WorkerCollecting.collected == false(),
        )
    )
    .exists()
    .correlate(DispatchingStatus)
)
PENDING_SENDINGS_EXIST = (
    (
        select(1)
        .select_from(WorkerSending)
        .where(
            WorkerSending.from_collector_id == DispatchingStatus.collector_id,
            WorkerSending.debtor_id == DispatchingStatus.debtor_id,
            WorkerSending.turn_id == DispatchingStatus.turn_id,
        )
    )
    .exists()
    .correlate(DispatchingStatus)
)
PENDING_RECEIVINGS_EXIST = (
    (
        select(1)
        .select_from(WorkerReceiving)
        .where(
            WorkerReceiving.to_collector_id == DispatchingStatus.collector_id,
            WorkerReceiving.debtor_id == DispatchingStatus.debtor_id,
            WorkerReceiving.turn_id == DispatchingStatus.turn_id,
            WorkerReceiving.received_amount == text("0"),
        )
    )
    .exists()
    .correlate(DispatchingStatus)
)
PENDING_DISPATCHINGS_EXIST = (
    (
        select(1)
        .select_from(WorkerDispatching)
        .where(
            WorkerDispatching.collector_id == DispatchingStatus.collector_id,
            WorkerDispatching.debtor_id == DispatchingStatus.debtor_id,
            WorkerDispatching.turn_id == DispatchingStatus.turn_id,
        )
    )
    .exists()
    .correlate(DispatchingStatus)
)
INSERT_BATCH_SIZE = 5000
SELECT_BATCH_SIZE = 20000

T = TypeVar("T")
atomic: Callable[[T], T] = db.atomic


def process_rescheduled_transfers() -> int:
    count = 0
    current_ts = datetime.now(tz=timezone.utc)
    batch_size = current_app.config["APP_RESCHEDULED_TRANSFERS_BURST_COUNT"]

    with db.engine.connect() as w_conn:
        with w_conn.execution_options(yield_per=batch_size).execute(
                select(
                    TransferAttempt.collector_id,
                    TransferAttempt.turn_id,
                    TransferAttempt.debtor_id,
                    TransferAttempt.creditor_id,
                    TransferAttempt.transfer_kind,
                )
                .where(
                    TransferAttempt.rescheduled_for != null(),
                    TransferAttempt.rescheduled_for <= current_ts,
                )
        ) as result:
            for rows in result.partitions():
                count += process_rescheduled_transfers_batch(
                    [tuple(row) for row in rows],
                    current_ts,
                )

    return count


def process_delayed_account_transfers() -> int:
    """Replay delayed account transfers if the worker is ready to
    process them.
    """
    cfg = current_app.config
    sharding_realm: ShardingRealm = cfg["SHARDING_REALM"]
    delete_parent_records = cfg["DELETE_PARENT_SHARD_RECORDS"]
    count = 0

    with db.engine.connect() as w_conn:
        min_turn_id = (
            w_conn.execute(
                select(func.min(DelayedAccountTransfer.turn_id))
                .select_from(DelayedAccountTransfer)
            )
            .scalar_one()
        )
        if min_turn_id is not None:
            w_conn.execute(SET_SEQSCAN_ON)

            # NOTE: Each row in this cursor contains a lot of data.
            # Therefore, we use `INSERT_BATCH_SIZE` here, because using
            # the bigger `SELECT_BATCH_SIZE` may consume too much memory.
            with w_conn.execution_options(yield_per=INSERT_BATCH_SIZE).execute(
                    select(
                        DelayedAccountTransfer.turn_id,
                        DelayedAccountTransfer.message_id,
                        DelayedAccountTransfer.creditor_id,
                        DelayedAccountTransfer.debtor_id,
                        DelayedAccountTransfer.creation_date,
                        DelayedAccountTransfer.transfer_number,
                        DelayedAccountTransfer.coordinator_type,
                        DelayedAccountTransfer.committed_at,
                        DelayedAccountTransfer.acquired_amount,
                        DelayedAccountTransfer.transfer_note_format,
                        DelayedAccountTransfer.transfer_note,
                        DelayedAccountTransfer.principal,
                        DelayedAccountTransfer.previous_transfer_number,
                        DelayedAccountTransfer.sender,
                        DelayedAccountTransfer.recipient,
                        DelayedAccountTransfer.ts,
                    )
                    .select_from(DelayedAccountTransfer)
                    .join(
                        WorkerTurn,
                        WorkerTurn.turn_id == DelayedAccountTransfer.turn_id,
                    )
                    .where(
                        WorkerTurn.turn_id >= min_turn_id,
                        or_(
                            WorkerTurn.phase > 3,
                            and_(
                                WorkerTurn.phase == 3,
                                WorkerTurn.worker_turn_subphase >= 5,
                            )
                        )
                    )
            ) as result:
                for rows in result.partitions(INSERT_BATCH_SIZE):
                    current_ts = datetime.now(tz=timezone.utc)
                    to_replay = [
                        dict(
                            creditor_id=row.creditor_id,
                            debtor_id=row.debtor_id,
                            creation_date=row.creation_date,
                            transfer_number=row.transfer_number,
                            coordinator_type=row.coordinator_type,
                            committed_at=row.committed_at,
                            acquired_amount=row.acquired_amount,
                            transfer_note_format=row.transfer_note_format,
                            transfer_note=row.transfer_note,
                            principal=row.principal,
                            previous_transfer_number=row.previous_transfer_number,
                            sender=row.sender,
                            recipient=row.recipient,
                            ts=row.ts,
                            inserted_at=current_ts,
                        )
                        for row in rows
                        if (
                                sharding_realm.match(row.creditor_id)
                                or not (
                                    # Replying the message more than once
                                    # is safe, and therefore we do not shy
                                    # from doing it unless we are certain
                                    # that the other shard is the one
                                    # responsible for this particular
                                    # message.
                                    delete_parent_records
                                    and sharding_realm.match(
                                        row.creditor_id, match_parent=True
                                    )
                                )
                        )
                    ]
                    if to_replay:
                        db.session.execute(
                            insert(ReplayedAccountTransferSignal)
                            .execution_options(
                                insertmanyvalues_page_size=INSERT_BATCH_SIZE,
                                synchronize_session=False,
                            ),
                            to_replay,
                        )
                    chosen = DelayedAccountTransfer.choose_rows(
                        [(row.turn_id, row.message_id) for row in rows]
                    )
                    db.session.execute(SET_INDEXSCAN_OFF)
                    db.session.execute(
                        delete(DelayedAccountTransfer)
                        .execution_options(synchronize_session=False)
                        .where(
                            DELAYED_ACCOUNT_TRANSFER_PK == tuple_(*chosen.c)
                        )
                    )
                    db.session.commit()
                    count += len(rows)

    db.session.close()
    return count


def signal_dispatching_statuses_ready_to_send() -> None:
    sharding_realm: ShardingRealm = current_app.config["SHARDING_REALM"]

    with db.engine.connect() as w_conn:
        with w_conn.execution_options(yield_per=SELECT_BATCH_SIZE).execute(
                select(
                    DispatchingStatus.collector_id,
                    DispatchingStatus.debtor_id,
                    DispatchingStatus.turn_id,
                )
                .where(
                    DispatchingStatus.started_sending == false(),
                    DispatchingStatus.awaiting_signal_flag == false(),
                    not_(PENDING_COLLECTINGS_EXIST),
                )
        ) as result:
            for rows in result.partitions(INSERT_BATCH_SIZE):
                db.session.execute(SET_FORCE_CUSTOM_PLAN)
                chosen = DispatchingStatus.choose_rows([
                    tuple(row)
                    for row in rows
                    if sharding_realm.match(row.collector_id)
                ])
                pks_to_update = [
                    tuple(row)
                    for row in db.session.execute(
                            select(
                                DispatchingStatus.collector_id,
                                DispatchingStatus.debtor_id,
                                DispatchingStatus.turn_id,
                            )
                            .join(
                                chosen,
                                DISPATCHING_STATUS_PK == tuple_(*chosen.c)
                            )
                            .where(
                                DispatchingStatus.started_sending == false(),
                                DispatchingStatus.awaiting_signal_flag == false(),
                            )
                            .with_for_update(skip_locked=True)
                    ).all()
                ]
                if pks_to_update:
                    to_update = DispatchingStatus.choose_rows(pks_to_update)
                    db.session.execute(
                        update(DispatchingStatus)
                        .execution_options(synchronize_session=False)
                        .where(DISPATCHING_STATUS_PK == tuple_(*to_update.c))
                        .values(awaiting_signal_flag=True)
                    )
                    db.session.execute(
                        insert(StartSendingSignal).execution_options(
                            insertmanyvalues_page_size=INSERT_BATCH_SIZE,
                            synchronize_session=False,
                        ),
                        [
                            {
                                "collector_id": collector_id,
                                "debtor_id": debtor_id,
                                "turn_id": turn_id,
                            }
                            for collector_id, debtor_id, turn_id in pks_to_update
                        ],
                    )

                db.session.commit()

    db.session.close()


def update_dispatching_statuses_with_everything_sent() -> None:
    sharding_realm: ShardingRealm = current_app.config["SHARDING_REALM"]

    with db.engine.connect() as w_conn:
        with w_conn.execution_options(yield_per=SELECT_BATCH_SIZE).execute(
                select(
                    DispatchingStatus.collector_id,
                    DispatchingStatus.debtor_id,
                    DispatchingStatus.turn_id,
                )
                .where(
                    DispatchingStatus.started_sending == true(),
                    DispatchingStatus.all_sent == false(),
                    not_(PENDING_SENDINGS_EXIST),
                )
        ) as result:
            for rows in result.partitions(INSERT_BATCH_SIZE):
                db.session.execute(SET_FORCE_CUSTOM_PLAN)
                chosen = DispatchingStatus.choose_rows([
                    tuple(row)
                    for row in rows
                    if sharding_realm.match(row.collector_id)
                ])
                pks_to_update = [
                    tuple(row)
                    for row in db.session.execute(
                            select(
                                DispatchingStatus.collector_id,
                                DispatchingStatus.debtor_id,
                                DispatchingStatus.turn_id,
                            )
                            .join(
                                chosen,
                                DISPATCHING_STATUS_PK == tuple_(*chosen.c)
                            )
                            .where(
                                DispatchingStatus.started_sending == true(),
                                DispatchingStatus.all_sent == false(),
                            )
                            .with_for_update(skip_locked=True)
                    ).all()
                ]
                if pks_to_update:
                    to_update = DispatchingStatus.choose_rows(pks_to_update)
                    db.session.execute(
                        update(DispatchingStatus)
                        .execution_options(synchronize_session=False)
                        .where(DISPATCHING_STATUS_PK == tuple_(*to_update.c))
                        .values(all_sent=True)
                    )

                db.session.commit()

    db.session.close()


def signal_dispatching_statuses_ready_to_dispatch() -> None:
    sharding_realm: ShardingRealm = current_app.config["SHARDING_REALM"]

    with db.engine.connect() as w_conn:
        with w_conn.execution_options(yield_per=SELECT_BATCH_SIZE).execute(
                select(
                    DispatchingStatus.collector_id,
                    DispatchingStatus.debtor_id,
                    DispatchingStatus.turn_id,
                )
                .where(
                    DispatchingStatus.all_sent == true(),
                    DispatchingStatus.started_dispatching == false(),
                    DispatchingStatus.awaiting_signal_flag == false(),
                    not_(PENDING_RECEIVINGS_EXIST),
                )
        ) as result:
            for rows in result.partitions(INSERT_BATCH_SIZE):
                db.session.execute(SET_FORCE_CUSTOM_PLAN)
                chosen = DispatchingStatus.choose_rows([
                    tuple(row)
                    for row in rows
                    if sharding_realm.match(row.collector_id)
                ])
                pks_to_update = [
                    tuple(row)
                    for row in db.session.execute(
                            select(
                                DispatchingStatus.collector_id,
                                DispatchingStatus.debtor_id,
                                DispatchingStatus.turn_id,
                            )
                            .join(
                                chosen,
                                DISPATCHING_STATUS_PK == tuple_(*chosen.c)
                            )
                            .where(
                                DispatchingStatus.all_sent == true(),
                                DispatchingStatus.started_dispatching == false(),
                                DispatchingStatus.awaiting_signal_flag == false(),
                            )
                            .with_for_update(skip_locked=True)
                    ).all()
                ]
                if pks_to_update:
                    to_update = DispatchingStatus.choose_rows(pks_to_update)
                    db.session.execute(
                        update(DispatchingStatus)
                        .execution_options(synchronize_session=False)
                        .where(DISPATCHING_STATUS_PK == tuple_(*to_update.c))
                        .values(awaiting_signal_flag=True)
                    )
                    db.session.execute(
                        insert(StartDispatchingSignal).execution_options(
                            insertmanyvalues_page_size=INSERT_BATCH_SIZE,
                            synchronize_session=False,
                        ),
                        [
                            {
                                "collector_id": collector_id,
                                "debtor_id": debtor_id,
                                "turn_id": turn_id,
                            }
                            for collector_id, debtor_id, turn_id in pks_to_update
                        ],
                    )

                db.session.commit()

    db.session.close()


def delete_dispatching_statuses_with_everything_dispatched() -> None:
    sharding_realm: ShardingRealm = current_app.config["SHARDING_REALM"]

    with db.engine.connect() as w_conn:
        with w_conn.execution_options(yield_per=SELECT_BATCH_SIZE).execute(
                select(
                    DispatchingStatus.collector_id,
                    DispatchingStatus.debtor_id,
                    DispatchingStatus.turn_id,
                )
                .where(
                    DispatchingStatus.started_dispatching == true(),
                    not_(PENDING_DISPATCHINGS_EXIST),
                )
        ) as result:
            for rows in result.partitions(INSERT_BATCH_SIZE):
                db.session.execute(SET_FORCE_CUSTOM_PLAN)
                chosen = DispatchingStatus.choose_rows([
                    tuple(row)
                    for row in rows
                    if sharding_realm.match(row.collector_id)
                ])
                pks_to_update = [
                    tuple(row)
                    for row in db.session.execute(
                            select(
                                DispatchingStatus.collector_id,
                                DispatchingStatus.debtor_id,
                                DispatchingStatus.turn_id,
                            )
                            .join(
                                chosen,
                                DISPATCHING_STATUS_PK == tuple_(*chosen.c)
                            )
                            .where(
                                DispatchingStatus.started_dispatching == true(),
                            )
                            .with_for_update(skip_locked=True)
                    ).all()
                ]
                if pks_to_update:
                    to_update = DispatchingStatus.choose_rows(pks_to_update)
                    db.session.execute(
                        delete(DispatchingStatus)
                        .execution_options(synchronize_session=False)
                        .where(DISPATCHING_STATUS_PK == tuple_(*to_update.c))
                    )

                db.session.commit()

    db.session.close()
