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
)
from swpt_pythonlib.utils import ShardingRealm
from swpt_trade.extensions import db
from swpt_trade.procedures import process_rescheduled_transfers_batch
from swpt_trade.models import (
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
INSERT_BATCH_SIZE = 5000
SELECT_BATCH_SIZE = 50000

T = TypeVar("T")
atomic: Callable[[T], T] = db.atomic


def process_rescheduled_transfers() -> int:
    count = 0
    batch_size = current_app.config["APP_RESCHEDULED_TRANSFERS_BURST_COUNT"]

    while True:
        n = process_rescheduled_transfers_batch(batch_size)
        count += n
        if n < batch_size:
            break

    return count


def process_delayed_account_transfers() -> int:
    cfg = current_app.config
    sharding_realm: ShardingRealm = cfg["SHARDING_REALM"]
    delete_parent_records = cfg["DELETE_PARENT_SHARD_RECORDS"]
    count = 0

    with db.engine.connect() as w_conn:
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
                    db.session.bulk_insert_mappings(
                        ReplayedAccountTransferSignal, to_replay
                    )
                db.session.execute(
                    delete(DelayedAccountTransfer)
                    .execution_options(synchronize_session=False)
                    .where(
                        DELAYED_ACCOUNT_TRANSFER_PK.in_(
                            (row.turn_id, row.message_id) for row in rows
                        )
                    )
                )
                db.session.commit()
                count += len(rows)

    db.session.close()
    return count


def signal_dispatching_statuses_ready_to_send() -> None:
    sharding_realm: ShardingRealm = current_app.config["SHARDING_REALM"]
    pending_collectings_subquery = (
        select(1)
        .select_from(WorkerCollecting)
        .where(
            WorkerCollecting.collector_id == DispatchingStatus.collector_id,
            WorkerCollecting.debtor_id == DispatchingStatus.debtor_id,
            WorkerCollecting.turn_id == DispatchingStatus.turn_id,
            WorkerCollecting.collected == false(),
        )
    ).exists()

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
                    not_(pending_collectings_subquery),
                )
        ) as result:
            for rows in result.partitions(INSERT_BATCH_SIZE):
                this_shard_rows = [
                    row for row in rows if
                    sharding_realm.match(row.collector_id)
                ]
                locked_rows = (
                    db.session.execute(
                        select(
                            DispatchingStatus.collector_id,
                            DispatchingStatus.debtor_id,
                            DispatchingStatus.turn_id,
                        )
                        .where(
                            DISPATCHING_STATUS_PK.in_(this_shard_rows),
                            DispatchingStatus.started_sending == false(),
                            DispatchingStatus.awaiting_signal_flag == false(),
                        )
                        .with_for_update(skip_locked=True)
                    )
                    .all()
                )
                if locked_rows:
                    current_ts = datetime.now(tz=timezone.utc)

                    db.session.execute(
                        update(DispatchingStatus)
                        .execution_options(synchronize_session=False)
                        .where(DISPATCHING_STATUS_PK.in_(locked_rows))
                        .values(awaiting_signal_flag=True)
                    )
                    db.session.execute(
                        insert(StartSendingSignal).execution_options(
                            insertmanyvalues_page_size=INSERT_BATCH_SIZE,
                            synchronize_session=False,
                        ),
                        [
                            {
                                "collector_id": row.collector_id,
                                "debtor_id": row.debtor_id,
                                "turn_id": row.turn_id,
                                "inserted_at": current_ts,
                            }
                            for row in locked_rows
                        ],
                    )

                db.session.commit()

    db.session.close()


def update_dispatching_statuses_with_everything_sent() -> None:
    sharding_realm: ShardingRealm = current_app.config["SHARDING_REALM"]
    pending_sendings_subquery = (
        select(1)
        .select_from(WorkerSending)
        .where(
            WorkerSending.from_collector_id == DispatchingStatus.collector_id,
            WorkerSending.debtor_id == DispatchingStatus.debtor_id,
            WorkerSending.turn_id == DispatchingStatus.turn_id,
        )
    ).exists()

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
                    not_(pending_sendings_subquery),
                )
        ) as result:
            for rows in result.partitions(INSERT_BATCH_SIZE):
                this_shard_rows = [
                    row for row in rows if
                    sharding_realm.match(row.collector_id)
                ]
                locked_rows = (
                    db.session.execute(
                        select(
                            DispatchingStatus.collector_id,
                            DispatchingStatus.debtor_id,
                            DispatchingStatus.turn_id,
                        )
                        .where(
                            DISPATCHING_STATUS_PK.in_(this_shard_rows),
                            DispatchingStatus.started_sending == true(),
                            DispatchingStatus.all_sent == false(),
                        )
                        .with_for_update(skip_locked=True)
                    )
                    .all()
                )
                if locked_rows:
                    db.session.execute(
                        update(DispatchingStatus)
                        .execution_options(synchronize_session=False)
                        .where(DISPATCHING_STATUS_PK.in_(locked_rows))
                        .values(all_sent=True)
                    )

                db.session.commit()

    db.session.close()


def signal_dispatching_statuses_ready_to_dispatch() -> None:
    sharding_realm: ShardingRealm = current_app.config["SHARDING_REALM"]
    pending_receivings_subquery = (
        select(1)
        .select_from(WorkerReceiving)
        .where(
            WorkerReceiving.to_collector_id == DispatchingStatus.collector_id,
            WorkerReceiving.debtor_id == DispatchingStatus.debtor_id,
            WorkerReceiving.turn_id == DispatchingStatus.turn_id,
            WorkerReceiving.received_amount == text("0"),
        )
    ).exists()

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
                    not_(pending_receivings_subquery),
                )
        ) as result:
            for rows in result.partitions(INSERT_BATCH_SIZE):
                this_shard_rows = [
                    row for row in rows if
                    sharding_realm.match(row.collector_id)
                ]
                locked_rows = (
                    db.session.execute(
                        select(
                            DispatchingStatus.collector_id,
                            DispatchingStatus.debtor_id,
                            DispatchingStatus.turn_id,
                        )
                        .where(
                            DISPATCHING_STATUS_PK.in_(this_shard_rows),
                            DispatchingStatus.all_sent == true(),
                            DispatchingStatus.started_dispatching == false(),
                            DispatchingStatus.awaiting_signal_flag == false(),
                        )
                        .with_for_update(skip_locked=True)
                    )
                    .all()
                )
                if locked_rows:
                    current_ts = datetime.now(tz=timezone.utc)

                    db.session.execute(
                        update(DispatchingStatus)
                        .execution_options(synchronize_session=False)
                        .where(DISPATCHING_STATUS_PK.in_(locked_rows))
                        .values(awaiting_signal_flag=True)
                    )
                    db.session.execute(
                        insert(StartDispatchingSignal).execution_options(
                            insertmanyvalues_page_size=INSERT_BATCH_SIZE,
                            synchronize_session=False,
                        ),
                        [
                            {
                                "collector_id": row.collector_id,
                                "debtor_id": row.debtor_id,
                                "turn_id": row.turn_id,
                                "inserted_at": current_ts,
                            }
                            for row in locked_rows
                        ],
                    )

                db.session.commit()

    db.session.close()


def delete_dispatching_statuses_with_everything_dispatched() -> None:
    sharding_realm: ShardingRealm = current_app.config["SHARDING_REALM"]
    pending_dispatchings_subquery = (
        select(1)
        .select_from(WorkerDispatching)
        .where(
            WorkerDispatching.collector_id == DispatchingStatus.collector_id,
            WorkerDispatching.debtor_id == DispatchingStatus.debtor_id,
            WorkerDispatching.turn_id == DispatchingStatus.turn_id,
        )
    ).exists()

    with db.engine.connect() as w_conn:
        with w_conn.execution_options(yield_per=SELECT_BATCH_SIZE).execute(
                select(
                    DispatchingStatus.collector_id,
                    DispatchingStatus.debtor_id,
                    DispatchingStatus.turn_id,
                )
                .where(
                    DispatchingStatus.started_dispatching == true(),
                    not_(pending_dispatchings_subquery),
                )
        ) as result:
            for rows in result.partitions(INSERT_BATCH_SIZE):
                this_shard_rows = [
                    row for row in rows if
                    sharding_realm.match(row.collector_id)
                ]
                locked_rows = (
                    db.session.execute(
                        select(
                            DispatchingStatus.collector_id,
                            DispatchingStatus.debtor_id,
                            DispatchingStatus.turn_id,
                        )
                        .where(
                            DISPATCHING_STATUS_PK.in_(this_shard_rows),
                            DispatchingStatus.started_dispatching == true(),
                        )
                        .with_for_update(skip_locked=True)
                    )
                    .all()
                )
                if locked_rows:
                    db.session.execute(
                        delete(DispatchingStatus)
                        .execution_options(synchronize_session=False)
                        .where(DISPATCHING_STATUS_PK.in_(locked_rows))
                    )

                db.session.commit()

    db.session.close()
