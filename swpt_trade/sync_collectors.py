import logging
from datetime import datetime, timezone, timedelta
from flask import current_app
from sqlalchemy import select, update, delete, bindparam
from sqlalchemy.sql.expression import func, text, null, tuple_
from sqlalchemy.dialects import postgresql
from swpt_pythonlib.utils import ShardingRealm
from swpt_trade.utils import u16_to_i16
from swpt_trade.extensions import db
from swpt_trade.models import (
    SET_SEQSCAN_ON,
    SET_FORCE_CUSTOM_PLAN,
    SET_DEFAULT_PLAN_CACHE_MODE,
    SET_STATISTICS_TARGET,
    HUGE_NEGLIGIBLE_AMOUNT,
    DEFAULT_CONFIG_FLAGS,
    WORKER_ACCOUNT_TABLES_JOIN_PREDICATE,
    CollectorAccount,
    CollectorStatusChange,
    NeededCollectorAccount,
    NeededWorkerAccount,
    WorkerAccount,
    ConfigureAccountSignal,
)
from swpt_trade import procedures

SELECT_BATCH_SIZE = 20000
UPDATE_BATCH_SIZE = 5000
INSERT_BATCH_SIZE = 5000

COLLECTOR_STATUS_CHANGE_PK = tuple_(
    CollectorStatusChange.collector_id,
    CollectorStatusChange.change_id,
)
NEEDED_COLLECTOR_ACCOUNT_PK = tuple_(
    NeededCollectorAccount.debtor_id,
    NeededCollectorAccount.collector_id,
)
NEEDED_WORKER_ACCOUNT_PK = tuple_(
    NeededWorkerAccount.creditor_id,
    NeededWorkerAccount.debtor_id,
)


def process_collector_status_changes():
    sharding_realm: ShardingRealm = current_app.config["SHARDING_REALM"]
    current_ts = datetime.now(tz=timezone.utc)
    ca = CollectorAccount.__table__
    ca_update_statement = (
        update(ca)
        .where(
            ca.c.debtor_id == bindparam("b_debtor_id"),
            ca.c.collector_id == bindparam("b_collector_id"),
            ca.c.status == bindparam("b_from_status"),
        )
        .values(
            status=bindparam("b_to_status"),
            account_id=func.coalesce(
                bindparam("b_account_id"), ca.c.account_id
            ),
            latest_status_change_at=current_ts,
        )
    )
    db.session.execute(SET_STATISTICS_TARGET)
    db.session.execute(text("ANALYZE collector_status_change"))
    db.session.commit()

    with db.engine.connect() as w_conn:
        w_conn.execute(SET_SEQSCAN_ON)
        with w_conn.execution_options(yield_per=SELECT_BATCH_SIZE).execute(
                select(
                    CollectorStatusChange.collector_id,
                    CollectorStatusChange.change_id,
                    CollectorStatusChange.debtor_id,
                    CollectorStatusChange.from_status,
                    CollectorStatusChange.to_status,
                    CollectorStatusChange.account_id,
                )
        ) as result:
            for rows in result.partitions(UPDATE_BATCH_SIZE):
                dicts_to_update = [
                    {
                        "b_collector_id": row.collector_id,
                        "b_debtor_id": row.debtor_id,
                        "b_from_status": row.from_status,
                        "b_to_status": row.to_status,
                        "b_account_id": row.account_id,
                    }
                    for row in rows
                    if sharding_realm.match(row.collector_id)
                ]
                if dicts_to_update:
                    db.session.execute(ca_update_statement, dicts_to_update)
                    db.session.commit()

                db.session.execute(SET_FORCE_CUSTOM_PLAN)
                chosen = CollectorStatusChange.choose_rows(
                    [(r.collector_id, r.change_id) for r in rows]
                )
                db.session.execute(
                    delete(CollectorStatusChange)
                    .execution_options(synchronize_session=False)
                    .where(COLLECTOR_STATUS_CHANGE_PK == tuple_(*chosen.c))
                )
                db.session.commit()

    db.session.close()


def create_needed_collector_accounts():
    sharding_realm: ShardingRealm = current_app.config["SHARDING_REALM"]

    db.session.execute(SET_STATISTICS_TARGET)
    db.session.execute(text("ANALYZE needed_collector_account"))
    db.session.commit()

    with db.engine.connect() as w_conn:
        w_conn.execute(SET_SEQSCAN_ON)
        with w_conn.execution_options(yield_per=SELECT_BATCH_SIZE).execute(
                select(
                    NeededCollectorAccount.debtor_id,
                    NeededCollectorAccount.collector_id,
                )
        ) as result:
            for rows in result.partitions(INSERT_BATCH_SIZE):
                pks = [tuple(row) for row in rows]
                pks_to_insert = [x for x in pks if sharding_realm.match(x[0])]
                if pks_to_insert:
                    procedures.insert_collector_accounts(pks_to_insert)
                    db.session.commit()

                db.session.execute(SET_FORCE_CUSTOM_PLAN)
                to_delete = NeededCollectorAccount.choose_rows(pks)
                db.session.execute(
                    delete(NeededCollectorAccount)
                    .execution_options(synchronize_session=False)
                    .where(NEEDED_COLLECTOR_ACCOUNT_PK == tuple_(*to_delete.c))
                )
                db.session.commit()

    db.session.close()


def process_pristine_collectors() -> None:
    cfg = current_app.config
    max_postponement = timedelta(days=cfg["APP_EXTREME_MESSAGE_DELAY_DAYS"])
    sharding_realm: ShardingRealm = cfg["SHARDING_REALM"]
    hash_prefix = u16_to_i16(sharding_realm.realm >> 16)
    hash_mask = u16_to_i16(sharding_realm.realm_mask >> 16)

    with db.engines["solver"].connect() as s_conn:
        s_conn.execute(SET_SEQSCAN_ON)
        with s_conn.execution_options(yield_per=SELECT_BATCH_SIZE).execute(
                select(
                    CollectorAccount.debtor_id,
                    CollectorAccount.collector_id,
                )
                .where(
                    CollectorAccount.status == text("0"),
                    CollectorAccount.collector_hash.op("&")(hash_mask)
                    == hash_prefix,
                )
        ) as result:
            for rows in result.partitions(INSERT_BATCH_SIZE):
                _process_pristine_collectors_batch(
                    worker_account_pks=[
                        (row.collector_id, row.debtor_id) for row in rows
                    ],
                    max_postponement=max_postponement,
                )

    db.session.close()


def _process_pristine_collectors_batch(
        worker_account_pks: list[tuple[int, int]],
        max_postponement: timedelta,
) -> None:
    current_ts = datetime.now(tz=timezone.utc)

    # First, we need to check if the `NeededWorkerAccount` records and
    # their corresponding accounts already exist.
    db.session.execute(SET_FORCE_CUSTOM_PLAN)
    chosen = NeededWorkerAccount.choose_rows(worker_account_pks)
    needed_worker_accounts = {
        (row[0], row[1]): (row[2], row[3])
        for row in db.session.execute(
                select(
                    NeededWorkerAccount.creditor_id,
                    NeededWorkerAccount.debtor_id,
                    NeededWorkerAccount.configured_at,
                    WorkerAccount.creditor_id != null(),
                )
                .select_from(NeededWorkerAccount)
                .join(chosen, NEEDED_WORKER_ACCOUNT_PK == tuple_(*chosen.c))
                .join(
                    WorkerAccount,
                    WORKER_ACCOUNT_TABLES_JOIN_PREDICATE,
                    isouter=True,
                )
        ).all()
    }
    db.session.execute(SET_DEFAULT_PLAN_CACHE_MODE)

    pks_to_create = set(
        pk
        for pk in worker_account_pks
        if pk not in needed_worker_accounts
    )
    if pks_to_create:
        # We need to create `NeededWorkerAccount` records, and send
        # `ConfigureAccount` messages, so as to configure new accounts
        # (see `pks_to_configure` bellow).
        db.session.execute(
            postgresql.insert(NeededWorkerAccount)
            .execution_options(
                insertmanyvalues_page_size=INSERT_BATCH_SIZE,
                synchronize_session=False,
            )
            .on_conflict_do_nothing(
                index_elements=[
                    NeededWorkerAccount.creditor_id,
                    NeededWorkerAccount.debtor_id,
                ]
            ),
            [
                {
                    "creditor_id": creditor_id,
                    "debtor_id": debtor_id,
                    "configured_at": current_ts,
                }
                for creditor_id, debtor_id in pks_to_create
            ],
        )

    pks_to_retry = set(
        pk
        for pk, (configured_at, has_account) in needed_worker_accounts.items()
        if configured_at + max_postponement < current_ts and not has_account
    )
    if pks_to_retry:
        # It's been a while since `ConfigureAccount` messages were
        # sent for these collector accounts, and yet there are no
        # accounts created. The only reasonable thing that we can do
        # in this case, is to send another `ConfigureAccount` messages
        # for the accounts, hoping that this will fix the problem (see
        # `pks_to_configure` bellow).

        db.session.execute(SET_FORCE_CUSTOM_PLAN)
        to_retry = NeededWorkerAccount.choose_rows(list(pks_to_retry))
        db.session.execute(
            update(NeededWorkerAccount)
            .execution_options(synchronize_session=False)
            .where(NEEDED_WORKER_ACCOUNT_PK == tuple_(*to_retry.c))
            .values(configured_at=current_ts)
        )
        db.session.execute(SET_DEFAULT_PLAN_CACHE_MODE)

        logger = logging.getLogger(__name__)
        for pk in pks_to_retry:
            logger.warning(
                "Failed to create a worker account for"
                " collector (debtor_id=%d, collector_id=%d,"
                " attempted_at=%s). Tying again.",
                pk[1],
                pk[0],
                needed_worker_accounts[pk][0],
            )

    pks_to_configure = pks_to_create | pks_to_retry
    if pks_to_configure:
        db.session.execute(
            ConfigureAccountSignal.insert_rows([
                (
                    creditor_id,
                    debtor_id,
                    current_ts,
                    0,
                    HUGE_NEGLIGIBLE_AMOUNT,
                    "",
                    DEFAULT_CONFIG_FLAGS,
                    current_ts,
                )
                for creditor_id, debtor_id in pks_to_configure
            ])
        )
        db.session.execute(
            CollectorStatusChange.insert_rows(
                [
                    (
                        collector_id,
                        None,  # change_id
                        debtor_id,
                        0,
                        1,
                        None
                    )
                    for collector_id, debtor_id in pks_to_configure
                ],
                default_columns=["change_id"]
            )
        )

    db.session.commit()
