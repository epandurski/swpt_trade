from typing import Iterable
from datetime import datetime, timezone
from flask import current_app
from sqlalchemy import select, update, delete, bindparam
from sqlalchemy.sql.expression import func, text, tuple_
from swpt_pythonlib.utils import ShardingRealm
from swpt_trade.extensions import db
from swpt_trade.models import (
    CollectorAccount,
    CollectorStatusChange,
    NeededCollectorAccount,
)
from swpt_trade import procedures

SELECT_BATCH_SIZE = 50000
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

    with db.engine.connect() as w_conn:
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

                db.session.execute(
                    delete(CollectorStatusChange)
                    .execution_options(synchronize_session=False)
                    .where(
                        COLLECTOR_STATUS_CHANGE_PK.in_(
                            (r.collector_id, r.change_id) for r in rows
                        )
                    )
                )
                db.session.commit()

    db.session.close()


def create_needed_collector_accounts():
    sharding_realm: ShardingRealm = current_app.config["SHARDING_REALM"]

    with db.engine.connect() as w_conn:
        with w_conn.execution_options(yield_per=SELECT_BATCH_SIZE).execute(
                select(
                    NeededCollectorAccount.debtor_id,
                    NeededCollectorAccount.collector_id,
                )
        ) as result:
            for rows in result.partitions(INSERT_BATCH_SIZE):
                to_insert = [
                    row for row in rows if sharding_realm.match(row.debtor_id)
                ]
                if to_insert:
                    procedures.insert_collector_accounts(to_insert)

                db.session.execute(
                    delete(NeededCollectorAccount)
                    .execution_options(synchronize_session=False)
                    .where(NEEDED_COLLECTOR_ACCOUNT_PK.in_(rows))
                )
                db.session.commit()

    db.session.close()


def iter_pristine_collectors(
        *,
        hash_mask: int,
        hash_prefix: int,
        yield_per: int,
) -> Iterable[list[tuple[int, int]]]:
    with db.engines["solver"].connect() as s_conn:
        with s_conn.execution_options(yield_per=yield_per).execute(
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
            for rows in result.partitions():
                yield rows
