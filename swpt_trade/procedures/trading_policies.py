from typing import TypeVar, Callable, Optional
from datetime import datetime, date
from swpt_trade.extensions import db
from swpt_trade.models import TradingPolicy

T = TypeVar("T")
atomic: Callable[[T], T] = db.atomic


@atomic
def process_updated_ledger_signal(
        *,
        creditor_id: int,
        debtor_id: int,
        update_id: int,
        account_id: str,
        creation_date: date,
        principal: int,
        last_transfer_number: int,
        ts: datetime,
) -> None:
    trading_policy = (
        TradingPolicy.query
        .filter_by(creditor_id=creditor_id, debtor_id=debtor_id)
        .with_for_update()
        .one_or_none()
    )
    if trading_policy:
        if trading_policy.latest_ledger_update_id < update_id:
            trading_policy.latest_ledger_update_id = update_id
            trading_policy.latest_ledger_update_ts = ts
            trading_policy.account_id = account_id
            trading_policy.creation_date = creation_date
            trading_policy.principal = principal
            trading_policy.last_transfer_number = last_transfer_number
    else:
        with db.retry_on_integrity_error():
            db.session.add(
                TradingPolicy(
                    debtor_id=debtor_id,
                    creditor_id=creditor_id,
                    latest_ledger_update_id=update_id,
                    latest_ledger_update_ts=ts,
                    account_id=account_id,
                    creation_date=creation_date,
                    principal=principal,
                    last_transfer_number=last_transfer_number,
                )
            )


@atomic
def process_updated_policy_signal(
        *,
        creditor_id: int,
        debtor_id: int,
        update_id: int,
        policy_name: Optional[str],
        min_principal: int,
        max_principal: int,
        peg_exchange_rate: Optional[float],
        peg_debtor_id: Optional[int],
        ts: datetime,
) -> None:
    trading_policy = (
        TradingPolicy.query
        .filter_by(creditor_id=creditor_id, debtor_id=debtor_id)
        .with_for_update()
        .one_or_none()
    )
    if trading_policy:
        if trading_policy.latest_policy_update_id < update_id:
            trading_policy.latest_policy_update_id = update_id
            trading_policy.latest_policy_update_ts = ts
            trading_policy.policy_name = policy_name
            trading_policy.min_principal = min_principal
            trading_policy.max_principal = max_principal
            trading_policy.peg_exchange_rate = peg_exchange_rate
            trading_policy.peg_debtor_id = peg_debtor_id
    else:
        with db.retry_on_integrity_error():
            db.session.add(
                TradingPolicy(
                    debtor_id=debtor_id,
                    creditor_id=creditor_id,
                    latest_policy_update_id=update_id,
                    latest_policy_update_ts=ts,
                    policy_name=policy_name,
                    min_principal=min_principal,
                    max_principal=max_principal,
                    peg_exchange_rate=peg_exchange_rate,
                    peg_debtor_id=peg_debtor_id,
                )
            )


@atomic
def process_updated_flags_signal(
        *,
        creditor_id: int,
        debtor_id: int,
        update_id: int,
        config_flags: int,
        ts: datetime,
) -> None:
    trading_policy = (
        TradingPolicy.query
        .filter_by(creditor_id=creditor_id, debtor_id=debtor_id)
        .with_for_update()
        .one_or_none()
    )
    if trading_policy:
        if trading_policy.latest_flags_update_id < update_id:
            trading_policy.latest_flags_update_id = update_id
            trading_policy.latest_flags_update_ts = ts
            trading_policy.config_flags = config_flags
    else:
        with db.retry_on_integrity_error():
            db.session.add(
                TradingPolicy(
                    debtor_id=debtor_id,
                    creditor_id=creditor_id,
                    latest_flags_update_id=update_id,
                    latest_flags_update_ts=ts,
                    config_flags=config_flags,
                )
            )
