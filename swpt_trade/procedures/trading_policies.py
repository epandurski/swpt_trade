from typing import TypeVar, Callable, Optional
from datetime import datetime, date
from sqlalchemy.orm import load_only
from swpt_trade.extensions import db
from swpt_trade.models import TradingPolicy

TRADING_POLICY_LOAD_ONLY_LATEST_LEDGER_UPDATE_ID = load_only(
    TradingPolicy.latest_ledger_update_id,
)
TRADING_POLICY_LOAD_ONLY_LATEST_POLICY_UPDATE_ID = load_only(
    TradingPolicy.latest_policy_update_id,
)
TRADING_POLICY_LOAD_ONLY_LATEST_FLAGS_UPDATE_ID = load_only(
    TradingPolicy.latest_flags_update_id,
)

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
        .options(TRADING_POLICY_LOAD_ONLY_LATEST_LEDGER_UPDATE_ID)
        .with_for_update()
        .one_or_none()
    )
    if trading_policy:
        if trading_policy.latest_ledger_update_id < update_id:
            trading_policy.latest_ledger_update_id = update_id
            trading_policy.latest_ledger_update_ts = ts
            trading_policy.creation_date = creation_date
            trading_policy.principal = principal
            trading_policy.last_transfer_number = last_transfer_number
            if account_id != "":
                trading_policy.account_id = account_id
                trading_policy.account_id_is_obsolete = False
            else:
                # NOTE: When the account does not have an ID anymore,
                # we do our best to not lose the old ID. This allows
                # us to continue making transfers to the account, even
                # when the account has been forcefully deleted by the
                # account's owner.
                trading_policy.account_id_is_obsolete = True
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
        .options(TRADING_POLICY_LOAD_ONLY_LATEST_POLICY_UPDATE_ID)
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
        .options(TRADING_POLICY_LOAD_ONLY_LATEST_FLAGS_UPDATE_ID)
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
