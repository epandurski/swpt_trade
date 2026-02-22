from __future__ import annotations
from sqlalchemy.sql.expression import null, or_, and_
from swpt_trade.extensions import db
from .common import (
    TS0,
    DATE0,
    MIN_INT64,
    MAX_INT64,
    DEFAULT_CONFIG_FLAGS,
    SqlUtilsMixin,
)


class TradingPolicy(db.Model, SqlUtilsMixin):
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)

    # Fields coming from `UpdatedLedger` messages:
    latest_ledger_update_id = db.Column(
        db.BigInteger, nullable=False, default=0
    )
    latest_ledger_update_ts = db.Column(
        db.TIMESTAMP(timezone=True), nullable=False, default=TS0
    )
    account_id = db.Column(db.String, nullable=False, default="")
    account_id_is_obsolete = db.Column(
        db.BOOLEAN,
        nullable=False,
        default=False,
    )
    creation_date = db.Column(db.DATE, nullable=False, default=DATE0)
    principal = db.Column(db.BigInteger, nullable=False, default=0)
    last_transfer_number = db.Column(db.BigInteger, nullable=False, default=0)

    # Fields coming from `UpdatedPolicy` messages:
    latest_policy_update_id = db.Column(
        db.BigInteger, nullable=False, default=0
    )
    latest_policy_update_ts = db.Column(
        db.TIMESTAMP(timezone=True), nullable=False, default=TS0
    )
    policy_name = db.Column(db.String)
    min_principal = db.Column(db.BigInteger, nullable=False, default=MIN_INT64)
    max_principal = db.Column(db.BigInteger, nullable=False, default=MAX_INT64)
    peg_debtor_id = db.Column(db.BigInteger)
    peg_exchange_rate = db.Column(db.FLOAT)

    # Fields coming from `UpdatedFlags` messages:
    latest_flags_update_id = db.Column(
        db.BigInteger, nullable=False, default=0
    )
    latest_flags_update_ts = db.Column(
        db.TIMESTAMP(timezone=True), nullable=False, default=TS0
    )
    config_flags = db.Column(
        db.Integer, nullable=False, default=DEFAULT_CONFIG_FLAGS
    )
    __table_args__ = (
        db.CheckConstraint(latest_ledger_update_id >= 0),
        db.CheckConstraint(latest_policy_update_id >= 0),
        db.CheckConstraint(latest_flags_update_id >= 0),
        db.CheckConstraint(last_transfer_number >= 0),
        db.CheckConstraint(peg_exchange_rate >= 0.0),
        db.CheckConstraint(
            or_(
                and_(
                    peg_debtor_id == null(),
                    peg_exchange_rate == null(),
                ),
                and_(
                    peg_debtor_id != null(),
                    peg_exchange_rate != null(),
                ),
            )
        ),
        {
            "comment": (
                "Represents important information about a given customer"
                " account. This includes things like: the account's ID, the"
                " available amount, the customer's trading policy etc."
            ),
        },
    )

    @property
    def is_useless(self) -> bool:
        return (
            (self.account_id == "" or self.account_id_is_obsolete)
            and self.creation_date == DATE0
            and self.principal == 0
            and self.last_transfer_number == 0
            and self.policy_name is None
            and self.min_principal == MIN_INT64
            and self.max_principal == MAX_INT64
            and self.peg_debtor_id is None
            and self.peg_exchange_rate is None
            and self.config_flags == DEFAULT_CONFIG_FLAGS
        )
