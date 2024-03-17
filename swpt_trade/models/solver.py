from __future__ import annotations
from sqlalchemy.sql.expression import null, or_, and_
from swpt_trade.extensions import db
from .common import get_now_utc, calc_i64_column_hash


class CollectorAccount(db.Model):
    __bind_key__ = "solver"
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    collector_id = db.Column(db.BigInteger, primary_key=True)
    collector_hash = db.Column(
        db.SmallInteger,
        nullable=False,
        default=lambda ctx: calc_i64_column_hash(ctx, "collector_id"),
    )
    account_id = db.Column(db.String, nullable=False, default="")
    status = db.Column(
        db.SmallInteger,
        nullable=False,
        default=0,
        comment=(
            "Collector account's status: 0) requested account creation;"
            " 1) created and operational; 2) disabled."
        ),
    )
    latest_status_change_at = db.Column(
        db.TIMESTAMP(timezone=True),
        nullable=False,
        default=get_now_utc,
    )
    __table_args__ = (
        db.CheckConstraint(and_(status >= 0, status <= 2)),
        db.Index(
            "idx_collector_account_creation_request",
            status,
            postgresql_where=status == 0,
        ),
    )


class Turn(db.Model):
    __bind_key__ = "solver"
    turn_id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    started_at = db.Column(
        db.TIMESTAMP(timezone=True),
        nullable=False,
        default=get_now_utc,
    )
    base_debtor_info_locator = db.Column(db.String, nullable=False)
    base_debtor_id = db.Column(db.BigInteger, nullable=False)
    max_distance_to_base = db.Column(db.SmallInteger, nullable=False)
    min_trade_amount = db.Column(db.BigInteger, nullable=False)
    phase = db.Column(
        db.SmallInteger,
        nullable=False,
        default=1,
        comment=(
            "Turn's phase: 1) gathering currencies info; 2) gathering"
            " buy and sell offers; 3) giving and taking; 4) done."
        ),
    )
    phase_deadline = db.Column(db.TIMESTAMP(timezone=True))
    collection_started_at = db.Column(db.TIMESTAMP(timezone=True))
    collection_deadline = db.Column(db.TIMESTAMP(timezone=True))
    __table_args__ = (
        db.CheckConstraint(base_debtor_id != 0),
        db.CheckConstraint(max_distance_to_base > 0),
        db.CheckConstraint(min_trade_amount > 0),
        db.CheckConstraint(and_(phase > 0, phase <= 4)),
        db.CheckConstraint(or_(phase > 2, phase_deadline != null())),
        db.CheckConstraint(or_(phase < 2, collection_deadline != null())),
        db.CheckConstraint(or_(phase < 3, collection_started_at != null())),
        db.Index(
            "idx_turn_phase",
            phase,
            postgresql_where=phase < 4,
        ),
        db.Index("idx_turn_started_at", started_at),
    )


class DebtorInfo(db.Model):
    __bind_key__ = "solver"
    turn_id = db.Column(db.Integer, primary_key=True)
    debtor_info_locator = db.Column(db.String, primary_key=True)
    debtor_id = db.Column(db.BigInteger, nullable=False)
    peg_debtor_info_locator = db.Column(db.String)
    peg_debtor_id = db.Column(db.BigInteger)
    peg_exchange_rate = db.Column(db.FLOAT)


class ConfirmedDebtor(db.Model):
    __bind_key__ = "solver"
    turn_id = db.Column(db.Integer, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)

    # NOTE: The `debtor_info_locator` column is not be part of the
    # primary key, but should be included in the primary key index to
    # allow index-only scans. Because SQLAlchemy does not support this
    # yet (2024-01-19), the migration file should be edited so as not
    # to create a "normal" index, but create a "covering" index
    # instead.
    debtor_info_locator = db.Column(db.String, nullable=False)


class CurrencyInfo(db.Model):
    __bind_key__ = "solver"
    turn_id = db.Column(db.Integer, primary_key=True)
    debtor_info_locator = db.Column(db.String, primary_key=True)
    debtor_id = db.Column(db.BigInteger, nullable=False)
    peg_debtor_info_locator = db.Column(db.String)
    peg_debtor_id = db.Column(db.BigInteger)
    peg_exchange_rate = db.Column(db.FLOAT)
    is_confirmed = db.Column(db.BOOLEAN, nullable=False)
    __table_args__ = (
        db.Index(
            "idx_currency_info_confirmed_debtor_id",
            turn_id,
            debtor_id,
            unique=True,
            postgresql_where=is_confirmed,
        ),
    )


class SellOffer(db.Model):
    __bind_key__ = "solver"
    turn_id = db.Column(db.Integer, primary_key=True)
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    amount = db.Column(db.BigInteger, nullable=False)
    collector_id = db.Column(db.BigInteger, nullable=False)
    __table_args__ = (
        db.CheckConstraint(amount > 0),
    )


class BuyOffer(db.Model):
    __bind_key__ = "solver"
    turn_id = db.Column(db.Integer, primary_key=True)
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    amount = db.Column(db.BigInteger, nullable=False)
    __table_args__ = (
        db.CheckConstraint(amount > 0),
    )


class CreditorTaking(db.Model):
    __bind_key__ = "solver"
    turn_id = db.Column(db.Integer, primary_key=True)
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    creditor_hash = db.Column(db.SmallInteger, nullable=False)
    amount = db.Column(db.BigInteger, nullable=False)
    collector_id = db.Column(db.BigInteger, nullable=False)
    __table_args__ = (
        db.CheckConstraint(amount > 0),
    )


class CollectorCollecting(db.Model):
    __bind_key__ = "solver"
    turn_id = db.Column(db.Integer, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    amount = db.Column(db.BigInteger, nullable=False)
    collector_id = db.Column(db.BigInteger, nullable=False)
    collector_hash = db.Column(db.SmallInteger, nullable=False)
    __table_args__ = (
        db.CheckConstraint(amount > 0),
    )


class CollectorSending(db.Model):
    __bind_key__ = "solver"
    turn_id = db.Column(db.Integer, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    from_collector_id = db.Column(db.BigInteger, primary_key=True)
    to_collector_id = db.Column(db.BigInteger, primary_key=True)
    from_collector_hash = db.Column(db.SmallInteger, nullable=False)
    amount = db.Column(db.BigInteger, nullable=False)
    __table_args__ = (
        db.CheckConstraint(amount > 0),
    )


class CollectorReceiving(db.Model):
    __bind_key__ = "solver"
    turn_id = db.Column(db.Integer, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    to_collector_id = db.Column(db.BigInteger, primary_key=True)
    from_collector_id = db.Column(db.BigInteger, primary_key=True)
    to_collector_hash = db.Column(db.SmallInteger, nullable=False)
    amount = db.Column(db.BigInteger, nullable=False)
    __table_args__ = (
        db.CheckConstraint(amount > 0),
    )


class CollectorDispatching(db.Model):
    __bind_key__ = "solver"
    turn_id = db.Column(db.Integer, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    amount = db.Column(db.BigInteger, nullable=False)
    collector_id = db.Column(db.BigInteger, nullable=False)
    collector_hash = db.Column(db.SmallInteger, nullable=False)
    __table_args__ = (
        db.CheckConstraint(amount > 0),
    )


class CreditorGiving(db.Model):
    __bind_key__ = "solver"
    turn_id = db.Column(db.Integer, primary_key=True)
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    creditor_hash = db.Column(db.SmallInteger, nullable=False)
    amount = db.Column(db.BigInteger, nullable=False)
    collector_id = db.Column(db.BigInteger, nullable=False)
    __table_args__ = (
        db.CheckConstraint(amount > 0),
    )
