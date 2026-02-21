from __future__ import annotations
from sqlalchemy.sql.expression import null, or_, and_
from swpt_trade.extensions import db
from .common import get_now_utc, calc_i64_column_hash, ChooseRowsMixin


class CollectorAccount(db.Model, ChooseRowsMixin):
    # NOTE: The `status` column is not be part of the primary key, but
    # it probably is a good idea to include it in the primary key
    # index to allow index-only scans. Because SQLAlchemy does not
    # support this yet (2024-01-19), the migration file should be
    # edited so as not to create a "normal" index, but create a
    # "covering" index instead.

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
            "Collector account's status: 0) pristine; 1) account creation"
            " has been requested; 2) the account has been created, and"
            " an account ID has been assigned to it; 3) disabled."
        ),
    )
    latest_status_change_at = db.Column(
        db.TIMESTAMP(timezone=True),
        nullable=False,
        default=get_now_utc,
    )
    __table_args__ = (
        db.CheckConstraint(and_(status >= 0, status <= 3)),
        db.Index(
            "idx_collector_account_creation_request",
            collector_hash,
            postgresql_where=status == 0,
        ),
        {
            "comment": (
                'Represents a planned or existing Swaptacular account, which'
                ' should be used to collect and dispatch transfers. "Worker"'
                ' servers will watch for new (pristine) records inserted in'
                ' this table, and will try to create and use all the accounts'
                ' catalogued in this table.'
            ),
        },
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
            turn_id,
            postgresql_where=phase < 4,
        ),
        db.Index("idx_turn_started_at", started_at),
        {
            "comment": (
                'Represents a circular trading round, created and managed by'
                ' the "solver" server. "Worker" servers will watch for'
                ' changes in this table, so as to participate in the different'
                ' phases of each trading round.'
            ),
        },
    )


class DebtorInfo(db.Model, ChooseRowsMixin):
    __bind_key__ = "solver"
    turn_id = db.Column(db.Integer, primary_key=True)
    debtor_info_locator = db.Column(db.String, primary_key=True)
    debtor_id = db.Column(db.BigInteger, nullable=False)
    peg_debtor_info_locator = db.Column(db.String)
    peg_debtor_id = db.Column(db.BigInteger)
    peg_exchange_rate = db.Column(db.FLOAT)
    __table_args__ = (
        {
            "comment": (
                'Represents relevant information about a given currency'
                ' (aka debtor), so that the currency can participate'
                ' in a given trading turn. "Worker" servers are responsible'
                ' for populating this table during the phase 1 of each turn.'
                ' The "solver" server will read from this table, and will'
                ' delete the records before advancing to phase 2 of the turn.'
            ),
        },
    )


class ConfirmedDebtor(db.Model, ChooseRowsMixin):
    __bind_key__ = "solver"
    turn_id = db.Column(db.Integer, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    debtor_info_locator = db.Column(db.String, nullable=False)
    __table_args__ = (
        {
            "comment": (
                'Represents the fact that a given currency (aka debtor) is'
                ' verified (confirmed), so that this currency can be traded'
                ' during the given trading turn. "Worker" servers are'
                ' responsible for populating this table during the phase 1 of'
                ' each turn. The "solver" server will read from this table,'
                ' and will delete the records before advancing to phase 2 of'
                ' the turn.'
            ),
        },
    )


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
        {
            "comment": (
                'Represents relevant information about a given currency'
                ' (aka debtor), so that the currency can participate'
                ' in a given trading turn. The "solver" server will populate'
                ' this table before the start of phase 2 of each turn, and'
                ' will delete the records before advancing to phase 3.'
                ' "Worker" servers will read from this table, so as to'
                ' generate relevant buy and sell offers.'
            ),
        },
    )


class SellOffer(db.Model, ChooseRowsMixin):
    __bind_key__ = "solver"
    turn_id = db.Column(db.Integer, primary_key=True)
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    amount = db.Column(db.BigInteger, nullable=False)
    collector_id = db.Column(db.BigInteger, nullable=False)
    __table_args__ = (
        db.CheckConstraint(amount > 0),
        {
            "comment": (
                'Represents a sell offer, participating in a given trading'
                ' turn. "Worker" servers are responsible for populating this'
                ' table during the phase 2 of each turn. The "solver" server'
                ' will read from this table, and will delete the records'
                ' before advancing to phase 3 of the turn.'
            ),
        },
    )


class BuyOffer(db.Model, ChooseRowsMixin):
    __bind_key__ = "solver"
    turn_id = db.Column(db.Integer, primary_key=True)
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    amount = db.Column(db.BigInteger, nullable=False)
    __table_args__ = (
        db.CheckConstraint(amount > 0),
        {
            "comment": (
                'Represents a buy offer, participating in a given trading'
                ' turn. "Worker" servers are responsible for populating this'
                ' table during the phase 2 of each turn. The "solver" server'
                ' will read from this table, and will delete the records'
                ' before advancing to phase 3 of the turn.'
            ),
        },
    )


class CreditorTaking(db.Model, ChooseRowsMixin):
    __bind_key__ = "solver"
    turn_id = db.Column(db.Integer, primary_key=True)
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    creditor_hash = db.Column(db.SmallInteger, nullable=False)
    amount = db.Column(db.BigInteger, nullable=False)
    collector_id = db.Column(db.BigInteger, nullable=False)
    __table_args__ = (
        db.CheckConstraint(amount > 0),
        {
            "comment": (
                'Informs the "worker" server responsible for the given'
                ' customer account, that the given amount must be withdrawn'
                ' (taken) from the account, as part of the given trading turn.'
                ' During the phase 3 of each turn, "Worker" servers should'
                ' make their own copy of the records in this table, and then'
                ' delete the original records.'
            ),
        },
    )


class CollectorCollecting(db.Model, ChooseRowsMixin):
    __bind_key__ = "solver"
    turn_id = db.Column(db.Integer, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    amount = db.Column(db.BigInteger, nullable=False)
    collector_id = db.Column(db.BigInteger, nullable=False)
    collector_hash = db.Column(db.SmallInteger, nullable=False)
    __table_args__ = (
        db.CheckConstraint(amount > 0),
        {
            "comment": (
                'Informs the "worker" server responsible for the given'
                ' collector, that the given amount will be withdrawn'
                ' (collected) from the given customer account, as part of the'
                ' given trading turn. During the phase 3 of each turn,'
                ' "Worker" servers should make their own copy of the records'
                ' in this table, and then delete the original records.'
            ),
        },
    )


class CollectorSending(db.Model, ChooseRowsMixin):
    __bind_key__ = "solver"
    turn_id = db.Column(db.Integer, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    from_collector_id = db.Column(db.BigInteger, primary_key=True)
    to_collector_id = db.Column(db.BigInteger, primary_key=True)
    from_collector_hash = db.Column(db.SmallInteger, nullable=False)
    amount = db.Column(db.BigInteger, nullable=False)
    __table_args__ = (
        db.CheckConstraint(amount > 0),
        db.CheckConstraint(from_collector_id != to_collector_id),
        {
            "comment": (
                'Informs the "worker" server responsible for the given'
                ' "from collector" account, that the given amount must be'
                ' transferred (sent) to another collector account, as part of'
                ' the given trading turn. During the phase 3 of each turn,'
                ' "Worker" servers should make their own copy of the records'
                ' in this table, and then delete the original records.'
            ),
        },
    )


class CollectorReceiving(db.Model, ChooseRowsMixin):
    __bind_key__ = "solver"
    turn_id = db.Column(db.Integer, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    to_collector_id = db.Column(db.BigInteger, primary_key=True)
    from_collector_id = db.Column(db.BigInteger, primary_key=True)
    to_collector_hash = db.Column(db.SmallInteger, nullable=False)
    amount = db.Column(db.BigInteger, nullable=False)
    __table_args__ = (
        db.CheckConstraint(amount > 0),
        db.CheckConstraint(from_collector_id != to_collector_id),
        {
            "comment": (
                'Informs the "worker" server responsible for the given'
                ' "to collector" account, that the given amount will be'
                ' transferred (received) from another collector account, as'
                ' part of the given trading turn. During the phase 3 of each'
                ' turn, "Worker" servers should make their own copy of the'
                ' records in this table, and then delete the original records.'
            ),
        },
    )


class CollectorDispatching(db.Model, ChooseRowsMixin):
    __bind_key__ = "solver"
    turn_id = db.Column(db.Integer, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    amount = db.Column(db.BigInteger, nullable=False)
    collector_id = db.Column(db.BigInteger, nullable=False)
    collector_hash = db.Column(db.SmallInteger, nullable=False)
    __table_args__ = (
        db.CheckConstraint(amount > 0),
        {
            "comment": (
                'Informs the "worker" server responsible for the given'
                ' collector, that the given amount must be deposited'
                ' (dispatched) to the given customer account, as part of the'
                ' given trading turn. During the phase 3 of each turn,'
                ' "Worker" servers should make their own copy of the records'
                ' in this table, and then delete the original records.'
            ),
        },
    )


class CreditorGiving(db.Model, ChooseRowsMixin):
    __bind_key__ = "solver"
    turn_id = db.Column(db.Integer, primary_key=True)
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    creditor_hash = db.Column(db.SmallInteger, nullable=False)
    amount = db.Column(db.BigInteger, nullable=False)
    collector_id = db.Column(db.BigInteger, nullable=False)
    __table_args__ = (
        db.CheckConstraint(amount > 0),
        {
            "comment": (
                'Informs the "worker" server responsible for the given'
                ' customer account, that the given amount will be deposited'
                ' (given) to this account, as part of the given trading turn.'
                ' During the phase 3 of each turn, "Worker" servers should'
                ' make their own copy of the records in this table, and then'
                ' delete the original records.'
            ),
        },
    )


class OverloadedCurrency(db.Model):
    __bind_key__ = "solver"
    turn_id = db.Column(db.Integer, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    collectors_count = db.Column(db.Integer, nullable=False)
    __table_args__ = (
        db.CheckConstraint(collectors_count > 0),
        {
            "comment": (
                'Represents the fact that during the given trading turn, the'
                ' number of active collector accounts with the given currency'
                ' have been too small to handle all the transfers.'
            ),
        },
    )


class HoardedCurrency(db.Model, ChooseRowsMixin):
    __bind_key__ = "solver"
    turn_id = db.Column(db.Integer, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    peg_debtor_id = db.Column(db.BigInteger)
    peg_exchange_rate = db.Column(db.FLOAT)
    __table_args__ = (
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
                'Represents the fact that during the given trading turn, the'
                ' owner of the creditors agent node wants to buy (hoard)'
                ' the given currency.'
            ),
        },
    )


class MostBoughtCurrency(db.Model):
    __bind_key__ = "solver"
    debtor_id = db.Column(db.BigInteger, primary_key=True, autoincrement=False)
    buyers_average_count = db.Column(db.REAL, nullable=False)
    debtor_info_locator = db.Column(db.String, nullable=False)
    __table_args__ = (
        db.CheckConstraint(buyers_average_count >= 0.0),
        {
            "comment": (
                'Represents the fact that currently, according to the'
                ' collected trade statistics, a given currency is among the'
                ' most bought currencies.'
            ),
        },
    )
