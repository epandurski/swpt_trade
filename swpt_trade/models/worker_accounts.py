from __future__ import annotations
from sqlalchemy.sql.expression import and_
from swpt_trade.extensions import db
from .common import get_now_utc


class NeededWorkerAccount(db.Model):
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    configured_at = db.Column(
        db.TIMESTAMP(timezone=True), nullable=False, default=get_now_utc
    )
    __table_args__ = (
        {
            "comment": (
                'Represents the fact that a "worker" server has requested'
                ' the configuration (aka creation) of a Swaptacular account,'
                ' which will be used to collect and dispatch transfers.'
            ),
        },
    )


class WorkerAccount(db.Model):
    CONFIG_SCHEDULED_FOR_DELETION_FLAG = 1 << 0

    creditor_id = db.Column(db.BigInteger, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    creation_date = db.Column(db.DATE, nullable=False)
    last_change_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
    last_change_seqnum = db.Column(db.Integer, nullable=False)
    principal = db.Column(db.BigInteger, nullable=False)
    interest = db.Column(db.FLOAT, nullable=False)
    interest_rate = db.Column(db.REAL, nullable=False)
    last_interest_rate_change_ts = db.Column(
        db.TIMESTAMP(timezone=True), nullable=False
    )
    config_flags = db.Column(db.Integer, nullable=False)
    account_id = db.Column(db.String, nullable=False)
    debtor_info_iri = db.Column(db.String)
    last_transfer_number = db.Column(db.BigInteger, nullable=False)
    last_transfer_committed_at = db.Column(
        db.TIMESTAMP(timezone=True), nullable=False
    )
    demurrage_rate = db.Column(db.FLOAT, nullable=False)
    commit_period = db.Column(db.Integer, nullable=False)
    transfer_note_max_bytes = db.Column(db.Integer, nullable=False)
    last_heartbeat_ts = db.Column(
        db.TIMESTAMP(timezone=True), nullable=False, default=get_now_utc
    )
    __table_args__ = (
        db.CheckConstraint(interest_rate >= -100.0),
        db.CheckConstraint(transfer_note_max_bytes >= 0),
        db.CheckConstraint(last_transfer_number >= 0),
        db.CheckConstraint(
            and_(demurrage_rate >= -100.0, demurrage_rate <= 0.0)
        ),
        db.CheckConstraint(commit_period >= 0),
        {
            "comment": (
                'Represents an existing Swaptacular account, managed by a '
                ' "worker" server. The account is used to collect and dispatch'
                ' transfers.'
            ),
        },
    )


class InterestRateChange(db.Model):
    # NOTE: The `interest_rate` column is not be part of the primary
    # key, but it probably is a good idea to include it in the primary
    # key index to allow index-only scans. Because SQLAlchemy does not
    # support this yet (2024-01-19), the migration file should be
    # edited so as not to create a "normal" index, but create a
    # "covering" index instead.

    # TODO: Consider solving the following hypothetical problem:
    #
    # When the interest rate has been changed twice (or more) in a
    # relatively short period of time (which normally should not
    # happen), and the `AccountUpdate` SMP message for the second
    # change has been received, but the `AccountUpdate` message for
    # the first change has not been received yet, the data in this
    # table will indicate the wrong overall interest rate, which may
    # lead to a wrong calculation for the amount that needs to be
    # transferred.
    #
    # One possible way to fix this problem (if it happens to be a
    # problem in practice) would be to detect when one or more
    # `AccountUpdate` messages have been missing, by keeping track of
    # the numbers in the `last_change_seqnum` field of all received
    # `AccountUpdate` messages. Then, in case is conceivable that some
    # changes in the interest rate may have gone unnoticed, the
    # calculation of the amounts that needs to be transferred would be
    # postponed until all missing `AccountUpdate` messages have been
    # received.

    creditor_id = db.Column(db.BigInteger, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    change_ts = db.Column(db.TIMESTAMP(timezone=True), primary_key=True)
    interest_rate = db.Column(db.REAL, nullable=False)
    __table_args__ = (
        db.CheckConstraint(interest_rate >= -100.0),
        {
            "comment": (
                "Indicates a change in the interest rate on a given collector"
                " account. The history of recent interest rate changes is"
                " needed in order to correctly determine the due interest"
                " on traded amounts."
            ),
        },
    )
