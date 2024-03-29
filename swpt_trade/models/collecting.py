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
