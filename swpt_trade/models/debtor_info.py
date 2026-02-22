from __future__ import annotations
from datetime import datetime, timedelta
from sqlalchemy.sql.expression import null, or_, and_
from swpt_trade.extensions import db
from .common import get_now_utc, SqlUtilsMixin


class DebtorInfoDocument(db.Model, SqlUtilsMixin):
    debtor_info_locator = db.Column(db.String, primary_key=True)
    debtor_id = db.Column(db.BigInteger, nullable=False)
    peg_debtor_info_locator = db.Column(db.String)
    peg_debtor_id = db.Column(db.BigInteger)
    peg_exchange_rate = db.Column(db.FLOAT)
    will_not_change_until = db.Column(db.TIMESTAMP(timezone=True))
    fetched_at = db.Column(
        db.TIMESTAMP(timezone=True), nullable=False, default=get_now_utc
    )
    __table_args__ = (
        db.CheckConstraint(peg_exchange_rate >= 0.0),
        db.CheckConstraint(
            or_(
                and_(
                    peg_debtor_info_locator == null(),
                    peg_debtor_id == null(),
                    peg_exchange_rate == null(),
                ),
                and_(
                    peg_debtor_info_locator != null(),
                    peg_debtor_id != null(),
                    peg_exchange_rate != null(),
                ),
            )
        ),
        {
            "comment": (
                "Represents relevant trading information about a given"
                " currency (aka debtor), that have been parsed from the"
                " debtor's debtor info document, obtained via HTTP request."
            ),
        },
    )

    def has_expired(
            self,
            current_ts: datetime,
            expiry_period: timedelta,
    ) -> bool:
        frozen_until = self.will_not_change_until
        is_frozen = frozen_until is not None and frozen_until > current_ts
        return not is_frozen and current_ts - self.fetched_at > expiry_period


class DebtorLocatorClaim(db.Model, SqlUtilsMixin):
    debtor_id = db.Column(db.BigInteger, primary_key=True, autoincrement=False)
    debtor_info_locator = db.Column(db.String)
    latest_locator_fetch_at = db.Column(db.TIMESTAMP(timezone=True))
    latest_discovery_fetch_at = db.Column(
        db.TIMESTAMP(timezone=True), nullable=False, default=get_now_utc
    )
    forced_locator_refetch_at = db.Column(db.TIMESTAMP(timezone=True))
    __table_args__ = (
        db.CheckConstraint(
            or_(
                and_(
                    debtor_info_locator == null(),
                    latest_locator_fetch_at == null(),
                ),
                and_(
                    debtor_info_locator != null(),
                    latest_locator_fetch_at != null(),
                ),
            )
        ),
        {
            "comment": (
                "Represents a reliable claim made by a given debtor,"
                " declaring what the official debtor info locator for"
                " the given debtor is."
            ),
        },
    )


class DebtorInfoFetch(db.Model, SqlUtilsMixin):
    iri = db.Column(db.String, primary_key=True)
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    is_locator_fetch = db.Column(db.BOOLEAN, nullable=False, default=False)
    is_discovery_fetch = db.Column(db.BOOLEAN, nullable=False, default=False)
    forced_iri = db.Column(db.String)
    recursion_level = db.Column(db.SmallInteger, nullable=False, default=0)
    inserted_at = db.Column(
        db.TIMESTAMP(timezone=True), nullable=False, default=get_now_utc
    )
    attempts_count = db.Column(db.SmallInteger, nullable=False, default=0)
    latest_attempt_at = db.Column(db.TIMESTAMP(timezone=True))
    latest_attempt_errorcode = db.Column(db.SmallInteger)
    next_attempt_at = db.Column(
        db.TIMESTAMP(timezone=True), nullable=False, default=get_now_utc
    )
    __table_args__ = (
        db.CheckConstraint(recursion_level >= 0),
        db.CheckConstraint(
            or_(
                and_(
                    attempts_count == 0,
                    latest_attempt_at == null(),
                    latest_attempt_errorcode == null(),
                ),
                and_(
                    attempts_count > 0,
                    latest_attempt_at != null(),
                ),
            )
        ),
        db.Index("idx_debtor_info_fetch_next_attempt_at", next_attempt_at),
        {
            "comment": (
                "Represents a scheduled HTTP request (HTTP fetch) to obtain"
                " relevant trading information about a given currency (aka"
                " debtor). There are two non-mutually exclusive request types:"
                " 1) a locator fetch, which wants to obtain the latest version"
                " of the debtor's debtor info document, from the official"
                " debtor info locator; 2) a discovery fetch, which wants to"
                " obtain a particular (possibly obsolete) version of the"
                " debtor's debtor info document, not necessarily from the"
                " official debtor info locator."
            ),
        },
    )
