import pytest
from datetime import datetime, timedelta
from swpt_trade.extensions import db
from swpt_trade import models as m
from swpt_pythonlib.utils import ShardingRealm
from swpt_trade.fetch_debtor_infos import (
    FetchResult,
    InvalidDebtorInfoDocument,
    process_debtor_info_fetches,
    _make_https_requests,
    _parse_debtor_info_document,
)


@pytest.fixture()
def restore_expiry(app):
    orig_debtor_info_expiry_days = app.config["APP_DEBTOR_INFO_EXPIRY_DAYS"]
    yield
    app.config["APP_DEBTOR_INFO_EXPIRY_DAYS"] = orig_debtor_info_expiry_days


def test_last_fetch_retry(mocker, app, db_session, restore_expiry):
    app.config["APP_DEBTOR_INFO_EXPIRY_DAYS"] = -10.0

    def make_https_requests(fetches, **kwargs):
        return [
            FetchResult(
                fetch=f,
                errorcode=500,
                retry=True,
            ) for f in fetches
        ]

    mocker.patch(
        "swpt_trade.fetch_debtor_infos._make_https_requests",
        new=make_https_requests,
    )

    db.session.add(
        m.DebtorInfoFetch(
            iri="https://example.com/666",
            debtor_id=666,
            is_locator_fetch=True,
            is_discovery_fetch=False,
        )
    )
    db.session.commit()

    assert process_debtor_info_fetches(1, 0.1) == 1
    assert len(m.DebtorInfoFetch.query.all()) == 0


def test_cached_and_wrong_shard(
        mocker,
        app,
        db_session,
        current_ts,
        restore_sharding_realm,
):
    app.config["SHARDING_REALM"] = ShardingRealm("0.#")

    def make_https_requests(fetches, **kwargs):
        assert len(fetches) == 0
        return []

    mocker.patch(
        "swpt_trade.fetch_debtor_infos._make_https_requests",
        new=make_https_requests,
    )

    db.session.add(
        m.DebtorInfoDocument(
            debtor_info_locator="https://example.com/666",
            debtor_id=666,
            will_not_change_until=current_ts + timedelta(days=1000),
        )
    )
    db.session.add(
        m.DebtorInfoFetch(
            iri="https://example.com/666",
            debtor_id=666,
            is_locator_fetch=True,
            is_discovery_fetch=False,
        )
    )
    db.session.add(
        m.DebtorInfoFetch(
            iri="https://example.com/888",
            debtor_id=888,
            is_locator_fetch=True,
            is_discovery_fetch=False,
        )
    )
    db.session.commit()

    assert process_debtor_info_fetches(1, 0.1) == 2
    assert len(m.DebtorInfoFetch.query.all()) == 0
    assert len(m.DebtorInfoDocument.query.all()) == 1


def test_process_debtor_info_fetches(mocker, app, db_session, current_ts):
    def make_https_requests(fetches, **kwargs):
        return [
            (
                FetchResult(
                    fetch=f,
                    document=m.DebtorInfoDocument(
                        debtor_info_locator=f.iri,
                        debtor_id=f.debtor_id,
                        peg_debtor_info_locator="https://example.com/777",
                        peg_debtor_id=777,
                        peg_exchange_rate=2.0,
                    ),
                    store_document=f.is_locator_fetch,
                ) if f.iri.startswith("https://") else FetchResult(
                    fetch=f,
                    errorcode=500,
                    retry=True,
                )
            ) for f in fetches
        ]

    mocker.patch(
        "swpt_trade.fetch_debtor_infos._make_https_requests",
        new=make_https_requests,
    )

    dif1 = m.DebtorInfoFetch(
        iri="https://example.com/666",
        debtor_id=666,
        is_locator_fetch=True,
        is_discovery_fetch=True,
    )
    dif2 = m.DebtorInfoFetch(
        iri="wrong IRI",
        debtor_id=999,
        is_locator_fetch=True,
        is_discovery_fetch=False,
      )
    db.session.add(dif1)
    db.session.add(dif2)
    db.session.commit()

    assert process_debtor_info_fetches(1, 0.1) == 2

    fetches = m.DebtorInfoFetch.query.all()
    assert len(fetches) == 1
    fetches[0].iri == "wrong IRI"
    fetches[0].debtor_id == 999
    fetches[0].is_locator_fetch is True
    fetches[0].is_discovery_fetch is False
    fetches[0].recursion_level == 0
    fetches[0].attempts_count == 1
    fetches[0].latest_attempt_at is not None
    fetches[0].latest_attempt_errorcode == 500
    fetches[0].next_attempt_at > current_ts + timedelta(seconds=15)

    fetch_signals = m.FetchDebtorInfoSignal.query.all()
    assert len(fetch_signals) == 1
    assert fetch_signals[0].iri == "https://example.com/777"
    assert fetch_signals[0].debtor_id == 777
    assert fetch_signals[0].is_locator_fetch is True
    assert fetch_signals[0].is_discovery_fetch is False
    assert fetch_signals[0].recursion_level == 1

    stored_signals = m.StoreDocumentSignal.query.all()
    assert len(stored_signals) == 1
    assert stored_signals[0].debtor_info_locator == "https://example.com/666"
    assert stored_signals[0].debtor_id == 666
    assert stored_signals[0].peg_debtor_info_locator == (
        "https://example.com/777"
    )
    assert stored_signals[0].peg_debtor_id == 777
    assert stored_signals[0].peg_exchange_rate == 2.0
    assert stored_signals[0].will_not_change_until is None

    confirmations = m.ConfirmDebtorSignal.query.all()
    assert len(confirmations) == 1
    assert confirmations[0].debtor_id == 666
    assert confirmations[0].debtor_info_locator == "https://example.com/666"


def test_make_https_requests(app, mocker):
    def parse_debtor_info_document(content_type, body):
        assert content_type == "text/html"
        assert isinstance(body, bytes)
        raise InvalidDebtorInfoDocument('ups!')

    mocker.patch(
        "swpt_trade.fetch_debtor_infos._parse_debtor_info_document",
        new=parse_debtor_info_document,
    )
    results = _make_https_requests(
        [
            m.DebtorInfoFetch(
                iri="https://raifj38jprjapt9j4at.com",
                debtor_id=555,
                is_locator_fetch=True,
            ),
            m.DebtorInfoFetch(
                iri="invalid://swaptacular.github.io/",
                debtor_id=666,
                is_locator_fetch=True,
            ),
            m.DebtorInfoFetch(
                iri="https://!q:2222hub.io/",
                debtor_id=777,
                is_locator_fetch=True,
            ),
            m.DebtorInfoFetch(
                iri="https://swaptacular.github.io/",
                debtor_id=888,
                is_locator_fetch=True,
            ),
        ],
        max_connections=10,
        timeout=30.0,
    )
    assert len(results) == 4
    results.sort(key=lambda r: r.fetch.debtor_id)

    assert results[0].fetch.debtor_id == 555
    assert results[0].document is None
    assert results[0].retry is True

    assert results[1].fetch.debtor_id == 666
    assert results[1].document is None
    assert results[1].retry is False

    assert results[2].fetch.debtor_id == 777
    assert results[2].document is None
    assert results[2].retry is False

    assert results[3].fetch.debtor_id == 888
    assert results[3].document is None
    assert results[3].retry is True


def test_parse_debtor_info_document(app):
    with pytest.raises(
            InvalidDebtorInfoDocument,
            match="Unknown debtor info document type",
    ):
        _parse_debtor_info_document("text/html", b'test')

    with pytest.raises(
            InvalidDebtorInfoDocument,
            match="Too long CoinInfo document",
    ):
        _parse_debtor_info_document(
            "application/vnd.swaptacular.coin-info+json", 100000 * b'x'
        )

    with pytest.raises(
            InvalidDebtorInfoDocument,
            match="Invalid CoinInfo document",
    ):
        _parse_debtor_info_document(
            "application/vnd.swaptacular.coin-info+json", b'test'
        )

    with pytest.raises(
            InvalidDebtorInfoDocument,
            match="Invalid CoinInfo document",
    ):
        _parse_debtor_info_document(
            "application/vnd.swaptacular.coin-info+json", b'{}'
        )

    d = _parse_debtor_info_document(
        "application/vnd.swaptacular.coin-info+json",
        b"""{
          "peg": {
            "type": "Peg",
            "exchangeRate": 3.14,
            "debtorIdentity": {
              "type": "DebtorIdentity",
              "uri": "swpt:4640381880"
            },
            "latestDebtorInfo": {
              "uri": "https://demo.swaptacular.org/debtors/4640381880"
            }
          },
          "debtorIdentity": {
            "type": "DebtorIdentity",
            "uri": "swpt:6199429176"
          },
          "latestDebtorInfo": {
            "uri": "https://demo.swaptacular.org/debtors/6199429176"
          },
          "type": "CoinInfo",
          "willNotChangeUntil": "2030-10-22T07:10:11Z"
        }""",
    )
    assert isinstance(d, m.DebtorInfoDocument)
    assert d.debtor_id == 6199429176
    assert d.debtor_info_locator == (
        "https://demo.swaptacular.org/debtors/6199429176"
    )
    assert d.will_not_change_until == (
        datetime.fromisoformat("2030-10-22T07:10:11Z")
    )
    assert d.peg_debtor_id == 4640381880
    assert d.peg_debtor_info_locator == (
        "https://demo.swaptacular.org/debtors/4640381880"
    )
    assert d.peg_exchange_rate == 3.14

    d = _parse_debtor_info_document(
        "application/vnd.swaptacular.coin-info+json",
        b"""{
          "debtorIdentity": {
            "type": "DebtorIdentity",
            "uri": "swpt:6199429176"
          },
          "latestDebtorInfo": {
            "uri": "https://demo.swaptacular.org/debtors/6199429176"
          },
          "type": "CoinInfo"
        }""",
    )
    assert isinstance(d, m.DebtorInfoDocument)
    assert d.debtor_id == 6199429176
    assert d.debtor_info_locator == (
        "https://demo.swaptacular.org/debtors/6199429176"
    )
    assert d.will_not_change_until is None
    assert d.peg_debtor_id is None
    assert d.peg_debtor_info_locator is None
    assert d.peg_exchange_rate is None

    with pytest.raises(
            InvalidDebtorInfoDocument,
            match="Invalid debtor URI",
    ):
        _parse_debtor_info_document(
            "application/vnd.swaptacular.coin-info+json",
            b"""{
              "debtorIdentity": {
                "type": "DebtorIdentity",
                "uri": "INVALID_DEBTOR_URI"
              },
              "latestDebtorInfo": {
                "uri": "https://demo.swaptacular.org/debtors/6199429176"
              },
              "type": "CoinInfo"
            }""",
        )

    with pytest.raises(
            InvalidDebtorInfoDocument,
            match="Invalid peg debtor URI",
    ):
        _parse_debtor_info_document(
            "application/vnd.swaptacular.coin-info+json",
            b"""{
              "debtorIdentity": {
                "type": "DebtorIdentity",
                "uri": "swpt:6199429176"
              },
              "latestDebtorInfo": {
                "uri": "https://demo.swaptacular.org/debtors/6199429176"
              },
              "peg": {
                "type": "Peg",
                "exchangeRate": 3.14,
                "debtorIdentity": {
                  "type": "DebtorIdentity",
                  "uri": "INVALID_DEBTOR_URI"
                },
                "latestDebtorInfo": {
                  "uri": "https://demo.swaptacular.org/debtors/4640381880"
                }
              },
              "type": "CoinInfo"
            }""",
        )
