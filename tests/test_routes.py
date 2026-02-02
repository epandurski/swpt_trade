import pytest
import csv
import io
from swpt_trade.models import MostBoughtCurrency
from swpt_pythonlib.swpt_uris import make_debtor_uri
from swpt_trade.extensions import db


@pytest.fixture(scope="function")
def client(app, db_session):
    return app.test_client()


@pytest.fixture(scope="function")
def most_bought_currencies(db_session):
    currencies = [
        (1000.0, "https://example.com/1", -1),
        (500.0, "https://example.com/2", 2),
    ]
    for t in currencies:
        db.session.add(
            MostBoughtCurrency(
                buyers_average_count=t[0],
                debtor_info_locator=t[1],
                debtor_id=t[2],
            )
        )
    db.session.commit()
    return currencies


def test_statistics(client, most_bought_currencies):
    r = client.get(
        "/trade/statistics/most-bought-currencies.csv",
        headers={"X-Swpt-User-Id": "creditors:3"},
    )
    assert r.status_code == 403

    r = client.get(
        "/trade/statistics/most-bought-currencies.csv",
        headers={"X-Swpt-User-Id": "INVALID"},
    )
    assert r.status_code == 403

    r = client.get(
        "/trade/statistics/most-bought-currencies.csv",
        headers={"X-Swpt-User-Id": "creditors:12345"},
    )
    assert r.status_code == 200
    assert r.content_type == "text/csv; charset=utf-8"
    assert "Cache-Control" in r.headers
    assert r.headers["Content-Disposition"].startswith(
        'attachment; filename="'
    )
    document = io.StringIO(r.data.decode("utf-8"))
    for row, t in zip(csv.reader(document), most_bought_currencies):
        assert float(row[0]) == t[0]
        assert row[1] == f"{t[1]}#{make_debtor_uri(t[2])}"

    # Ensure superuser is allowed.
    r = client.get("/trade/statistics/most-bought-currencies.csv")
    assert r.status_code == 200


def test_health_check(client):
    r = client.get("/trade/health/check/public")
    assert r.status_code == 200
    assert r.content_type == "text/plain"
