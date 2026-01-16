import pytest
from swpt_trade.models import CollectorAccount
from swpt_trade.routes import schemas


@pytest.fixture(scope="function")
def client(app, db_session):
    return app.test_client()


def test_collector_account_schema(app, current_ts):
    cas = schemas.CollectorAccountSchema()
    ca = CollectorAccount(
        debtor_id=666,
        collector_id=12345678,
        account_id="test_account_id",
        status=2,
        latest_status_change_at=current_ts,
    )
    data = cas.dump(ca)
    assert data == {
        "type": "CollectorAccount",
        "debtorId": 666,
        "creditorId": 12345678,
        "accountId": "test_account_id",
        "status": 2,
        "latestStatusChangeAt": current_ts.isoformat(),
    }


def test_ensure_collectors(client):
    json_request = {
        "type": "ActivateCollectorsRequest",
        "numberOfAccounts": 5,
    }

    r = client.get(
        "/trade/collectors/666/list",
        headers={"X-Swpt-User-Id": "creditors:3"},
    )
    assert r.status_code == 403

    r = client.get(
        "/trade/collectors/666/list",
        headers={"X-Swpt-User-Id": "creditors:12345"},
    )
    assert r.status_code == 200
    assert r.content_type == "application/json"
    assert r.json == {
        "type": "DebtorCollectorsList",
        "debtorId": 666,
        "collectors": [],
    }

    r = client.post(
        "/trade/collectors/0/activate",
        json=json_request,
    )
    assert r.status_code == 500
    assert len(CollectorAccount.query.all()) == 0

    r = client.post(
        "/trade/collectors/666/activate",
        headers={"X-Swpt-User-Id": "creditors:3"},
        json=json_request,
    )
    assert r.status_code == 403
    assert len(CollectorAccount.query.all()) == 0

    r = client.post(
        "/trade/collectors/666/activate",
        headers={"X-Swpt-User-Id": "INVALID"},
        json=json_request,
    )
    assert r.status_code == 403
    assert len(CollectorAccount.query.all()) == 0

    r = client.post(
        "/trade/collectors/666/activate",
        headers={"X-Swpt-User-Id": "creditors-superuser"},
        json={
            "type": "WrongType",
            "debtorId": 666,
            "numberOfAccounts": 5,
        },
    )
    assert r.status_code == 422
    assert len(CollectorAccount.query.all()) == 0

    r = client.post(
        "/trade/collectors/666/activate",
        headers={"X-Swpt-User-Id": "creditors-supervisor"},
        json={},
    )
    assert r.status_code == 204
    cas = CollectorAccount.query.all()
    assert len(cas) == 1

    r = client.get(
        "/trade/collectors/666/list",
        headers={"X-Swpt-User-Id": "creditors-supervisor"},
    )
    assert r.status_code == 200
    assert r.content_type == "application/json"
    assert r.json == {
        "type": "DebtorCollectorsList",
        "debtorId": 666,
        "collectors": [
            {
                "type": "CollectorAccount",
                "debtorId": 666,
                "creditorId": 4294967763,
                "status": 0,
                "latestStatusChangeAt":
                cas[0].latest_status_change_at.isoformat(),
            }
        ],
    }

    r = client.post(
        "/trade/collectors/666/activate",
        headers={"X-Swpt-User-Id": "creditors-superuser"},
        json=json_request,
    )
    assert r.status_code == 204
    cas = CollectorAccount.query.all()
    assert len(cas) == 5
    assert all(x.debtor_id == 666 for x in cas)

    r = client.post(
        "/trade/collectors/666/activate",
        headers={"X-Swpt-User-Id": "creditors-superuser"},
        json=json_request,
    )
    assert r.status_code == 204
    assert len(CollectorAccount.query.all()) == 5

    r = client.post(
        "/trade/collectors/666/activate",
        json=json_request,
    )
    assert r.status_code == 204
    assert len(CollectorAccount.query.all()) == 5


def test_health_check(client):
    r = client.get("/trade/health/check/public")
    assert r.status_code == 200
    assert r.content_type == "text/plain"
