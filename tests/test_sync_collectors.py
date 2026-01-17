from swpt_pythonlib.utils import ShardingRealm
from swpt_trade.extensions import db
from swpt_trade import models as m
from swpt_trade import sync_collectors


def test_apply_collector_changes(
        app,
        db_session,
        restore_sharding_realm,
        current_ts,
):

    app.config["SHARDING_REALM"] = sr = ShardingRealm("1.#")
    app.config["DELETE_PARENT_SHARD_RECORDS"] = False

    assert sr.match(123)
    assert sr.match(127)
    assert sr.match(128)
    assert sr.match(130)
    assert not sr.match(129)

    ca1 = m.CollectorAccount(
        debtor_id=666, collector_id=123, status=0
    )
    ca2 = m.CollectorAccount(
        debtor_id=666, collector_id=127, status=1
    )
    ca3 = m.CollectorAccount(
        debtor_id=777, collector_id=128, status=3, account_id="777-128"
    )
    ca4 = m.CollectorAccount(
        debtor_id=888, collector_id=128, status=3, account_id="888-128"
    )
    ca5 = m.CollectorAccount(
        debtor_id=888, collector_id=129, status=3, account_id="888-129"
    )
    ca6 = m.CollectorAccount(
        debtor_id=888, collector_id=130, status=2, account_id="888-130"
    )
    db.session.add(ca1)
    db.session.add(ca2)
    db.session.add(ca3)
    db.session.add(ca4)
    db.session.add(ca5)
    db.session.add(ca6)
    db.session.commit()

    db.session.add(
        m.CollectorStatusChange(
            collector_id=123,
            debtor_id=666,
            from_status=0,
            to_status=1,
        )
    )
    db.session.add(
        m.CollectorStatusChange(
            collector_id=127,
            debtor_id=666,
            from_status=1,
            to_status=2,
            account_id="test_account_id",
        )
    )
    db.session.add(
        m.CollectorStatusChange(
            collector_id=128,
            debtor_id=777,
            from_status=3,
            to_status=2,
        )
    )
    db.session.add(
        m.CollectorStatusChange(
            collector_id=128,
            debtor_id=888,
            from_status=3,
            to_status=1,
        )
    )
    db.session.add(
        # Not in this shard.
        m.CollectorStatusChange(
            collector_id=129,
            debtor_id=888,
            from_status=3,
            to_status=1,
        )
    )
    db.session.add(
        # Incorrect from_status.
        m.CollectorStatusChange(
            collector_id=130,
            debtor_id=888,
            from_status=3,
            to_status=1,
        )
    )

    db.session.add(
        m.NeededCollectorAccount(
            debtor_id=123,
            collector_id=1111,
        )
    )
    db.session.add(
        # Not in this shard.
        m.NeededCollectorAccount(
            debtor_id=129,
            collector_id=2222,
        )
    )
    db.session.commit()

    assert len(m.CollectorAccount.query.all()) == 6
    sync_collectors.process_collector_status_changes()
    sync_collectors.create_needed_collector_accounts()

    cas = m.CollectorAccount.query.all()
    cas.sort(key=lambda x: (x.debtor_id, x.collector_id))
    assert len(cas) == 7
    assert cas[0].status == 0
    assert cas[0].account_id == ""
    assert cas[0].debtor_id == 123
    assert cas[0].collector_id == 1111

    assert cas[1].status == 1
    assert cas[1].account_id == ""
    assert cas[1].debtor_id == 666
    assert cas[1].collector_id == 123

    assert cas[2].status == 2
    assert cas[2].account_id == "test_account_id"
    assert cas[2].debtor_id == 666
    assert cas[2].collector_id == 127

    assert cas[3].status == 2
    assert cas[3].account_id == "777-128"
    assert cas[3].debtor_id == 777
    assert cas[3].collector_id == 128

    assert cas[4].status == 1
    assert cas[4].account_id == "888-128"
    assert cas[4].debtor_id == 888
    assert cas[4].collector_id == 128

    assert cas[5].status == 3
    assert cas[5].account_id == "888-129"
    assert cas[5].debtor_id == 888
    assert cas[5].collector_id == 129

    assert cas[6].status == 2
    assert cas[6].account_id == "888-130"
    assert cas[6].debtor_id == 888
    assert cas[6].collector_id == 130
