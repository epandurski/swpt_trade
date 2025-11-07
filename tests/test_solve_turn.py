from swpt_trade.solve_turn import try_to_advance_turn_to_phase3
from swpt_trade.utils import calc_hash
from swpt_trade.models import (
    CollectorAccount,
    Turn,
    CurrencyInfo,
    SellOffer,
    BuyOffer,
    CollectorDispatching,
    CollectorReceiving,
    CollectorSending,
    CollectorCollecting,
    CreditorGiving,
    CreditorTaking,
    OverloadedCurrency,
    TS0,
)


def test_try_to_advance_turn_to_phase3(db_session):
    turn = Turn(
        phase=2,
        phase_deadline=TS0,
        collection_started_at=TS0,
        collection_deadline=TS0,
        base_debtor_info_locator="https://example.com/101",
        base_debtor_id=101,
        max_distance_to_base=5,
        min_trade_amount=5000,
    )
    db_session.add(turn)
    db_session.flush()
    db_session.commit()
    turn_id = turn.turn_id

    db_session.add(
        CurrencyInfo(
            turn_id=turn_id,
            debtor_info_locator='https://example.com/101',
            debtor_id=101,
            peg_debtor_info_locator=None,
            peg_debtor_id=None,
            peg_exchange_rate=None,
            is_confirmed=True,
        )
    )
    db_session.add(
        CurrencyInfo(
            turn_id=turn_id,
            debtor_info_locator='https://example.com/102',
            debtor_id=102,
            peg_debtor_info_locator='https://example.com/101',
            peg_debtor_id=101,
            peg_exchange_rate=2.0,
            is_confirmed=True,
        )
    )
    db_session.add(
        CollectorAccount(
            debtor_id=101, collector_id=997, account_id="997", status=2
        )
    )
    db_session.add(
        CollectorAccount(
            debtor_id=101, collector_id=998, account_id="998", status=2
        )
    )
    db_session.add(
        CollectorAccount(
            debtor_id=102, collector_id=999, account_id="999", status=2
        )
    )
    db_session.add(
        SellOffer(
            turn_id=turn_id,
            creditor_id=1,
            debtor_id=101,
            amount=5000,
            collector_id=997,
        )
    )
    db_session.add(
        SellOffer(
            turn_id=turn_id,
            creditor_id=2,
            debtor_id=101,
            amount=6000,
            collector_id=998,
        )
    )
    db_session.add(
        SellOffer(
            turn_id=turn_id,
            creditor_id=3,
            debtor_id=102,
            amount=10000,
            collector_id=999,
        )
    )
    db_session.add(
        BuyOffer(
            turn_id=turn_id,
            creditor_id=1,
            debtor_id=102,
            amount=20000,
        )
    )
    db_session.add(
        BuyOffer(
            turn_id=turn_id,
            creditor_id=2,
            debtor_id=102,
            amount=20000,
        )
    )
    db_session.add(
        BuyOffer(
            turn_id=turn_id,
            creditor_id=3,
            debtor_id=101,
            amount=30000,
        )
    )
    db_session.commit()

    ca = CollectorAccount.query.all()
    ca.sort(key=lambda row: row.collector_id)
    assert len(ca) == 3
    assert ca[0].collector_id == 997
    assert ca[0].collector_hash == calc_hash(ca[0].collector_id)
    assert ca[1].collector_id == 998
    assert ca[1].collector_hash == calc_hash(ca[1].collector_id)
    assert ca[2].collector_id == 999
    assert ca[2].collector_hash == calc_hash(ca[2].collector_id)

    try_to_advance_turn_to_phase3(turn)

    turn = Turn.query.filter_by(turn_id=turn_id).one()
    assert turn.phase == 3
    assert turn.phase_deadline is None
    assert turn.collection_started_at is not None

    assert len(CurrencyInfo.query.all()) == 0
    assert len(SellOffer.query.all()) == 0
    assert len(BuyOffer.query.all()) == 0

    ct = CreditorTaking.query.all()
    ct.sort(key=lambda row: (row.debtor_id, row.creditor_id))
    assert all(row.turn_id == turn_id for row in ct)
    assert len(ct) == 3
    assert ct[0].debtor_id == 101
    assert ct[0].creditor_id == 1
    assert ct[0].amount == 5000
    assert ct[0].collector_id == 997
    assert ct[0].creditor_hash == calc_hash(1)
    assert ct[1].debtor_id == 101
    assert ct[1].creditor_id == 2
    assert ct[1].amount == 6000
    assert ct[1].collector_id == 998
    assert ct[1].creditor_hash == calc_hash(2)
    assert ct[2].debtor_id == 102
    assert ct[2].creditor_id == 3
    assert ct[2].amount == 5500
    assert ct[2].collector_id == 999
    assert ct[2].creditor_hash == calc_hash(3)

    cc = CollectorCollecting.query.all()
    cc.sort(key=lambda row: (row.debtor_id, row.creditor_id))
    assert all(row.turn_id == turn_id for row in cc)
    assert len(cc) == 3
    assert cc[0].debtor_id == 101
    assert cc[0].creditor_id == 1
    assert cc[0].amount == 5000
    assert cc[0].collector_id == 997
    assert cc[0].collector_hash == calc_hash(997)
    assert cc[1].debtor_id == 101
    assert cc[1].creditor_id == 2
    assert cc[1].amount == 6000
    assert cc[1].collector_id == 998
    assert cc[1].collector_hash == calc_hash(998)
    assert cc[2].debtor_id == 102
    assert cc[2].creditor_id == 3
    assert cc[2].amount == 5500
    assert cc[2].collector_id == 999
    assert cc[2].collector_hash == calc_hash(999)

    cs = CollectorSending.query.all()
    cs.sort(key=lambda row: (row.debtor_id, row.from_collector_id))
    assert all(row.turn_id == turn_id for row in cs)
    assert len(cs) == 1
    assert cs[0].debtor_id == 101
    assert cs[0].from_collector_id != cs[0].to_collector_id
    assert cs[0].from_collector_id in [997, 998]
    assert cs[0].to_collector_id in [997, 998]
    c101 = cs[0].to_collector_id
    assert cs[0].amount == 6000 if c101 == 997 else 5000
    assert cs[0].from_collector_hash == calc_hash(cs[0].from_collector_id)

    cr = CollectorReceiving.query.all()
    cr.sort(key=lambda row: (row.debtor_id, row.to_collector_id))
    assert all(row.turn_id == turn_id for row in cr)
    assert len(cr) == 1
    assert cr[0].debtor_id == 101
    assert cr[0].from_collector_id == cs[0].from_collector_id
    assert cr[0].to_collector_id == cs[0].to_collector_id == c101
    assert cr[0].amount == 6000 if c101 == 997 else 5000
    assert cr[0].to_collector_hash == calc_hash(cr[0].to_collector_id)

    cd = CollectorDispatching.query.all()
    cd.sort(key=lambda row: (row.debtor_id, row.creditor_id))
    assert all(row.turn_id == turn_id for row in cd)
    assert len(cd) == 3
    assert cd[0].debtor_id == 101
    assert cd[0].creditor_id == 3
    assert cd[0].amount == 11000
    assert cd[0].collector_id == c101
    assert cd[0].collector_hash == calc_hash(c101)
    assert cd[1].debtor_id == 102
    assert cd[1].creditor_id == 1
    assert cd[1].amount == 2500
    assert cd[1].collector_id == 999
    assert cd[1].collector_hash == calc_hash(999)
    assert cd[2].debtor_id == 102
    assert cd[2].creditor_id == 2
    assert cd[2].amount == 3000
    assert cd[2].collector_id == 999
    assert cd[2].collector_hash == calc_hash(999)

    cg = CreditorGiving.query.all()
    cg.sort(key=lambda row: (row.debtor_id, row.creditor_id))
    assert all(row.turn_id == turn_id for row in cg)
    assert len(cg) == 3
    assert cg[0].debtor_id == 101
    assert cg[0].creditor_id == 3
    assert cg[0].amount == 11000
    assert cg[0].collector_id == c101
    assert cg[0].creditor_hash == calc_hash(3)
    assert cg[1].debtor_id == 102
    assert cg[1].creditor_id == 1
    assert cg[1].amount == 2500
    assert cg[1].collector_id == 999
    assert cg[1].creditor_hash == calc_hash(1)
    assert cg[2].debtor_id == 102
    assert cg[2].creditor_id == 2
    assert cg[2].amount == 3000
    assert cg[2].collector_id == 999
    assert cg[2].creditor_hash == calc_hash(2)

    oc = OverloadedCurrency.query.all()
    oc.sort(key=lambda row: row.debtor_id)
    assert len(oc) == 2
    assert oc[0].turn_id == turn_id
    assert oc[0].debtor_id == 101
    assert oc[0].collectors_count == 2
    assert oc[1].turn_id == turn_id
    assert oc[1].debtor_id == 102
    assert oc[1].collectors_count == 1
