# distutils: language = c++

import pytest
import math
from datetime import date
from . import cytest
from swpt_trade.solver.pricing cimport (
    compare_prices,
    Key128,
    Currency,
    CurrencyRegistry,
    AuxData,
    Bid,
    BidRegistry,
    BidProcessor,
)
from swpt_trade.solver import CandidateOfferAuxData


@cytest
def test_compare_prices():
    assert compare_prices(1.0, 1.0)
    assert compare_prices(-1.0, -1.0)
    assert compare_prices(1.0, 1.000001)
    assert compare_prices(-1.0, -1.000001)
    assert compare_prices(1e-25, 1.000001e-25)
    assert compare_prices(1e+25, 1.000001e+25)
    assert compare_prices(-1e-25, -1.000001e-25)
    assert compare_prices(-1e+25, -1.000001e+25)

    assert not compare_prices(0.0, 0.000001)
    assert not compare_prices(0.0, 0.0)
    assert not compare_prices(1.0, 0.0)
    assert not compare_prices(0.0, 1.0)
    assert not compare_prices(-1.0, 0.0)
    assert not compare_prices(0.0, -1.0)
    assert not compare_prices(-1.0, 1.0)
    assert not compare_prices(1.0, -1.0)
    assert not compare_prices(1.0, 1.000011)
    assert not compare_prices(1e-25, 1.000011e-25)
    assert not compare_prices(1e+25, 1.000011e+25)
    assert not compare_prices(-1e-25, -1.000011e-25)
    assert not compare_prices(-1e+25, -1.000011e+25)
    assert not compare_prices(1.0, math.inf)
    assert not compare_prices(1.0, -math.inf)
    assert not compare_prices(1.0, math.nan)
    assert not compare_prices(math.inf, 1.0)
    assert not compare_prices(math.inf, math.inf)
    assert not compare_prices(math.inf, -math.inf)
    assert not compare_prices(math.inf, math.nan)
    assert not compare_prices(-math.inf, 1.0)
    assert not compare_prices(-math.inf, math.inf)
    assert not compare_prices(-math.inf, -math.inf)
    assert not compare_prices(-math.inf, math.nan)
    assert not compare_prices(math.nan, 1.0)
    assert not compare_prices(math.nan, math.nan)
    assert not compare_prices(math.nan, math.inf)
    assert not compare_prices(math.nan, -math.inf)


@cytest
def test_bid():
    cdef Bid* bid = new Bid(1, 101, -5000, 0, 1.0)
    assert bid != NULL
    assert bid.creditor_id == 1
    assert bid.debtor_id == 101
    assert bid.amount == -5000
    assert bid.peg_ptr == NULL
    assert bid.peg_exchange_rate == 1.0
    assert bid.aux_data.creation_date == 0
    assert bid.aux_data.last_transfer_number == 0
    
    del bid


@cytest
def test_bid_aux_data():
    cdef AuxData aux_data = AuxData()
    aux_data.creation_date = 1
    aux_data.last_transfer_number = 2
    cdef Bid* bid = new Bid(1, 101, -5000, 0, 1.0, aux_data)
    assert bid != NULL
    assert bid.creditor_id == 1
    assert bid.debtor_id == 101
    assert bid.amount == -5000
    assert bid.peg_ptr == NULL
    assert bid.peg_exchange_rate == 1.0
    assert bid.aux_data.creation_date == 1
    assert bid.aux_data.last_transfer_number == 2

    del bid


@cytest
def test_bid_registry():
    cdef BidRegistry* r = new BidRegistry(101)
    cdef AuxData aux_data = AuxData()
    aux_data.creation_date = 1
    aux_data.last_transfer_number = 2

    assert r.base_debtor_id == 101
    r.add_bid(1, 0, 1000, 101, 1.0, aux_data)  # ignored

    # priceable
    r.add_bid(1, 101, 6000, 0, 0.0)
    r.add_bid(1, 102, 5000, 101, 1.0, aux_data)
    r.add_bid(1, 122, 5000, 101, 1.0, aux_data)
    r.add_bid(1, 103, 4000, 102, 10.0, aux_data)
    r.add_bid(1, 133, 4000, 102, 10.0, aux_data)

    # not priceable
    r.add_bid(1, 104, 3000, 0, 1.0)
    r.add_bid(1, 105, 2000, 104, 1.0)
    r.add_bid(1, 155, 2000, 666, 1.0)

    # not priceable (a peg cylce)
    r.add_bid(1, 106, 900, 107, 1.0)
    r.add_bid(1, 107, 800, 106, 1.0)

    # not priceable (a different trader)
    r.add_bid(2, 123, 5000, 102, 1.0)

    with pytest.raises(RuntimeError):
        r.add_bid(2, 123, 5000, 102, 1.0)  # duplicated

    debtor_ids = []
    while (bid := r.get_priceable_bid()) != NULL:
        if bid.debtor_id != 101:
            assert bid.aux_data.creation_date == 1
            assert bid.aux_data.last_transfer_number == 2
        debtor_ids.append(bid.debtor_id)

    # The base (101) bid for trader `2` has been added automatically.
    assert len(debtor_ids) == 6
    assert sorted(debtor_ids) == [101, 101, 102, 103, 122, 133]

    with pytest.raises(RuntimeError):
        r.add_bid(1, 108, 700, 0, 1.0)

    del r

@cytest
def test_empty_bid_registry():
    cdef BidRegistry* r = new BidRegistry(101)
    assert r.get_priceable_bid() == NULL

    with pytest.raises(RuntimeError):
        r.add_bid(1, 108, 700, 0, 1.0)

    del r


@cytest
def test_key128_calc_hash():
    seen_values = set()
    for i in range(0, 500000, 5000):
        for j in range(1000000, 1700000, 7000):
            k = Key128(i, j)
            h = k.calc_hash()
            assert h not in seen_values
            seen_values.add(h)


@cytest
def test_currency():
    cdef Currency* c = new Currency(101, Key128(0, 0), 102, 2.0)
    assert c != NULL
    assert c.debtor_id == 101
    assert c.peg_exchange_rate == 2.0
    assert c.peg_ptr == NULL
    assert c.confirmed() is False
    assert c.tradable() is False
    del c


@cytest
def test_currency_registry():
    import math

    cdef CurrencyRegistry* r = new CurrencyRegistry(Key128(100, 1), 101, 2)
    assert r.base_debtor_key.first == 100
    assert r.base_debtor_key.second == 1
    assert r.base_debtor_id == 101
    assert r.max_distance_to_base == 2

    r.add_currency(False, Key128(100, 1), 101, Key128(0, 0), 0, math.nan)
    r.add_currency(True, Key128(100, 2), 102, Key128(100, 1), 101, 2.0)
    r.add_currency(True, Key128(100, 3), 103, Key128(100, 2), 102, 3.0)
    r.add_currency(False, Key128(100, 4), 104, Key128(100, 2), 102, 4.0)
    r.add_currency(False, Key128(100, 5), 105, Key128(100, 3), 103, 5.0)
    r.add_currency(True, Key128(100, 6), 106, Key128(100, 4), 104, 6.0)
    r.add_currency(False, Key128(100, 7), 107, Key128(100, 1), 101, 0.5)
    r.add_currency(True, Key128(100, 8), 108, Key128(100, 7), 107, 1.0)
    r.add_currency(False, Key128(100, 9), 109, Key128(100, 7), 107, 2.0)
    r.add_currency(True, Key128(100, 10), 110, Key128(100, 1), 0, 1.0)

    # ignored invalid debtor_id
    r.add_currency(False, Key128(100, 20), 0, Key128(100, 1), 101, 2.0)

    with pytest.raises(RuntimeError):
        # duplicated debtor key
        r.add_currency(True, Key128(100, 2), 102, Key128(100, 1), 101, 2.0)

    with pytest.raises(RuntimeError):
        # not prepared
        r.get_currency_price(101)

    with pytest.raises(RuntimeError):
        # not prepared
        r.get_tradable_currency(101)

    r.prepare_for_queries()

    with pytest.raises(RuntimeError):
        # after preparation
        r.add_currency(False, Key128(100, 11), 111, Key128(100, 7), 107, 1.0)

    for _ in range(2):
        assert math.isnan(r.get_currency_price(101))
        assert r.get_currency_price(102) == 2.0
        assert r.get_currency_price(103) == 2.0 * 3.0
        assert math.isnan(r.get_currency_price(104))
        assert math.isnan(r.get_currency_price(105))
        assert math.isnan(r.get_currency_price(106))
        assert math.isnan(r.get_currency_price(107))
        assert r.get_currency_price(108) == 0.5
        assert math.isnan(r.get_currency_price(109))
        assert math.isnan(r.get_currency_price(110))
        assert math.isnan(r.get_currency_price(666))

        assert r.get_tradable_currency(101) == NULL

        c102 = r.get_tradable_currency(102)
        assert c102.debtor_id == 102
        assert c102.peg_exchange_rate == 2.0
        assert c102.peg_ptr.debtor_id == 101
        assert c102.confirmed()
        assert c102.tradable()

        c103 = r.get_tradable_currency(103)
        assert c103.debtor_id == 103
        assert c103.peg_exchange_rate == 3.0
        assert c103.peg_ptr.debtor_id == 102
        assert c103.confirmed()
        assert c103.tradable()

        assert r.get_tradable_currency(104) == NULL
        assert r.get_tradable_currency(105) == NULL
        assert r.get_tradable_currency(106) == NULL
        assert r.get_tradable_currency(107) == NULL

        c108 = r.get_tradable_currency(108)
        assert c108.debtor_id == 108
        assert c108.peg_exchange_rate == 1.0
        assert c108.peg_ptr.debtor_id == 107
        assert c108.confirmed()
        assert c108.tradable()

        assert r.get_tradable_currency(109) == NULL
        assert r.get_tradable_currency(110) == NULL
        assert r.get_tradable_currency(666) == NULL

        r.prepare_for_queries()  # this should be possible

    del r


@cytest
def test_bp_calc_key128():
    import hashlib
    import sys

    bp = BidProcessor('', 1)
    for x in range(20):
        s = f'test{x}'
        key = bp._calc_key128(s)
        m = hashlib.sha256()
        m.update(s.encode('utf8'))
        digest = m.digest()
        first = int.from_bytes(digest[:8], sys.byteorder, signed="True")
        second = int.from_bytes(digest[8:16], sys.byteorder, signed="True")
        assert first == key.first
        assert second == key.second


@cytest
def test_bp_candidate_offers():
    bp = BidProcessor('https://x.com/101', 101, 2, 1000)
    bp.register_currency(False, 'https://x.com/101', 101)  # base
    bp.register_currency(
        True, 'https://x.com/102', 102, 'https://x.com/101', 101, 2.0
    )
    bp.register_currency(
        True, 'https://x.com/103', 103, 'https://x.com/102', 102, 3.0
    )
    bp.register_currency(
        True, 'https://x.com/104', 104, 'https://x.com/103', 103, 4.0
    )
    bp.register_currency(
        False, 'https://x.com/105', 105, 'https://x.com/101', 101, 5.0
    )
    bp.register_currency(
        True, 'https://x.com/106', 106, 'https://x.com/105', 105, 6.0
    )
    bp.register_currency(
        False, 'https://x.com/107', 107, 'https://x.com/102', 102, 7.0
    )
    bp.register_currency(
        True, 'https://x.com/108', 108, 'https://x.com/102', 666, 1.0
    )
    bp.register_currency(
        True, 'https://x.com/109', 109, 'https://x.com/666', 102, 1.0
    )
    assert math.isnan(bp.get_currency_price(101))
    assert bp.get_currency_price(102) == 2.0
    assert bp.get_currency_price(103) == 6.0
    assert math.isnan(bp.get_currency_price(104))
    assert math.isnan(bp.get_currency_price(107))
    assert math.isnan(bp.get_currency_price(108))
    assert math.isnan(bp.get_currency_price(109))
    assert math.isnan(bp.get_currency_price(105))
    assert bp.get_currency_price(106) == 30.0
    assert bp.get_tradable_currency_peg(106) == (105, 6.0)
    no_peg = bp.get_tradable_currency_peg(101)
    assert no_peg[0] == 0
    assert math.isnan(no_peg[1])
    no_peg = bp.get_tradable_currency_peg(107)
    assert no_peg[0] == 0
    assert math.isnan(no_peg[1])

    aux_data = CandidateOfferAuxData(
        creation_date=date(2023, 1, 4),
        last_transfer_number=1234,
    )

    with pytest.raises(TypeError):
        bp.register_bid(1, 105, -666666, 101, 5.0, "WRONG OBJECT TYPE")

    # No base bid is registered, but it will be added automatically!
    bp.register_bid(1, 105, -666666, 101, 5.0)  # not tradable
    bp.register_bid(1, 106, -50000, 105, 6.000005, aux_data)  # OK!
    bp.register_bid(1, 102, 10000, 101, 2.0)  # OK!
    bp.register_bid(1, 103, 100, 102, 3.0)  # abs(amount) is too small

    bp.register_bid(2, 101, 666666)  # not tradable
    bp.register_bid(2, 105, -666666, 101, 5.0)  # not tradable
    bp.register_bid(2, 106, -50000, 105, 6.0)  # OK, but no buyer for this!
    bp.register_bid(2, 102, 10000, 101, 1.999) # wrongly priced
    bp.register_bid(2, 103, 10000, 102, 3.0)  # pegged to wrongly priced

    bp.register_bid(3, 101, 666666)  # not tradable
    bp.register_bid(3, 105, -666666, 101, 5.0)  # not tradable
    bp.register_bid(3, 106, 50000, 105, 6.0)  # OK, but no seller for this!
    bp.register_bid(3, 102, -100, 101, 2.0)  # abs(amount) is too small
    bp.register_bid(3, 103, 10000, 102, 3.0)  # OK, but no seller for this!
    bp.register_bid(3, 104, -10000, 103, 4.0)  # too big distance to base

    offers = bp.analyze_bids()
    assert len(offers) == 2

    offer0, offer1 = offers
    if offer0.debtor_id != 102:
        offer0, offer1 = offer1, offer0

    assert offer0.debtor_id == 102
    assert offer0.creditor_id == 1
    assert offer0.amount == 10000
    assert offer0.aux_data.creation_date == date(1970, 1, 1)
    assert offer0.aux_data.last_transfer_number == 0
    assert offer0.is_buy_offer()
    assert not offer0.is_sell_offer()

    assert offer1.debtor_id == 106
    assert offer1.creditor_id == 1
    assert offer1.amount == -50000
    assert offer1.aux_data.creation_date == date(2023, 1, 4)
    assert offer1.aux_data.last_transfer_number == 1234
    assert not offer1.is_buy_offer()
    assert offer1.is_sell_offer()

    l = sorted(list(bp.currencies_to_be_confirmed()))
    assert l == [104, 105]

    assert len(bp.analyze_bids()) == 0
    assert len(bp.analyze_bids()) == 0

    bp.register_bid(5, 101, 666666)  # not tradable
    bp.register_bid(5, 105, -666666, 101, 5.0)  # not tradable
    bp.register_bid(5, 106, -50000, 105, 6.000005)  # OK!
    bp.register_bid(5, 102, 10000, 101, 2.0)  # OK!
    bp.register_bid(5, 103, 10000, 102, 3.0)  # OK!
    bp.register_bid(5, 107, -10000, 102, 7.0)  # not tradable
    assert len(bp.analyze_bids()) == 3
    assert len(bp.analyze_bids()) == 0

    with pytest.raises(RuntimeError):
        bp.register_currency(
            True, 'https://x.com/110', 110, 'https://x.com/102', 102, 1.0
        )

    l = sorted(list(bp.currencies_to_be_confirmed()))
    assert l == [104, 105, 107]

    bp.remove_currency_to_be_confirmed(104)
    l = sorted(list(bp.currencies_to_be_confirmed()))
    assert l == [105, 107]
    bp.remove_currency_to_be_confirmed(104)
    bp.remove_currency_to_be_confirmed(105)
    bp.remove_currency_to_be_confirmed(105)
    l = sorted(list(bp.currencies_to_be_confirmed()))
    assert l == [107]
    bp.remove_currency_to_be_confirmed(107)
    bp.remove_currency_to_be_confirmed(107)
    assert len(list(bp.currencies_to_be_confirmed())) == 0
