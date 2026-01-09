# distutils: language = c++
import cython
import random
from cython.operator cimport dereference as deref, postincrement
from libcpp.utility cimport pair as Pair
from libcpp.unordered_set cimport unordered_set
from libcpp.unordered_map cimport unordered_multimap
from libc.stdlib cimport rand
from libc.math cimport NAN
from libcpp cimport bool
from .pricing cimport distance, BidProcessor
from .pricing import (
    DEFAULT_MAX_DISTANCE_TO_BASE,
    DEFAULT_MIN_TRADE_AMOUNT,
)
from .matching cimport Digraph
from collections import namedtuple


AccountChange = namedtuple('AccountChange', [
    'creditor_id',
    'debtor_id',
    'amount',
    'collector_id',
])


CollectorTransfer = namedtuple('CollectorTransfer', [
    'debtor_id',
    'from_creditor_id',
    'to_creditor_id',
    'amount',
])


cdef class Solver:
    """Builds a digraph, then finds the cycles and aggregates them.

    The currencies are organized in a tree-like structure, each
    currency pointing to its peg currency. At the root of the tree is
    the "base currency". The base currency is identified by a ("debtor
    info locator", "debtor ID") pair.

    Currencies that are separated from the base currency by more than
    `max_distance_to_base` pegs will be ignored, and will not be
    included in the currency tree.

    All suggested trades will be for amounts greater or equal than the
    specified `min_trade_amount`. Possible trades for lesser amounts
    will be ignored.

    Usage example:

    >>> s = Solver('https://example.com/101', 101)

    To register currencies:

    >>> s.register_currency(True, 'https://example.com/101', 101)
    >>> s.register_currency(True, 'https://example.com/102', 102,
    ...   'https://example.com/101', 101, 2.0)

    To register collector accounts:

    >>> s.register_collector_account(998, 101)
    >>> s.register_collector_account(998, 102)

    Sell and buy offer can be registered in any order, but this must
    happen only after all the currencies, and all the collector
    accounts have been registered. Note that when there are lots and
    lots of registered currencies, the first call to
    `register_sell_offer` or `register_buy_offer` methods may take
    some time, during which the registered currencies will be
    analyzed. To avoid this, the `analyze_currencies` method can be
    called, explicitly requesting the processing of the registered
    currencies.

    >>> s.register_sell_offer(1, 101, 8000, 999)
    # 1 is seller's creditor ID, 999 is collector account's creditor ID

    >>> s.register_buy_offer(1, 102, 6000)
    >>> s.register_sell_offer(2, 102, 3000, 999)
    >>> s.register_buy_offer(2, 101, 5000)

    Only after all sell and buy offers have been registered, we can
    see all the amounts that should be taken from sellers. These
    amounts will be received to the corresponding collector accounts:

    >>> list(s.takings_iter())
    [AccountChange(
       creditor_id=2, debtor_id=102, amount=-2500, collector_id=999),
     AccountChange(
       creditor_id=1, debtor_id=101, amount=-5000, collector_id=999)]

    Note that when there are lots and lots of registered offers, the
    production of the first item from the `s.takings_iter`,
    `s.collector_transfers_iter`, or `s.givings_iter` generator
    functions may take a significant amount of time, during which the
    registered offers will be analyzed. To avoid this, the
    `analyze_offers` method can be called, explicitly requesting the
    processing of the registered offers.

    Before we start giving to buyers, we should perform some transfers
    between collector accounts, so that each collector account
    receives exactly the same amount as it should give to buyers.

    >>> list(s.collector_transfers_iter())
    [CollectorTransfer(
       debtor_id=102, from_creditor_id=999, to_creditor_id=998, amount=2500),
     CollectorTransfer(
        debtor_id=101, from_creditor_id=999, to_creditor_id=998, amount=5000)]

    Finally, we want to see all the amounts that should be given to
    buyers. These amounts will be taken from the corresponding
    collector accounts:

    >>> list(s.givings_iter())
    [AccountChange(
       creditor_id=2, debtor_id=101, amount=5000, collector_id=998),
     AccountChange(
       creditor_id=1, debtor_id=102, amount=2500, collector_id=998)]
    """
    def __cinit__(
        self,
        str base_debtor_info_locator,
        i64 base_debtor_id,
        distance max_distance_to_base=DEFAULT_MAX_DISTANCE_TO_BASE,
        i64 min_trade_amount=DEFAULT_MIN_TRADE_AMOUNT,
    ):
        assert base_debtor_id != 0
        assert max_distance_to_base > 0
        assert min_trade_amount > 0
        self.base_debtor_info_locator = base_debtor_info_locator
        self.base_debtor_id = base_debtor_id
        self.max_distance_to_base = max_distance_to_base
        self.min_trade_amount = min_trade_amount
        self.debtor_ids_mask = random.getrandbits(63)
        self.bid_processor = BidProcessor(
            base_debtor_info_locator,
            base_debtor_id,
            max_distance_to_base,
            min_trade_amount,
        )
        self.graph = Digraph()
        self.currencies_analysis_done = False
        self.offers_analysis_done = False

    def register_currency(
        self,
        bool confirmed,
        str debtor_info_locator,
        i64 debtor_id,
        str peg_debtor_info_locator='',
        i64 peg_debtor_id=0,
        float peg_exchange_rate=NAN,
    ):
        """Register a currency, which might be pegged to another
        currency.

        When the `confirmed` flag is `True`, this means that a
        collector account has been successfully created in this
        currency, currency's parameters have been verified to be
        suitable for doing automated trades, and the currency's debtor
        info document have been verified to be correct.

        Both the pegged currency and the peg currency are identified
        by a ("debtor info locator", "debtor ID") pair.

        The given `peg_exchange_rate` specifies the exchange rate
        between the pegged currency and the peg currency. For example,
        `2.0` would mean that pegged currency's tokens are twice as
        valuable as peg currency's tokens. Note that 0.0, +inf, and
        NAN are also acceptable exchange rate values.
        """
        if self.currencies_analysis_done:
            raise RuntimeError(
                "A currency has been registered after currencies analysis."
            )

        self.bid_processor.register_currency(
            confirmed,
            debtor_info_locator,
            debtor_id,
            peg_debtor_info_locator,
            peg_debtor_id,
            peg_exchange_rate,
        )

        # NOTE: Here we XOR `debtor_id` with a random mask, so as to
        # randomize the order in which `self.debtor_ids` will be
        # iterated. The goal is not to favor some ID over others.
        self.debtor_ids.insert(debtor_id ^ self.debtor_ids_mask)

    def register_collector_account(self, i64 creditor_id, i64 debtor_id):
        """Registers a collector account.

        Collector accounts are used to receive traded amounts from the
        sellers, so that they can be later transferred to the buyers.

        Normally, at least one collector account should be registered
        for each tradable currency (each currency is uniquely
        identified a `debtor_id`). However, if no collector accounts
        are registered for some or all of the traded currencies, the
        sell offers' `collector_id` parameter (see the
        `register_sell_offer` method) will determine which collector
        account will be used for the corresponding trades.

        When more than one collector accounts are registered for a
        given currency, the outgoing transfers to the buyers of this
        currency will be evenly distributed between the registered
        collector accounts.
        """
        if self.currencies_analysis_done:
            raise RuntimeError(
                "A collector account has been registered after currencies"
                " analysis."
            )

        self.collector_accounts.insert(
            CollectorAccount(creditor_id, debtor_id)
        )

    cpdef void analyze_currencies(self):
        """Analyze registered currencies.

        This may take some time when there are lots and lots of
        registered currencies.

        After this method has been called, no currencies or collector
        accounts can be registered.
        """
        if not self.currencies_analysis_done:
            self.bid_processor.analyze_bids()
            min_trade_amount = float(self.min_trade_amount)

            for masked_debtor_id in self.debtor_ids:
                debtor_id = masked_debtor_id ^ self.debtor_ids_mask
                price = self.bid_processor.get_currency_price(debtor_id)
                if price > 0.0:
                    self.graph.add_currency(
                        debtor_id,  min_trade_amount * price
                    )

            self.debtor_ids.clear()
            self.currencies_analysis_done = True

    def register_sell_offer(
        self,
        i64 seller_creditor_id,
        i64 debtor_id,
        i64 amount,
        i64 collector_id,
    ):
        """Declares that a given seller (`seller_creditor_id`) wants
        to sell a given amount of a given currency (`debtor_id`).

        The `amount` should be positive.

        The `collector_id` argument is the creditor ID of the
        collector account that will eventually receive the traded
        amount from the seller, so that it can be transferred to the
        buyer(s).
        """
        if self.offers_analysis_done:
            raise RuntimeError(
                "A sell offer has been registered after offer analysis."
            )
        if not self.currencies_analysis_done:
            self.analyze_currencies()

        price = self.bid_processor.get_currency_price(debtor_id)
        if price > 0.0:
            self.graph.add_supply(
                amount * price, debtor_id, seller_creditor_id
            )
            seller_account = Account(seller_creditor_id, debtor_id)
            self.changes[seller_account] = AccountData(0, collector_id)

    def register_buy_offer(
        self,
        i64 buyer_creditor_id,
        i64 debtor_id,
        i64 amount,
    ):
        """Declares that a given buyer (`buyer_creditor_id`) wants to
        buy a given amount of a given currency (`debtor_id`).

        The `amount` should be positive.
        """
        if self.offers_analysis_done:
            raise RuntimeError(
                "A buy offer has been registered after offer analysis."
            )
        if not self.currencies_analysis_done:
            self.analyze_currencies()

        price = self.bid_processor.get_currency_price(debtor_id)
        if price > 0.0:
            self.graph.add_demand(amount * price, debtor_id, buyer_creditor_id)

    cpdef void analyze_offers(self):
        """Analyze registered offers.

        This may take a significant amount of time when there are lots
        and lots of registered offers.

        After this method has been called, no buying or selling offers
        can be registered.
        """
        cdef tuple t
        cdef double amount
        cdef i64[:] cycle

        if not self.offers_analysis_done:
            self.analyze_currencies()

            while True:
                t = self.graph.find_cycle()
                if t is None:
                    break
                amount, cycle = t
                self._process_cycle(amount, cycle)

            self._calc_collector_transfers()
            self.offers_analysis_done = True

    def takings_iter(self):
        """Iterate over the amounts that should be taken from the
        sellers.

        Each returned item will be a `AccountChange` namedtuple
        containing four `i64` numbers. The `amount` field determines
        the amount that should be taken, and it will always be a
        negative number.
        """
        self.analyze_offers()
        t = AccountChange

        for pair in self.changes:
            account = pair.first
            data = pair.second
            if data.amount_change < 0:
                yield t(
                    account.creditor_id,
                    account.debtor_id,
                    data.amount_change,
                    data.collector_id,
                )

    def collector_transfers_iter(self):
        """Iterate over the transfers that must be performed between
        collector accounts, so that at the end, each collector account
        receives exactly the same amount as it should give to buyers.

        Each returned item will be `CollectorTransfer` namedtuple
        containing four `i64` numbers. The `amount` field determines
        the amount that should be transferred, and it will always be a
        positive number.
        """
        self.analyze_offers()
        t = CollectorTransfer
        it = self.collector_transfers.begin()
        end = self.collector_transfers.end()

        while it != end:
            yield t(
                deref(it).debtor_id,
                deref(it).from_creditor_id,
                deref(it).to_creditor_id,
                deref(it).amount,
            )
            postincrement(it)

    def givings_iter(self):
        """Iterate over the amounts that should be given to the
        buyers.

        Each returned item will be a `AccountChange` namedtuple
        containing four `i64` numbers. The `amount` field determines
        the amount that should be given, and it will always be a
        positive number.
        """
        self.analyze_offers()
        t = AccountChange

        for pair in self.changes:
            account = pair.first
            data = pair.second
            if data.amount_change > 0:
                yield t(
                    account.creditor_id,
                    account.debtor_id,
                    data.amount_change,
                    data.collector_id,
                )

    cdef void _process_cycle(self, double amount, i64[:] cycle):
        cdef int n = len(cycle)
        cdef int i = 0
        cdef i64 amt

        while i < n:
            debtor_id = cycle[i]
            giver_data = &self.changes[Account(cycle[i - 1], debtor_id)]
            if giver_data.collector_id == 0:
                raise RuntimeError("invalid collector_id")

            taker_data = &self.changes[Account(cycle[i + 1], debtor_id)]
            if taker_data.collector_id == 0:
                taker_data.collector_id = self._get_random_collector_id(
                    giver_data.collector_id, debtor_id
                )

            price = self.bid_processor.get_currency_price(debtor_id)
            amt = calc_amt(amount, price)

            # If the transferring `amt` would cause an i64 overflow,
            # here we will decrease `amt`'s value to avoid the
            # overflow. In theory, a negligible overflowing may occur
            # in some edge cases, as a result of rounding errors.
            amt = -check_add(giver_data.amount_change, -amt)
            amt = +check_add(taker_data.amount_change, +amt)
            amt = self._update_collectors(
                giver_data.collector_id,
                taker_data.collector_id,
                debtor_id,
                amt,
            )

            giver_data.amount_change -= amt
            taker_data.amount_change += amt
            i += 2

    cdef i64 _update_collector(self, i64 creditor_id, i64 debtor_id, i64 amt):
        account = Account(creditor_id, debtor_id)
        amount_ptr = &self.collection_amounts[account]
        amt = check_add(deref(amount_ptr), amt)

        amount_ptr[0] = deref(amount_ptr) + amt
        return amt

    cdef i64 _update_collectors(
        self,
        i64 giver_collector_id,
        i64 taker_collector_id,
        i64 debtor_id,
        i64 amt,
    ):
        giver_account = Account(giver_collector_id, debtor_id)
        giver_amount_ptr = &self.collection_amounts[giver_account]
        amt = +check_add(deref(giver_amount_ptr), +amt)

        taker_account = Account(taker_collector_id, debtor_id)
        taker_amount_ptr = &self.collection_amounts[taker_account]
        amt = -check_add(deref(taker_amount_ptr), -amt)

        giver_amount_ptr[0] = deref(giver_amount_ptr) + amt
        taker_amount_ptr[0] = deref(taker_amount_ptr) - amt
        return amt

    cdef void _calc_collector_transfers(self):
        """Generate the list of transfers (`self.collector_transfers`)
        to be performed between collector accounts, so that each
        collector account receives exactly the same amount as it
        gives.
        """
        cdef unordered_multimap[CollectorAccount, i64] collector_amounts
        cdef unordered_set[i64] debtor_ids
        cdef i64 amt, giver_amount, taker_amount

        self.collector_transfers.clear()

        # Gather all "giver" and "taker" collector accounts into an
        # `unordered_multimap`. This multimap is used to efficiently
        # group collector accounts by their `debtor_id`s.
        for pair in self.collection_amounts:
            if pair.second != 0:
                creditor_id = pair.first.creditor_id
                debtor_id = pair.first.debtor_id
                collector_amounts.insert(
                    Pair[CollectorAccount, i64](
                        CollectorAccount(creditor_id, debtor_id),
                        pair.second,
                    )
                )
                debtor_ids.insert(debtor_id)

        for debtor_id in debtor_ids:
            # For each `debtor_id` we create two iterators: one for
            # iterating over "giver" collector accounts (amount > 0),
            # and one for iterating over "taker" collector accounts
            # (amount < 0).
            debtor_root_account = CollectorAccount(0, debtor_id)
            givers = collector_amounts.equal_range(debtor_root_account)
            takers = collector_amounts.equal_range(debtor_root_account)

            # Continue advancing both iterators, and generate
            # transfers from "giver" collector accounts to "taker"
            # collector accounts, until all collector accounts are
            # equalized (amount == 0).
            while givers.first != givers.second:
                if deref(givers.first).second > 0:
                    while takers.first != takers.second:
                        giver_amount = deref(givers.first).second
                        taker_amount = deref(takers.first).second
                        if taker_amount < 0:
                            amt = min(giver_amount, -taker_amount)
                            deref(givers.first).second = giver_amount - amt
                            deref(takers.first).second = taker_amount + amt
                            self.collector_transfers.push_back(
                                Transfer(
                                    debtor_id,
                                    deref(givers.first).first.creditor_id,
                                    deref(takers.first).first.creditor_id,
                                    amt,
                                )
                            )
                            if deref(givers.first).second == 0:
                                break
                            assert deref(takers.first).second == 0
                        postincrement(takers.first)
                    else:
                       raise RuntimeError("can not equalize collectors")

                postincrement(givers.first)

            # Ensure there are no "taker" collectors accounts left.
            while takers.first != takers.second:
                if deref(takers.first).second < 0:
                    raise RuntimeError("can not equalize collectors")
                postincrement(takers.first)

    @cython.cdivision(True)
    cdef i64 _get_random_collector_id(
        self,
        i64 giver_collector_id,
        i64 debtor_id
    ):
        cdef size_t count
        cdef int n

        account = CollectorAccount(giver_collector_id, debtor_id)
        count = self.collector_accounts.count(account)

        if count == 1:
            # There is only one matching collector account. (This
            # should be by far the most common case.)
            return deref(self.collector_accounts.find(account)).creditor_id
        elif count > 1:
            # Randomly select one of the matching collector accounts.
            n = rand() % count
            pair = self.collector_accounts.equal_range(account)
            it = pair.first
            while n > 0:
                postincrement(it)
                n -= 1
            return deref(it).creditor_id
        else:
            # There are no matching collector accounts. Normally this
            # should never happen. Nevertheless, using the giver's
            # collector account to pay the taker seems to be a pretty
            # reliable fallback.
            return giver_collector_id
