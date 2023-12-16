# distutils: language = c++
import hashlib
from cpython cimport array
from libc.math cimport NAN
from libcpp cimport bool

cdef i64 DEFAULT_MIN_TRADE_AMOUNT = 1000
cdef distance DEFAULT_MAX_DISTANCE_TO_BASE = 10


cdef class CandidateOffer:
    """A trader bid, that may eventually become a confirmed offer.

    Instances of this class have the following fields:

    cdef readonly i64 amount
    cdef readonly i64 debtor_id
    cdef readonly i64 creditor_id

    The `amount` field can be negative (the trader wants to sell), or
    positive (the trader wants to buy). The amount can not be zero.
    """
    def is_buy_offer(self):
        return self.amount > 0

    def is_sell_offer(self):
        return self.amount < 0


cdef class BidProcessor:
    """Processes traders' bids, consulting the tree of currencies.

    The currencies are organized in a tree-like structure, each
    currency pointing to its peg currency. At the root of the tree is
    the "base currency". The base currency is identified by a ("debtor
    info URI", "debtor ID") pair.

    Currencies that are separated from the base currency by no more
    than `max_distance_to_base` pegs will be considered "priceable",
    and will be included in the currency tree.

    The attempted trades in all currencies will be for amounts greater
    of equal than the specified `min_trade_amount`. Possible trades
    for lesser amounts will be ignored.

    Usage example:

    >>> bp = BidProcessor('https://x.com/101', 101)
    >>> bp.register_currency(True, 'https://example.com/101', 101)
    >>> bp.register_currency(True, 'https://example.com/102', 102,
    ...   'https://example.com/101', 101, 2.0)
    >>> bp.register_currency(False, 'https://x.com/103', 103,
    ...   'https://example.com/101', 101, 1.0)
    >>> bp.get_currency_price(101)
    1.0
    >>> bp.get_currency_price(102)
    2.0
    >>> bp.get_currency_price(103)
    nan
    >>> bp.register_bid(1, 101, 8000)
    >>> bp.register_bid(1, 102, -6000, 101, 2.0)
    >>> bp.analyze_bids()
    [<CandidateOffer object at ....>, <CandidateOffer object at ....>]
    >>> bp.register_bid(2, 101, 5000)
    >>> bp.register_bid(2, 102, -4000, 101, 2.0)
    >>> bp.register_bid(2, 103, -3000, 101, 1.0)
    >>> bp.register_bid(3, 101, 2000)  # another trader
    >>> bp.analyze_bids()
    [<CandidateOffer object at ....>, <CandidateOffer object at ....>]
    >>> list(bp.currencies_to_be_confirmed())
    >>> [103]
    """
    def __cinit__(
        self,
        str base_debtor_info_uri,
        i64 base_debtor_id,
        distance max_distance_to_base=DEFAULT_MAX_DISTANCE_TO_BASE,
        i64 min_trade_amount=DEFAULT_MIN_TRADE_AMOUNT,
    ):
        self.base_debtor_info_uri = base_debtor_info_uri
        self.base_debtor_id = base_debtor_id
        self.max_distance_to_base = max_distance_to_base
        self.min_trade_amount = min_trade_amount
        self.bid_registry_ptr = new BidRegistry(base_debtor_id)
        self.currency_registry_ptr = new CurrencyRegistry(
            self._calc_key128(base_debtor_info_uri),
            base_debtor_id,
            max_distance_to_base,
        )
        self.candidate_offers = []

    def __dealloc__(self):
        del self.bid_registry_ptr
        del self.currency_registry_ptr

    def register_currency(
        self,
        bool confirmed,
        str debtor_info_uri,
        i64 debtor_id,
        str peg_debtor_info_uri='',
        i64 peg_debtor_id=0,
        float peg_exchange_rate=NAN,
    ):
        """Register a currency, which might be pegged to another
        currency.

        When the `confirmed` flag is `True`, this means that a system
        account has been successfully created in this currency, and
        the currency's debtor info document has been confirmed as
        correct.

        Both the pegged currency and the peg currency are identified
        by a ("debtor info URI", "debtor ID") pair.

        The given `peg_exchange_rate` specifies the exchange rate
        between the pegged currency and the peg currency. For example,
        `2.0` would mean that pegged currency's tokens are twice as
        valuable as peg currency's tokens. Note that 0.0, +inf, -inf,
        and NAN are also acceptable exchange rate values.
        """
        self.currency_registry_ptr.add_currency(
            confirmed,
            self._calc_key128(debtor_info_uri),
            debtor_id,
            self._calc_key128(peg_debtor_info_uri),
            peg_debtor_id,
            peg_exchange_rate,
        )

    def get_currency_price(self, i64 debtor_id):
        """Return the price of a tradable currency, expressed in base
        currency's tokens.

        This method should be called only after all the participating
        currencies have been registered (by calling the
        `register_currency` method for each one of them).

        `NAN` will be returned if the currency determined by the given
        `debtor_id` is not tradable (that is: the currency it is not
        both confirmed and priceable).
        """
        currency_registry = self.currency_registry_ptr
        currency_registry.prepare_for_queries()
        return currency_registry.get_currency_price(debtor_id)

    def register_bid(
        self,
        i64 creditor_id,
        i64 debtor_id,
        i64 amount,
        i64 peg_debtor_id=0,
        float peg_exchange_rate=NAN,
    ):
        """Tells the disposition of a given trader to a given
        currency.

        The trader is determined by the `creditor_id` argument. The
        currency is determined by the `debtor_id` argument.

        The `amount` field can be negative (the trader wants to sell),
        positive (the trader wants to buy), or zero (the trader do not
        want to trade). Bids with zero amounts must also be
        registered, because they may declare an approved by the trader
        exchange rate to another currency (the `peg_debtor_id` and
        `peg_exchange_rate` arguments).
        """
        self.bid_registry_ptr.add_bid(
            creditor_id,
            debtor_id,
            amount,
            peg_debtor_id,
            peg_exchange_rate,
        )

    def analyze_bids(self):
        """Analyze registered bids and return a list of candidate
        offers.

        This method should be called only after all the participating
        currencies have been registered (by calling the
        `register_currency` method for each one of them).

        Note that this method causes all bids that have been
        registered so far to be analyzed and discarded. This means
        that an immediate second call to `analyze_bids` will return an
        empty list or candidate offers.

        It is possible however, after a call to this method, to
        register a new batch of bids, and call the `analyze_bids`
        method again. In this case, the second call will analyze and
        discard the second batch of bids. This process can be repeated
        as many times as needed.

        IMPORTANT: All bids coming from one trader should be included
        in a single batch of registered bids. That is: When we start
        registering bids from a given trader (by calling the
        `register_bid` method), we should not call `analyze_bids`
        until all the bids from that trader had been registered.
        """
        self.currency_registry_ptr.prepare_for_queries()
        bid_registry = self.bid_registry_ptr

        while (bid := bid_registry.get_priceable_bid()) != NULL:
            currency = self._find_tradable_currency(bid)
            if (
                currency != NULL
                and compare_prices(bid.currency_price, currency.currency_price)
                and abs(bid.amount) >= self.min_trade_amount
            ):
                self._add_candidate_offer(bid)

        # Obviously, no deals can be arranged for traders which do not
        # have at least one buy offer, and at least one sell offer.
        # Therefore, we eliminate offers from such traders.
        cdef CandidateOffer o
        candidate_offers = [
            o for o in self.candidate_offers if (
                self.buyers.count(o.creditor_id)
                if o.amount < 0
                else self.sellers.count(o.creditor_id)
            ) != 0
        ]

        # Free unused memory.
        del bid_registry
        self.bid_registry_ptr = new BidRegistry(self.base_debtor_id)
        self.candidate_offers.clear()
        self.buyers.clear()
        self.sellers.clear()

        return candidate_offers

    def currencies_to_be_confirmed(self):
        """Return an iterator over debtor IDs of non-confirmed,
        on-sale currencies.

        While the `analyze_bids` method analyzes the registered bids,
        it may discover sell offers for currencies for which a system
        account has not been created yet. Every `BidProcessor`
        instance will maintain an ever-growing set of debtor IDs of
        such currencies, so that system accounts could be created for
        them eventually.

        IMPORTANT: Successive calls to the `analyze_bids` method will
        not annul the maintained ever-growing set of non-confirmed,
        on-sale currencies.
        """
        for debtor_id in self.to_be_confirmed:
            yield debtor_id

    cdef Key128 _calc_key128(self, str uri):
        m = hashlib.sha256()
        m.update(uri.encode('utf8'))
        cdef i64[:] data = array.array('q', m.digest())
        return Key128(data[0], data[1])

    cdef Currency* _find_tradable_currency(self, Bid* bid):
        tc = self.currency_registry_ptr.get_tradable_currency(bid.debtor_id)
        if tc == NULL and bid.amount <= -self.min_trade_amount:
            # We must try to create a system account for this
            # currency, so that it can be traded in the future.
            self.to_be_confirmed.insert(bid.debtor_id)

        return tc

    cdef void _add_candidate_offer(self, Bid* bid):
        if bid.amount > 0:
            self.buyers.insert(bid.creditor_id)
        else:
            self.sellers.insert(bid.creditor_id)

        cdef CandidateOffer o = CandidateOffer()
        o.amount = bid.amount
        o.debtor_id = bid.debtor_id
        o.creditor_id = bid.creditor_id
        self.candidate_offers.append(o)
