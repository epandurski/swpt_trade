# distutils: language = c++
from libcpp cimport bool
from libcpp.unordered_set cimport unordered_set
from libcpp.unordered_set cimport unordered_multiset
from libcpp.unordered_map cimport unordered_map
from libcpp.vector cimport vector
from .pricing cimport distance, BidProcessor
from .matching cimport Digraph


cdef extern from *:
    """
    #ifndef AGGREGATION_CLASSES_H
    #define AGGREGATION_CLASSES_H

    #include <unordered_map>
    #include <stdexcept>
    #include <climits>
    #include <cmath>

    typedef long long i64;

    class Account {
    public:
      i64 creditor_id;
      i64 debtor_id;

      Account()
        : creditor_id(0), debtor_id(0) {
      }
      Account(i64 creditor_id, i64 debtor_id)
        : creditor_id(creditor_id), debtor_id(debtor_id) {
      }
      Account(const Account& other) {
        creditor_id = other.creditor_id;
        debtor_id = other.debtor_id;
      }
      bool operator== (const Account& other) const {
        return (
          creditor_id == other.creditor_id
          && debtor_id == other.debtor_id
        );
      }
      size_t calc_hash();
    };

    namespace std {
      template <> struct hash<Account>
      {
        inline size_t operator()(const Account& account) const
        {
          // Combine creditor_id and debtor_id to calculate a hash value.
          size_t h = hash<i64>()(account.creditor_id);
          size_t k = hash<i64>()(account.debtor_id);
          h ^= k + 0x9e3779b9 + (h << 6) + (h >> 2);
          return h;
        }
      };
    }

    inline size_t Account::calc_hash() {
      return std::hash<Account>()(*this);
    }

    class AccountData {
    public:
      i64 amount_change;
      i64 collector_id;

      AccountData()
        : amount_change(0), collector_id(0) {
      }
      AccountData(i64 amount_change, i64 collector_id)
        : amount_change(amount_change), collector_id(collector_id) {
      }
    };

    class CollectorAccount {
    public:
      i64 creditor_id;
      i64 debtor_id;

      CollectorAccount()
        : creditor_id(0), debtor_id(0) {
      }
      CollectorAccount(i64 creditor_id, i64 debtor_id)
        : creditor_id(creditor_id), debtor_id(debtor_id) {
      }
      CollectorAccount(const CollectorAccount& other) {
        creditor_id = other.creditor_id;
        debtor_id = other.debtor_id;
      }
      bool operator== (const CollectorAccount& other) const {
        return debtor_id == other.debtor_id;
      }
      size_t calc_hash();
    };

    namespace std {
      template <> struct hash<CollectorAccount>
      {
        inline size_t operator()(const CollectorAccount& account) const
        {
          // Ignore creditor_id and use only debtor_id.
          return hash<i64>()(account.debtor_id);
        }
      };
    }

    inline size_t CollectorAccount::calc_hash() {
      return std::hash<CollectorAccount>()(*this);
    }

    class Transfer {
    public:
      i64 debtor_id;
      i64 from_creditor_id;
      i64 to_creditor_id;
      i64 amount;

      Transfer(
        i64 debtor_id,
        i64 from_creditor_id,
        i64 to_creditor_id,
        i64 amount
      ) : debtor_id(debtor_id),
          from_creditor_id(from_creditor_id),
          to_creditor_id(to_creditor_id),
          amount(amount) {
      }
    };

    inline i64 check_add(i64 n, i64 amt)
    {
      // Returns the closest integer which can be added to `n` without
      // causing an overflow. If adding `amt` to `n` does not cause an
      // overflow, this function will return `amt` unmodified. Note that
      // when the result of the addition is -0x800000000000000, this is
      // treated as an overflow.
      if (n > 0) {
        if (LLONG_MAX - n < amt) {
          // would overflow
          return LLONG_MAX - n;
        }
      } else if (n < -LLONG_MAX) {
        if (amt < 0) {
          // already underflown
          return 0;
        }
      } else {
        if (amt < -(LLONG_MAX + n)) {
          // would underflow
          return -(LLONG_MAX + n);
        }
      }
      return amt;
    }

    inline i64 calc_amt(double amount, float price) {
      // Return `amount / price` as i64, handling all edge cases.
      if (price > 0.0) {
        double amt = amount / price;
        if (amt > 0.0) {
          return amt < LLONG_MAX ? llround(amt) : LLONG_MAX;
        }
      }
      return 0;
    }

    #endif
    """
    ctypedef long long i64

    cdef cppclass Account:
        """Uniquely indetifies an account.
        """
        const i64 creditor_id
        const i64 debtor_id
        Account() noexcept
        Account(i64, i64) noexcept
        size_t calc_hash() noexcept

    cdef cppclass AccountData:
        """Information about account's pending change.
        """
        i64 amount_change
        i64 collector_id
        AccountData() noexcept
        AccountData(i64, i64) noexcept

    cdef cppclass CollectorAccount:
        """Uniquely indetifies an account.

        The only difference between this class and the `Account` class
        is that the `creditor_id` field will be ignored during
        comparisons between `CollectorAccount` instances. This allows
        us to store collector accounts in unordered miltisets, so that
        we can quickly find all registered collector accounts for a
        given debtor ID.
        """
        const i64 creditor_id
        const i64 debtor_id
        CollectorAccount() noexcept
        CollectorAccount(i64, i64) noexcept
        size_t calc_hash() noexcept

    cdef cppclass Transfer:
        const i64 debtor_id
        const i64 from_creditor_id
        const i64 to_creditor_id
        const i64 amount
        Transfer(i64, i64, i64, i64)

    cdef i64 check_add(i64, i64)
    cdef i64 calc_amt(double, float)


cdef class Solver:
    cdef readonly str base_debtor_info_locator
    cdef readonly i64 base_debtor_id
    cdef readonly distance max_distance_to_base
    cdef readonly i64 min_trade_amount
    cdef readonly i64 debtor_ids_mask
    cdef BidProcessor bid_processor
    cdef Digraph graph
    cdef unordered_set[i64] debtor_ids
    cdef unordered_map[Account, AccountData] changes
    cdef unordered_multiset[CollectorAccount] collector_accounts
    cdef unordered_map[Account, i64] collection_amounts
    cdef vector[Transfer] collector_transfers
    cdef bool currencies_analysis_done
    cdef bool offers_analysis_done
    cpdef void analyze_currencies(self)
    cpdef void analyze_offers(self)
    cdef void _process_cycle(self, double, i64[:])
    cdef i64 _update_collector(self, i64, i64, i64)
    cdef i64 _update_collectors(self, i64, i64, i64, i64)
    cdef i64 _get_random_collector_id(self, i64, i64)
    cdef void _calc_collector_transfers(self)
