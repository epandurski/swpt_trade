import pytest
import json
from swpt_trade import schemas
from marshmallow import ValidationError
from datetime import datetime, date


def test_updated_ledger_message():
    s = schemas.UpdatedLedgerMessageSchema()

    data = s.loads("""{
    "type": "UpdatedLedger",
    "creditor_id": 1,
    "debtor_id": 2,
    "update_id": 3,
    "account_id": "test_account",
    "creation_date": "2022-01-01",
    "principal": 5000,
    "last_transfer_number": 234,
    "ts": "2022-01-01T00:00:00Z",
    "unknown": "ignored"
    }""")

    assert data['type'] == 'UpdatedLedger'
    assert data['creditor_id'] == 1
    assert type(data['creditor_id']) is int
    assert data['debtor_id'] == 2
    assert type(data['debtor_id']) is int
    assert data['update_id'] == 3
    assert type(data['update_id']) is int
    assert data['account_id'] == "test_account"
    assert data['creation_date'] == date(2022, 1, 1)
    assert data['principal'] == 5000
    assert type(data['principal']) is int
    assert data['last_transfer_number'] == 234
    assert type(data['last_transfer_number']) is int
    assert data['ts'] == datetime.fromisoformat('2022-01-01T00:00:00+00:00')
    assert "unknown" not in data

    with pytest.raises(ValidationError, match='Invalid type.'):
        data = s.loads("""{
        "type": "WrongType",
        "creditor_id": 1,
        "debtor_id": 2,
        "update_id": 3,
        "account_id": "test_account",
        "creation_date": "2022-01-01",
        "principal": 5000,
        "last_transfer_number": 234,
        "ts": "2022-01-01T00:00:00Z"
        }""")

    try:
        s.loads('{}')
    except ValidationError as e:
        assert len(e.messages) == len(data)
        assert all(m == ['Missing data for required field.']
                   for m in e.messages.values())


def test_updated_policy_message():
    s = schemas.UpdatedPolicyMessageSchema()

    data = s.loads("""{
    "type": "UpdatedPolicy",
    "creditor_id": 1,
    "debtor_id": 2,
    "update_id": 3,
    "policy_name": null,
    "min_principal": 1000,
    "max_principal": 2000,
    "peg_exchange_rate": 3.14,
    "peg_debtor_id": 666,
    "ts": "2022-01-01T00:00:00Z",
    "unknown": "ignored"
    }""")

    assert data['type'] == 'UpdatedPolicy'
    assert data['creditor_id'] == 1
    assert type(data['creditor_id']) is int
    assert data['debtor_id'] == 2
    assert type(data['debtor_id']) is int
    assert data['update_id'] == 3
    assert type(data['update_id']) is int
    assert data['policy_name'] is None
    assert data['min_principal'] == 1000
    assert type(data['min_principal']) is int
    assert data['max_principal'] == 2000
    assert type(data['max_principal']) is int
    assert data['peg_exchange_rate'] == 3.14
    assert data['peg_debtor_id'] == 666
    assert type(data['peg_debtor_id']) is int
    assert data['ts'] == datetime.fromisoformat('2022-01-01T00:00:00+00:00')
    assert "unknown" not in data

    data = s.loads("""{
    "type": "UpdatedPolicy",
    "creditor_id": 1,
    "debtor_id": 2,
    "update_id": 3,
    "min_principal": 1000,
    "max_principal": 2000,
    "ts": "2022-01-01T00:00:00Z",
    "unknown": "ignored"
    }""")

    assert data['type'] == 'UpdatedPolicy'
    assert data['creditor_id'] == 1
    assert type(data['creditor_id']) is int
    assert data['debtor_id'] == 2
    assert type(data['debtor_id']) is int
    assert data['update_id'] == 3
    assert type(data['update_id']) is int
    assert data['policy_name'] is None
    assert data['min_principal'] == 1000
    assert type(data['min_principal']) is int
    assert data['max_principal'] == 2000
    assert type(data['max_principal']) is int
    assert data['peg_exchange_rate'] is None
    assert data['peg_debtor_id'] is None
    assert data['ts'] == datetime.fromisoformat('2022-01-01T00:00:00+00:00')
    assert "unknown" not in data

    with pytest.raises(ValidationError, match='Invalid type.'):
        s.loads("""{
            "type": "WrongType",
            "creditor_id": 1,
            "debtor_id": 2,
            "update_id": 3,
            "policy_name": null,
            "min_principal": 1000,
            "max_principal": 2000,
            "peg_exchange_rate": 3.14,
            "peg_debtor_id": 666,
            "ts": "2022-01-01T00:00:00Z"
            }""")

    with pytest.raises(
            ValidationError,
            match='max_principal must be equal or greater than min_principal'
    ):
        s.loads("""{
            "type": "UpdatedPolicy",
            "creditor_id": 1,
            "debtor_id": 2,
            "update_id": 3,
            "min_principal": 1000,
            "max_principal": 500,
            "ts": "2022-01-01T00:00:00Z"
            }""")

    with pytest.raises(
            ValidationError,
            match='peg_exchange_rate and peg_debtor_id fields must',
    ):
        s.loads("""{
            "type": "UpdatedPolicy",
            "creditor_id": 1,
            "debtor_id": 2,
            "update_id": 3,
            "min_principal": 1000,
            "max_principal": 2000,
            "peg_exchange_rate": 3.14,
            "ts": "2022-01-01T00:00:00Z"
            }""")

    with pytest.raises(
            ValidationError,
            match='peg_exchange_rate and peg_debtor_id fields must',
    ):
        s.loads("""{
            "type": "UpdatedPolicy",
            "creditor_id": 1,
            "debtor_id": 2,
            "update_id": 3,
            "min_principal": 1000,
            "max_principal": 2000,
            "peg_debtor_id": 666,
            "ts": "2022-01-01T00:00:00Z"
            }""")

    try:
        s.loads('{}')
    except ValidationError as e:
        assert len(e.messages) == len(data) - 3
        assert all(m == ['Missing data for required field.']
                   for m in e.messages.values())


def test_updated_flags_message():
    s = schemas.UpdatedFlagsMessageSchema()

    data = s.loads("""{
    "type": "UpdatedFlags",
    "creditor_id": 1,
    "debtor_id": 2,
    "update_id": 3,
    "config_flags": -123,
    "ts": "2022-01-01T00:00:00Z",
    "unknown": "ignored"
    }""")

    assert data['type'] == 'UpdatedFlags'
    assert data['creditor_id'] == 1
    assert type(data['creditor_id']) is int
    assert data['debtor_id'] == 2
    assert type(data['debtor_id']) is int
    assert data['update_id'] == 3
    assert type(data['update_id']) is int
    assert data['config_flags'] == -123
    assert type(data['config_flags']) is int
    assert data['ts'] == datetime.fromisoformat('2022-01-01T00:00:00+00:00')
    assert "unknown" not in data

    with pytest.raises(ValidationError, match='Invalid type.'):
        s.loads("""{
        "type": "WrongType",
        "creditor_id": 1,
        "debtor_id": 2,
        "update_id": 3,
        "config_flags": -123,
        "ts": "2022-01-01T00:00:00Z"
        }""")

    try:
        s.loads('{}')
    except ValidationError as e:
        assert len(e.messages) == len(data)
        assert all(m == ['Missing data for required field.']
                   for m in e.messages.values())


def test_fetch_debtor_info_message():
    s = schemas.FetchDebtorInfoMessageSchema()

    data = s.loads("""{
    "type": "FetchDebtorInfo",
    "iri": "https://example.com/test",
    "debtor_id": 2,
    "is_locator_fetch": true,
    "is_discovery_fetch": false,
    "forced_iri": "https://example.com/",
    "recursion_level": 5,
    "ts": "2022-01-01T00:00:00Z",
    "unknown": "ignored"
    }""")

    assert data['type'] == 'FetchDebtorInfo'
    assert data['iri'] == 'https://example.com/test'
    assert data['debtor_id'] == 2
    assert type(data['debtor_id']) is int
    assert data['is_locator_fetch'] is True
    assert data['is_discovery_fetch'] is False
    assert data['forced_iri'] == "https://example.com/"
    assert data['recursion_level'] == 5
    assert type(data['recursion_level']) is int
    assert data['ts'] == datetime.fromisoformat('2022-01-01T00:00:00+00:00')
    assert "unknown" not in data

    with pytest.raises(ValidationError, match='Invalid type.'):
        s.loads("""{
        "type": "WrongType",
        "iri": "https://example.com/test",
        "debtor_id": 2,
        "is_locator_fetch": true,
        "is_discovery_fetch": false,
        "recursion_level": 5,
        "ts": "2022-01-01T00:00:00Z"
        }""")

    try:
        s.loads('{}')
    except ValidationError as e:
        assert len(e.messages) == len(data) - 1
        assert all(m == ['Missing data for required field.']
                   for m in e.messages.values())


def test_store_document_message():
    s = schemas.StoreDocumentMessageSchema()

    data = s.loads("""{
    "type": "StoreDocument",
    "debtor_info_locator": "https://example.com/test",
    "debtor_id": 2,
    "peg_debtor_info_locator": "https://example.com/test2",
    "peg_debtor_id": 22,
    "peg_exchange_rate": 3.14,
    "will_not_change_until": "2032-01-01T00:00:00Z",
    "ts": "2022-01-01T00:00:00Z",
    "unknown": "ignored"
    }""")

    assert data['type'] == 'StoreDocument'
    assert data['debtor_info_locator'] == 'https://example.com/test'
    assert data['debtor_id'] == 2
    assert type(data['debtor_id']) is int
    assert data['peg_debtor_info_locator'] == 'https://example.com/test2'
    assert data['peg_debtor_id'] == 22
    assert type(data['peg_debtor_id']) is int
    assert data['peg_exchange_rate'] == 3.14
    assert data['will_not_change_until'] == datetime.fromisoformat(
        '2032-01-01T00:00:00+00:00'
    )
    assert data['ts'] == datetime.fromisoformat('2022-01-01T00:00:00+00:00')
    assert "unknown" not in data

    data = s.loads("""{
    "type": "StoreDocument",
    "debtor_info_locator": "https://example.com/test",
    "debtor_id": 2,
    "ts": "2022-01-01T00:00:00Z"
    }""")

    assert data['type'] == 'StoreDocument'
    assert data['debtor_info_locator'] == 'https://example.com/test'
    assert data['debtor_id'] == 2
    assert type(data['debtor_id']) is int
    assert data['peg_debtor_info_locator'] is None
    assert data['peg_debtor_id'] is None
    assert data['peg_exchange_rate'] is None
    assert data['will_not_change_until'] is None
    assert data['ts'] == datetime.fromisoformat('2022-01-01T00:00:00+00:00')

    with pytest.raises(ValidationError, match='or must all be present'):
        s.loads("""{
        "type": "StoreDocument",
        "debtor_info_locator": "https://example.com/test",
        "debtor_id": 2,
        "peg_debtor_info_locator": "https://example.com/test2",
        "ts": "2022-01-01T00:00:00Z"
        }""")

    with pytest.raises(ValidationError, match='Invalid type.'):
        s.loads("""{
        "type": "WrongType",
        "debtor_info_locator": "https://example.com/test",
        "debtor_id": 2,
        "ts": "2022-01-01T00:00:00Z"
        }""")

    try:
        s.loads('{}')
    except ValidationError as e:
        assert len(e.messages) == len(data) - 4
        assert all(m == ['Missing data for required field.']
                   for m in e.messages.values())


def test_discover_debtor_message():
    s = schemas.DiscoverDebtorMessageSchema()

    data = s.loads("""{
    "type": "DiscoverDebtor",
    "debtor_id": 2,
    "iri": "https://example.com/test",
    "force_locator_refetch": false,
    "ts": "2022-01-01T00:00:00Z",
    "unknown": "ignored"
    }""")

    assert data['type'] == 'DiscoverDebtor'
    assert data['iri'] == 'https://example.com/test'
    assert data['force_locator_refetch'] is False
    assert data['debtor_id'] == 2
    assert type(data['debtor_id']) is int
    assert data['ts'] == datetime.fromisoformat('2022-01-01T00:00:00+00:00')
    assert "unknown" not in data

    with pytest.raises(ValidationError, match='Invalid type.'):
        data = s.loads("""{
        "type": "WrongType",
        "debtor_id": 2,
        "iri": "https://example.com/test",
        "force_locator_refetch": false,
        "ts": "2022-01-01T00:00:00Z"
        }""")

    try:
        s.loads('{}')
    except ValidationError as e:
        assert len(e.messages) == len(data)
        assert all(m == ['Missing data for required field.']
                   for m in e.messages.values())


def test_confirm_debtor_message():
    s = schemas.ConfirmDebtorMessageSchema()

    data = s.loads("""{
    "type": "ConfirmDebtor",
    "debtor_id": 2,
    "debtor_info_locator": "https://example.com/test",
    "ts": "2022-01-01T00:00:00Z",
    "unknown": "ignored"
    }""")

    assert data['type'] == 'ConfirmDebtor'
    assert data['debtor_info_locator'] == 'https://example.com/test'
    assert data['debtor_id'] == 2
    assert type(data['debtor_id']) is int
    assert data['ts'] == datetime.fromisoformat('2022-01-01T00:00:00+00:00')
    assert "unknown" not in data

    with pytest.raises(ValidationError, match='Invalid type.'):
        data = s.loads("""{
        "type": "WrongType",
        "debtor_id": 2,
        "debtor_info_locator": "https://example.com/test",
        "ts": "2022-01-01T00:00:00Z"
        }""")

    try:
        s.loads('{}')
    except ValidationError as e:
        assert len(e.messages) == len(data)
        assert all(m == ['Missing data for required field.']
                   for m in e.messages.values())


def test_candidate_offer_message():
    s = schemas.CandidateOfferMessageSchema()

    data = s.loads("""{
    "type": "CandidateOffer",
    "turn_id": 3,
    "debtor_id": 666,
    "creditor_id": 123,
    "amount": -9223372036854775807,
    "last_transfer_number": 678,
    "account_creation_date": "2024-03-11",
    "ts": "2022-01-01T00:00:00Z",
    "unknown": "ignored"
    }""")

    assert data['type'] == 'CandidateOffer'
    assert type(data['debtor_id']) is int
    assert data['debtor_id'] == 666
    assert type(data['creditor_id']) is int
    assert data['creditor_id'] == 123
    assert type(data['amount']) is int
    assert data['amount'] == -9223372036854775807
    assert type(data['last_transfer_number']) is int
    assert data['last_transfer_number'] == 678
    assert data['account_creation_date'] == date(2024, 3, 11)
    assert data['ts'] == datetime.fromisoformat('2022-01-01T00:00:00+00:00')
    assert "unknown" not in data

    with pytest.raises(ValidationError, match='Invalid type.'):
        data = s.loads("""{
        "type": "WrongType",
        "turn_id": 3,
        "debtor_id": 666,
        "creditor_id": 123,
        "amount": -9223372036854775807,
        "last_transfer_number": 678,
        "account_creation_date": "2024-03-11",
        "ts": "2022-01-01T00:00:00Z"
        }""")

    with pytest.raises(ValidationError, match='Must be greater than'):
        data = s.loads("""{
        "type": "CandidateOffer",
        "turn_id": 3,
        "debtor_id": 666,
        "creditor_id": 123,
        "amount": -9223372036854775808,
        "last_transfer_number": 678,
        "account_creation_date": "2024-03-11",
        "ts": "2022-01-01T00:00:00Z"
        }""")

    with pytest.raises(ValidationError, match='Amount can not be zero'):
        data = s.loads("""{
        "type": "CandidateOffer",
        "turn_id": 3,
        "debtor_id": 666,
        "creditor_id": 123,
        "amount": 0,
        "last_transfer_number": 678,
        "account_creation_date": "2024-03-11",
        "ts": "2022-01-01T00:00:00Z"
        }""")

    try:
        s.loads('{}')
    except ValidationError as e:
        assert len(e.messages) == len(data)
        assert all(m == ['Missing data for required field.']
                   for m in e.messages.values())


def test_needed_collector_message():
    s = schemas.NeededCollectorMessageSchema()

    data = s.loads("""{
    "type": "NeededCollector",
    "debtor_id": 666,
    "ts": "2022-01-01T00:00:00Z",
    "unknown": "ignored"
    }""")

    assert data['type'] == 'NeededCollector'
    assert type(data['debtor_id']) is int
    assert data['debtor_id'] == 666
    assert data['ts'] == datetime.fromisoformat('2022-01-01T00:00:00+00:00')
    assert "unknown" not in data

    with pytest.raises(ValidationError, match='Invalid type.'):
        data = s.loads("""{
        "type": "WrongType",
        "debtor_id": 666,
        "ts": "2022-01-01T00:00:00Z"
        }""")

    with pytest.raises(ValidationError, match='Invalid debtor ID'):
        data = s.loads("""{
        "type": "NeededCollector",
        "debtor_id": 0,
        "ts": "2022-01-01T00:00:00Z"
        }""")

    try:
        s.loads('{}')
    except ValidationError as e:
        assert len(e.messages) == len(data)
        assert all(m == ['Missing data for required field.']
                   for m in e.messages.values())


def test_revise_account_lock_message():
    s = schemas.ReviseAccountLockMessageSchema()

    data = s.loads("""{
    "type": "ReviseAccountLock",
    "creditor_id": 123,
    "debtor_id": 666,
    "turn_id": 1,
    "ts": "2022-01-01T00:00:00Z",
    "unknown": "ignored"
    }""")

    assert data['type'] == 'ReviseAccountLock'
    assert type(data['creditor_id']) is int
    assert data['creditor_id'] == 123
    assert type(data['debtor_id']) is int
    assert data['debtor_id'] == 666
    assert type(data['turn_id']) is int
    assert data['turn_id'] == 1
    assert data['ts'] == datetime.fromisoformat('2022-01-01T00:00:00+00:00')
    assert "unknown" not in data

    with pytest.raises(ValidationError, match='Invalid type.'):
        data = s.loads("""{
        "type": "WrongType",
        "creditor_id": 123,
        "debtor_id": 666,
        "turn_id": 1,
        "ts": "2022-01-01T00:00:00Z"
        }""")

    with pytest.raises(ValidationError, match='Invalid debtor ID'):
        data = s.loads("""{
        "type": "ReviseAccountLock",
        "creditor_id": 123,
        "debtor_id": 0,
        "turn_id": 1,
        "ts": "2022-01-01T00:00:00Z"
        }""")

    try:
        s.loads('{}')
    except ValidationError as e:
        assert len(e.messages) == len(data)
        assert all(m == ['Missing data for required field.']
                   for m in e.messages.values())


def test_trigger_transfer_message():
    s = schemas.TriggerTransferMessageSchema()

    data = s.loads("""{
    "type": "TriggerTransfer",
    "collector_id": 999,
    "debtor_id": 666,
    "turn_id": 1,
    "creditor_id": 123,
    "transfer_kind": 1,
    "ts": "2022-01-01T00:00:00Z",
    "unknown": "ignored"
    }""")

    assert data['type'] == 'TriggerTransfer'
    assert type(data['collector_id']) is int
    assert data['collector_id'] == 999
    assert type(data['turn_id']) is int
    assert data['turn_id'] == 1
    assert type(data['debtor_id']) is int
    assert data['debtor_id'] == 666
    assert type(data['creditor_id']) is int
    assert data['creditor_id'] == 123
    assert data['transfer_kind'] == 1
    assert data['ts'] == datetime.fromisoformat('2022-01-01T00:00:00+00:00')
    assert "unknown" not in data

    with pytest.raises(ValidationError, match='Invalid type.'):
        data = s.loads("""{
        "type": "WrongType",
        "collector_id": 999,
        "debtor_id": 666,
        "turn_id": 1,
        "creditor_id": 123,
        "transfer_kind": 1,
        "ts": "2022-01-01T00:00:00Z"
        }""")

    with pytest.raises(ValidationError, match='Invalid debtor ID'):
        data = s.loads("""{
        "type": "TriggerTransfer",
        "collector_id": 999,
        "debtor_id": 0,
        "turn_id": 1,
        "creditor_id": 123,
        "transfer_kind": 1,
        "ts": "2022-01-01T00:00:00Z"
        }""")

    try:
        s.loads('{}')
    except ValidationError as e:
        assert len(e.messages) == len(data)
        assert all(m == ['Missing data for required field.']
                   for m in e.messages.values())


def test_account_id_request_message():
    s = schemas.AccountIdRequestMessageSchema()

    data = s.loads("""{
    "type": "AccountIdRequest",
    "collector_id": 999,
    "debtor_id": 666,
    "turn_id": 1,
    "creditor_id": 123,
    "transfer_kind": 1,
    "ts": "2022-01-01T00:00:00Z",
    "unknown": "ignored"
    }""")

    assert data['type'] == 'AccountIdRequest'
    assert type(data['collector_id']) is int
    assert data['collector_id'] == 999
    assert type(data['turn_id']) is int
    assert data['turn_id'] == 1
    assert type(data['debtor_id']) is int
    assert data['debtor_id'] == 666
    assert type(data['creditor_id']) is int
    assert data['creditor_id'] == 123
    assert data['transfer_kind'] == 1
    assert data['ts'] == datetime.fromisoformat('2022-01-01T00:00:00+00:00')
    assert "unknown" not in data

    with pytest.raises(ValidationError, match='Invalid type.'):
        data = s.loads("""{
        "type": "WrongType",
        "collector_id": 999,
        "debtor_id": 666,
        "turn_id": 1,
        "creditor_id": 123,
        "transfer_kind": 1,
        "ts": "2022-01-01T00:00:00Z"
        }""")

    with pytest.raises(ValidationError, match='Invalid debtor ID'):
        data = s.loads("""{
        "type": "AccountIdRequest",
        "collector_id": 999,
        "debtor_id": 0,
        "turn_id": 1,
        "creditor_id": 123,
        "transfer_kind": 1,
        "ts": "2022-01-01T00:00:00Z"
        }""")

    try:
        s.loads('{}')
    except ValidationError as e:
        assert len(e.messages) == len(data)
        assert all(m == ['Missing data for required field.']
                   for m in e.messages.values())


def test_account_id_response_message():
    s = schemas.AccountIdResponseMessageSchema()

    data = s.loads("""{
    "type": "AccountIdResponse",
    "collector_id": 999,
    "debtor_id": 666,
    "turn_id": 1,
    "creditor_id": 123,
    "transfer_kind": 1,
    "account_id": "12345",
    "account_id_version": 789,
    "ts": "2022-01-01T00:00:00Z",
    "unknown": "ignored"
    }""")

    assert data['type'] == 'AccountIdResponse'
    assert type(data['collector_id']) is int
    assert data['collector_id'] == 999
    assert type(data['turn_id']) is int
    assert data['turn_id'] == 1
    assert type(data['debtor_id']) is int
    assert data['debtor_id'] == 666
    assert type(data['creditor_id']) is int
    assert data['creditor_id'] == 123
    assert data['transfer_kind'] == 1
    assert data['account_id'] == "12345"
    assert type(data['account_id_version']) is int
    assert data['account_id_version'] == 789
    assert data['ts'] == datetime.fromisoformat('2022-01-01T00:00:00+00:00')
    assert "unknown" not in data

    with pytest.raises(ValidationError, match='Invalid type.'):
        data = s.loads("""{
        "type": "WrongType",
        "collector_id": 999,
        "debtor_id": 666,
        "turn_id": 1,
        "creditor_id": 123,
        "transfer_kind": 1,
        "account_id": "12345",
        "account_id_version": 789,
        "ts": "2022-01-01T00:00:00Z"
        }""")

    with pytest.raises(ValidationError, match='Invalid debtor ID'):
        data = s.loads("""{
        "type": "AccountIdResponse",
        "collector_id": 999,
        "debtor_id": 0,
        "turn_id": 1,
        "creditor_id": 123,
        "transfer_kind": 1,
        "account_id": "12345",
        "account_id_version": 789,
        "ts": "2022-01-01T00:00:00Z"
        }""")

    try:
        s.loads('{}')
    except ValidationError as e:
        assert len(e.messages) == len(data)
        assert all(m == ['Missing data for required field.']
                   for m in e.messages.values())


def test_start_sending_message():
    s = schemas.StartSendingMessageSchema()

    data = s.loads("""{
    "type": "StartSending",
    "collector_id": 999,
    "debtor_id": 666,
    "turn_id": 1,
    "ts": "2022-01-01T00:00:00Z",
    "unknown": "ignored"
    }""")

    assert data['type'] == 'StartSending'
    assert type(data['collector_id']) is int
    assert data['collector_id'] == 999
    assert type(data['turn_id']) is int
    assert data['turn_id'] == 1
    assert type(data['debtor_id']) is int
    assert data['debtor_id'] == 666
    assert data['ts'] == datetime.fromisoformat('2022-01-01T00:00:00+00:00')
    assert "unknown" not in data

    with pytest.raises(ValidationError, match='Invalid type.'):
        data = s.loads("""{
        "type": "WrongType",
        "collector_id": 999,
        "debtor_id": 666,
        "turn_id": 1,
        "ts": "2022-01-01T00:00:00Z"
        }""")

    with pytest.raises(ValidationError, match='Invalid debtor ID'):
        data = s.loads("""{
        "type": "StartSending",
        "collector_id": 999,
        "debtor_id": 0,
        "turn_id": 1,
        "ts": "2022-01-01T00:00:00Z"
        }""")

    try:
        s.loads('{}')
    except ValidationError as e:
        assert len(e.messages) == len(data)
        assert all(m == ['Missing data for required field.']
                   for m in e.messages.values())


def test_start_dispatching_message():
    s = schemas.StartDispatchingMessageSchema()

    data = s.loads("""{
    "type": "StartDispatching",
    "collector_id": 999,
    "debtor_id": 666,
    "turn_id": 1,
    "ts": "2022-01-01T00:00:00Z",
    "unknown": "ignored"
    }""")

    assert data['type'] == 'StartDispatching'
    assert type(data['collector_id']) is int
    assert data['collector_id'] == 999
    assert type(data['turn_id']) is int
    assert data['turn_id'] == 1
    assert type(data['debtor_id']) is int
    assert data['debtor_id'] == 666
    assert data['ts'] == datetime.fromisoformat('2022-01-01T00:00:00+00:00')
    assert "unknown" not in data

    with pytest.raises(ValidationError, match='Invalid type.'):
        data = s.loads("""{
        "type": "WrongType",
        "collector_id": 999,
        "debtor_id": 666,
        "turn_id": 1,
        "ts": "2022-01-01T00:00:00Z"
        }""")

    with pytest.raises(ValidationError, match='Invalid debtor ID'):
        data = s.loads("""{
        "type": "StartDispatching",
        "collector_id": 999,
        "debtor_id": 0,
        "turn_id": 1,
        "ts": "2022-01-01T00:00:00Z"
        }""")

    try:
        s.loads('{}')
    except ValidationError as e:
        assert len(e.messages) == len(data)
        assert all(m == ['Missing data for required field.']
                   for m in e.messages.values())


def test_calculate_surplus_message():
    s = schemas.CalculateSurplusMessageSchema()

    data = s.loads("""{
    "type": "CalculateSurplus",
    "collector_id": 999,
    "debtor_id": 666,
    "turn_id": 123,
    "ts": "2022-01-01T00:00:00Z",
    "unknown": "ignored"
    }""")

    assert data['type'] == 'CalculateSurplus'
    assert type(data['collector_id']) is int
    assert data['collector_id'] == 999
    assert type(data['debtor_id']) is int
    assert data['debtor_id'] == 666
    assert type(data['turn_id']) is int
    assert data['turn_id'] == 123
    assert data['ts'] == datetime.fromisoformat('2022-01-01T00:00:00+00:00')
    assert "unknown" not in data

    with pytest.raises(ValidationError, match='Invalid type.'):
        data = s.loads("""{
        "type": "WrongType",
        "collector_id": 999,
        "debtor_id": 666,
        "turn_id": 123,
        "ts": "2022-01-01T00:00:00Z"
        }""")

    with pytest.raises(ValidationError, match='Invalid debtor ID'):
        data = s.loads("""{
        "type": "CalculateSurplus",
        "collector_id": 999,
        "debtor_id": 0,
        "turn_id": 123,
        "ts": "2022-01-01T00:00:00Z"
        }""")

    try:
        s.loads('{}')
    except ValidationError as e:
        assert len(e.messages) == len(data)
        assert all(m == ['Missing data for required field.']
                   for m in e.messages.values())


def test_coin_info_document():
    s = schemas.CoinInfoDocumentSchema()

    data = s.loads(r"""{
      "summary": "This is a test currency.\n",
      "debtorName": "Evgeni Pandurski",
      "debtorHomepage": {
        "uri": "https://swaptacular.org/"
      },
      "amountDivisor": 100,
      "decimalPlaces": 2,
      "unit": "USD",
      "peg": {
        "type": "Peg",
        "exchangeRate": 1,
        "debtorIdentity": {
          "type": "DebtorIdentity",
          "uri": "swpt:4640381880"
        },
        "latestDebtorInfo": {
          "uri": "https://demo.swaptacular.org/debtors/4640381880/public"
        },
        "display": {
          "type": "PegDisplay",
          "amountDivisor": 100,
          "decimalPlaces": 2,
          "unit": "USD"
        }
      },
      "debtorIdentity": {
        "type": "DebtorIdentity",
        "uri": "swpt:6199429176"
      },
      "revision": 82,
      "latestDebtorInfo": {
        "uri": "https://demo.swaptacular.org/debtors/6199429176/public"
      },
      "type": "CoinInfo"
    }""")

    assert data['type'] == 'CoinInfo'
    assert data['latest_debtor_info']['uri'] == (
        'https://demo.swaptacular.org/debtors/6199429176/public'
    )
    assert data['debtor_identity']['type'] == 'DebtorIdentity'
    assert data['debtor_identity']['uri'] == 'swpt:6199429176'
    assert 'optional_peg' in data
    assert data['optional_peg']['debtor_identity']['type'] == 'DebtorIdentity'
    assert data['optional_peg']['debtor_identity']['uri'] == 'swpt:4640381880'
    assert data['optional_peg']['latest_debtor_info']['uri'] == (
        'https://demo.swaptacular.org/debtors/4640381880/public'
    )
    assert data['optional_peg']['exchange_rate'] == 1.0
    assert isinstance(data['optional_peg']['exchange_rate'], float)
    assert 'optional_will_not_change_until' not in data

    try:
        s.loads('{}')
    except ValidationError as e:
        assert len(e.messages) == 3
        assert all(m == ['Missing data for required field.']
                   for m in e.messages.values())

    with pytest.raises(json.JSONDecodeError):
        s.loads('invalid JSON')
