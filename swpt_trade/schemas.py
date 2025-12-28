from marshmallow import (
    Schema,
    fields,
    validate,
    validates,
    validates_schema,
    ValidationError,
    EXCLUDE,
)
from swpt_trade.models import (
    MAX_INT16,
    MIN_INT32,
    MAX_INT32,
    MIN_INT64,
    MAX_INT64,
    ACCOUNT_ID_MAX_BYTES,
)


class ValidateTypeMixin:
    @validates("type")
    def validate_type(self, value):
        if f"{value}MessageSchema" != type(self).__name__:
            raise ValidationError("Invalid type.")


class FetchDebtorInfoMessageSchema(ValidateTypeMixin, Schema):
    """``FetchDebtorInfo`` message schema."""

    class Meta:
        unknown = EXCLUDE

    type = fields.String(required=True)
    iri = fields.String(required=True)
    debtor_id = fields.Integer(
        required=True, validate=validate.Range(min=MIN_INT64, max=MAX_INT64)
    )
    is_locator_fetch = fields.Boolean(required=True)
    is_discovery_fetch = fields.Boolean(required=True)
    forced_iri = fields.String(load_default=None)
    recursion_level = fields.Integer(
        required=True, validate=validate.Range(min=0, max=MAX_INT16)
    )
    ts = fields.DateTime(required=True)


class StoreDocumentMessageSchema(ValidateTypeMixin, Schema):
    """``StoreDocument`` message schema."""

    class Meta:
        unknown = EXCLUDE

    type = fields.String(required=True)
    debtor_info_locator = fields.String(required=True)
    debtor_id = fields.Integer(
        required=True, validate=validate.Range(min=MIN_INT64, max=MAX_INT64)
    )
    peg_debtor_info_locator = fields.String(load_default=None)
    peg_debtor_id = fields.Integer(
        load_default=None,
        validate=validate.Range(min=MIN_INT64, max=MAX_INT64),
    )
    peg_exchange_rate = fields.Float(
        load_default=None, validate=validate.Range(min=0.0)
    )
    will_not_change_until = fields.DateTime(load_default=None)
    ts = fields.DateTime(required=True)

    @validates_schema
    def validate_peg(self, data, **kwargs):
        a = data["peg_debtor_info_locator"]
        b = data["peg_debtor_id"]
        c = data["peg_exchange_rate"]
        if not (
            (a is not None and b is not None and c is not None)
            or (a is None and b is None and c is None)
        ):
            raise ValidationError(
                "peg_exchange_rate, peg_debtor_id, and peg_exchange_rate"
                " fields must all be missing, or must all be present."
            )


class DiscoverDebtorMessageSchema(ValidateTypeMixin, Schema):
    """``DiscoverDebtor`` message schema."""

    class Meta:
        unknown = EXCLUDE

    type = fields.String(required=True)
    debtor_id = fields.Integer(
        required=True, validate=validate.Range(min=MIN_INT64, max=MAX_INT64)
    )
    iri = fields.String(required=True)
    force_locator_refetch = fields.Boolean(required=True)
    ts = fields.DateTime(required=True)


class ConfirmDebtorMessageSchema(ValidateTypeMixin, Schema):
    """``ConfirmDebtor`` message schema."""

    class Meta:
        unknown = EXCLUDE

    type = fields.String(required=True)
    debtor_id = fields.Integer(
        required=True, validate=validate.Range(min=MIN_INT64, max=MAX_INT64)
    )
    debtor_info_locator = fields.String(required=True)
    ts = fields.DateTime(required=True)


class CandidateOfferMessageSchema(ValidateTypeMixin, Schema):
    """``CandidateOffer`` message schema."""

    class Meta:
        unknown = EXCLUDE

    type = fields.String(required=True)
    turn_id = fields.Integer(
        required=True, validate=validate.Range(min=MIN_INT32, max=MAX_INT32)
    )
    debtor_id = fields.Integer(
        required=True, validate=validate.Range(min=MIN_INT64, max=MAX_INT64)
    )
    creditor_id = fields.Integer(
        required=True, validate=validate.Range(min=MIN_INT64, max=MAX_INT64)
    )
    amount = fields.Integer(
        required=True, validate=validate.Range(min=-MAX_INT64, max=MAX_INT64)
    )
    last_transfer_number = fields.Integer(
        required=True, validate=validate.Range(min=0, max=MAX_INT64)
    )
    account_creation_date = fields.Date(required=True)
    ts = fields.DateTime(required=True)

    @validates("amount")
    def validate_amount(self, value):
        if value == 0:
            raise ValidationError("Amount can not be zero.")


class NeededCollectorMessageSchema(ValidateTypeMixin, Schema):
    """``NeededCollector`` message schema."""

    class Meta:
        unknown = EXCLUDE

    type = fields.String(required=True)
    debtor_id = fields.Integer(
        required=True, validate=validate.Range(min=MIN_INT64, max=MAX_INT64)
    )
    ts = fields.DateTime(required=True)

    @validates("debtor_id")
    def validate_debtor_id(self, value):
        if value == 0:
            raise ValidationError("Invalid debtor ID.")


class ReviseAccountLockMessageSchema(ValidateTypeMixin, Schema):
    """``ReviseAccountLock`` message schema."""

    class Meta:
        unknown = EXCLUDE

    type = fields.String(required=True)
    creditor_id = fields.Integer(
        required=True, validate=validate.Range(min=MIN_INT64, max=MAX_INT64)
    )
    debtor_id = fields.Integer(
        required=True, validate=validate.Range(min=MIN_INT64, max=MAX_INT64)
    )
    turn_id = fields.Integer(
        required=True, validate=validate.Range(min=MIN_INT32, max=MAX_INT32)
    )
    ts = fields.DateTime(required=True)

    @validates("debtor_id")
    def validate_debtor_id(self, value):
        if value == 0:
            raise ValidationError("Invalid debtor ID.")


class TriggerTransferMessageSchema(ValidateTypeMixin, Schema):
    """``TriggerTransfer`` message schema."""

    class Meta:
        unknown = EXCLUDE

    type = fields.String(required=True)
    collector_id = fields.Integer(
        required=True, validate=validate.Range(min=MIN_INT64, max=MAX_INT64)
    )
    turn_id = fields.Integer(
        required=True, validate=validate.Range(min=MIN_INT32, max=MAX_INT32)
    )
    debtor_id = fields.Integer(
        required=True, validate=validate.Range(min=MIN_INT64, max=MAX_INT64)
    )
    creditor_id = fields.Integer(
        required=True, validate=validate.Range(min=MIN_INT64, max=MAX_INT64)
    )
    transfer_kind = fields.Integer(
        required=True, validate=validate.Range(min=0, max=3)
    )
    ts = fields.DateTime(required=True)

    @validates("debtor_id")
    def validate_debtor_id(self, value):
        if value == 0:
            raise ValidationError("Invalid debtor ID.")


class AccountIdRequestMessageSchema(ValidateTypeMixin, Schema):
    """``AccountIdRequest`` message schema."""

    class Meta:
        unknown = EXCLUDE

    type = fields.String(required=True)
    collector_id = fields.Integer(
        required=True, validate=validate.Range(min=MIN_INT64, max=MAX_INT64)
    )
    turn_id = fields.Integer(
        required=True, validate=validate.Range(min=MIN_INT32, max=MAX_INT32)
    )
    debtor_id = fields.Integer(
        required=True, validate=validate.Range(min=MIN_INT64, max=MAX_INT64)
    )
    creditor_id = fields.Integer(
        required=True, validate=validate.Range(min=MIN_INT64, max=MAX_INT64)
    )
    transfer_kind = fields.Integer(
        required=True, validate=validate.Range(min=0, max=3)
    )
    ts = fields.DateTime(required=True)

    @validates("debtor_id")
    def validate_debtor_id(self, value):
        if value == 0:
            raise ValidationError("Invalid debtor ID.")


class AccountIdResponseMessageSchema(ValidateTypeMixin, Schema):
    """``AccountIdResponse`` message schema."""

    class Meta:
        unknown = EXCLUDE

    type = fields.String(required=True)
    collector_id = fields.Integer(
        required=True, validate=validate.Range(min=MIN_INT64, max=MAX_INT64)
    )
    turn_id = fields.Integer(
        required=True, validate=validate.Range(min=MIN_INT32, max=MAX_INT32)
    )
    debtor_id = fields.Integer(
        required=True, validate=validate.Range(min=MIN_INT64, max=MAX_INT64)
    )
    creditor_id = fields.Integer(
        required=True, validate=validate.Range(min=MIN_INT64, max=MAX_INT64)
    )
    transfer_kind = fields.Integer(
        required=True, validate=validate.Range(min=0, max=3)
    )
    account_id = fields.String(
        required=True, validate=validate.Length(max=ACCOUNT_ID_MAX_BYTES)
    )
    account_id_version = fields.Integer(
        required=True, validate=validate.Range(min=MIN_INT64, max=MAX_INT64)
    )
    ts = fields.DateTime(required=True)

    @validates("debtor_id")
    def validate_debtor_id(self, value):
        if value == 0:
            raise ValidationError("Invalid debtor ID.")


class StartSendingMessageSchema(ValidateTypeMixin, Schema):
    """``StartSending`` message schema."""

    class Meta:
        unknown = EXCLUDE

    type = fields.String(required=True)
    collector_id = fields.Integer(
        required=True, validate=validate.Range(min=MIN_INT64, max=MAX_INT64)
    )
    turn_id = fields.Integer(
        required=True, validate=validate.Range(min=MIN_INT32, max=MAX_INT32)
    )
    debtor_id = fields.Integer(
        required=True, validate=validate.Range(min=MIN_INT64, max=MAX_INT64)
    )
    ts = fields.DateTime(required=True)

    @validates("debtor_id")
    def validate_debtor_id(self, value):
        if value == 0:
            raise ValidationError("Invalid debtor ID.")


class StartDispatchingMessageSchema(ValidateTypeMixin, Schema):
    """``StartDispatching`` message schema."""

    class Meta:
        unknown = EXCLUDE

    type = fields.String(required=True)
    collector_id = fields.Integer(
        required=True, validate=validate.Range(min=MIN_INT64, max=MAX_INT64)
    )
    turn_id = fields.Integer(
        required=True, validate=validate.Range(min=MIN_INT32, max=MAX_INT32)
    )
    debtor_id = fields.Integer(
        required=True, validate=validate.Range(min=MIN_INT64, max=MAX_INT64)
    )
    ts = fields.DateTime(required=True)

    @validates("debtor_id")
    def validate_debtor_id(self, value):
        if value == 0:
            raise ValidationError("Invalid debtor ID.")


class CalculateSurplusMessageSchema(ValidateTypeMixin, Schema):
    """``CalculateSurplus`` message schema."""

    class Meta:
        unknown = EXCLUDE

    type = fields.String(required=True)
    collector_id = fields.Integer(
        required=True, validate=validate.Range(min=MIN_INT64, max=MAX_INT64)
    )
    debtor_id = fields.Integer(
        required=True, validate=validate.Range(min=MIN_INT64, max=MAX_INT64)
    )
    turn_id = fields.Integer(
        required=True, validate=validate.Range(min=MIN_INT32, max=MAX_INT32)
    )
    ts = fields.DateTime(required=True)

    @validates("debtor_id")
    def validate_debtor_id(self, value):
        if value == 0:
            raise ValidationError("Invalid debtor ID.")


class UpdatedLedgerMessageSchema(ValidateTypeMixin, Schema):
    """``UpdatedLedger`` message schema."""

    class Meta:
        unknown = EXCLUDE

    type = fields.String(required=True)
    debtor_id = fields.Integer(
        required=True, validate=validate.Range(min=MIN_INT64, max=MAX_INT64)
    )
    creditor_id = fields.Integer(
        required=True, validate=validate.Range(min=MIN_INT64, max=MAX_INT64)
    )
    update_id = fields.Integer(
        required=True, validate=validate.Range(min=1, max=MAX_INT64)
    )
    account_id = fields.String(
        required=True, validate=validate.Length(max=ACCOUNT_ID_MAX_BYTES)
    )
    creation_date = fields.Date(required=True)
    principal = fields.Integer(
        required=True, validate=validate.Range(min=MIN_INT64, max=MAX_INT64)
    )
    last_transfer_number = fields.Integer(
        required=True, validate=validate.Range(min=0, max=MAX_INT64)
    )
    ts = fields.DateTime(required=True)


class UpdatedPolicyMessageSchema(ValidateTypeMixin, Schema):
    """``UpdatedPolicy`` message schema."""

    class Meta:
        unknown = EXCLUDE

    type = fields.String(required=True)
    debtor_id = fields.Integer(
        required=True, validate=validate.Range(min=MIN_INT64, max=MAX_INT64)
    )
    creditor_id = fields.Integer(
        required=True, validate=validate.Range(min=MIN_INT64, max=MAX_INT64)
    )
    update_id = fields.Integer(
        required=True, validate=validate.Range(min=1, max=MAX_INT64)
    )
    policy_name = fields.String(load_default=None)
    min_principal = fields.Integer(
        required=True, validate=validate.Range(min=MIN_INT64, max=MAX_INT64)
    )
    max_principal = fields.Integer(
        required=True, validate=validate.Range(min=MIN_INT64, max=MAX_INT64)
    )
    peg_exchange_rate = fields.Float(
        load_default=None, validate=validate.Range(min=0.0)
    )
    peg_debtor_id = fields.Integer(
        load_default=None,
        validate=validate.Range(min=MIN_INT64, max=MAX_INT64),
    )
    ts = fields.DateTime(required=True)

    @validates_schema
    def validate_max_principal(self, data, **kwargs):
        if data["min_principal"] > data["max_principal"]:
            raise ValidationError(
                "max_principal must be equal or greater than min_principal."
            )

    @validates_schema
    def validate_peg(self, data, **kwargs):
        a = data["peg_exchange_rate"]
        b = data["peg_debtor_id"]
        if (a is None and b is not None) or (a is not None and b is None):
            raise ValidationError(
                "peg_exchange_rate and peg_debtor_id fields must both be"
                " missing, or must both be present."
            )


class UpdatedFlagsMessageSchema(ValidateTypeMixin, Schema):
    """``UpdatedFlags`` message schema."""

    class Meta:
        unknown = EXCLUDE

    type = fields.String(required=True)
    debtor_id = fields.Integer(
        required=True, validate=validate.Range(min=MIN_INT64, max=MAX_INT64)
    )
    creditor_id = fields.Integer(
        required=True, validate=validate.Range(min=MIN_INT64, max=MAX_INT64)
    )
    update_id = fields.Integer(
        required=True, validate=validate.Range(min=1, max=MAX_INT64)
    )
    config_flags = fields.Integer(
        required=True, validate=validate.Range(min=MIN_INT32, max=MAX_INT32)
    )
    ts = fields.DateTime(required=True)


class ShortLinkSchema(Schema):
    uri = fields.String(required=True, validate=validate.Length(max=200))


class DebtorIdentitySchema(Schema):
    class Meta:
        unknown = EXCLUDE

    type = fields.String(
        required=True,
        validate=validate.Regexp("^DebtorIdentity(-v[1-9][0-9]{0,5})?$"),
    )
    uri = fields.String(required=True, validate=validate.Length(max=100))


class PegSchema(Schema):
    class Meta:
        unknown = EXCLUDE

    type = fields.String(
        required=True, validate=validate.Regexp("^Peg(-v[1-9][0-9]{0,5})?$")
    )
    latest_debtor_info = fields.Nested(
        ShortLinkSchema, required=True, data_key="latestDebtorInfo"
    )
    debtor_identity = fields.Nested(
        DebtorIdentitySchema, required=True, data_key="debtorIdentity"
    )
    exchange_rate = fields.Float(
        required=True,
        validate=validate.Range(min=0.0),
        data_key="exchangeRate",
    )


class CoinInfoDocumentSchema(Schema):
    """A debtor info document in ``CoinInfo`` JSON format.

    NOTE: This schema validates only the fields that we are interested
    in, ignoring any possible errors in the fields we *are not*
    interested in. As a result, an invalid "CoinInfo" document may be
    treated as a valid one. Nevertheless, this should be OK for our
    purposes.
    """
    class Meta:
        unknown = EXCLUDE

    type = fields.String(
        required=True,
        validate=validate.Regexp("^CoinInfo(-v[1-9][0-9]{0,5})?$"),
    )
    latest_debtor_info = fields.Nested(
        ShortLinkSchema, required=True, data_key="latestDebtorInfo"
    )
    debtor_identity = fields.Nested(
        DebtorIdentitySchema, required=True, data_key="debtorIdentity"
    )
    optional_will_not_change_until = fields.DateTime(
        data_key="willNotChangeUntil"
    )
    optional_peg = fields.Nested(PegSchema, data_key="peg")
