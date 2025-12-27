from copy import copy
from marshmallow import (
    Schema,
    fields,
    pre_dump,
    validate,
    validates,
    ValidationError,
)
from swpt_trade.models import MAX_INT64, CollectorAccount


TYPE_DESCRIPTION = (
    "The type of this object. Will always be present in the responses from the"
    " server."
)


class ValidateTypeMixin:
    @validates("type")
    def validate_type(self, value):
        if f"{value}Schema" != type(self).__name__:
            raise ValidationError("Invalid type.")


class ActivateCollectorsRequestSchema(ValidateTypeMixin, Schema):
    type = fields.String(
        load_default="ActivateCollectorsRequest",
        load_only=True,
        metadata=dict(
            description=TYPE_DESCRIPTION,
            example="ActivateCollectorsRequest",
        ),
    )
    number_of_accounts = fields.Integer(
        load_default=1,
        validate=validate.Range(min=1, max=MAX_INT64),
        data_key="numberOfAccounts",
        metadata=dict(
            format="int64",
            description="The number of needed collector accounts.",
            example=3,
        ),
    )


class CollectorAccountSchema(Schema):
    type = fields.Function(
        lambda obj: "CollectorAccount",
        required=True,
        metadata=dict(
            type="string",
            description=TYPE_DESCRIPTION,
            example="CollectorAccount",
        ),
    )
    debtor_id = fields.Integer(
        data_key="debtorId",
        required=True,
        dump_only=True,
        metadata=dict(
            format="int64",
            description="The debtor ID for the collector.",
            example=12345678,
        ),
    )
    collector_id = fields.Integer(
        data_key="creditorId",
        required=True,
        dump_only=True,
        metadata=dict(
            format="int64",
            description="The creditor ID for the collector.",
            example=12345678,
        ),
    )
    status = fields.Integer(
        required=True,
        dump_only=True,
        metadata=dict(
            description=(
                "Collector account's status: 0) pristine; 1) account creation"
                " has been requested; 2) the account has been created, and"
                " an account ID has been assigned to it; 3) disabled."
            ),
            example=2,
        ),
    )
    latest_status_change_at = fields.DateTime(
        required=True,
        dump_only=True,
        data_key="latestStatusChangeAt",
        metadata=dict(
            description=(
                "The moment at which the latest status change happened."
            ),
            example="2020-04-03T18:42:44Z",
        ),
    )
    optional_account_id = fields.String(
        dump_only=True,
        data_key="accountId",
        metadata=dict(
            description=(
                "When this field is present, it contains the account ID"
                " assigned to the collector account."
            ),
            example="12345678",
        ),
    )

    @pre_dump
    def process_collector_account_instance(self, obj, many):
        assert isinstance(obj, CollectorAccount)
        obj = copy(obj)
        if obj.account_id:
            obj.optional_account_id = obj.account_id

        return obj


class DebtorCollectorsListSchema(Schema):
    type = fields.Function(
        lambda obj: "DebtorCollectorsList",
        required=True,
        metadata=dict(
            type="string",
            description=TYPE_DESCRIPTION,
            example="DebtorCollectorsList",
        ),
    )
    debtor_id = fields.Integer(
        data_key="debtorId",
        required=True,
        dump_only=True,
        metadata=dict(
            format="int64",
            description="The ID of the debtor.",
            example=1234,
        ),
    )
    collectors = fields.Nested(
        CollectorAccountSchema(many=True),
        required=True,
        dump_only=True,
        metadata=dict(
            description=(
                "Contains an array of collectors accounts"
                " for the given debtor."
            ),
            example=[
                {
                    "type": "CollectorAccount",
                    "debtorId": 1234,
                    "creditorId": 12345678,
                    "status": 0,
                    "latestStatusChangeAt": "2020-04-03T18:42:44Z",
                }
            ],
        ),
    )
