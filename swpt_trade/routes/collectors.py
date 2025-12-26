from dataclasses import dataclass
from typing import Sequence
from flask import current_app, g
from flask.views import MethodView
from flask_smorest import abort
from swpt_trade import procedures
from swpt_trade.models import CollectorAccount
from .common import ensure_owner, Blueprint
from .specs import DID
from . import specs
from . import schemas


@dataclass
class DebtorCollectorsList:
    debtor_id: int
    collectors: Sequence[CollectorAccount]


collectors_api = Blueprint(
    "collectors",
    __name__,
    url_prefix="/trade",
    description="""**Manage collector accounts.**""",
)
collectors_api.before_request(ensure_owner)


@collectors_api.route("collectors/<i64:debtorId>/", parameters=[DID])
class DebtorCollectorsListEndpoint(MethodView):
    @collectors_api.response(200, schemas.DebtorCollectorsListSchema)
    @collectors_api.doc(
        operationId="getDebtorCollectorsList",
        security=specs.SCOPE_ACCESS_READONLY
    )
    def get(self, debtorId):
        """Return the list of collector accounts for a given debtor.
        """

        return DebtorCollectorsList(
            debtor_id=debtorId,
            collectors=procedures.get_collector_accounts(debtorId),
        )


@collectors_api.route(
    "collectors/<i64:debtorId>/activate", parameters=[DID]
)
class ActivateCollectorsEndpoint(MethodView):
    @collectors_api.arguments(schemas.ActivateCollectorsRequestSchema)
    @collectors_api.response(204)
    @collectors_api.doc(
        operationId="activateCollectors", security=specs.SCOPE_ACTIVATE
    )
    def post(self, activate_collectors_request, debtorId):
        """Ensure a number of alive collector accounts.
        """

        if not g.superuser:
            abort(403)

        if debtorId == 0:
            abort(500)

        try:
            procedures.ensure_collector_accounts(
                debtor_id=debtorId,
                min_collector_id=current_app.config["MIN_COLLECTOR_ID"],
                max_collector_id=current_app.config["MAX_COLLECTOR_ID"],
                number_of_accounts=(
                    activate_collectors_request["number_of_accounts"]
                ),
            )
        except RuntimeError:
            abort(500)
