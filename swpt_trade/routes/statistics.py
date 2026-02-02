import csv
import io
from datetime import date
from flask import make_response
from flask.views import MethodView
from swpt_pythonlib.swpt_uris import make_debtor_uri
from swpt_trade import procedures
from .common import ensure_owner, Blueprint
from . import specs

statistics_api = Blueprint(
    "statistics",
    __name__,
    url_prefix="/trade",
    description="""**Trade statistics.** These are endpoints
    from which the "owner" user can obtain trading statistics.""",
)
statistics_api.before_request(ensure_owner)


@statistics_api.route("statistics/most-bought-currencies.csv")
class MostBoughtCurrenciesCsvEndpoint(MethodView):
    @statistics_api.response(200)
    @statistics_api.doc(
        operationId="getMostBoughtCurrenciesCsv",
        security=specs.SCOPE_ACCESS_READONLY,
        responses={200: specs.MOST_BOUGHT_CURRENCIES_CSV_EXAMPLE},
    )
    def get(self):
        """Return a CSV document with the list of most bought currencies.

        The MIME type of the returned document will be `text/csv`.
        """

        filename = f"most-bought-currencies-{date.today()}.csv"
        document = io.StringIO()
        csv_writer = csv.writer(document)
        for row in procedures.get_most_bought_currencies():
            csv_writer.writerow([
                f"{row.buyers_average_count:g}",
                f"{row.debtor_info_locator}#{make_debtor_uri(row.debtor_id)}",
            ])

        headers = {
            "Content-Type": "text/csv; charset=utf-8",
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Cache-Control": "max-age=3600",
        }
        return make_response(document.getvalue(), headers)
