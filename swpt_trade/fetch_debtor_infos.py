import logging
import random
import ssl
import asyncio
import aiohttp
import json
from typing import TypeVar, Callable, Tuple, List, Optional, Union
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from marshmallow import ValidationError
from flask import current_app
from sqlalchemy import select
from sqlalchemy.sql.expression import tuple_, and_, null, func
from sqlalchemy.orm.attributes import flag_modified
from swpt_pythonlib.utils import ShardingRealm
from swpt_pythonlib.swpt_uris import parse_debtor_uri
from swpt_trade.extensions import db
from swpt_trade.schemas import CoinInfoDocumentSchema
from swpt_trade.models import (
    DebtorInfoFetch,
    DebtorInfoDocument,
    ConfirmDebtorSignal,
    FetchDebtorInfoSignal,
    StoreDocumentSignal,
    MAX_INT16,
    T_INFINITY,
)

T = TypeVar("T")
FetchTuple = Tuple[DebtorInfoFetch, Optional[DebtorInfoDocument]]
Classifcation = Tuple[List[FetchTuple], List[FetchTuple], List[FetchTuple]]
atomic: Callable[[T], T] = db.atomic

DEBTOR_INFO_FETCH_PK = tuple_(
    DebtorInfoFetch.iri,
    DebtorInfoFetch.debtor_id,
)
coininfo_schema = CoinInfoDocumentSchema()


@dataclass
class FetchResult:
    fetch: DebtorInfoFetch
    errorcode: Optional[int] = None
    retry: bool = False
    document: Optional[DebtorInfoDocument] = None
    store_document: bool = False


class InvalidDebtorInfoDocument(Exception):
    """Invalid debtor info document."""


def process_debtor_info_fetches(
        max_connections: int,
        timeout: float,
        cafile: str,
) -> int:
    count = 0
    current_ts = datetime.now(tz=timezone.utc)
    cfg = current_app.config
    batch_size = cfg["APP_DEBTOR_INFO_FETCH_BURST_COUNT"]
    retry_min_seconds = (
        cfg["TRANSFERS_HEALTHY_MAX_COMMIT_DELAY"].total_seconds()
    )
    ssl_context = ssl.create_default_context(cafile=cafile)

    with db.engine.connect() as w_conn:
        with w_conn.execution_options(yield_per=batch_size).execute(
                select(
                    DebtorInfoFetch.iri,
                    DebtorInfoFetch.debtor_id,
                )
                .where(DebtorInfoFetch.next_attempt_at <= current_ts)
        ) as result:
            for rows in result.partitions():
                count += _process_debtor_info_fetches_batch(
                    rows,
                    current_ts,
                    max_connections,
                    timeout,
                    retry_min_seconds,
                    ssl_context,
                )

    return count


@atomic
def _process_debtor_info_fetches_batch(
        debtor_info_fetch_pks: list,
        current_ts: datetime,
        max_connections: int,
        timeout: float,
        retry_min_seconds: float,
        ssl_context: ssl.SSLContext,
) -> int:
    cfg = current_app.config
    expiry_period = timedelta(days=cfg["APP_DEBTOR_INFO_EXPIRY_DAYS"])
    max_distance_to_base = cfg["MAX_DISTANCE_TO_BASE"]
    assert max_distance_to_base > 1
    assert max_connections > 0
    assert timeout > 0.0

    fetch_results = _query_and_resolve_pending_fetches(
        debtor_info_fetch_pks,
        current_ts,
        expiry_period,
        max_connections,
        timeout,
        ssl_context,
    )
    for r in fetch_results:
        fetch = r.fetch
        errorcode = r.errorcode
        retry = r.retry
        document = r.document
        store_document = r.store_document

        if document:
            assert not retry
            debtor_info_locator = document.debtor_info_locator
            debtor_id = document.debtor_id

            if fetch.is_discovery_fetch and fetch.debtor_id == debtor_id:
                db.session.add(
                    ConfirmDebtorSignal(
                        debtor_id=debtor_id,
                        debtor_info_locator=debtor_info_locator,
                    )
                )

            if fetch.is_locator_fetch and fetch.iri == debtor_info_locator:
                peg_debtor_info_locator = document.peg_debtor_info_locator
                peg_debtor_id = document.peg_debtor_id
                peg_exchange_rate = document.peg_exchange_rate
                will_not_change_until = document.will_not_change_until
                recursion_level = fetch.recursion_level

                if (
                    fetch.debtor_id == debtor_id
                    and peg_debtor_info_locator is not None
                    and peg_debtor_id is not None
                    and peg_exchange_rate is not None
                    and recursion_level < max_distance_to_base
                ):
                    # Follow the peg-chain recursively.
                    db.session.add(
                        FetchDebtorInfoSignal(
                            iri=peg_debtor_info_locator,
                            debtor_id=peg_debtor_id,
                            is_locator_fetch=True,
                            is_discovery_fetch=False,
                            recursion_level=recursion_level + 1,
                        )
                    )
                if store_document:
                    db.session.add(
                        StoreDocumentSignal(
                            debtor_info_locator=debtor_info_locator,
                            debtor_id=debtor_id,
                            peg_debtor_info_locator=peg_debtor_info_locator,
                            peg_debtor_id=peg_debtor_id,
                            peg_exchange_rate=peg_exchange_rate,
                            will_not_change_until=will_not_change_until,
                        )
                    )

        if retry:
            _retry_fetch(fetch, errorcode, expiry_period, retry_min_seconds)
        else:
            db.session.delete(fetch)

    return len(fetch_results)


def _query_and_resolve_pending_fetches(
        debtor_info_fetch_pks: list,
        current_ts: datetime,
        expiry_period: timedelta,
        max_connections: int,
        timeout: float,
        ssl_context: ssl.SSLContext,
) -> List[FetchResult]:
    # Lock the `DebtorInfoFetch`es that need to be processed.
    fetch_tuples: List[FetchTuple] = (
        db.session.query(DebtorInfoFetch, DebtorInfoDocument)
        .outerjoin(
            DebtorInfoDocument,
            and_(
                DebtorInfoDocument.debtor_info_locator == DebtorInfoFetch.iri,
                DebtorInfoFetch.forced_iri == null(),
            ),
        )
        .filter(
            DEBTOR_INFO_FETCH_PK.in_(debtor_info_fetch_pks),

            # The reason we use this complex expression here (instead
            # of the simple `DebtorInfoFetch.next_attempt_at <=
            # current_ts`), is to ensure that the planner will not
            # decide to use the index on the `next_attempt_at` column
            # for the prepared generic plan.
            func.least(DebtorInfoFetch.next_attempt_at, T_INFINITY)
            <= current_ts,
        )
        .with_for_update(of=DebtorInfoFetch, skip_locked=True)
        .all()
    )

    # Resolve the `DebtorInfoFetch`es that we have locked.
    wrong_shard, cached, new = _classify_fetch_tuples(
        fetch_tuples, current_ts, expiry_period
    )
    wrong_shard_results = [FetchResult(fetch=f) for f, _ in wrong_shard]
    cached_results = [FetchResult(fetch=f, document=d) for f, d in cached]
    new_results = _make_https_requests(
        [f for f, _ in new],
        max_connections=max_connections,
        timeout=timeout,
        ssl_context=ssl_context,
    )

    all_results = wrong_shard_results + cached_results + new_results
    assert len(all_results) == len(fetch_tuples)
    return all_results


def _classify_fetch_tuples(
        fetch_tuples: List[FetchTuple],
        current_ts: datetime,
        expiry_period: timedelta,
) -> Classifcation:
    """Classify the list of pending fetches in 3 categories.

    1. Fetches this shard is not responsible for (and must be ignored).
    2. Fetches for which we have a cached response.
    3. Fetches for which an HTTPS request must be made.
    """
    sharding_realm: ShardingRealm = current_app.config["SHARDING_REALM"]
    wrong_shard: List[FetchTuple] = []
    cached: List[FetchTuple] = []
    new: List[FetchTuple] = []

    for t in fetch_tuples:
        fetch, document = t
        if not sharding_realm.match_str(fetch.iri):
            wrong_shard.append(t)
        elif document and not document.has_expired(current_ts, expiry_period):
            cached.append(t)
        else:
            new.append(t)

    return wrong_shard, cached, new


def _retry_fetch(
        fetch: DebtorInfoFetch,
        errorcode: Optional[int],
        expiry_period: timedelta,
        retry_min_seconds: float,
) -> None:
    """Try to re-schedule a new fetch attempt with randomized
    exponential backoff. Give up if this would take more time than the
    passed `expiry_period`.
    """
    n = min(fetch.attempts_count, 100)  # We must avoid float overflows!
    wait_seconds = retry_min_seconds * (1.5 ** n)

    if wait_seconds < expiry_period.total_seconds():
        current_ts = datetime.now(tz=timezone.utc)
        wait_seconds *= (0.5 + 0.5 * random.random())

        if fetch.attempts_count < MAX_INT16:
            fetch.attempts_count += 1

        fetch.latest_attempt_at = current_ts
        fetch.latest_attempt_errorcode = errorcode
        fetch.next_attempt_at = current_ts + timedelta(seconds=wait_seconds)
        flag_modified(fetch, "attempts_count")
        flag_modified(fetch, "latest_attempt_errorcode")
    else:
        db.session.delete(fetch)


def _make_https_requests(
        fetches: List[DebtorInfoFetch],
        *,
        max_connections: int,
        timeout: float,
        ssl_context: ssl.SSLContext,
) -> List[FetchResult]:
    results: List[FetchResult] = []
    logger = logging.getLogger(__name__)
    loop = _get_asyncio_loop()
    results_and_errors = loop.run_until_complete(
        _gather_https_request_results(
            fetches,
            max_connections,
            timeout,
            ssl_context,
        )
    )
    assert len(fetches) == len(results_and_errors)

    for fetch, obj in zip(fetches, results_and_errors):
        if isinstance(obj, Exception):  # pragma: no cover
            # Normally this should never happen. However, it seems
            # that due to some bug in aiohttp, it sometimes raises
            # unexpected assertion errors when presented with invalid
            # URLs ("invalid://swaptacular.github.io/" for example).
            # Here we catch and log such errors as warnings.
            logger.warning(
                "Caught error during request to %s",
                fetch.forced_iri or fetch.iri,
                exc_info=obj,
            )
            results.append(FetchResult(fetch=fetch))
        else:
            assert isinstance(obj, FetchResult)
            results.append(obj)

    return results


async def _gather_https_request_results(
        fetches: List[DebtorInfoFetch],
        max_connections: int,
        timeout: float,
        ssl_context: ssl.SSLContext,
) -> List[Union[FetchResult, Exception]]:
    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(
            limit=max_connections,
            ttl_dns_cache=3600,
            ssl=current_app.config["APP_VERIFY_SSL_CERTS"] and ssl_context,
        ),
        timeout=aiohttp.ClientTimeout(total=timeout),
    ) as client:
        return await asyncio.gather(
            *(_make_https_request(client, f) for f in fetches),
            return_exceptions=True,
        )


async def _make_https_request(
        client: aiohttp.ClientSession,
        fetch: DebtorInfoFetch,
) -> FetchResult:
    iri = fetch.forced_iri or fetch.iri
    try:
        if not iri.startswith("https://"):
            raise aiohttp.InvalidURL(iri)

        async with client.get(iri, max_redirects=2) as response:
            if response.status == 200:
                return FetchResult(
                    fetch=fetch,
                    document=_parse_debtor_info_document(
                        response.content_type, await response.read()
                    ),
                    store_document=True,
                )
            else:  # pragma: no cover
                return FetchResult(
                    fetch=fetch,
                    errorcode=response.status,
                    retry=response.status != 410,
                )

    except aiohttp.ClientError as e:
        logger = logging.getLogger(__name__)
        logger.info(
            "Failed request to %s (%s: %s)",
            iri,
            type(e).__name__,
            str(e),
        )
        is_invalid_url = isinstance(e, aiohttp.InvalidURL)
        return FetchResult(fetch=fetch, retry=not is_invalid_url)

    except asyncio.TimeoutError:  # pragma: no cover
        logger = logging.getLogger(__name__)
        logger.info("Timed out request to %s", iri)
        return FetchResult(fetch=fetch, retry=True)

    except InvalidDebtorInfoDocument:
        logger = logging.getLogger(__name__)
        logger.info("Invalid debtor info document at %s", iri)
        return FetchResult(fetch=fetch, retry=True)


def _get_asyncio_loop():
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:  # pragma: nocover
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    return loop


def _parse_debtor_info_document(
        content_type: str,
        body: bytes,
) -> DebtorInfoDocument:
    """Parse the HTTP request body.

    NOTE: Currently, the only supported format for debtor info
    documents is the "CoinInfo" JSON document format.
    """
    if content_type != "application/vnd.swaptacular.coin-info+json":
        raise InvalidDebtorInfoDocument(
            "Unknown debtor info document type: %s", content_type
        )

    if len(body) > current_app.config["APP_COIN_INFO_DOCUMENT_MAX_LENGTH"]:
        raise InvalidDebtorInfoDocument("Too long CoinInfo document")

    try:
        obj = json.loads(body.decode("utf8"))
        coininfo = coininfo_schema.load(obj)
    except (UnicodeError, json.JSONDecodeError, ValidationError):
        raise InvalidDebtorInfoDocument("Invalid CoinInfo document")

    try:
        debtor_id = parse_debtor_uri(coininfo["debtor_identity"]["uri"])
    except ValueError:
        raise InvalidDebtorInfoDocument("Invalid debtor URI")

    debtor_info_locator = coininfo["latest_debtor_info"]["uri"]
    will_not_change_until = coininfo.get("optional_will_not_change_until")

    peg = coininfo.get("optional_peg")
    if peg is not None:
        try:
            peg_debtor_id = parse_debtor_uri(peg["debtor_identity"]["uri"])
        except ValueError:
            raise InvalidDebtorInfoDocument("Invalid peg debtor URI")

        peg_debtor_info_locator = peg["latest_debtor_info"]["uri"]
        peg_exchange_rate = peg["exchange_rate"]
    else:
        peg_debtor_id = None
        peg_debtor_info_locator = None
        peg_exchange_rate = None

    return DebtorInfoDocument(
        debtor_id=debtor_id,
        debtor_info_locator=debtor_info_locator,
        peg_debtor_id=peg_debtor_id,
        peg_debtor_info_locator=peg_debtor_info_locator,
        peg_exchange_rate=peg_exchange_rate,
        will_not_change_until=will_not_change_until,
    )
