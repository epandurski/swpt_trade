import logging
import pika
import click
import time
import sys
from sqlalchemy import select
from flask import current_app
from flask.cli import with_appcontext
from swpt_pythonlib.utils import ShardingRealm
from swpt_trade.extensions import db
from .common import swpt_trade

# TODO: Consider implementing a CLI command which extracts trading
# policies from the "swpt_creditors" microservice via its admin Web
# API, and loads them into the "trading policies" table. This CLI
# command is intended to be run only once at the beginning, to
# synchronize the swpt_trade's database with the swpt_creditors's
# database.


@swpt_trade.command()
@with_appcontext
@click.option(
    "-u",
    "--url",
    type=str,
    help="The RabbitMQ connection URL.",
)
@click.option(
    "-q",
    "--queue",
    type=str,
    help="The name of the queue to declare and subscribe.",
)
@click.option(
    "-k",
    "--queue-routing-key",
    type=str,
    help="The RabbitMQ binding key for the queue.",
)
def subscribe(url, queue, queue_routing_key):  # pragma: no cover
    """Declare a RabbitMQ queue, and subscribe it to receive incoming
    messages.

    If some of the available options are not specified directly, the
    values of the following environment variables will be used:

    * PROTOCOL_BROKER_URL (default "amqp://guest:guest@localhost:5672")

    * PROTOCOL_BROKER_QUEUE (defalut "swpt_trade")

    * PROTOCOL_BROKER_QUEUE_ROUTING_KEY (default "#")
    """

    from swpt_trade.extensions import (
        CREDITORS_IN_EXCHANGE,
        CREDITORS_OUT_EXCHANGE,
        TO_TRADE_EXCHANGE,
    )
    CA_LOOPBACK_FILTER_EXCHANGE = "ca.loopback_filter"

    logger = logging.getLogger(__name__)
    queue_name = queue or current_app.config["PROTOCOL_BROKER_QUEUE"]
    routing_key = (
        queue_routing_key
        or current_app.config["PROTOCOL_BROKER_QUEUE_ROUTING_KEY"]
    )
    dead_letter_queue_name = queue_name + ".XQ"
    broker_url = url or current_app.config["PROTOCOL_BROKER_URL"]
    connection = pika.BlockingConnection(pika.URLParameters(broker_url))
    channel = connection.channel()

    # declare exchanges
    channel.exchange_declare(
        CREDITORS_IN_EXCHANGE, exchange_type="headers", durable=True
    )
    channel.exchange_declare(
        CA_LOOPBACK_FILTER_EXCHANGE, exchange_type="headers", durable=True
    )
    channel.exchange_declare(
        CREDITORS_OUT_EXCHANGE,
        exchange_type="topic",
        durable=True,
        arguments={"alternate-exchange": CA_LOOPBACK_FILTER_EXCHANGE},
    )
    channel.exchange_declare(
        TO_TRADE_EXCHANGE, exchange_type="topic", durable=True
    )

    channel.exchange_bind(
        source=CREDITORS_IN_EXCHANGE,
        destination=TO_TRADE_EXCHANGE,
        arguments={
            "x-match": "all",
            "ca-trade": True,
        },
    )
    logger.info(
        'Created a binding from "%s" to the "%s" exchange.',
        CREDITORS_IN_EXCHANGE,
        TO_TRADE_EXCHANGE,
    )

    # Declare a queue and a corresponding dead-letter queue.
    #
    # TODO: Using a "quorum" queue here (with a "stream" dead-letter
    # queue) looks like a good idea, but quorum queues consume lots of
    # memory when there are lots of messages in the queue. In our
    # case, we can have lots of internal messages generated in a very
    # short period of time. A possible solution for this would be to
    # use two queues instead of one: One queue (a quorum queue) for
    # the external messages, promising high-availability; and another
    # queue (a classic queue, or a length-limited quorum queue) for
    # the internal messages, of which we could have a lot, but for
    # which we do not necessarily need high-availability.
    channel.queue_declare(dead_letter_queue_name, durable=True)
    logger.info('Declared "%s" dead-letter queue.', dead_letter_queue_name)

    channel.queue_declare(
        queue_name,
        durable=True,
        arguments={
            "x-dead-letter-exchange": "",
            "x-dead-letter-routing-key": dead_letter_queue_name,
        },
    )
    logger.info('Declared "%s" queue.', queue_name)

    # bind the queue
    channel.queue_bind(
        exchange=TO_TRADE_EXCHANGE,
        queue=queue_name,
        routing_key=routing_key,
    )
    logger.info(
        'Created a binding from "%s" to "%s" with routing key "%s".',
        TO_TRADE_EXCHANGE,
        queue_name,
        routing_key,
    )


@swpt_trade.command("unsubscribe")
@with_appcontext
@click.option(
    "-u",
    "--url",
    type=str,
    help="The RabbitMQ connection URL.",
)
@click.option(
    "-q",
    "--queue",
    type=str,
    help="The name of the queue to unsubscribe.",
)
@click.option(
    "-k",
    "--queue-routing-key",
    type=str,
    help="The RabbitMQ binding key for the queue.",
)
def unsubscribe(url, queue, queue_routing_key):  # pragma: no cover
    """Unsubscribe a RabbitMQ queue from receiving incoming messages.

    If some of the available options are not specified directly, the
    values of the following environment variables will be used:

    * PROTOCOL_BROKER_URL (default "amqp://guest:guest@localhost:5672")

    * PROTOCOL_BROKER_QUEUE (defalut "swpt_trade")

    * PROTOCOL_BROKER_QUEUE_ROUTING_KEY (default "#")
    """

    from swpt_trade.extensions import TO_TRADE_EXCHANGE

    logger = logging.getLogger(__name__)
    queue_name = queue or current_app.config["PROTOCOL_BROKER_QUEUE"]
    routing_key = (
        queue_routing_key
        or current_app.config["PROTOCOL_BROKER_QUEUE_ROUTING_KEY"]
    )
    broker_url = url or current_app.config["PROTOCOL_BROKER_URL"]
    connection = pika.BlockingConnection(pika.URLParameters(broker_url))
    channel = connection.channel()

    channel.queue_unbind(
        exchange=TO_TRADE_EXCHANGE,
        queue=queue_name,
        routing_key=routing_key,
    )
    logger.info(
        'Removed binding from "%s" to "%s" with routing key "%s".',
        TO_TRADE_EXCHANGE,
        queue_name,
        routing_key,
    )


@swpt_trade.command("delete_queue")
@with_appcontext
@click.option(
    "-u",
    "--url",
    type=str,
    help="The RabbitMQ connection URL.",
)
@click.option(
    "-q",
    "--queue",
    type=str,
    help="The name of the queue to delete.",
)
def delete_queue(url, queue):  # pragma: no cover
    """Try to safely delete a RabbitMQ queue.

    When the queue is not empty or is currently in use, this command
    will continuously try to delete the queue, until the deletion
    succeeds or fails for some other reason.

    If some of the available options are not specified directly, the
    values of the following environment variables will be used:

    * PROTOCOL_BROKER_URL (default "amqp://guest:guest@localhost:5672")

    * PROTOCOL_BROKER_QUEUE (defalut "swpt_trade")
    """

    logger = logging.getLogger(__name__)
    queue_name = queue or current_app.config["PROTOCOL_BROKER_QUEUE"]
    broker_url = url or current_app.config["PROTOCOL_BROKER_URL"]
    connection = pika.BlockingConnection(pika.URLParameters(broker_url))
    REPLY_CODE_PRECONDITION_FAILED = 406

    while True:
        channel = connection.channel()
        try:
            channel.queue_delete(
                queue=queue_name,
                if_unused=True,
                if_empty=True,
            )
            logger.info('Deleted "%s" queue.', queue_name)
            break
        except pika.exceptions.ChannelClosedByBroker as e:
            if e.reply_code != REPLY_CODE_PRECONDITION_FAILED:
                raise
            time.sleep(3.0)


@swpt_trade.command("verify_shard_content")
@with_appcontext
def verify_shard_content():
    """Verify that the worker contains only records belonging to the
    worker's shard.

    If the verification is successful, the exit code will be 0. If a
    record has been found that does not belong to the worker's shard,
    the exit code will be 1.
    """

    from swpt_trade import models

    class InvalidRecord(Exception):
        """The record does not belong the shard."""

    sr: ShardingRealm = current_app.config["SHARDING_REALM"]
    yield_per = current_app.config["APP_VERIFY_SHARD_YIELD_PER"]
    sleep_seconds = current_app.config["APP_VERIFY_SHARD_SLEEP_SECONDS"]

    def verify_table(conn, *table_columns, match_str=False):
        with conn.execution_options(yield_per=yield_per).execute(
                select(*table_columns)
        ) as result:
            for n, row in enumerate(result):
                if n % yield_per == 0 and sleep_seconds > 0.0:
                    time.sleep(sleep_seconds)
                match = sr.match_str(row[0]) if match_str else sr.match(*row)
                if not match:
                    raise InvalidRecord

    with db.engine.connect() as conn:
        logger = logging.getLogger(__name__)
        try:
            verify_table(conn, models.AccountLock.creditor_id)
            verify_table(conn, models.CreditorParticipation.creditor_id)
            verify_table(conn, models.InterestRateChange.creditor_id)
            verify_table(conn, models.NeededWorkerAccount.creditor_id)
            verify_table(conn, models.TradingPolicy.creditor_id)
            verify_table(conn, models.DebtorLocatorClaim.debtor_id)
            verify_table(conn, models.RecentlyNeededCollector.debtor_id)
            verify_table(conn, models.DispatchingStatus.collector_id)
            verify_table(conn, models.TransferAttempt.collector_id)
            verify_table(conn, models.WorkerAccount.creditor_id)
            verify_table(conn, models.WorkerCollecting.collector_id)
            verify_table(conn, models.WorkerDispatching.collector_id)
            verify_table(conn, models.WorkerReceiving.to_collector_id)
            verify_table(conn, models.WorkerSending.from_collector_id)
            verify_table(
                conn,
                models.DebtorInfoDocument.debtor_info_locator,
                match_str=True,
            )
        except InvalidRecord:
            logger.error(
                "At least one record has been found that does not belong to"
                " the shard."
            )
            sys.exit(1)
