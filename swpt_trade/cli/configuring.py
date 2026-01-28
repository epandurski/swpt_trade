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
from swpt_trade.models import SET_SEQSCAN_ON
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
    help="Connection URL for the SMP messages RabbitMQ broker.",
)
@click.option(
    "-q",
    "--queue",
    type=str,
    help="The name of the SMP messages queue to declare and subscribe.",
)
@click.option(
    "-k",
    "--queue-routing-key",
    type=str,
    help="The RabbitMQ binding key for the queue.",
)
@click.option(
    "-U",
    "--internal-url",
    type=str,
    help="Connection URL for the internal messages RabbitMQ broker.",
)
@click.option(
    "-Q",
    "--internal-queue",
    type=str,
    help="The name of the internal messages queue to declare and subscribe.",
)
def subscribe(
        url,
        queue,
        queue_routing_key,
        internal_url,
        internal_queue,
):  # pragma: no cover
    """Declare a RabbitMQ queue pair (for both SMP and internal
    messages), and subscribe the two queues to receive messages.

    If some of the available options are not specified directly, the
    values of the following environment variables will be used:

    * PROTOCOL_BROKER_URL (default "amqp://guest:guest@localhost:5672")

    * PROTOCOL_BROKER_QUEUE (defalut "swpt_trade")

    * PROTOCOL_BROKER_QUEUE_ROUTING_KEY (default "#")

    * INTERNAL_BROKER_URL (default "amqp://guest:guest@localhost:5672")

    * INTERNAL_BROKER_QUEUE (defalut "$PROTOCOL_BROKER_QUEUE-internal")

    """

    from swpt_trade.extensions import (
        CREDITORS_IN_EXCHANGE,
        CREDITORS_OUT_EXCHANGE,
        TO_TRADE_EXCHANGE,
    )
    CA_LOOPBACK_FILTER_EXCHANGE = "ca.loopback_filter"

    logger = logging.getLogger(__name__)
    cfg = current_app.config
    routing_key = queue_routing_key or cfg["PROTOCOL_BROKER_QUEUE_ROUTING_KEY"]
    url = url or cfg["PROTOCOL_BROKER_URL"]
    queue = queue or cfg["PROTOCOL_BROKER_QUEUE"]
    internal_url = internal_url or cfg["INTERNAL_BROKER_URL"]
    internal_queue = internal_queue or cfg["INTERNAL_BROKER_QUEUE"]

    def declare_necessary_creditors_agent_exchanges(broker_url):
        connection = pika.BlockingConnection(pika.URLParameters(broker_url))
        channel = connection.channel()
        logger.info('Connected to "%s".', broker_url)

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

        channel.close()
        connection.close()
        logger.info('Disconnected from "%s".', broker_url)

    def declare_and_bind_queue(broker_url, queue_name):
        connection = pika.BlockingConnection(pika.URLParameters(broker_url))
        channel = connection.channel()
        logger.info('Connected to "%s".', broker_url)

        channel.exchange_declare(
            TO_TRADE_EXCHANGE, exchange_type="topic", durable=True
        )

        dead_letter_queue_name = queue_name + ".XQ"
        channel.queue_declare(
            dead_letter_queue_name,
            durable=True,
            arguments={"x-queue-type": "stream"},
        )
        logger.info(
            'Declared "%s" dead-letter queue.', dead_letter_queue_name
        )
        channel.queue_declare(
            queue_name,
            durable=True,
            arguments={
                "x-queue-type": "quorum",
                "overflow": "reject-publish",
                "x-dead-letter-exchange": "",
                "x-dead-letter-routing-key": dead_letter_queue_name,
            },
        )
        logger.info('Declared "%s" queue.', queue_name)

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

        channel.close()
        connection.close()
        logger.info('Disconnected from "%s".', broker_url)

    declare_necessary_creditors_agent_exchanges(url)
    declare_and_bind_queue(url, queue)
    declare_and_bind_queue(internal_url, internal_queue)


@swpt_trade.command("unsubscribe")
@with_appcontext
@click.option(
    "-u",
    "--url",
    type=str,
    help="Connection URL for the SMP messages RabbitMQ broker.",
)
@click.option(
    "-q",
    "--queue",
    type=str,
    help="The name of the SMP messages queue to unsubscribe.",
)
@click.option(
    "-k",
    "--queue-routing-key",
    type=str,
    help="The RabbitMQ binding key for the queue.",
)
@click.option(
    "-U",
    "--internal-url",
    type=str,
    help="Connection URL for the internal messages RabbitMQ broker.",
)
@click.option(
    "-Q",
    "--internal-queue",
    type=str,
    help="The name of the internal messages queue to unsubscribe.",
)
def unsubscribe(
        url,
        queue,
        queue_routing_key,
        internal_url,
        internal_queue,
):  # pragma: no cover
    """Unsubscribe a RabbitMQ queue pair (for both SMP and internal
    messages) from receiving messages.

    If some of the available options are not specified directly, the
    values of the following environment variables will be used:

    * PROTOCOL_BROKER_URL (default "amqp://guest:guest@localhost:5672")

    * PROTOCOL_BROKER_QUEUE (defalut "swpt_trade")

    * PROTOCOL_BROKER_QUEUE_ROUTING_KEY (default "#")

    * INTERNAL_BROKER_URL (default "amqp://guest:guest@localhost:5672")

    * INTERNAL_BROKER_QUEUE (defalut "$PROTOCOL_BROKER_QUEUE-internal")

    """

    from swpt_trade.extensions import TO_TRADE_EXCHANGE

    logger = logging.getLogger(__name__)
    cfg = current_app.config
    routing_key = queue_routing_key or cfg["PROTOCOL_BROKER_QUEUE_ROUTING_KEY"]
    url = url or cfg["PROTOCOL_BROKER_URL"]
    queue = queue or cfg["PROTOCOL_BROKER_QUEUE"]
    internal_url = internal_url or cfg["INTERNAL_BROKER_URL"]
    internal_queue = internal_queue or cfg["INTERNAL_BROKER_QUEUE"]

    def unbind_queue(broker_url, queue_name):
        connection = pika.BlockingConnection(pika.URLParameters(broker_url))
        channel = connection.channel()
        logger.info('Connected to "%s".', broker_url)

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

        channel.close()
        connection.close()
        logger.info('Disconnected from "%s".', broker_url)

    unbind_queue(url, queue)
    unbind_queue(internal_url, internal_queue)


@swpt_trade.command("delete_queue")
@with_appcontext
@click.option(
    "-u",
    "--url",
    type=str,
    help="Connection URL for the SMP messages RabbitMQ broker.",
)
@click.option(
    "-q",
    "--queue",
    type=str,
    help="The name of the SMP messages queue to delete.",
)
@click.option(
    "-U",
    "--internal-url",
    type=str,
    help="Connection URL for the internal messages RabbitMQ broker.",
)
@click.option(
    "-Q",
    "--internal-queue",
    type=str,
    help="The name of the internal messages queue to delete.",
)
def delete_queue(url, queue, internal_url, internal_queue):  # pragma: no cover
    """Try to safely delete a RabbitMQ queue pair (for both SMP and
    internal messages).

    When the queue is not empty or is currently in use, this command
    will continuously try to delete the queue, until the deletion
    succeeds or fails for some other reason.

    If some of the available options are not specified directly, the
    values of the following environment variables will be used:

    * PROTOCOL_BROKER_URL (default "amqp://guest:guest@localhost:5672")

    * PROTOCOL_BROKER_QUEUE (defalut "swpt_trade")

    * INTERNAL_BROKER_URL (default "amqp://guest:guest@localhost:5672")

    * INTERNAL_BROKER_QUEUE (defalut "$PROTOCOL_BROKER_QUEUE-internal")

    """

    REPLY_CODE_NOT_FOUND = 404
    logger = logging.getLogger(__name__)
    cfg = current_app.config
    url = url or cfg["PROTOCOL_BROKER_URL"]
    queue = queue or cfg["PROTOCOL_BROKER_QUEUE"]
    internal_url = internal_url or cfg["INTERNAL_BROKER_URL"]
    internal_queue = internal_queue or cfg["INTERNAL_BROKER_QUEUE"]

    def delete_queue(broker_url, queue_name):
        connection = pika.BlockingConnection(pika.URLParameters(broker_url))
        logger.info('Connected to "%s".', broker_url)

        # Wait for the queue to become empty. Note that passing
        # `if_empty=True` to queue_delete() currently does not work for
        # quorum queues. Instead, we check the number of messages in the
        # queue before deleting it.
        while True:
            channel = connection.channel()
            try:
                status = channel.queue_declare(
                    queue_name,
                    durable=True,
                    passive=True,
                )
            except pika.exceptions.ChannelClosedByBroker as e:
                if e.reply_code != REPLY_CODE_NOT_FOUND:
                    raise
                break  # already deleted

            if status.method.message_count == 0:
                channel.queue_delete(queue=queue_name)
                logger.info('Deleted "%s" queue.', queue_name)
                break

            channel.close()
            time.sleep(3.0)

        if channel.is_open:
            channel.close()
        connection.close()
        logger.info('Disconnected from "%s".', broker_url)

    delete_queue(url, queue)
    delete_queue(internal_url, internal_queue)


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
        conn.execute(SET_SEQSCAN_ON)
        logger = logging.getLogger(__name__)
        try:
            # NOTE: Basically, every table that relies on a table
            # scanner to clean up the records from the parent shard
            # must be listed here.
            verify_table(conn, models.AccountLock.creditor_id)
            verify_table(conn, models.CreditorParticipation.creditor_id)
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

            # These are the SMP signals that this app emits.
            verify_table(conn, models.ConfigureAccountSignal.creditor_id)
            verify_table(conn, models.PrepareTransferSignal.creditor_id)
            verify_table(conn, models.FinalizeTransferSignal.creditor_id)
        except InvalidRecord:
            logger.error(
                "At least one record has been found that does not belong to"
                " the shard."
            )
            sys.exit(1)
