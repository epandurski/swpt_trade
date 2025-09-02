Swaptacular "Circular Trade" reference implementation
=====================================================

This project implements automated currency exchanges for
[Swaptacular], in the spirit of [Circular Multilateral
Barter](https://swaptacular.github.io/public/docs/cmb-general.pdf).
The deliverables are two [docker images]: the *app-image*, and the
*swagger-ui-image*. Both images are generated from the project's
[Dockerfile](../master/Dockerfile).

* The `app-image` implements 3 types of servers that the "Circular
  Trade" service consists of:

  1. **One "solver" server.**
  
  This server periodically performs *trading turns*, gathering
  currencies information, together with buy and sell offers, and
  analyzing ("solving") them so as to propose a list of transactions
  that will be beneficial for all the participants.

  2. **One or more "worker" servers.**

  These servers work together to collect all the information that the
  solver server need, and to actually perform the transactions
  proposed by the solver server. Each worker server is responsible for
  a different [database shard].

  3. **An optional "admin API" web server.**

  This server implements a Web-API that provides various
  administrative tools, useful for the continued proper functioning of
  the "Circular Trade" service.

* The `swagger-ui-image` is a simple [Swagger UI] client for the admin
  Web API, mainly useful for testing.

**Note:** This implementation uses [JSON Serialization for the
Swaptacular Messaging Protocol].


Dependencies
------------

Containers started from the *app-image* must have access to the
following servers:

1. One [PostgreSQL] server instance, which stores the solver server's
   data.

2. One PostgreSQL server instance **for each worker server**.

3. A [RabbitMQ] server instance, which acts as broker for [Swaptacular
   Messaging Protocol] (SMP) messages. The [rabbitmq_random_exchange
   plugin] should be enabled.

   The following [RabbitMQ topic exchanges] must be configured on the
   broker instance:

   - **`creditors_out`**: For messages that must be sent to accounting
     authorities. The routing key will represent the debtor ID as
     hexadecimal (lowercase). For example, for debtor ID equal to 10, the
     routing key will be "00.00.00.00.00.00.00.0a".

   - **`to_trade`**: For messages that must be processed by one of the
     worker servers. The routing key will represent the highest 24
     bits of the MD5 digest of the sharding key. For example, if the
     sharding key for a given message type is the creditor ID, and the
     creditor ID is 123, the routing key will be
     "1.1.1.1.1.1.0.0.0.0.0.1.0.0.0.0.0.1.1.0.0.0.1.1". This allows
     different messages to be handled by different worker servers
     (sharding).

     The following types of messages will be published on this
     exchange:

     * Incoming SMP messages related to *collector accounts*. (To do
       their jobs, worker servers create and use system accounts,
       called "collector accounts", which act as distribution hubs for
       money.)

     * Incoming SMP messages concerning transfers with *"agent"*
       coordinator type. (The "agent" coordinator type is reserved for
       transfers initiated by the "Circular Trade" service, on behalf
       of users.)

     * Policy, ledger, and flag update notifications, sent by the
       subsystem that is responsible for managing users' accounts.
       (The "Circular Trade" reference implementation is designed to
       work in tandem with the ["Creditors Agent" reference
       implementation].)

     * Internal messages. (To do their jobs, worker servers will send
       messages to each other. Even when there is only one worker
       server, it will use the `to_trade` exchange to send messages to
       itself.)

   Also, **for each worker server** one [RabbitMQ queue] must be
   configured on the broker instance, so that all messages published
   on the `to_trade` exchange, are routed to one of these queues
   (determined by the rouging key).

   **Note:** If you execute the "configure" command (see below), with
   the environment variable `SETUP_RABBITMQ_BINDINGS` set to `yes`, an
   attempt will be made to automatically setup all the required
   RabbitMQ queues, exchanges, and the bindings between them.

4. An optional [OAuth 2.0] authorization server, which authorizes
   clients' requests to the *admin API*. There is a plethora of
   popular Oauth 2.0 server implementations. Normally, they maintain
   their own user database, and go together with UI for user
   registration, login, and authorization consent.

   Also, to increase security and performance, it is highly
   recommended that you configure HTTP reverse-proxy server(s) (like
   [nginx]) between the admin clients and the "admin API".


Configuring the "solver" server
-------------------------------

The behavior of the solver server can be tuned with environment
variables. Here are the most important settings with some random
example values:

```shell
# Determiene when new trading turns will be started. In this
# example (these are the default values), a new turn will be
# started every day at 2:00am, the time allotted to the first
# turn phase (the currency info collection phase) will be 10
# minutes, and the time allotted to the second turn
# phase (buy/sell offers collection phase) will be 1 hour. The
# solver server will check the status of ongoing trading turns
# every 60 seconds.
TURN_PERIOD=1d
TURN_PERIOD_OFFSET=2h
TURN_PHASE1_DURATION=10m
TURN_PHASE2_DURATION=1h
TURN_CHECK_INTERVAL=60s

# In order to be traded, currencies must be pegged to other
# currencies, thus forming a "peg-tree". At the root of the
# peg-tree sits the "base currency". The base currency is
# specified by its "debtor info locator" and its debtor
# ID ("$BASE_DEBTOR_ID" and "$BASE_DEBTOR_INFO_LOCATOR").
# Currencies with distance to the root bigger
# than "$MAX_DISTANCE_TO_BASE" will be ighored (default 10).
BASE_DEBTOR_INFO_LOCATOR=https://currencies.swaptacular.org/USD
BASE_DEBTOR_ID=666
MAX_DISTANCE_TO_BASE=10

# The amount of every arranged trade must rounded to an integer
# number. For this reason, trading small amounts may result in
# signifficant rounding errors. To avoid this, trades for amounts
# smaller than "$MIN_TRADE_AMOUNT" will be not be arranged (the
# default is 1000).
MIN_TRADE_AMOUNT=10000

# Connection string for the solver's PostgreSQL database server.
SOLVER_POSTGRES_URL=postgresql+psycopg://swpt_solver:swpt_solver@localhost:5435/test

# The solver server maintains a pool of database connections
# to the solver's PostgreSQL database server. This variable
# determines the maximum number of connections in this pool. If
# zero is specified (the default) there is no limit to the
# connection pool's size.
SOLVER_CLIENT_POOL_SIZE=0

# Set the minimum level of severity for log messages ("info",
# "warning", or "error"). The default is "warning".
APP_LOG_LEVEL=info

# Set format for log messages ("text" or "json"). The default is
# "text".
APP_LOG_FORMAT=text
```

For more configuration options, check the
[development.env](../master/development.env) file.


Configuring "worker" servers
----------------------------

The behavior of worker servers can be tuned with environment
variables. Here are the most important settings with some random
example values:

```shell
# All collector accounts will have their creditor IDs
# between "$MIN_COLLECTOR_ID" and "$MAX_COLLECTOR_ID". This can
# be passed as a decimal number (like "4294967296"), or a
# hexadecimal number (like "0x100000000"). Numbers between
# 0x8000000000000000 and 0xffffffffffffffff will be automatically
# converted to their corresponding two's complement negative
# numbers. Normally, you would not need this interval to contain
# more than a few thousand IDs. The defaults are: from
# "0x0000010000000000" to "0x00000100000007ff".
MIN_COLLECTOR_ID=0x0000010000000000
MAX_COLLECTOR_ID=0x00000100000007ff

# When a currency is about to be traded for the first time, at least
# one collector account must be created. This setting determines how
# many collector accounts will be created for newly encountered
# currencies (default 1).
DEFAULT_NUMBER_OF_COLLECTOR_ACCOUNTS=2

# When the outgouing transfers are committed, a deadline for each
# transfer should be specified. This allows the "Circular Trade"
# service to make a resonable estimate for the maximum possible
# losses coming form negative interest rates. This variable
# specifies the period during which it is highly likely that all
# scheduled outgoing transfers will be successfully performed.
# The default is 2 hours. As a rule of thumb, this period should
# be twice as big as the TURN_PHASE2_DURATION period.
TRANSFERS_HEALTHY_MAX_COMMIT_DELAY=2.5h

# For each arranged transfer, a very small portion of the
# transferred amount will be seized as a reward for performing a
# transaction that is beneficial for all the participants (and as
# a safety-margin as well). This variable specifies how big the
# seized portion is. The default is "1e-5", wich means that
# 0.001% of the amount will be seized.
TRANSFERS_AMOUNT_CUT=1e-6

# Connection string for this worker's PostgreSQL database server.
WORKER_POSTGRES_URL=postgresql+psycopg://swpt_worker:swpt_worker@localhost:5435/test

# Connection string for the solver's PostgreSQL database server.
SOLVER_POSTGRES_URL=postgresql+psycopg://swpt_solver:swpt_solver@localhost:5435/test

# Each worker server maintains a pool of database connections to
# the solver's PostgreSQL database server. This variable
# determines the maximum number of connections in this pool. If
# zero is specified (the default) there is no limit to the
# connection pool's size.
SOLVER_CLIENT_POOL_SIZE=0

# Parameters for the communication with the RabbitMQ server which is
# responsible for brokering SMP messages. The container will connect
# to "$PROTOCOL_BROKER_URL" (default
# "amqp://guest:guest@localhost:5672"), will consume messages from the
# queue named "$PROTOCOL_BROKER_QUEUE" (default "swpt_trade"),
# prefetching at most "$PROTOCOL_BROKER_PREFETCH_COUNT" messages at
# once (default 1). The specified number of processes
# ("$PROTOCOL_BROKER_PROCESSES") will be spawned to consume and
# process messages (default 1), each process will run
# "$PROTOCOL_BROKER_THREADS" threads in parallel (default 1). Note
# that PROTOCOL_BROKER_PROCESSES can be set to 0, in which case, the
# container will not consume any messages from the queue.
PROTOCOL_BROKER_URL=amqp://guest:guest@localhost:5672
PROTOCOL_BROKER_QUEUE=swpt_trade
PROTOCOL_BROKER_PROCESSES=1
PROTOCOL_BROKER_THREADS=3
PROTOCOL_BROKER_PREFETCH_COUNT=10

# The binding key with which the "$PROTOCOL_BROKER_QUEUE"
# RabbitMQ queue is bound to the `to_trade` topic
# exchange (default "#"). The binding key must consist of zero or
# more 0s or 1s, separated by dots, ending with a hash symbol.
# For example: "0.1.#", "1.#", or "#".
PROTOCOL_BROKER_QUEUE_ROUTING_KEY=#

# All outgoing RabbitMQ messages are first recorded in the
# worker's PostgreSQL database, and then are "fulshed" to the
# RabbitMQ message broker. The specified number of
# processes ("$FLUSH_PROCESSES") will be spawned to flush
# messages (default 1). Note that FLUSH_PROCESSES can be set to
# 0, in which case, the container will not flush any messages.
# The "$FLUSH_PERIOD" value specifies the number of seconds to
# wait between two sequential flushes (default 2).
FLUSH_PROCESSES=2
FLUSH_PERIOD=1.5

# Worker servers should periodically perform scheduled HTTP
# requests to fetch debtor info documents. The specified number
# of processes ("$HTTP_FETCH_PROCESSES") will be spawned to do
# this job (default 1). Each process will open a maximum number
# of "$HTTP_FETCH_CONNECTIONS" parallel HTTP connections (default
# 100), and will give up after not receiving a response
# for "$HTTP_FETCH_TIMEOUT" seconds (default 10). Note that
# HTTP_FETCH_PROCESSES can be set to 0, in which case, the
# container will not try to fetch any debtor info documents.
# The "$HTTP_FETCH_PERIOD" value specifies the number of seconds
# to wait between two sequential database queries to obtain
# scheduled HTTP fetches whose time to be performed has
# come (default 5).
HTTP_FETCH_PROCESSES=1
HTTP_FETCH_CONNECTIONS=100
HTTP_FETCH_TIMEOUT=10.0
HTTP_FETCH_PERIOD=2.5

# Worker servers should periodically trigger scheduled transfer
# attempts. The specified number of
# processes ("$TRIGGER_TRANSFERS_PROCESSES") will be spawned to
# do this job (default 1). Note that TRIGGER_TRANSFERS_PROCESSES
# can be set to 0, in which case, the container will not trigger
# any transfer attempts. The "$TRIGGER_TRANSFERS_PERIOD" value
# specifies the number of seconds to wait between two sequential
# database queries to obtain scheduled transfer attempts whose
# time to be triggered has come (default 5).
TRIGGER_TRANSFERS_PROCESSES=1
TRIGGER_TRANSFERS_PERIOD=2.5

# Worker servers should periodically query the solver's database
# for new ("pristine") collector accounts that need to be
# created. The "$HANDLE_PRISTINE_COLLECTORS_PERIOD" value
# specifies the number of seconds to wait between two sequential
# database queries to obtain new collector accounts from the
# solver's database (default 60). The specified number of
# threads ("$HANDLE_PRISTINE_COLLECTORS_THREADS") will be spawned
# to actually create the needed collector accounts (default 1).
HANDLE_PRISTINE_COLLECTORS_THREADS=1
HANDLE_PRISTINE_COLLECTORS_PERIOD=60.0

# Set this to "true" after splitting a parent database shard into
# two children shards. You may set this back to "false", once all
# left-over records from the parent have been deleted from the
# child shard.
DELETE_PARENT_SHARD_RECORDS=false

# Set the minimum level of severity for log messages ("info",
# "warning", or "error"). The default is "warning".
APP_LOG_LEVEL=info

# Set format for log messages ("text" or "json"). The default is
# "text".
APP_LOG_FORMAT=text
```

For more configuration options, check the
[development.env](../master/development.env) file.

 
Configuring the "admin API" server
----------------------------------

The behavior of the admin API server can be tuned with environment
variables. Here are the most important settings with some random
example values:

```shell
# All collector accounts will have their creditor IDs
# between "$MIN_COLLECTOR_ID" and "$MAX_COLLECTOR_ID". This can
# be passed as a decimal number (like "4294967296"), or a
# hexadecimal number (like "0x100000000"). Numbers between
# 0x8000000000000000 and 0xffffffffffffffff will be automatically
# converted to their corresponding two's complement negative
# numbers. Normally, you would not need this interval to contain
# more than a few thousand IDs. The defaults are: from
# "0x0000010000000000" to "0x00000100000007ff".
MIN_COLLECTOR_ID=0x0000010000000000
MAX_COLLECTOR_ID=0x00000100000007ff

# Requests to the "admin API" are protected by an OAuth
# 2.0 authorization server. With every request, the client (a Web
# browser, for example) presents a token, and to verify the
# validity of the token, internally, a request is made to the
# OAuth 2.0 authorization server. This is called "token
# introspection". The OAUTH2_INTROSPECT_URL variable sets the URL
# at which internal token introspection requests will be sent.
#
# IMPORTANT NOTE: The response to the "token introspection" request
# will contain a "username" field. In order to be allowed to use the
# admin API, the username returned by the OAuth 2.0 authorization
# server must one of the following:
#
# 1) "$OAUTH2_SUPERUSER_USERNAME" -- This user will be allowed
#    to do everything. The default value for
#    OAUTH2_SUPERUSER_USERNAME is "creditors-superuser".
#
# 2) "$OAUTH2_SUPERVISOR_USERNAME" -- This user will be
#    allowed to view the admin data, and to perform non-critical
#    admin tasks. The default value for
#    OAUTH2_SUPERVISOR_USERNAME is "creditors-supervisor".
OAUTH2_INTROSPECT_URL=http://localhost:4445/oauth2/introspect
OAUTH2_SUPERUSER_USERNAME=creditors-superuser
OAUTH2_SUPERVISOR_USERNAME=creditors-supervisor

# Connection string for the solver's PostgreSQL database server.
SOLVER_POSTGRES_URL=postgresql+psycopg://swpt_solver:swpt_solver@localhost:5435/test

# The admin API server maintains a pool of database connections
# to the solver's PostgreSQL database server. This variable
# determines the maximum number of connections in this pool. If
# zero is specified (the default) there is no limit to the
# connection pool's size.
SOLVER_CLIENT_POOL_SIZE=0

# The specified number of processes ("$WEBSERVER_PROCESSES") will
# be spawned to handle "admin API" requests (default 1), each
# process will run "$WEBSERVER_THREADS" threads in
# parallel (default 3). The container will listen for "Payments
# Web API" requests on port "$WEBSERVER_PORT" (default 8080).
WEBSERVER_PROCESSES=2
WEBSERVER_THREADS=10
WEBSERVER_PORT=8003

# Set the minimum level of severity for log messages ("info",
# "warning", or "error"). The default is "warning".
APP_LOG_LEVEL=info

# Set format for log messages ("text" or "json"). The default is
# "text".
APP_LOG_FORMAT=text
```

For more configuration options, check the
[development.env](../master/development.env) file.


Available commands
------------------

The [entrypoint](../master/docker/entrypoint.sh) of the docker
container allows you to execute the following *documented commands*:

* `solver`

  Starts a solver server.

  **IMPORTANT NOTE: You must start exactly one container with this
  command.**

* `worker`

  Starts a worker server. Also, this is the command that will be
  executed if no arguments are passed to the entrypoint.

  **IMPORTANT NOTE: For each worker database instance, you must start
  exactly one container with this command.**

* `webserver`

  Starts an admin API server. This command allows you to start as many
  admin API web servers as you like.

* `configure`

  Initializes a new empty solver PostgreSQL database, and a new empty
  worker PostgreSQL database.

  - `configure` will initialize both solver's and worker's databases.
  - `configure solver` will initialize only the solver's database.
  - `configure worker` will initialize only the worker's database.

  **IMPORTANT NOTE: This command has to be run only once (at the
  beginning), but running it multiple times should not do any harm.**

* `await_migrations`

  Blocks until the latest migration applied to solver's and worker's
  databases match the latest known migration.

* `consume_messages`

  Starts only the processes that consume SMP messages. This command
  allows you to start as many additional dedicated RabbitMQ message
  processors as necessary, to handle the load. If the
  `--draining-mode` option is specified, periodic pauses will be made
  during consumption, to allow the queue to be deleted safely.

* `flush_all`

  Starts only the worker processes that send outgoing messages to the
  RabbitMQ broker, and remove the messages from the PostgreSQL database.

* `flush_configure_accounts`, `flush_prepare_transfers`,
  `flush_finalize_transfers`, `flush_fetch_debtor_infos`,
  `flush_store_documents`, `flush_discover_debtors`,
  `flush_confirm_debtors`, `flush_activate_collectors`,
  `flush_candidate_offers`, `flush_needed_collectors`,
  `flush_revise_account_locks`, `flush_trigger_transfers`,
  `flush_account_id_requests`, `flush_account_id_responses`

  Starts additional worker processes that send particular type of outgoing
  messages to the RabbitMQ broker, and remove the messages from the
  PostgreSQL database. These commands allow you to start processes dedicated
  to the flushing of particular type of messages. (See "FLUSH_PROCESSES" and
  "FLUSH_PERIOD" environment variables.)

* `fetch_debtor_infos`

  Starts additional worker processes that perform scheduled HTTP
  requests to fetch debtor info documents. These commands allow you to
  start processes dedicated to fetching debtor info documents. (See
  "HTTP_FETCH_PROCESSES", "HTTP_FETCH_CONNECTIONS",
  "HTTP_FETCH_TIMEOUT" and "HTTP_FETCH_PERIOD" environment variables.)

* `trigger_transfers`

  Starts additional worker processes that trigger scheduled transfer
  attempts. These commands allow you to start processes dedicated to
  triggering transfer attempts. (See "TRIGGER_TRANSFERS_PROCESSES" and
  "TRIGGER_TRANSFERS_PERIOD" environment variables.)

* `subscribe`

  Declares a RabbitMQ queue, and subscribes it to receive incoming
  messages. Normally, this command should not be executed directly.

* `unsubscribe`

  Unsubscribes a RabbitMQ queue from receiving incoming messages.
  Normally, this command should not be executed directly.

* `delete_queue`

  Tries to safely delete a RabbitMQ queue. Normally, this command
  should not be executed directly.

* `verify_shard_content`

  Verifies that the worker contains only records belonging to the
  worker's shard. Normally, this command should not be executed
  directly.

**Note:** The Docker image comes with `sh` and `psql` installed.


How to run the tests
--------------------

1.  Install [Docker Engine] and [Docker Compose].

2.  To create an *.env* file with reasonable defalut values, run this
    command:

        $ cp development.env .env

3.  To run the unit tests, use the following commands:

        $ docker-compose build
        $ docker-compose run tests-config test


How to setup a development environment
--------------------------------------

1.  Install [Poetry](https://poetry.eustace.io/docs/).

2.  Create a new [Python](https://docs.python.org/) virtual
    environment and activate it.

3.  To install dependencies, run this command:

        $ poetry install

4.  To run the minimal set of services needed for development, use
    this command:

        $ docker-compose up --build

    This will start its own PostgreSQL and Redis server instances in
    docker containers, but will rely on being able to connect to a
    RabbitMQ server instance at
    "amqp://guest:guest@localhost:5672". The OAuth 2.0 authorization
    will be bypassed.

    Note that because the RabbitMQ "guest" user [can only connect from
    localhost], you should either explicitly allow the "guest" user to
    connect from anywhere, or create a new RabbitMQ user, and change
    the RabbitMQ connection URLs accordingly (`PROTOCOL_BROKER_URL` in
    the *.env* file).

    Moreover, you need to enable the `rabbitmq_random_exchange` plugin by
    running:

        $ sudo rabbitmq-plugins enable rabbitmq_random_exchange

5.  You can use `flask run -p 5000` to run a local web server, and
    `pytest --cov=swpt_trade --cov-report=html` to run the tests and
    generate a test coverage report.


How to run all services (production-like)
-----------------------------------------

The "Circular Trade" service is intended to work in tandem with the
"Creditors Agent" service. To start them both, see the ["Creditors
Agent" reference implementation].


[Swaptacular]: https://swaptacular.github.io/overview
[docker images]: https://www.geeksforgeeks.org/what-is-docker-images/
[database shard]: https://en.wikipedia.org/wiki/Shard_(database_architecture)
[Swagger UI]: https://swagger.io/tools/swagger-ui/
[JSON Serialization for the Swaptacular Messaging Protocol]: https://github.com/swaptacular/swpt_accounts/blob/master/protocol-json.rst
[PostgreSQL]: https://www.postgresql.org/
[RabbitMQ]: https://www.rabbitmq.com/
[RabbitMQ queue]: https://www.cloudamqp.com/blog/part1-rabbitmq-for-beginners-what-is-rabbitmq.html
[RabbitMQ topic exchanges]: https://www.cloudamqp.com/blog/part4-rabbitmq-for-beginners-exchanges-routing-keys-bindings.html
[rabbitmq_random_exchange plugin]: https://github.com/rabbitmq/rabbitmq-random-exchange
[Swaptacular Messaging Protocol]: https://github.com/swaptacular/swpt_accounts/blob/master/protocol.rst
["Creditors Agent" reference implementation]: https://github.com/swaptacular/swpt_creditors
[OAuth 2.0]: https://oauth.net/2/
[nginx]: https://en.wikipedia.org/wiki/Nginx
[Docker Engine]: https://docs.docker.com/engine/
[Docker Compose]: https://docs.docker.com/compose/
[can only connect from localhost]: https://www.rabbitmq.com/access-control.html#loopback-users
