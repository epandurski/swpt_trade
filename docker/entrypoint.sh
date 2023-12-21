#!/bin/sh
set -e

# During development and testing, we should be able to connect to
# services installed on "localhost" from the container. To allow this,
# we find the IP address of the docker host, and then for each
# variable name in "$SUBSTITUTE_LOCALHOST_IN_VARS", we substitute
# "localhost" with that IP address.
host_ip=$(ip route show | awk '/default/ {print $3}')
for envvar_name in $SUBSTITUTE_LOCALHOST_IN_VARS; do
    eval envvar_value=\$$envvar_name
    if [[ -n "$envvar_value" ]]; then
        eval export $envvar_name=$(echo "$envvar_value" | sed -E "s/(.*@|.*\/\/)localhost\b/\1$host_ip/")
    fi
done

# The WEBSERVER_* variables should be used instead of the GUNICORN_*
# variables, because we do not want to tie the public interface to the
# "gunicorn" server, which we may, or may not use in the future.
export GUNICORN_WORKERS=${WEBSERVER_PROCESSES:-1}
export GUNICORN_THREADS=${WEBSERVER_THREADS:-3}

# The POSTGRES_URL variable should be used instead of the
# SQLALCHEMY_DATABASE_URI variable, because we do not want to tie the
# public interface to the "sqlalchemy" library, which we may, or may
# not use in the future.
export SQLALCHEMY_DATABASE_URI=${POSTGRES_URL}

# This function tries to upgrade the database schema with exponential
# backoff. This is necessary during development, because the database
# might not be running yet when this script executes.
perform_db_upgrade() {
    local retry_after=1
    local time_limit=$(($retry_after << 5))
    local error_file="$APP_ROOT_DIR/flask-db-upgrade.error"
    echo -n 'Running database schema upgrade ...'
    while [[ $retry_after -lt $time_limit ]]; do
        if flask db upgrade &>$error_file; then
            perform_db_initialization
            echo ' done.'
            return 0
        fi
        sleep $retry_after
        retry_after=$((2 * retry_after))
    done
    echo
    cat "$error_file"
    return 1
}

# This function tries to set up the needed RabbitMQ objects (queues,
# exchanges, bindings) with exponential backoff. This is necessary
# during development, because the RabbitMQ server might not be running
# yet when this script executes.
setup_rabbitmq_bindings() {
    local retry_after=1
    local time_limit=$(($retry_after << 5))
    local error_file="$APP_ROOT_DIR/flask-setup-bindings.error"
    echo -n 'Setting up message broker objects ...'
    while [[ $retry_after -lt $time_limit ]]; do
        if flask swpt_trade subscribe &>$error_file; then
            echo ' done.'
            return 0
        fi
        sleep $retry_after
        retry_after=$((2 * retry_after))
    done
    echo
    cat "$error_file"
    return 1
}

# This function tries to create the RabbitMQ queue for trade'
# chores, with exponential backoff. This is necessary during
# development, because the RabbitMQ server might not be running yet
# when this script executes.
create_chores_queue() {
    local retry_after=1
    local time_limit=$(($retry_after << 5))
    local error_file="$APP_ROOT_DIR/flask-create-queue.error"
    echo -n 'Creating chores queue ...'
    while [[ $retry_after -lt $time_limit ]]; do
        if flask swpt_trade create_chores_queue &>$error_file; then
            echo ' done.'
            return 0
        fi
        sleep $retry_after
        retry_after=$((2 * retry_after))
    done
    echo
    cat "$error_file"
    return 1
}

# This function is intended to perform additional one-time database
# initialization. Make sure that it is idempotent.
# (https://en.wikipedia.org/wiki/Idempotence)
perform_db_initialization() {
    return 0
}

case $1 in
    develop-run-flask)
        # Do not run this in production!
        shift
        exec flask run --host=0.0.0.0 --port ${WEBSERVER_PORT:-5000} --without-threads "$@"
        ;;
    test)
        # Do not run this in production!
        perform_db_upgrade
        exec pytest
        ;;
    configure)
        perform_db_upgrade
        create_chores_queue
        if [[ "$SETUP_RABBITMQ_BINDINGS" == "yes" ]]; then
            setup_rabbitmq_bindings
        fi
        ;;
    webserver)
        exec gunicorn --config "$APP_ROOT_DIR/gunicorn.conf.py" -b :${WEBSERVER_PORT:-8080} wsgi:app
        ;;
    consume_messages)
        exec flask swpt_trade "$@"
        ;;
    consume_chore_messages)
        exec flask swpt_trade "$@"
        ;;
    flush_configure_accounts | flush_prepare_transfers | flush_finalize_transfers \
        | flush_all)

        flush_configure_accounts=ConfigureAccountSignal
        flush_prepare_transfers=PrepareTransferSignal
        flush_finalize_transfers=FinalizeTransferSignal
        flush_all=

        # For example: if `$1` is "flush_configure_accounts",
        # `signal_name` will be "ConfigureAccountSignal".
        eval signal_name=\$$1

        shift
        exec flask swpt_trade flush_messages $signal_name "$@"
        ;;
    all)
        # Spawns all the necessary processes in one container.
        exec supervisord -c "$APP_ROOT_DIR/supervisord.conf"
        ;;
    *)
        exec "$@"
        ;;
esac
