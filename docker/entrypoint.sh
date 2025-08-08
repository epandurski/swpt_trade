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

# This function tries to upgrade the database schema with exponential
# backoff. This is necessary during development, because the database
# might not be running yet when this script executes.
perform_db_upgrade() {
    local retry_after=1
    local time_limit=$(($retry_after << 5))
    local error_file="$APP_ROOT_DIR/flask-db-upgrade.error"
    echo -n 'Running database schema upgrade ...'
    while [[ $retry_after -lt $time_limit ]]; do
        if flask db upgrade -x only="$1"  &>$error_file; then
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

# This function is intended to perform additional one-time database
# initialization. Make sure that it is idempotent.
# (https://en.wikipedia.org/wiki/Idempotence)
perform_db_initialization() {
    return 0
}

generate_oathkeeper_configuration() {
    envsubst '$WEBSERVER_PORT $OAUTH2_INTROSPECT_URL $TOKEN_INTROSPECTION_CACHE_MAX_COST' \
             < "$APP_ROOT_DIR/oathkeeper/config.yaml.template" \
             > "$APP_ROOT_DIR/oathkeeper/config.yaml"
    envsubst '$RESOURCE_SERVER' \
             < "$APP_ROOT_DIR/oathkeeper/rules.json.template" \
             > "$APP_ROOT_DIR/oathkeeper/rules.json"
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
        perform_db_upgrade "$2"
        if [[ "$SETUP_RABBITMQ_BINDINGS" == "yes" ]]; then
            setup_rabbitmq_bindings
        fi
        ;;
    subscribe | unsubscribe | delete_queue)
        export SOLVER_POSTGRES_URL=postgresql+psycopg://localhost:5432/dummy
        exec flask swpt_trade "$@"
        ;;
    verify_shard_content)
        exec flask swpt_trade "$@"
        ;;
    webserver)
        generate_oathkeeper_configuration
        exec supervisord -c "$APP_ROOT_DIR/supervisord-webserver.conf"
        ;;
    handle_pristine_collectors | consume_messages | scan_all \
        | scan_debtor_info_documents | scan_debtor_locator_claims \
        | scan_trading_policies | scan_worker_accounts \
        | scan_interest_rate_changes | scan_account_locks \
        | scan_needed_worker_accounts | scan_recently_needed_collectors \
        | scan_creditor_participations | scan_dispatching_statuses \
        | scan_worker_collectings | scan_worker_sendings \
        | scan_worker_receivings | scan_worker_dispatchings \
        | scan_transfer_attempts | roll_turns | roll_worker_turns \
        | roll_transfers | fetch_debtor_infos | trigger_transfers)
        exec flask swpt_trade "$@"
        ;;
    flush_configure_accounts | flush_prepare_transfers | flush_finalize_transfers \
        | flush_fetch_debtor_infos | flush_store_documents | flush_discover_debtors \
        | flush_confirm_debtors | flush_activate_collectors | flush_candidate_offers \
        | flush_needed_collectors | flush_revise_account_locks | flush_trigger_transfers \
        | flush_account_id_requests | flush_account_id_responses | flush_all)

        flush_configure_accounts=ConfigureAccountSignal
        flush_prepare_transfers=PrepareTransferSignal
        flush_finalize_transfers=FinalizeTransferSignal
        flush_fetch_debtor_infos=FetchDebtorInfoSignal
        flush_store_documents=StoreDocumentSignal
        flush_discover_debtors=DiscoverDebtorSignal
        flush_confirm_debtors=ConfirmDebtorSignal
        flush_activate_collectors=ActivateCollectorSignal
        flush_candidate_offers=CandidateOfferSignal
        flush_needed_collectors=NeededCollectorSignal
        flush_revise_account_locks=ReviseAccountLockSignal
        flush_trigger_transfers=TriggerTransferSignal
        flush_account_id_requests=AccountIdRequestSignal
        flush_account_id_responses=AccountIdResponseSignal
        flush_start_sendings=StartSendingSignal
        flush_start_dispatchings=StartDispatchingSignal
        flush_all=

        # For example: if `$1` is "flush_configure_accounts",
        # `signal_name` will be "ConfigureAccountSignal".
        eval signal_name=\$$1

        shift
        exec flask swpt_trade flush_messages $signal_name "$@"
        ;;
    worker)
        # Spawns all the necessary worker processes in one container.
        exec supervisord -c "$APP_ROOT_DIR/supervisord-worker.conf"
        ;;
    solver)
        # Spawns all the necessary solver processes in one container.
        exec supervisord -c "$APP_ROOT_DIR/supervisord-solver.conf"
        ;;
    await_migrations)
        echo Awaiting database migrations to be applied...
        while [[ $(flask db current 2> /dev/null | grep -c '(head)') != 2 ]]; do
            sleep 10
        done
        ;;
    *)
        exec "$@"
        ;;
esac
