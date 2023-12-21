FROM python:3.11.5-alpine3.18 AS venv-image
WORKDIR /usr/src/app

ENV POETRY_VERSION="1.7.1"
RUN apk add --no-cache \
    file \
    make \
    build-base \
    curl \
    gcc \
    git \
    musl-dev \
    libffi-dev \
    python3-dev \
    postgresql-dev \
    openssl-dev \
    cargo \
  && curl -sSL https://install.python-poetry.org | python - \
  && ln -s "$HOME/.local/bin/poetry" "/usr/local/bin"

COPY pyproject.toml poetry.lock build.py README.md ./
COPY swpt_trade/ swpt_trade/
COPY tests/ tests/
RUN poetry config virtualenvs.create false --local \
  && python -m venv /opt/venv \
  && source /opt/venv/bin/activate \
  && poetry install --only main --no-interaction


# This is the second and final image. Starting from a clean alpine
# image, it copies over the previously created virtual environment.
FROM python:3.11.5-alpine3.18 AS app-image
ARG FLASK_APP=swpt_trade

ENV FLASK_APP=$FLASK_APP
ENV APP_ROOT_DIR=/usr/src/app
ENV APP_ASSOCIATED_LOGGERS="swpt_pythonlib.flask_signalbus.signalbus_cli swpt_pythonlib.multiproc_utils"
ENV PYTHONPATH="$APP_ROOT_DIR"
ENV PATH="/opt/venv/bin:$PATH"
ENV GUNICORN_LOGLEVEL=warning

RUN apk add --no-cache \
    libffi \
    postgresql-libs \
    supervisor \
    gettext \
    && addgroup -S "$FLASK_APP" \
    && adduser -S -D -h "$APP_ROOT_DIR" "$FLASK_APP" "$FLASK_APP"

WORKDIR /usr/src/app

COPY --from=venv-image /opt/venv /opt/venv
COPY --from=venv-image /usr/src/app/swpt_trade/*.so swpt_trade/
COPY --from=venv-image /usr/src/app/tests/*.so tests/

COPY docker/entrypoint.sh \
     docker/gunicorn.conf.py \
     docker/supervisord.conf \
     docker/trigger_supervisor_process.py \
     wsgi.py \
     pytest.ini \
     ./
COPY migrations/ migrations/
COPY $FLASK_APP/ $FLASK_APP/
COPY tests/ tests/
RUN python -m compileall -x '^\./(migrations|tests)/' . \
    && rm -f .env \
    && chown -R "$FLASK_APP:$FLASK_APP" .

USER $FLASK_APP
ENTRYPOINT ["/usr/src/app/entrypoint.sh"]
CMD ["all"]
