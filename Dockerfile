FROM alpine:3.11
LABEL maintainer="contact@graphsense.info"


ARG USER=dockeruser
ARG HOME_DIR=/home/$USER
ARG BIN_DIR=/usr/local/bin/

COPY requirements.txt /tmp/requirements.txt

RUN apk --no-cache --update add bash python3 libstdc++ && \
    adduser --system --uid 10000 $USER && \
    apk --no-cache --update --virtual build-dependendencies add \
        build-base \
        linux-headers \
        pcre-dev \
        python3-dev && \
    python3 -m ensurepip && \
    rm -r /usr/lib/python*/ensurepip && \
    pip3 install --upgrade pip setuptools && \
    pip3 install -r /tmp/requirements.txt && \
    apk del build-dependendencies && \
    rm -rf /root/.cache

COPY ./docker/entrypoint.sh /
COPY scripts/*.py $BIN_DIR
COPY scripts/schema.cql /opt/graphsense/schema.cql

USER $USER
WORKDIR $HOME_DIR
