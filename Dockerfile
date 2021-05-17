FROM alpine:3.11
LABEL maintainer="contact@graphsense.info"


ARG BIN_DIR=/usr/local/bin/
ARG INSTALL_DIR=/opt/graphsense-ethereum-etl/
ARG USER=dockeruser
ARG HOME_DIR=/home/$USER
ARG PANDAS_ENV=$HOME_DIR/pandas-venv

COPY requirements*.txt $INSTALL_DIR

# install JRE (required by dsbulk) and Python prerequisites,
# then pip3 install cassandra-driver and ethereum-etl
RUN apk --no-cache --update add bash python3 openjdk8-jre git curl && \
    adduser --system --uid 10000 $USER && \
    apk --no-cache --update --virtual build-dependendencies add \
        gcc \
        linux-headers \
        musl-dev \
        pcre-dev \
        python3-dev && \
    python3 -m ensurepip && \
    rm -r /usr/lib/python*/ensurepip && \
    pip3 install --upgrade pip setuptools && \
    pip3 install -r $INSTALL_DIR/requirements.txt && \
    apk del build-dependendencies && \
    rm -rf /root/.cache && \
    ln -s /usr/bin/ethereumetl $BIN_DIR


# install Datastax dsbulk
WORKDIR $INSTALL_DIR
RUN curl -OL https://downloads.datastax.com/dsbulk/dsbulk-1.8.0.tar.gz && \
    tar -xzf $INSTALL_DIR/dsbulk-1.8.0.tar.gz -C  $INSTALL_DIR && \
    ln -s $INSTALL_DIR/dsbulk-1.8.0/bin/dsbulk $BIN_DIR


COPY scripts/*.py $BIN_DIR
COPY scripts/*.sh $BIN_DIR
COPY scripts/schema.cql /opt/graphsense/schema.cql

COPY ./docker/entrypoint.sh /


# setup a venv for exchange rate ingestion, and install pandas
RUN apk --no-cache --update --virtual build-dependendencies add \
        gcc \
        linux-headers \
        musl-dev \
        pcre-dev \
        build-base\
        python3-dev && \
    pip3 install virtualenv &&\
    virtualenv $PANDAS_ENV &&\
    $PANDAS_ENV/bin/pip3 install -r $INSTALL_DIR/requirements_venv.txt  &&\
    apk del build-dependendencies && \
    rm -rf /root/.cache


USER $USER
WORKDIR $HOME_DIR
