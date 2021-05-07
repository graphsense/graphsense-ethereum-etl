FROM alpine:3.11
LABEL maintainer="contact@graphsense.info"


ARG BINDIR=/usr/local/bin/
ARG INSTALLDIR=/opt/graphsense-ethereum-etl/
ARG USER=dockeruser
ARG UHOME=/home/$USER
ARG PANDAS_ENV=$UHOME/pandas-venv

COPY requirements.txt $INSTALLDIR

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
    pip3 install -r $INSTALLDIR/requirements.txt && \
    apk del build-dependendencies && \
    rm -rf /root/.cache && \
    ln -s /usr/bin/ethereumetl $BINDIR


# install Datastax dsbulk
WORKDIR $INSTALLDIR
RUN curl -OL https://downloads.datastax.com/dsbulk/dsbulk-1.8.0.tar.gz && \
    tar -xzf $INSTALLDIR/dsbulk-1.8.0.tar.gz -C  $INSTALLDIR && \
    ln -s $INSTALLDIR/dsbulk-1.8.0/bin/dsbulk $BINDIR


COPY scripts/*.py $BINDIR
COPY scripts/*.sh $BINDIR
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
    $PANDAS_ENV/bin/pip3 install pandas==1.2.4 cassandra-driver==3.24.0 requests==2.25.1 &&\
    apk del build-dependendencies && \
    rm -rf /root/.cache


USER $USER
WORKDIR $UHOME
