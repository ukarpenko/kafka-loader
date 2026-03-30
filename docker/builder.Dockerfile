FROM quay.io/pypa/manylinux_2_24_x86_64 AS builder

ENV PYTHON_VERSION=3.11.1
ENV PYTHON_PREFIX=/opt/python-shared
ENV PYTHON_BIN=/opt/python-shared/bin/python3
ENV LD_LIBRARY_PATH=/opt/python-shared/lib
ENV PIP_DISABLE_PIP_VERSION_CHECK=1
ENV PIP_NO_CACHE_DIR=1

WORKDIR /build

RUN printf '%s\n' \
    'deb http://archive.debian.org/debian stretch main' \
    'deb http://archive.debian.org/debian-security stretch/updates main' \
    > /etc/apt/sources.list && \
    printf '%s\n' \
    'Acquire::Check-Valid-Until "false";' \
    'Acquire::AllowInsecureRepositories "true";' \
    > /etc/apt/apt.conf.d/99archive && \
    apt-get update && \
    apt-get install -y --allow-unauthenticated \
      build-essential \
      curl \
      libbz2-dev \
      libffi-dev \
      liblzma-dev \
      libncurses5-dev \
      libreadline-dev \
      libsqlite3-dev \
      libssl-dev \
      tk-dev \
      uuid-dev \
      zlib1g-dev && \
    rm -rf /var/lib/apt/lists/*

RUN curl -fsSLO https://www.python.org/ftp/python/${PYTHON_VERSION}/Python-${PYTHON_VERSION}.tgz && \
    tar -xzf Python-${PYTHON_VERSION}.tgz && \
    cd Python-${PYTHON_VERSION} && \
    ./configure --prefix=${PYTHON_PREFIX} --enable-shared --with-ensurepip=install && \
    make -j"$(nproc)" && \
    make install && \
    cd /build && \
    rm -rf Python-${PYTHON_VERSION} Python-${PYTHON_VERSION}.tgz

COPY wheelhouse/ /build/wheelhouse/
RUN ${PYTHON_BIN} -m pip install --no-index --find-links=/build/wheelhouse \
    pyinstaller \
    altgraph \
    pyinstaller-hooks-contrib \
    packaging \
    setuptools \
    confluent-kafka==2.4.0

COPY kafka_loader/ /build/kafka_loader/
RUN ${PYTHON_BIN} -m PyInstaller --onefile --name kafka-loader \
    /build/kafka_loader/__main__.py

FROM scratch AS artifact
COPY --from=builder /build/dist/kafka-loader /kafka-loader
