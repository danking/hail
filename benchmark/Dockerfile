FROM --platform=linux/amd64 {{ global.docker_root_image }}

ENV LANG C.UTF-8
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && \
  apt-get -y install \
    git \
    htop \
    unzip bzip2 zip tar \
    curl \
    rsync \
    emacs-nox \
    mysql-client \
    xsltproc pandoc \
    jq \
    python \
    python3.8 python3-pip python3.8-dev \
    liblapack3 && \
  update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.8 1 && \
  rm -rf /var/lib/apt/lists/*

RUN curl -L https://packages.adoptium.net/artifactory/api/gpg/key/public | apt-key add - && \
  echo "deb https://packages.adoptium.net/artifactory/deb $(awk -F= '/^VERSION_CODENAME/{print$2}' /etc/os-release) main" | tee /etc/apt/sources.list.d/adoptium.list && \
  apt update && \
  apt -y install temurin-11-jdk

RUN mkdir hailtop

COPY hail/python/pinned-requirements.txt hail/python/pinned-requirements.txt
COPY hail/python/dev/pinned-requirements.txt hail/python/dev/pinned-requirements.txt

RUN python3 -m pip install --upgrade --no-cache-dir --upgrade pip && \
  python3 -m pip install --upgrade --no-cache-dir setuptools && \
  python3 -m pip install --upgrade --no-cache-dir -r hail/python/pinned-requirements.txt -r hail/python/dev/pinned-requirements.txt && \
  python3 -m pip install --upgrade --no-cache-dir aiomysql && \
  python3 -m pip check

COPY hail/build/deploy/dist/hail-0.2.116-py3-none-any.whl hail/build/deploy/dist/hail-0.2.116-py3-none-any.whl .

RUN python3 -m pip install --upgrade --no-cache-dir --quiet hail-{{ hail_pip_version }}-py3-none-any.whl && \
  python3 -m pip check

COPY benchmark/python/dist/benchmark_hail-{{ hail_pip_version }}-py3-none-any.whl .

RUN python3 -m pip install --upgrade --no-cache-dir --quiet benchmark_hail-{{ hail_pip_version }}-py3-none-any.whl && \
  python3 -m pip check
