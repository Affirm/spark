# PySpark wheel builder — compilation environment for building PySpark wheels on Thor.
# Base image matches batch-infra DTs (Ubuntu 22.04, Python 3.9).
ARG BASE_IMAGE=998571911837.dkr.ecr.us-east-1.amazonaws.com/affirm-base:0.92.7-py3.9.17

FROM ${BASE_IMAGE}

ARG JAVA_VERSION=17

# Java >=17 required for Spark 4.x compilation
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-${JAVA_VERSION}-jdk-headless \
    git=1:2.34.1-1ubuntu1.17 \
    curl=7.81.0-1ubuntu1.23 \
    wget=1.21.2-2ubuntu1.1 \
    && rm -rf /var/lib/apt/lists/*

# Python tooling for building and uploading wheels
RUN python3 -m pip install --no-cache-dir wheel==0.46.3 twine==6.2.0 setuptools==81.0.0

ENV JAVA_HOME="/usr/lib/jvm/java-${JAVA_VERSION}-openjdk-amd64"
ENV PATH="$JAVA_HOME/bin:$PATH"
ENV MAVEN_OPTS="-Xss128m -XX:MaxRAMPercentage=80.0 -XX:InitialRAMPercentage=60.0 -XX:ReservedCodeCacheSize=2g"

WORKDIR /build

COPY build-pyspark-wheels.sh /build/build-pyspark-wheels.sh
RUN chmod +x /build/build-pyspark-wheels.sh

# VERSIONS.md is validated at runtime: productive releases must have a section with
# a matching Commit SHA. No version-specific file folders are needed; all Affirm changes
# must live in the commit SHA recorded in VERSIONS.md.
COPY VERSIONS.md /build/VERSIONS.md

ENTRYPOINT ["/build/build-pyspark-wheels.sh"]
