# ---------------------------------------------------------------------------
# LDBC SNB Datagen (Spark) — skew-enhanced fork, self-contained container.
#
# Stages:
#   build-jar : JDK 11 + sbt, builds the assembly jar from source
#   runtime   : Ubuntu + JRE 11 + Spark 3.2.1 + datagen jar + enhance assets
#
# Base image override for environments without Docker Hub access:
#   docker build --build-arg BASE=ldbc/ubuntu-base:20.04 -t ldbc/datagen-enhance .
# (scripts/build-image.sh provisions ldbc/ubuntu-base:20.04 automatically,
#  via Docker Hub pull when reachable, else via cdimage.ubuntu.com import)
# ---------------------------------------------------------------------------
ARG BASE=ubuntu:20.04

FROM ${BASE} AS build-jar
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
      curl openjdk-11-jdk-headless \
    && rm -rf /var/lib/apt/lists/*
ARG SBT_VERSION=1.9.1
RUN curl -fSsL "https://github.com/sbt/sbt/releases/download/v${SBT_VERSION}/sbt-${SBT_VERSION}.tgz" \
      | tar xvz -C /opt
ENV PATH=/opt/sbt/bin:$PATH
WORKDIR /build
COPY build.sbt build.sbt
COPY project project
RUN sbt update
COPY src src
RUN sbt assembly

FROM ${BASE} AS runtime
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
      curl openjdk-11-jre-headless python3 procps \
    && rm -rf /var/lib/apt/lists/*
ARG SPARK_VERSION=3.2.1
RUN curl -fSsL "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.2.tgz" \
      | tar xvz -C /opt \
    && mv /opt/spark-${SPARK_VERSION}-bin-hadoop3.2 /opt/spark
COPY --from=build-jar /build/target/ldbc_snb_datagen_*-jar-with-dependencies.jar /opt/datagen/datagen.jar
COPY tools/run.py /opt/datagen/run.py
# skew-enhancement assets: parameter files, structural verifier, docs
COPY conf /opt/enhance/conf
COPY scripts/verify_enhance.py /opt/enhance/scripts/verify_enhance.py
COPY docs/ENHANCE.md /opt/enhance/docs/ENHANCE.md
COPY scripts/datagen-entrypoint.sh /usr/local/bin/datagen-entrypoint
RUN chmod +x /usr/local/bin/datagen-entrypoint

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 \
    SPARK_HOME=/opt/spark \
    PATH=/opt/spark/bin:$PATH \
    LDBC_SNB_DATAGEN_JAR=/opt/datagen/datagen.jar \
    ENHANCE_HOME=/opt/enhance

WORKDIR /work
ENTRYPOINT ["datagen-entrypoint"]
