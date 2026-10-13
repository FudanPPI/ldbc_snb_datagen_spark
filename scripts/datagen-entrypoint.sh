#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Container entrypoint: wraps spark-submit for the skew-enhanced datagen jar.
#
# Environment knobs:
#   MASTER         spark master, default local[*]
#   DRIVER_MEMORY  default 4g
#   DRIVER_CORES   default (unset)
#   SPARK_CONF     extra '-c k=v' pairs, space-separated:
#                  SPARK_CONF="spark.local.dir=/tmp spark.shuffle.partitions=960"
#
# All arguments are passed through to LdbcDatagen, e.g.
#   datagen-entrypoint --scale-factor 1 -f csv --output-dir /out \
#     --enhance-param-file $ENHANCE_HOME/conf/enhance-defaults.ini
# ---------------------------------------------------------------------------
set -euo pipefail

: "${JAVA_HOME:=/usr/lib/jvm/java-11-openjdk-amd64}"
: "${SPARK_HOME:=/opt/spark}"
: "${LDBC_SNB_DATAGEN_JAR:=/opt/datagen/datagen.jar}"
export JAVA_HOME PATH="$JAVA_HOME/bin:$SPARK_HOME/bin:$PATH"

extra_conf=()
for kv in ${SPARK_CONF:-}; do extra_conf+=(-c "$kv"); done

exec "$SPARK_HOME/bin/spark-submit" \
  --master "${MASTER:-local[*]}" \
  ${DRIVER_CORES:+--driver-cores "$DRIVER_CORES"} \
  --driver-memory "${DRIVER_MEMORY:-4g}" \
  ${extra_conf[@]+"${extra_conf[@]}"} \
  --class ldbc.snb.datagen.LdbcDatagen \
  "$LDBC_SNB_DATAGEN_JAR" \
  "$@"
