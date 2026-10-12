#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# SF1000 (~1TB raw CSV) skew-enhanced datagen on a single 80-core machine.
#
# Usage:
#   scripts/gen-sf1000.sh <output-dir> [--verify [native-dir]]
#
# Environment overrides:
#   JAVA_HOME, SPARK_HOME, THREADS (default 60), DRIVER_MEM (default 90g),
#   SHUFFLE_PARTS (default 960), LOG (default /tmp/sf1000-gen.log)
#
# The output directory must not exist (refuses to overwrite a terabyte run).
# With --verify, scripts/verify_enhance.py runs after generation succeeds.
# ---------------------------------------------------------------------------
set -euo pipefail

OUT=${1:?usage: gen-sf1000.sh <output-dir> [--verify [native-dir]]}
VERIFY_NATIVE=""
if [[ ${2:-} == "--verify" ]]; then
  VERIFY_NATIVE=${3:-}
fi

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
JAR="$REPO/target/ldbc_snb_datagen_2.12_spark3.2-0.6.0-enhance-jar-with-dependencies.jar"
[[ -f $JAR ]] || { echo "jar missing: $JAR (run sbt assembly first)" >&2; exit 1; }

JAVA_HOME=${JAVA_HOME:-$HOME/.local/jdk}
SPARK_HOME=${SPARK_HOME:-$HOME/.local/opt/spark-3.2.2-bin-hadoop3.2}
THREADS=${THREADS:-60}
DRIVER_MEM=${DRIVER_MEM:-90g}
SHUFFLE_PARTS=${SHUFFLE_PARTS:-960}
LOG=${LOG:-/tmp/sf1000-gen.log}
export JAVA_HOME PATH="$JAVA_HOME/bin:$PATH"

[[ -d $OUT ]] && { echo "output dir already exists: $OUT (refusing to overwrite)" >&2; exit 1; }
[[ -d $SPARK_HOME ]] || { echo "SPARK_HOME not found: $SPARK_HOME" >&2; exit 1; }

# keep shuffle spill off the output disk when possible
SPARK_LOCAL_DIR=${SPARK_LOCAL_DIR:-/tmp/sf1000-sparktmp}
mkdir -p "$SPARK_LOCAL_DIR"

echo "=== SF1000 generation starting $(date -Is) ==="
echo "    output: $OUT"
echo "    log:    $LOG"

"$SPARK_HOME/bin/spark-submit" \
  --master "local[$THREADS]" \
  --driver-memory "$DRIVER_MEM" \
  --conf spark.local.dir="$SPARK_LOCAL_DIR" \
  --conf spark.shuffle.partitions="$SHUFFLE_PARTS" \
  --conf spark.sql.shuffle.partitions="$SHUFFLE_PARTS" \
  --conf spark.ui.showConsoleProgress=false \
  --conf spark.driver.maxResultSize=8g \
  --class ldbc.snb.datagen.LdbcDatagen \
  "$JAR" \
  --scale-factor 1000 \
  -f csv \
  -n "$THREADS" \
  --oversize-factor 4 \
  --output-dir "$OUT" \
  --enhance-param-file "$REPO/conf/enhance-sf1000.ini" \
  2>&1 | tee "$LOG"

echo "=== generation finished $(date -Is) ==="

if [[ -n ${2:-} ]]; then
  echo "=== running structural verification ==="
  if [[ -n $VERIFY_NATIVE ]]; then
    python3 "$REPO/scripts/verify_enhance.py" "$OUT" "$VERIFY_NATIVE"
  else
    python3 "$REPO/scripts/verify_enhance.py" "$OUT"
  fi
fi
