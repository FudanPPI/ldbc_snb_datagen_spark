#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# SF1000 (~1TB raw CSV) skew-enhanced datagen on a single 80-core machine.
#
# Usage:
#   scripts/gen-sf1000.sh <output-dir> [--verify [native-dir]]
#
# Environment overrides:
#   JAVA_HOME, SPARK_HOME, THREADS (default 32), DRIVER_MEM (default 75g),
#   SHUFFLE_PARTS (default 960), LOG (default /tmp/sf1000-gen.log)
#
# Memory sizing: local[N] mode runs driver+executor in one JVM; its resident
# footprint reaches roughly heap + 20% (off-heap/netty/GC/stacks). On a 125G
# host, 75g heap keeps total RSS ~90G, leaving headroom for the OS and any
# other work — with 90g heap the kernel OOM-killed the run once.
#
# Three prior runs taught us:
#   1. 90g heap + docker build in parallel → kernel OOM (RSS 104g)
#   2. 75g heap, 60 threads → 840 concurrent parquet writers saturate exec
#      memory pool, thrash GC into a 5-hour hang, then kernel OOM fallback
#      (271 "exceeds 95% of heap" warnings)
#   3. 75g heap, 32 threads → writer burst tamed, zero heap-exceed warnings,
#      but Old Gen still hits 99.99% full with 320 Full GCs — the working
#      set simply does not fit on 75g GC-managed heap at SF1000 scale.
#
# Fix (this run): move execution memory off-heap via Spark's off-heap pool.
# Old Gen now holds only Spark metadata + broadcast vars (small), while
# shuffle data, broadcast payloads, and parquet writer buffers live in a
# 15g off-heap region that bypasses GC entirely. Memory fraction 0.75
# inside the heap expands the execution pool within GC-managed space.
# Shuffle partitions doubled to 1920 so each task holds less transient data.
# Concurrency stays at 32 and the dirty-page sync loop is kept from run #2.
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
THREADS=${THREADS:-32}
DRIVER_MEM=${DRIVER_MEM:-75g}
SHUFFLE_PARTS=${SHUFFLE_PARTS:-1920}
OFFHEAP_SIZE=${OFFHEAP_SIZE:-15g}
LOG=${LOG:-/tmp/sf1000-gen.log}
export JAVA_HOME PATH="$JAVA_HOME/bin:$PATH"

[[ -d $OUT ]] && { echo "output dir already exists: $OUT (refusing to overwrite)" >&2; exit 1; }
[[ -d $SPARK_HOME ]] || { echo "SPARK_HOME not found: $SPARK_HOME" >&2; exit 1; }

# keep shuffle spill off the output disk when possible
SPARK_LOCAL_DIR=${SPARK_LOCAL_DIR:-/tmp/sf1000-sparktmp}
mkdir -p "$SPARK_LOCAL_DIR"

# cap dirty page cache growth for the duration of the run
( while true; do sleep 30; sync; done ) &
SYNC_PID=$!
trap 'kill "$SYNC_PID" 2>/dev/null || true' EXIT

echo "=== SF1000 generation starting $(date -Is) ==="
echo "    output: $OUT"
echo "    log:    $LOG"

"$SPARK_HOME/bin/spark-submit" \
  --master "local[$THREADS]" \
  --driver-memory "$DRIVER_MEM" \
  --conf spark.local.dir="$SPARK_LOCAL_DIR" \
  --conf spark.shuffle.partitions="$SHUFFLE_PARTS" \
  --conf spark.sql.shuffle.partitions="$SHUFFLE_PARTS" \
  --conf spark.memory.fraction=0.75 \
  --conf spark.memory.offHeap.enabled=true \
  --conf spark.memory.offHeap.size="$OFFHEAP_SIZE" \
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
