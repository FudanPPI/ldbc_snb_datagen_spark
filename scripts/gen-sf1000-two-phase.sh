#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# SF1000 two-phase generation — avoids the Old-Gen death spiral that killed
# four single-phase runs (75g/90g heap × 32/60 threads × off-heap) at the
# same Stage 35 (groupByKey). Each phase owns the whole heap:
#
#   Phase 1  generate native SF1000 IR → <output-dir>/native/
#            (GenerationStage + TransformationStage in native pass-through;
#             no enhance modules — this stage is fast and memory-light)
#   Phase 2  enhance-only from native IR → <output-dir>/enhanced/
#            (TransformationStage only: IdRemap → TopologySkew → Zipf →
#             Burst → DirtyData; no GenerationStage competing for heap)
#
# Usage:
#   scripts/gen-sf1000-two-phase.sh <root-out-dir> [--verify]
#
# Environment overrides (apply to both phases):
#   JAVA_HOME, SPARK_HOME, THREADS (default 24), DRIVER_MEM (default 50g),
#   OFFHEAP_SIZE (default 10g), SHUFFLE_PARTS (default 2400),
#   SCALE_FACTOR (default 1000), ENHANCE_PARAM_FILE, SPARK_LOCAL_DIR
# ---------------------------------------------------------------------------
set -euo pipefail

ROOT=${1:?usage: gen-sf1000-two-phase.sh <root-out-dir> [--verify]}
VERIFY_ONLY=""
if [[ ${2:-} == "--verify" ]]; then VERIFY_ONLY=1; fi

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
JAR="$REPO/target/ldbc_snb_datagen_2.12_spark3.2-0.6.0-enhance-jar-with-dependencies.jar"
[[ -f $JAR ]] || { echo "jar missing: $JAR (run sbt assembly first)" >&2; exit 1; }

JAVA_HOME=${JAVA_HOME:-$HOME/.local/jdk}
SPARK_HOME=${SPARK_HOME:-$HOME/.local/opt/spark-3.2.2-bin-hadoop3.2}
THREADS=${THREADS:-24}
DRIVER_MEM=${DRIVER_MEM:-50g}
OFFHEAP_SIZE=${OFFHEAP_SIZE:-10g}
SHUFFLE_PARTS=${SHUFFLE_PARTS:-2400}
SCALE_FACTOR=${SCALE_FACTOR:-1000}
ENHANCE_PARAM_FILE=${ENHANCE_PARAM_FILE:-$REPO/conf/enhance-sf1000.ini}
export JAVA_HOME PATH="$JAVA_HOME/bin:$PATH"

NATIVE_OUT="$ROOT/native"
ENH_OUT="$ROOT/enhanced"
LOG_P1="/tmp/sf1000-phase1.log"
LOG_P2="/tmp/sf1000-phase2.log"

[[ -d $ROOT ]] || mkdir -p "$ROOT"
[[ -d $SPARK_HOME ]] || { echo "SPARK_HOME not found: $SPARK_HOME" >&2; exit 1; }

SPARK_LOCAL_DIR=${SPARK_LOCAL_DIR:-/tmp/sf1000-sparktmp}
mkdir -p "$SPARK_LOCAL_DIR"

# cap dirty page cache growth for the duration of the run
( while true; do sleep 30; sync; done ) &
SYNC_PID=$!
trap 'kill "$SYNC_PID" 2>/dev/null || true' EXIT

run_phase() {
  local phase="$1" log="$2" out="$3" skip_gen="$4" input_dir="${5:-}" skip_trans="$6"
  local enh_param="${7:-}"
  local -a extra_args=()
  [[ -n "$skip_gen" ]] && extra_args+=(--skip-generation)
  [[ -n "$skip_trans" ]] && extra_args+=(--skip-transformation)
  [[ -n "$input_dir" ]] && extra_args+=(--input-dir "$input_dir")
  [[ -n "$enh_param" ]] && extra_args+=(--enhance-param-file "$enh_param")

  echo "=== Phase $phase starting $(date -Is) ==="
  echo "    output:  $out"
  echo "    log:     $log"
  echo "    threads: $THREADS  driver-mem: $DRIVER_MEM  off-heap: $OFFHEAP_SIZE"
  [[ -n "$skip_gen" ]] && echo "    skip-generation: yes"
  [[ -n "$skip_trans" ]] && echo "    skip-transformation: yes"
  [[ -n "$input_dir" ]] && echo "    input-dir: $input_dir"

  "$SPARK_HOME/bin/spark-submit" \
    --master "local[$THREADS]" \
    --driver-memory "$DRIVER_MEM" \
    --conf spark.local.dir="$SPARK_LOCAL_DIR" \
    --conf spark.shuffle.partitions="$SHUFFLE_PARTS" \
    --conf spark.sql.shuffle.partitions="$SHUFFLE_PARTS" \
    --conf spark.memory.fraction=0.75 \
    --conf spark.memory.offHeap.enabled=true \
    --conf spark.memory.offHeap.size="$OFFHEAP_SIZE" \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.ui.showConsoleProgress=false \
    --conf spark.driver.maxResultSize=8g \
    --class ldbc.snb.datagen.LdbcDatagen \
    "$JAR" \
    --scale-factor "$SCALE_FACTOR" \
    -f csv \
    -n "$THREADS" \
    --oversize-factor 4 \
    --output-dir "$out" \
    "${extra_args[@]}" \
    2>&1 | tee "$log"

  echo "=== Phase $phase finished $(date -Is) ==="
}

phase1() {
  # Phase 1: generate native SF IR only (skip TransformationStage — Phase 2 reads it directly)
  [[ -d $NATIVE_OUT ]] && { echo "Phase 1 output already exists: $NATIVE_OUT (refusing to overwrite)" >&2; exit 1; }
  run_phase 1 "$LOG_P1" "$NATIVE_OUT" "" "" "yes" ""
}

phase2() {
  # Phase 2: enhance-only from native IR → enhanced CSV
  [[ ! -d $NATIVE_OUT ]] && { echo "Phase 1 output not found: $NATIVE_OUT (run Phase 1 first)" >&2; exit 1; }
  [[ -d $ENH_OUT ]] && { echo "Phase 2 output already exists: $ENH_OUT (refusing to overwrite)" >&2; exit 1; }

  run_phase 2 "$LOG_P2" "$ENH_OUT" "yes" "$NATIVE_OUT" "" "$ENHANCE_PARAM_FILE"
}

phase1
phase2

if [[ -n "$VERIFY_ONLY" ]]; then
  echo "=== running Spark-based structural verification ==="
  "$SPARK_HOME/bin/spark-submit" \
    --master "local[$THREADS]" \
    --driver-memory "$DRIVER_MEM" \
    "$REPO/scripts/verify_enhance_spark.py" \
    "$ENH_OUT" \
    --quick
fi

echo "=== all done $(date -Is) ==="
