#!/usr/bin/env bash
# Builds the bench module and produces peak-ewma-bench/target/report/REPORT.md:
#   1. LB overhead report: CPU, allocation, retained heap, latency per policy x fleet size (fresh JVM per row)
#   2. Routing quality: slow + instantly-failing backend
#   3. JMH: pick path (PickerBenchmark) and full RPC (RpcBenchmark), with -prof gc
#
#   peak-ewma-bench/run-benchmarks.sh            # full run (~25-35 min)
#   QUICK=1 peak-ewma-bench/run-benchmarks.sh    # smoke run (~5 min)
#   RIGOROUS=1 peak-ewma-bench/run-benchmarks.sh # 5 interleaved runs/row + 3 JMH forks, with CIs (~2-3 h)
#   REPEATS=n FORKS=n ...                        # explicit repeat/fork counts
#   POLICIES=round_robin,peak_ewma_p2c SIZES=10,500 ...   # restrict the matrix
#   MVN_ARGS="-s ~/my-settings.xml" ...          # extra Maven args
set -euo pipefail
cd "$(dirname "$0")/.."

#   SKIP_OVERHEAD=1 ...                         # reuse an existing overhead.md in OUT
#   JAR=/path/benchmarks.jar OUT=dir ...       # use a pre-built (e.g. frozen copy) jar, skip the build
if [[ -z "${JAR:-}" ]]; then
  ./mvnw -B -q ${MVN_ARGS:-} package -pl peak-ewma-bench -am -DskipTests -Dspotless.check.skip=true
  JAR=peak-ewma-bench/target/benchmarks.jar
fi
OUT=${OUT:-peak-ewma-bench/target/report}
mkdir -p "$OUT"

REPEATS=${REPEATS:-$([[ "${RIGOROUS:-0}" == "1" ]] && echo 5 || echo 1)}
FORKS=${FORKS:-$([[ "${RIGOROUS:-0}" == "1" ]] && echo 3 || echo 1)}
FILTER=()
[[ -n "${POLICIES:-}" ]] && FILTER+=(-p "policy=$POLICIES")
OVERHEAD_FILTER=()
[[ -n "${POLICIES:-}" ]] && OVERHEAD_FILTER+=(--policies "$POLICIES")

if [[ "${QUICK:-0}" == "1" ]]; then
  OVERHEAD=(--seconds 3 --warmup 2 --sizes 3,100)
  JMH=(-wi 1 -i 2 -w 1 -r 1 -f 1)
  PICK_SIZES=(-p backends=10,500)
  RPC_SIZES=(-p backends=10,100)
else
  OVERHEAD=(--seconds 10 --warmup 5 --sizes "${SIZES:-3,10,100,500}")
  JMH=(-f "$FORKS")
  PICK_SIZES=(${SIZES:+-p "backends=$SIZES"})
  RPC_SIZES=(${SIZES:+-p "backends=$SIZES"})
fi

[[ "${SKIP_OVERHEAD:-0}" == "1" ]] || java -cp "$JAR" dev.parkerharrelson.grpc.peakewma.bench.OverheadReport ${OVERHEAD[@]+"${OVERHEAD[@]}"} ${OVERHEAD_FILTER[@]+"${OVERHEAD_FILTER[@]}"} --repeats "$REPEATS" --out "$OUT/overhead.md"
java -jar "$JAR" 'PickerBenchmark' ${JMH[@]+"${JMH[@]}"} ${PICK_SIZES[@]+"${PICK_SIZES[@]}"} ${FILTER[@]+"${FILTER[@]}"} -prof gc -rf csv -rff "$OUT/picker.csv"
java -jar "$JAR" 'RpcBenchmark' ${JMH[@]+"${JMH[@]}"} ${RPC_SIZES[@]+"${RPC_SIZES[@]}"} ${FILTER[@]+"${FILTER[@]}"} -prof gc -rf csv -rff "$OUT/rpc.csv"
java -cp "$JAR" dev.parkerharrelson.grpc.peakewma.bench.JmhReport "$OUT/picker.csv" "$OUT/picker.md" >/dev/null
java -cp "$JAR" dev.parkerharrelson.grpc.peakewma.bench.JmhReport "$OUT/rpc.csv" "$OUT/rpc.md" >/dev/null

{
  echo "# peak_ewma_p2c vs grpc-java built-in policies"
  echo
  echo "Generated $(date -u +%Y-%m-%dT%H:%MZ) at $(git rev-parse --short HEAD)$(git diff --quiet || echo '-dirty')."
  echo
  sed 's/^# LB overhead report//' "$OUT/overhead.md"
  echo
  echo "# JMH: LB pick path (pickSubchannel + tracer lifecycle), real picker over in-process subchannels"
  echo
  cat "$OUT/picker.md"
  echo "# JMH: full unary RPC (in-process transport)"
  echo
  cat "$OUT/rpc.md"
} > "$OUT/REPORT.md"
echo "Report: $OUT/REPORT.md"
