#!/usr/bin/env bash
# kind smoke run for #94 (<= 10 min): 6 backend pods behind a headless Service, one loadgen Job per
# policy running concurrently against them, and a brownout (latency x5) on one pod mid-run.
#
#   ./mvnw -pl peak-ewma-loadtest -am package -DskipTests
#   peak-ewma-loadtest/k8s/kind-smoke.sh
#
# Uses its own kubeconfig file (KUBECONFIG_FILE, default k8s/.kubeconfig); it never reads or
# changes ~/.kube/config or your current context.
#
#   RUN_ID=cluster-2026-10-04 RPS=300 BUILD_CONTEXT=/path/with/target/loadtest.jar ...
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
MODULE="$(dirname "$HERE")"
CLUSTER=${CLUSTER:-peakewma}
KUBECONFIG_FILE=${KUBECONFIG_FILE:-$HERE/.kubeconfig}
export KUBECONFIG="$KUBECONFIG_FILE" # only for this script's processes
RUN_ID=${RUN_ID:-cluster-$(date +%Y-%m-%d)}
OUT="$MODULE/results/$RUN_ID/kind-smoke"
RPS=${RPS:-300}
REPLICAS=${REPLICAS:-6}
WARMUP=${WARMUP:-20}
DURATION=${DURATION:-90}
FAULT_START=${FAULT_START:-20} # seconds after measurement start
FAULT_LEN=${FAULT_LEN:-40}
NS=default
POLICIES=(round_robin peak_ewma_p2c least_request_experimental lr_od)
BUILD_CONTEXT=${BUILD_CONTEXT:-$MODULE}
K="kubectl --kubeconfig $KUBECONFIG_FILE"

cleanup() {
  [[ -n "${PF_PID:-}" ]] && kill "$PF_PID" 2>/dev/null || true
  [[ "${KEEP_CLUSTER:-0}" == "1" ]] || kind delete cluster --name "$CLUSTER" --kubeconfig "$KUBECONFIG_FILE" || true
}
trap cleanup EXIT

mkdir -p "$OUT"
kind get clusters 2>/dev/null | grep -qx "$CLUSTER" ||
  kind create cluster --name "$CLUSTER" --kubeconfig "$KUBECONFIG_FILE" --wait 120s
docker build -q -t peak-ewma-loadtest:dev -f "$MODULE/Dockerfile" "$BUILD_CONTEXT"
kind load docker-image peak-ewma-loadtest:dev --name "$CLUSTER"

sed "s/replicas: 6/replicas: $REPLICAS/" "$HERE/backend.yaml" | $K apply -f -
$K rollout status deploy/loadtest-backend --timeout=180s
$K wait --for=condition=Ready pod -l app=loadtest-backend --timeout=120s

BACKENDS=$($K get pods -l app=loadtest-backend -o jsonpath='{range .items[*]}{.status.podIP}:9000 {end}')
VICTIM_POD=$($K get pods -l app=loadtest-backend -o jsonpath='{.items[0].metadata.name}')
VICTIM_IP=$($K get pod "$VICTIM_POD" -o jsonpath='{.status.podIP}')

FAULT_END=$((FAULT_START + FAULT_LEN))
for p in "${POLICIES[@]}"; do
  name="lg-${p//_/-}"
  $K delete job "$name" --ignore-not-found >/dev/null
  sed -e "s/__NAME__/$name/" -e "s/__POLICY__/$p/g" -e "s/__RPS__/$RPS/" -e "s/__NS__/$NS/" \
      -e "s/__CLIENT__/$p-c0/" -e "s/__WARMUP__/$WARMUP/" -e "s/__DURATION__/$DURATION/" \
      -e "s/__FAULT_START__/$FAULT_START/" -e "s/__FAULT_END__/$FAULT_END/" \
      "$HERE/loadgen.yaml" | $K apply -f -
done
for p in "${POLICIES[@]}"; do
  for _ in $(seq 120); do
    [[ "$($K get pods -l "policy=$p" -o jsonpath='{.items[0].status.phase}' 2>/dev/null)" == "Running" ]] && break
    sleep 1
  done
done
START_EPOCH=$(date +%s)

# Brownout: latency x5 on one pod, through a port-forward to its admin port (the JRE image has
# no curl).
$K port-forward "pod/$VICTIM_POD" 18080:8080 >/dev/null 2>&1 &
PF_PID=$!
WAIT=$((WARMUP + FAULT_START - ($(date +%s) - START_EPOCH)))
(( WAIT > 0 )) && sleep "$WAIT"
curl -sf "http://127.0.0.1:18080/set?latencyFactor=5" >/dev/null
echo "brownout on $VICTIM_POD ($VICTIM_IP) at $(date +%T)"
sleep "$FAULT_LEN"
curl -sf "http://127.0.0.1:18080/set?latencyFactor=1" >/dev/null
echo "healed at $(date +%T)"
curl -sf "http://127.0.0.1:18080/stats" > "$OUT/victim-stats.json" || true

for p in "${POLICIES[@]}"; do
  $K wait --for=condition=complete "job/lg-${p//_/-}" --timeout=$((WARMUP + DURATION + 120))s
  $K logs "job/lg-${p//_/-}" | grep '^{' > "$OUT/$p-c0.jsonl"
done
for pod in $($K get pods -l app=loadtest-backend -o jsonpath='{.items[*].metadata.name}'); do
  $K get pod "$pod" -o jsonpath='{.status.podIP}{"\n"}'
done > "$OUT/backend-ips.txt"

python3 - "$OUT" "$VICTIM_IP" "$FAULT_START" "$FAULT_END" "$BACKENDS" <<'PY'
import json, subprocess, sys
out, victim, fs, fe, backends = sys.argv[1:6]
def sh(c):
    try: return subprocess.check_output(c, shell=True, text=True).strip()
    except Exception: return None
run = {
    "tier": "kind-smoke",
    "scenario": "brownout",
    "fault": {"start_s": float(fs), "end_s": float(fe), "kind": "latencyFactor=5",
              "pods": [victim + ":9000"]},
    "backends": backends.split(),
    "clients": [{"file": f"{p}-c0.jsonl", "policy": p} for p in
                ["round_robin", "peak_ewma_p2c", "least_request_experimental", "lr_od"]],
    "env": {"commit": sh("git rev-parse --short HEAD"), "kind": sh("kind version"),
            "docker": sh("docker version --format '{{.Server.Version}}'"),
            "node": "kind (Docker Desktop VM, 18 CPU / 8 GB)",
            "backend_resources": "cpu 50m req / 500m limit, 256Mi, -Xmx160m SerialGC, slots=8",
            "image": "eclipse-temurin:25-jre"},
}
json.dump(run, open(f"{out}/run.json", "w"), indent=2)
PY
echo "results: $OUT"
