# Running the load test on Kubernetes (#94)

The same `loadtest.jar` runs as backend pods behind a **headless Service** and as loadgen Jobs that
resolve it with `dns:///`, so the LB sees real pod IPs, real DNS and real pod churn.

| File | What it is |
|---|---|
| `../Dockerfile` | `eclipse-temurin:25-jre` + `loadtest.jar`; manifests pick the main class |
| `backend.yaml` | headless Service `loadtest-backend` (9000 gRPC, 8080 admin) + Deployment |
| `loadgen.yaml` | Job template, one per (policy, client); prints its JSONL to stdout when done |
| `kind-smoke.sh` | local kind smoke run (≤ 10 min): 6 pods, 4 policies concurrently, one brownout |

## kind smoke run

```bash
./mvnw -pl peak-ewma-loadtest -am package -DskipTests
peak-ewma-loadtest/k8s/kind-smoke.sh          # RUN_ID=... RPS=300 REPLICAS=6 KEEP_CLUSTER=1 ...
```

The script uses its own kubeconfig (`k8s/.kubeconfig`, gitignored) and never touches
`~/.kube/config` or your current context. Results land in
`peak-ewma-loadtest/results/<RUN_ID>/kind-smoke/` (`<policy>-c0.jsonl` + `run.json`), the same
format as the local runs, so `report.py` reads them. The cluster is deleted at the end.

## Real cluster (small EKS / GKE node pool)

1. Push the image to a registry the cluster can pull from, and set `image:` in both manifests.
2. Use a dedicated namespace and node pool; pin loadgen and backends to different nodes
   (nodeSelector / anti-affinity) so client CPU numbers aren't polluted by backends.
3. Default Tier 2 fleet: `kubectl -n lt scale deploy/loadtest-backend --replicas=20`, then shape
   it through each pod's admin port (`/set?...`, see `Backend`): 3 noisy (`latencyFactor=2&sigma=0.8`),
   1 GC-pausing (`gcPeriodMillis=10000&gcPauseMillis=500`), 1 flaky (`errorRate=0.05`),
   1 throttled (`slots=4`). From inside the cluster: `kubectl run curl --image=curlimages/curl`,
   or `kubectl port-forward pod/<pod> 18080:8080` from a laptop.
4. Start clients split across policies (`round_robin`, `peak_ewma_p2c`,
   `least_request_experimental`, `lr_od`, and `control`), N Jobs per policy for the herd test, by
   substituting the placeholders in `loadgen.yaml` (see `kind-smoke.sh`). Set `__NS__` to your
   namespace.
5. Faults: brownout / crash-loop / black hole through `/set` (`latencyFactor=5`,
   `mode=unavailable`, `mode=blackhole`); rolling restart with
   `kubectl rollout restart deploy/loadtest-backend`; scale-up / down with `kubectl scale`;
   GOAWAY with `--max-connection-age-ms 10000` on the backend command; node contention by
   cordoning + scheduling a CPU burner on one node.
6. Collect: `kubectl logs job/<name> | grep '^{' > results/<RUN_ID>/<scenario>/<policy>-c<i>.jsonl`,
   write a `run.json` (fault window in seconds after measurement start, faulty pod IPs), and run
   `report.py`.
7. Record node type, pod resources and the commit in `run.json` `env`.

Note: with `dns:///`, grpc-java re-resolves only when a connection fails or a policy asks for it
(peak_ewma_p2c does on TRANSIENT_FAILURE / IDLE), so a scale-up is picked up on the next
re-resolution, not instantly; the local runner's `file:///` resolver polls every 500 ms instead.
