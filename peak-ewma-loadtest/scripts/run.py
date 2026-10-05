#!/usr/bin/env python3
"""Scenario runner for the #94 load test, local mode.

Every simulated pod is its own JVM (its own process, heap, GC and CPU accounting) talking real
gRPC over loopback TCP; client pods are separate loadgen JVMs, one LB policy each, all running
concurrently against the same backends. Pod churn is a rewrite of the address file the clients
resolve (`file:///...`), which is what a headless Service's DNS answer does.

    run.py --suite quick --out peak-ewma-loadtest/results/<id>   # ~35 min: the discriminating subset
    run.py --suite all   --out peak-ewma-loadtest/results/<id>   # ~80 min at 2 repeats: everything
    run.py --suite tier1|tier2|smoke ...  [--repeats N] [--only s03_brownout,...]

Raw output (one JSONL per client and run, plus run.json / servers.jsonl per Tier 2 run) is what
report.py consumes. See ../README.md for the layout.
"""
import argparse
import json
import math
import os
import platform
import signal
import subprocess
import sys
import threading
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
JAR = ROOT / "peak-ewma-loadtest" / "target" / "loadtest.jar"
PKG = "dev.parkerharrelson.grpc.peakewma.loadtest"

POLICIES = ["round_robin", "peak_ewma_p2c", "least_request_experimental", "lr_od"]
TIER1_POLICIES = ["control"] + POLICIES

# ---------------------------------------------------------------------------------------------
# processes


class Ports:
    """Never reuse a port within a run: a restarted pod gets a new address, like a new pod IP."""

    def __init__(self, start=21000):
        self.next = start
        self.lock = threading.Lock()

    def take(self, n=1):
        with self.lock:
            out = list(range(self.next, self.next + n))
            self.next += n
            if self.next > 29000:
                self.next = 21000
            return out


PORTS = Ports()
LIVE = set()  # every process we started, killed on exit


def java(main, args, log, jvm):
    cmd = ["java", *jvm, "-cp", str(JAR), f"{PKG}.{main}", *[str(a) for a in args]]
    f = open(log, "w")
    p = subprocess.Popen(cmd, stdout=f, stderr=subprocess.STDOUT, start_new_session=True)
    p._log = f
    LIVE.add(p)
    return p


def stop(p, grace=6.0):
    if p.poll() is None:
        p.send_signal(signal.SIGTERM)
        try:
            p.wait(grace)
        except subprocess.TimeoutExpired:
            p.kill()
            p.wait()
    p._log.close()
    LIVE.discard(p)


def kill_all():
    for p in list(LIVE):
        try:
            p.kill()
        except Exception:
            pass


def http(admin, path, timeout=2.0):
    with urllib.request.urlopen(f"http://127.0.0.1:{admin}{path}", timeout=timeout) as r:
        return r.read().decode()


def wait_healthy(admins, timeout=30.0):
    end = time.time() + timeout
    pending = set(admins)
    while pending and time.time() < end:
        for a in list(pending):
            try:
                if http(a, "/health", 0.5) == "ok":
                    pending.discard(a)
            except Exception:
                pass
        if pending:
            time.sleep(0.1)
    if pending:
        raise RuntimeError(f"backends not healthy: {sorted(pending)}")


def write_addrs(path, addrs):
    tmp = Path(f"{path}.{threading.get_ident()}.tmp")
    tmp.write_text("".join(f"{a}\n" for a in addrs))
    os.replace(tmp, path)


def set_behaviour(admin, port, **kv):
    q = "&".join(f"{k}={v}" for k, v in kv.items())
    return http(admin, f"/set?ports={port}&{q}")


# ---------------------------------------------------------------------------------------------
# fleet


ROLES = {
    # role: behaviour overrides (on top of slots=8, sigma=0.3)
    "normal": {},
    "noisy": {"latencyFactor": 2.0, "sigma": 0.8},
    "gc": {"gcPeriodMillis": 10000, "gcPauseMillis": 500},
    "flaky": {"errorRate": 0.05},
    "throttled": {"latencyFactor": 1.5, "slots": 4},
}
SLOTS = 8


def het_roles(n):
    """The issue's default fleet at 20 (14 normal, 3 noisy, 1 GC, 1 flaky, 1 throttled), scaled."""
    if n < 10:
        bad = ["noisy", "flaky"]
    else:
        k = n / 20
        bad = (
            ["noisy"] * max(1, round(3 * k))
            + ["gc"] * max(1, round(k))
            + ["flaky"] * max(1, round(k))
            + ["throttled"] * max(1, round(k))
        )
    return bad + ["normal"] * (n - len(bad))


def role_capacity(role, mean_ms):
    b = {"latencyFactor": 1.0, "sigma": 0.3, "slots": SLOTS, **ROLES[role]}
    mean = mean_ms * b["latencyFactor"] * math.exp(b["sigma"] ** 2 / 2)
    cap = b["slots"] / (mean / 1000.0)
    if role == "gc":
        cap *= 1 - 500 / 10000
    if role == "flaky":
        cap /= 1 - 0.05  # failed calls cost nothing
    return cap


class Fleet:
    def __init__(self, run_dir, max_age_ms=0):
        self.run_dir = run_dir
        self.addrs_file = run_dir / "addrs.txt"
        self.pods = {}  # addr -> dict(proc, admin, port, role)
        self.max_age_ms = max_age_ms
        self.lock = threading.Lock()

    def start(self, roles, wait=True):
        started = []
        for role in roles:
            port, = PORTS.take()
            admin = port + 10000
            sets = {"slots": SLOTS, "sigma": 0.3, **ROLES[role]}
            args = ["--ports", port, "--admin-port", admin, "--scheduler-threads", 2]
            for k, v in sets.items():
                args += ["--set", f"{k}={v}"]
            if self.max_age_ms:
                args += ["--max-connection-age-ms", self.max_age_ms]
            jvm = ["-Xms64m", "-Xmx128m", "-XX:+UseSerialGC", "-XX:ActiveProcessorCount=2",
                   "-XX:TieredStopAtLevel=1", "-Xss512k"]
            p = java("Backend", args, self.run_dir / "logs" / f"backend-{port}.log", jvm)
            addr = f"127.0.0.1:{port}"
            with self.lock:
                self.pods[addr] = {"proc": p, "admin": admin, "port": port, "role": role}
            started.append(addr)
        if wait:
            wait_healthy([self.pods[a]["admin"] for a in started])
        return started

    def publish(self):
        # Under the lock: concurrent retirements must not publish stale lists out of order.
        with self.lock:
            live = [a for a, p in self.pods.items() if not p.get("gone")]
            write_addrs(self.addrs_file, live)

    def retire(self, addr, after_s=1.0):
        """Drop from resolution, then SIGTERM (graceful GOAWAY) once clients have re-resolved."""
        with self.lock:
            self.pods[addr]["gone"] = True
        self.publish()
        time.sleep(after_s)
        stop(self.pods[addr]["proc"])

    def stop_all(self):
        ps = [p["proc"] for p in self.pods.values()]
        for p in ps:
            if p.poll() is None:
                p.send_signal(signal.SIGTERM)
        for p in ps:
            stop(p, grace=4)

    def poll_stats(self):
        out = {}
        with self.lock:
            items = [(a, p) for a, p in self.pods.items() if not p.get("gone")]
        for addr, p in items:
            try:
                s = json.loads(http(p["admin"], "/stats", 0.5))
                port = s["ports"][str(p["port"])]
                out[addr] = {
                    "calls": port["calls"], "ok": port["ok"], "errors": port["errors"],
                    "busy_ns": port["busy_ns"], "cpu_ns": s["cpu_ns"], "queued": port["queued"],
                    "role": p["role"],
                }
            except Exception:
                pass
        return out


# ---------------------------------------------------------------------------------------------
# Tier 2


T2_JVM = ["-Xms256m", "-Xmx384m", "-XX:+UseG1GC", "-XX:ActiveProcessorCount=2",
          "-Dpeakewma.sampledTimers=true"]

SINGLE = {"methods": "U10_get", "mean_ms": 10.0}
MULTI = {
    # 5 methods with different latencies and rates
    "methods": "U1_ping:40,U10_get:30,U50_list:20,U200_search:8,U1000_report:2",
    "mean_ms": 0.40 * 1 + 0.30 * 10 + 0.20 * 50 + 0.08 * 200 + 0.02 * 1000,
}


def tier2_scenarios():
    """name -> spec. Faults hit pod 0 (or pods 0..3 for node contention) from t=20 s to 60 s."""
    F = (20, 60)
    return {
        "s01_steady_het": {"fleet": "het"},
        "s02_steady_homo": {"fleet": "homo"},
        "s03_brownout": {"fleet": "homo", "fault": F, "pods": [0], "set": {"latencyFactor": 5}},
        "s04_crashloop": {"fleet": "homo", "fault": F, "pods": [0], "set": {"mode": "unavailable"}},
        "s05_blackhole": {"fleet": "homo", "fault": F, "pods": [0], "set": {"mode": "blackhole"}},
        "s06_gc_pause": {"fleet": "homo", "fault": F, "pods": [0],
                         "set": {"gcPeriodMillis": 10000, "gcPauseMillis": 500}},
        "s07_rolling_restart": {"fleet": "homo", "fault": F, "action": "rolling"},
        "s08_scale_up_down": {"fleet": "homo", "fault": (20, 55), "action": "scale"},
        "s09_max_connection_age": {"fleet": "homo", "max_age_ms": 10000},
        "s10_node_cpu_contention": {"fleet": "homo", "fault": F, "pods": [0, 1, 2, 3],
                                    "set": {"latencyFactor": 2, "slots": 4}},
        "s11_client_restart": {"fleet": "het", "restart_at": 30},
        "s12_cross_zone": {"fleet": "homo", "zone": True},
        "s13_steady_het_5_methods": {"fleet": "het", "workload": "multi"},
    }


QUICK_SCENARIOS = ["s01_steady_het", "s02_steady_homo", "s03_brownout", "s04_crashloop",
                   "s05_blackhole", "s10_node_cpu_contention", "s11_client_restart"]
QUICK_CELLS = ["centre", "backends_50", "backends_500", "corner_big"]


def heal_values(spec):
    base = {"latencyFactor": 1.0, "slots": SLOTS, "mode": "normal", "gcPeriodMillis": 0,
            "gcPauseMillis": 0}
    return {k: base[k] for k in spec["set"]}


def run_tier2(out, name, spec, repeat, n_backends=20, load=0.70, clients_per_policy=2,
              policies=POLICIES, warmup=20, duration=90, tag=None):
    run_dir = out / "tier2" / (tag or name) / f"r{repeat}"
    if (run_dir / "run.json").exists():
        print(f"  skip {run_dir} (done)")
        return
    (run_dir / "logs").mkdir(parents=True, exist_ok=True)
    workload = MULTI if spec.get("workload") == "multi" else SINGLE
    roles = het_roles(n_backends) if spec["fleet"] == "het" else ["normal"] * n_backends
    fleet = Fleet(run_dir, spec.get("max_age_ms", 0))
    events = []
    try:
        addrs = fleet.start(roles)
        if spec.get("zone"):
            # a third of the fleet in another zone: +2 ms RTT
            for a in addrs[: n_backends // 3]:
                p = fleet.pods[a]
                set_behaviour(p["admin"], p["port"], extraDelayMillis=2)
                p["role"] = p["role"] + "+zone"
        fleet.publish()
        capacity = sum(role_capacity(r, workload["mean_ms"]) for r in roles)
        total_rps = load * capacity
        n_clients = clients_per_policy * len(policies)
        rps_each = max(10, int(total_rps / n_clients))

        fs, fe = spec.get("fault", (None, None))
        phases = []
        if fs is not None:
            phases = [f"before:0:{fs}", f"fault:{fs}:{fe}", f"after:{fe}:{duration}"]
        clients = []
        for pol in policies:
            for c in range(clients_per_policy):
                cid = f"{pol}-c{c}"
                args = ["--target", f"file://{fleet.addrs_file}", "--policy", pol,
                        "--rps", rps_each, "--threads", 2, "--methods", workload["methods"],
                        "--warmup-s", warmup, "--duration-s", duration, "--client-id", cid,
                        "--out", run_dir / f"{cid}.jsonl", "--costs", "true"]
                for ph in phases:
                    args += ["--phase", ph]
                if spec.get("restart_at") is not None:
                    args += ["--restart-at-s", spec["restart_at"]]
                clients.append({"client": cid, "policy": pol, "file": f"{cid}.jsonl",
                                "proc": java("LoadGen", args, run_dir / "logs" / f"{cid}.log",
                                             T2_JVM)})
        # Measurement t=0 for each client is its own start + warmup; align on the latest.
        t0 = None
        deadline = time.time() + 30
        while time.time() < deadline:
            starts = []
            for c in clients:
                f = run_dir / c["file"]
                if f.exists() and f.stat().st_size > 0:
                    try:
                        starts.append(json.loads(f.open().readline())["start_epoch_ms"])
                    except Exception:
                        pass
            if len(starts) == len(clients):
                t0 = max(starts) / 1000.0 + warmup
                break
            time.sleep(0.2)
        if t0 is None:
            raise RuntimeError("clients did not start")

        stop_poll = threading.Event()

        def poller():
            with open(run_dir / "servers.jsonl", "w") as f:
                while not stop_poll.is_set():
                    f.write(json.dumps({"epoch_ms": int(time.time() * 1000),
                                        "pods": fleet.poll_stats()}) + "\n")
                    f.flush()
                    stop_poll.wait(1.0)

        th = threading.Thread(target=poller, daemon=True)
        th.start()

        def at(t_rel):
            d = t0 + t_rel - time.time()
            if d > 0:
                time.sleep(d)

        fault_pods = []
        if fs is not None and "set" in spec:
            fault_pods = [addrs[i] for i in spec["pods"]]
            at(fs)
            for a in fault_pods:
                p = fleet.pods[a]
                set_behaviour(p["admin"], p["port"], **spec["set"])
            events.append({"epoch_ms": int(time.time() * 1000), "what": "fault start"})
            at(fe)
            for a in fault_pods:
                p = fleet.pods[a]
                set_behaviour(p["admin"], p["port"], **heal_values(spec))
            events.append({"epoch_ms": int(time.time() * 1000), "what": "heal"})
        elif spec.get("action") == "rolling":
            at(fs)
            step = (fe - fs) / len(addrs)
            for i, old in enumerate(addrs):
                at(fs + i * step)
                new, = fleet.start(["normal"])
                fleet.publish()
                events.append({"epoch_ms": int(time.time() * 1000), "what": f"add {new}"})
                threading.Thread(target=fleet.retire, args=(old,), daemon=True).start()
                events.append({"epoch_ms": int(time.time() * 1000), "what": f"remove {old}"})
        elif spec.get("action") == "scale":
            at(fs)
            new = fleet.start(["normal"] * 5)
            fleet.publish()
            fault_pods = new
            events.append({"epoch_ms": int(time.time() * 1000), "what": f"scale up +5 {new}"})
            at(fe)
            for old in addrs[:5]:
                threading.Thread(target=fleet.retire, args=(old,), daemon=True).start()
            events.append({"epoch_ms": int(time.time() * 1000),
                           "what": f"scale down -5 {addrs[:5]}"})

        for c in clients:
            try:
                c["proc"].wait(timeout=max(5, t0 + duration + 40 - time.time()))
            except subprocess.TimeoutExpired:
                c["proc"].kill()
            c["exit"] = c["proc"].returncode
            stop(c["proc"])
        stop_poll.set()
        th.join(3)

        info = {
            "tier": 2, "scenario": name, "tag": tag or name, "repeat": repeat,
            "backends_n": n_backends, "load": load, "capacity_rps": capacity,
            "total_rps": rps_each * n_clients, "rps_per_client": rps_each,
            "clients_per_policy": clients_per_policy, "policies": policies,
            "workload": workload["methods"], "warmup_s": warmup, "duration_s": duration,
            "measure_start_epoch_ms": int(t0 * 1000),
            "fleet": {a: p["role"] for a, p in fleet.pods.items()},
            "initial_backends": addrs,
            "fault": None if fs is None else {
                "start_s": fs, "end_s": fe, "pods": fault_pods,
                "kind": spec.get("action") or ",".join(f"{k}={v}" for k, v in
                                                       spec.get("set", {}).items())},
            "restart_at_s": spec.get("restart_at"),
            "max_connection_age_ms": spec.get("max_age_ms", 0),
            "events": events,
            "clients": [{k: c[k] for k in ("client", "policy", "file", "exit")} for c in clients],
        }
        (run_dir / "run.json").write_text(json.dumps(info, indent=1))
        bad = [c["client"] for c in clients if c["exit"] != 0]
        print(f"  done {run_dir.relative_to(out)}  rps/client={rps_each}"
              + (f"  FAILED clients: {bad}" if bad else ""), flush=True)
    finally:
        fleet.stop_all()


# ---------------------------------------------------------------------------------------------
# Tier 1


T1_JVM = ["-Xms1g", "-Xmx1g", "-XX:+UseG1GC", "-XX:ActiveProcessorCount=4",
          "-Dpeakewma.sampledTimers=true"]
CENTRE = {"backends": 10, "methods": 10, "rps": 10000, "threads": 8, "mix": "unary",
          "payload": 100}


def tier1_cells():
    cells = {"centre": {}}
    for b in (3, 50, 200, 500):
        cells[f"backends_{b}"] = {"backends": b}
    for m in (1, 100):
        cells[f"methods_{m}"] = {"methods": m}
    cells["methods_1000_churn"] = {"methods": "churn"}
    for r in (1000, 50000):
        cells[f"rps_{r // 1000}k"] = {"rps": r}
    cells["rps_saturation"] = {"rps": 0}
    for t in (1, 32):
        cells[f"threads_{t}"] = {"threads": t}
    cells["mix_unary80_stream20"] = {"mix": "stream"}
    cells["mix_watch_plus_unary"] = {"mix": "watch"}
    cells["payload_16k"] = {"payload": 16384}
    cells["corner_big"] = {"backends": 500, "methods": 100, "rps": 50000}
    cells["corner_small"] = {"backends": 3, "methods": 1, "rps": 1000}
    return {k: {**CENTRE, **v} for k, v in cells.items()}


def loadgen_args_t1(cell, addrs_file, ctl_file, policy, out_file, warmup, duration):
    args = ["--target", f"file://{addrs_file}", "--control-target", f"file://{ctl_file}",
            "--policy", policy, "--rps", cell["rps"], "--threads", cell["threads"],
            "--payload", cell["payload"], "--warmup-s", warmup, "--duration-s", duration,
            "--method-latency-ms", 0, "--out", out_file, "--client-id", policy]
    if cell["methods"] == "churn":
        args += ["--churn", 1000, "--churn-per-s", 10]
    else:
        args += ["--method-count", cell["methods"]]
    if cell["mix"] == "stream":
        args += ["--stream-fraction", 0.2]
    if cell["mix"] == "watch":
        args += ["--watches", 50]
    if cell["rps"] == 0:
        args += ["--outstanding", 32]
    return args


def run_tier1(out, repeats, warmup, duration, only=None):
    cells = tier1_cells()
    full = {"centre", "corner_big", "corner_small"}
    for cname, cell in cells.items():
        if only and cname not in only:
            continue
        cdir = out / "tier1" / cname
        (cdir / "logs").mkdir(parents=True, exist_ok=True)
        reps = repeats if cname in full else min(repeats, max(2, repeats - 1))
        todo = [(r, p) for r in range(reps) for p in rotate(TIER1_POLICIES, r)
                if not (cdir / f"{p}-r{r}.jsonl").exists() or not is_complete(cdir / f"{p}-r{r}.jsonl")]
        if not todo:
            print(f"  skip tier1/{cname} (done)")
            continue
        ports = PORTS.take(cell["backends"])
        admin = ports[0] + 10000
        backend = java("Backend", ["--ports", ",".join(map(str, ports)), "--admin-port", admin,
                                   "--scheduler-threads", 2],
                       cdir / "logs" / "backend.log",
                       ["-Xms1g", "-Xmx2g", "-XX:+UseParallelGC", "-Xss512k"])
        try:
            wait_healthy([admin], timeout=60)
            addrs_file, ctl_file = cdir / "addrs.txt", cdir / "control.txt"
            write_addrs(addrs_file, [f"127.0.0.1:{p}" for p in ports])
            write_addrs(ctl_file, [f"127.0.0.1:{ports[0]}"])
            (cdir / "cell.json").write_text(json.dumps({"cell": cname, **cell, "repeats": reps,
                                                        "warmup_s": warmup,
                                                        "duration_s": duration}, indent=1))
            for r, pol in todo:
                f = cdir / f"{pol}-r{r}.jsonl"
                p = java("LoadGen", loadgen_args_t1(cell, addrs_file, ctl_file, pol, f, warmup,
                                                    duration),
                         cdir / "logs" / f"{pol}-r{r}.log", T1_JVM)
                try:
                    p.wait(timeout=warmup + duration + 60)
                except subprocess.TimeoutExpired:
                    p.kill()
                rc = p.returncode
                stop(p)
                s = summary(f)
                print(f"  tier1/{cname} {pol} r{r}: exit={rc} "
                      + (f"rps={s['rps']:.0f} cpu_us/rpc={s['cpu_us_per_rpc']:.2f} "
                         f"B/rpc={s['alloc_b_per_rpc']:.0f}" if s else "NO SUMMARY"), flush=True)
        finally:
            stop(backend)


def run_tier1_extras(out, warmup):
    """Heap under 1,000 churning methods (must plateau), and JFR profiles for flame graphs."""
    cells = tier1_cells()
    edir = out / "tier1_extra"
    (edir / "logs").mkdir(parents=True, exist_ok=True)
    jobs = [("memory_churn", cells["methods_1000_churn"], "peak_ewma_p2c", 240,
             ["--heap-gc-every-s", 10]),
            ("memory_churn", cells["methods_1000_churn"], "round_robin", 60,
             ["--heap-gc-every-s", 10])]
    for c in ("centre", "corner_big", "corner_small"):
        for pol in ("peak_ewma_p2c", "round_robin"):
            jobs.append((f"jfr_{c}", cells[c], pol, 15, ["--jfr", edir / f"jfr_{c}-{pol}.jfr",
                                                         "--jfr-settings", "loadtest"]))
    for name, cell, pol, dur, extra in jobs:
        f = edir / f"{name}-{pol}.jsonl"
        if f.exists() and is_complete(f):
            continue
        ports = PORTS.take(cell["backends"])
        admin = ports[0] + 10000
        backend = java("Backend", ["--ports", ",".join(map(str, ports)), "--admin-port", admin],
                       edir / "logs" / f"backend-{name}-{pol}.log",
                       ["-Xms1g", "-Xmx2g", "-XX:+UseParallelGC", "-Xss512k"])
        try:
            wait_healthy([admin], timeout=60)
            af, cf = edir / f"addrs-{name}.txt", edir / f"control-{name}.txt"
            write_addrs(af, [f"127.0.0.1:{p}" for p in ports])
            write_addrs(cf, [f"127.0.0.1:{ports[0]}"])
            p = java("LoadGen", loadgen_args_t1(cell, af, cf, pol, f, warmup, dur) + extra,
                     edir / "logs" / f"{name}-{pol}.log", T1_JVM)
            p.wait(timeout=warmup + dur + 90)
            stop(p)
            print(f"  tier1_extra/{name} {pol}: exit={p.returncode}", flush=True)
        finally:
            stop(backend)


# ---------------------------------------------------------------------------------------------


def rotate(xs, k):
    k %= len(xs)
    return xs[k:] + xs[:k]


def summary(f):
    try:
        last = f.read_text().strip().splitlines()[-1]
        d = json.loads(last)
        return d if d.get("type") == "summary" else None
    except Exception:
        return None


def is_complete(f):
    return summary(f) is not None


def manifest(out, args):
    def sh(*c):
        try:
            return subprocess.check_output(c, cwd=ROOT, text=True, stderr=subprocess.DEVNULL).strip()
        except Exception:
            return None

    m = {
        "commit": sh("git", "rev-parse", "--short", "HEAD"),
        "dirty": bool(sh("git", "status", "--porcelain")),
        "branch": sh("git", "rev-parse", "--abbrev-ref", "HEAD"),
        "jdk": sh("java", "-version") or subprocess.run(["java", "-version"], capture_output=True,
                                                         text=True).stderr.splitlines()[0],
        "grpc": "1.78.0",
        "os": f"{platform.system()} {platform.release()} {platform.machine()}",
        "cpus": os.cpu_count(),
        "mem_gb": round(int(sh("sysctl", "-n", "hw.memsize") or 0) / 2**30, 1),
        "mode": "local multi-process (one JVM per pod, loopback TCP, file:/// re-resolution)",
        "backend_jvm": "-Xmx128m SerialGC, 2 cpus, C1 (TieredStopAtLevel=1)",
        "tier1_client_jvm": " ".join(T1_JVM),
        "tier2_client_jvm": " ".join(T2_JVM),
        "backend_slots": SLOTS,
        "roles": ROLES,
        "lr_od_config": "outlier_detection_experimental: interval 10s, baseEjectionTime 30s, "
                        "maxEjectionPercent 20, successRate (stdevFactor 1900, minimumHosts 5, "
                        "requestVolume 100), failurePercentage (threshold 50, minimumHosts 5, "
                        "requestVolume 50); child least_request_experimental choiceCount 2",
        "args": vars(args),
    }
    p = out / "manifest.json"
    old = json.loads(p.read_text()) if p.exists() else {}
    m["started"] = old.get("started", time.strftime("%Y-%m-%dT%H:%M:%S%z"))
    m["updated"] = time.strftime("%Y-%m-%dT%H:%M:%S%z")
    p.write_text(json.dumps(m, indent=1))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--suite", default="all",
                    choices=["quick", "all", "tier1", "tier2", "tier2-extra", "tier1-extra", "smoke"])
    ap.add_argument("--out", required=True)
    ap.add_argument("--repeats", type=int, default=2)
    ap.add_argument("--only", default=None, help="comma list of scenario / cell names")
    ap.add_argument("--t2-warmup", type=int, default=20)
    ap.add_argument("--t2-duration", type=int, default=90)
    ap.add_argument("--t1-warmup", type=int, default=4)
    ap.add_argument("--t1-duration", type=int, default=6)
    args = ap.parse_args()
    out = Path(args.out).resolve()
    out.mkdir(parents=True, exist_ok=True)
    only = set(args.only.split(",")) if args.only else None
    if not JAR.exists():
        sys.exit(f"build first: {JAR} missing")
    manifest(out, args)
    signal.signal(signal.SIGTERM, lambda *_: (kill_all(), sys.exit(1)))
    t_start = time.time()
    try:
        if args.suite == "quick":
            # The scenarios and cells that separate the policies (~35 min at 2 repeats); the rest
            # of the matrix tied in the first full run (results/cluster-2026-10-04).
            sc = tier2_scenarios()
            for r in range(args.repeats):
                for n in QUICK_SCENARIOS:
                    print(f"[{(time.time() - t_start) / 60:5.1f} min] tier2 {n} r{r}", flush=True)
                    run_tier2(out, n, sc[n], r, warmup=args.t2_warmup, duration=args.t2_duration)
            print(f"[{(time.time() - t_start) / 60:5.1f} min] tier1", flush=True)
            run_tier1(out, args.repeats, args.t1_warmup, args.t1_duration, only=set(QUICK_CELLS))
            return
        if args.suite == "smoke":
            sc = tier2_scenarios()
            for n in ("s01_steady_het", "s03_brownout", "s07_rolling_restart"):
                run_tier2(out, n, sc[n], 0, warmup=8, duration=40 if n != "s03_brownout" else 70)
            run_tier1(out, 1, 3, 4, only={"centre", "methods_1000_churn"})
            return
        if args.suite in ("all", "tier2"):
            sc = tier2_scenarios()
            for r in range(args.repeats):
                for n, spec in sc.items():
                    if only and n not in only:
                        continue
                    print(f"[{(time.time() - t_start) / 60:5.1f} min] tier2 {n} r{r}", flush=True)
                    run_tier2(out, n, spec, r, warmup=args.t2_warmup, duration=args.t2_duration)
        if args.suite in ("all", "tier2", "tier2-extra") and not only:
            het = tier2_scenarios()["s01_steady_het"]
            # Sensitivity: fleet load x backend count (steady heterogeneous fleet)
            for n_b in (5, 20, 50):
                for load in (0.3, 0.7, 0.9):
                    tag = f"sens_b{n_b}_load{int(load * 100)}"
                    print(f"[{(time.time() - t_start) / 60:5.1f} min] tier2 {tag}", flush=True)
                    run_tier2(out, "s01_steady_het", het, 0, n_backends=n_b, load=load,
                              clients_per_policy=1, warmup=15, duration=45, tag=tag)
            # Herd: many peak clients on one fleet (and round_robin for reference)
            for pol, n_c in (("peak_ewma_p2c", 1), ("peak_ewma_p2c", 4), ("peak_ewma_p2c", 16),
                             ("round_robin", 16)):
                tag = f"herd_{pol}_{n_c}"
                print(f"[{(time.time() - t_start) / 60:5.1f} min] tier2 {tag}", flush=True)
                run_tier2(out, "s01_steady_het", het, 0, clients_per_policy=n_c,
                          policies=[pol], warmup=15, duration=45, tag=tag)
        if args.suite in ("all", "tier1"):
            print(f"[{(time.time() - t_start) / 60:5.1f} min] tier1", flush=True)
            run_tier1(out, args.repeats, args.t1_warmup, args.t1_duration, only)
        if args.suite in ("all", "tier1", "tier1-extra") and not only:
            print(f"[{(time.time() - t_start) / 60:5.1f} min] tier1 extras", flush=True)
            run_tier1_extras(out, args.t1_warmup)
    finally:
        kill_all()
        manifest(out, args)
        print(f"finished in {(time.time() - t_start) / 60:.1f} min", flush=True)


if __name__ == "__main__":
    main()
