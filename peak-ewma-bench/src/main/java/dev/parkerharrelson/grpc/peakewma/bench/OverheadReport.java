package dev.parkerharrelson.grpc.peakewma.bench;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

/**
 * Whole-process CPU and memory overhead of each LB policy, plus a routing-quality scenario.
 *
 * <p>Every (policy, fleet size) runs in its own fresh JVM so JIT state, GC history and heap don't
 * leak between configurations. Each child:
 *
 * <ol>
 *   <li>starts N in-process zero-latency backends, GCs and records baseline heap,
 *   <li>builds the channel and warms up, GCs again: <b>retained heap</b> = channel + LB state,
 *   <li>runs a closed-loop unary workload on T threads and records process CPU time (all threads:
 *       caller, channel executor, JIT, GC), bytes allocated by all threads, GC time and latency.
 * </ol>
 *
 * Server-side work is identical across policies, so the deltas vs round_robin are the LB's cost.
 *
 * <pre>
 * java -cp peak-ewma-bench/target/benchmarks.jar dev.parkerharrelson.grpc.peakewma.bench.OverheadReport \
 *     [--seconds 10] [--warmup 5] [--threads 8] [--sizes 3,10,100,500] [--policies a,b] \
 *     [--out target/overhead-report.md] [--skip-quality]
 * </pre>
 */
public final class OverheadReport {

    private OverheadReport() {}

    public static void main(String[] argv) throws Exception {
        Map<String, String> a = parse(argv);
        if (a.containsKey("child")) {
            child(a);
            return;
        }
        if (a.containsKey("quality-child")) {
            qualityChild(a);
            return;
        }
        orchestrate(a);
    }

    // ------------------------------------------------------------------ orchestrator

    private static void orchestrate(Map<String, String> a) throws Exception {
        int seconds = Integer.parseInt(a.getOrDefault("seconds", "10"));
        int warmup = Integer.parseInt(a.getOrDefault("warmup", "5"));
        int threads = Integer.parseInt(a.getOrDefault("threads", "8"));
        List<Integer> sizes =
                Arrays.stream(a.getOrDefault("sizes", "3,10,100,500").split(","))
                        .map(Integer::parseInt)
                        .toList();
        List<String> policies =
                a.containsKey("policies")
                        ? List.of(a.get("policies").split(","))
                        : BenchCluster.POLICIES;
        Path out = Path.of(a.getOrDefault("out", "target/overhead-report.md"));

        StringBuilder md = new StringBuilder();
        md.append("# LB overhead report\n\n")
                .append(
                        String.format(
                                "JVM %s, %d CPUs, %s %s. Closed loop, %d caller threads, %ds warmup"
                                        + " + %ds measured per row, fresh JVM per row, in-process"
                                        + " transport, zero-latency backends.%n%n",
                                System.getProperty("java.version"),
                                Runtime.getRuntime().availableProcessors(),
                                System.getProperty("os.name"),
                                System.getProperty("os.arch"),
                                threads,
                                warmup,
                                seconds));

        for (int n : sizes) {
            List<Map<String, String>> rows = new ArrayList<>();
            for (String p : policies) {
                System.err.printf("[overhead] %s backends=%d ...%n", p, n);
                Map<String, String> r =
                        runChild(
                                List.of(
                                        "--child",
                                        "",
                                        "--policy",
                                        p,
                                        "--backends",
                                        "" + n,
                                        "--seconds",
                                        "" + seconds,
                                        "--warmup",
                                        "" + warmup,
                                        "--threads",
                                        "" + threads));
                r.put("policy", p);
                rows.add(r);
            }
            md.append(overheadTable(n, rows)).append('\n');
            System.out.println(overheadTable(n, rows));
        }

        if (!a.containsKey("skip-quality")) {
            List<Map<String, String>> rows = new ArrayList<>();
            for (String p : policies) {
                System.err.printf("[quality] %s ...%n", p);
                Map<String, String> r =
                        runChild(
                                List.of(
                                        "--quality-child",
                                        "",
                                        "--policy",
                                        p,
                                        "--seconds",
                                        "" + Math.max(seconds, 15)));
                r.put("policy", p);
                rows.add(r);
            }
            md.append(qualityTable(rows));
            System.out.println(qualityTable(rows));
        }

        Files.createDirectories(out.toAbsolutePath().getParent());
        Files.writeString(out, md);
        System.err.println("[overhead] wrote " + out.toAbsolutePath());
    }

    private static Map<String, String> runChild(List<String> args) throws Exception {
        List<String> cmd = new ArrayList<>();
        cmd.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
        cmd.addAll(
                List.of(
                        "-Xms1g",
                        "-Xmx1g",
                        "-XX:+UseG1GC",
                        "-cp",
                        System.getProperty("java.class.path"),
                        OverheadReport.class.getName()));
        cmd.addAll(args);
        Process proc =
                new ProcessBuilder(cmd).redirectError(ProcessBuilder.Redirect.INHERIT).start();
        Map<String, String> result = null;
        try (BufferedReader br = new BufferedReader(new InputStreamReader(proc.getInputStream()))) {
            String line;
            while ((line = br.readLine()) != null) {
                if (line.startsWith("RESULT ")) result = decode(line.substring(7));
            }
        }
        int rc = proc.waitFor();
        if (result == null) {
            result = new LinkedHashMap<>();
            result.put("error", "child exited " + rc);
        }
        return result;
    }

    private static String overheadTable(int n, List<Map<String, String>> rows) {
        Map<String, String> rr =
                rows.stream()
                        .filter(r -> "round_robin".equals(r.get("policy")))
                        .findFirst()
                        .orElse(null);
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("## %d backends%n%n", n));
        sb.append(
                "| policy | RPC/s | CPU µs/RPC | Δ CPU vs RR | alloc B/RPC | Δ alloc vs RR"
                        + " | retained heap (KB) | GC ms/s | p50 µs | p99 µs |\n");
        sb.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n");
        for (Map<String, String> r : rows) {
            if (r.containsKey("error")) {
                sb.append(String.format("| %s | %s |||||||||%n", r.get("policy"), r.get("error")));
                continue;
            }
            double cpu = d(r, "cpuNsPerRpc") / 1000.0;
            double alloc = d(r, "allocBytesPerRpc");
            String dCpu =
                    rr == null || rr.containsKey("error")
                            ? ""
                            : pct(cpu, d(rr, "cpuNsPerRpc") / 1000.0);
            String dAlloc =
                    rr == null || rr.containsKey("error")
                            ? ""
                            : pct(alloc, d(rr, "allocBytesPerRpc"));
            sb.append(
                    String.format(
                            Locale.ROOT,
                            "| %s | %,.0f | %.2f | %s | %,.0f | %s | %,.0f | %.1f | %.0f | %.0f"
                                    + " |%n",
                            r.get("policy"),
                            d(r, "rps"),
                            cpu,
                            dCpu,
                            alloc,
                            dAlloc,
                            d(r, "retainedHeapBytes") / 1024.0,
                            d(r, "gcMsPerSec"),
                            d(r, "p50us"),
                            d(r, "p99us")));
        }
        return sb.toString();
    }

    private static String qualityTable(List<Map<String, String>> rows) {
        StringBuilder sb = new StringBuilder();
        sb.append("## Routing quality: 10 backends @ 2 ms, b0 slow (20 ms), b1 fails instantly\n\n")
                .append(
                        "Closed loop, 16 threads. Lower share for b0/b1 and higher success are"
                                + " better.\n\n")
                .append(
                        "| policy | RPC/s | success % | p50 µs (ok) | p99 µs (ok) | share b0 (slow)"
                                + " | share b1 (failing) | share min..max of healthy |\n")
                .append("|---|---:|---:|---:|---:|---:|---:|---|\n");
        for (Map<String, String> r : rows) {
            if (r.containsKey("error")) {
                sb.append(String.format("| %s | %s |||||||%n", r.get("policy"), r.get("error")));
                continue;
            }
            sb.append(
                    String.format(
                            Locale.ROOT,
                            "| %s | %,.0f | %.1f | %.0f | %.0f | %.1f%% | %.1f%% | %s |%n",
                            r.get("policy"),
                            d(r, "rps"),
                            d(r, "successPct"),
                            d(r, "p50us"),
                            d(r, "p99us"),
                            d(r, "shareSlow"),
                            d(r, "shareFailing"),
                            r.get("healthyShareRange")));
        }
        return sb.toString();
    }

    // ------------------------------------------------------------------ overhead child

    private static void child(Map<String, String> a) throws Exception {
        String policy = a.get("policy");
        int n = Integer.parseInt(a.get("backends"));
        int seconds = Integer.parseInt(a.get("seconds"));
        int warmup = Integer.parseInt(a.get("warmup"));
        int threads = Integer.parseInt(a.get("threads"));

        try (BenchCluster c = new BenchCluster(n, Duration.ZERO)) {
            long heap0 = settledHeap();
            c.connect(policy, false);
            closedLoop(c, threads, warmup * 1000L, null);
            long heap1 = settledHeap();

            var os =
                    (com.sun.management.OperatingSystemMXBean)
                            ManagementFactory.getOperatingSystemMXBean();
            var tmx = (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
            long cpu0 = os.getProcessCpuTime();
            long alloc0 = tmx.getTotalThreadAllocatedBytes();
            long gc0 = gcMillis();
            long t0 = System.nanoTime();
            Histogram h = new Histogram();
            long rpcs = closedLoop(c, threads, seconds * 1000L, h);
            long wall = System.nanoTime() - t0;
            long cpu = os.getProcessCpuTime() - cpu0;
            long alloc = tmx.getTotalThreadAllocatedBytes() - alloc0;
            long gc = gcMillis() - gc0;

            Map<String, Object> r = new LinkedHashMap<>();
            r.put("rpcs", rpcs);
            r.put("rps", rpcs * 1e9 / wall);
            r.put("cpuNsPerRpc", (double) cpu / rpcs);
            r.put("allocBytesPerRpc", (double) alloc / rpcs);
            r.put("retainedHeapBytes", heap1 - heap0);
            r.put("gcMsPerSec", gc * 1e9 / wall);
            r.put("p50us", h.percentileMicros(50));
            r.put("p99us", h.percentileMicros(99));
            System.out.println("RESULT " + encode(r));
        }
        System.exit(0);
    }

    // ------------------------------------------------------------------ quality child

    private static void qualityChild(Map<String, String> a) throws Exception {
        String policy = a.get("policy");
        int seconds = Integer.parseInt(a.get("seconds"));
        try (BenchCluster c = new BenchCluster(10, Duration.ofMillis(2))) {
            c.connect(policy, false);
            closedLoop(c, 16, 5_000, null);
            c.behaviours.get(0).setSlow(Duration.ofMillis(20), 0.2);
            c.behaviours.get(1).setOutage();
            closedLoop(c, 16, 2_000, null); // let policies react
            long[] before = c.servedPerBackend();
            Histogram okLatency = new Histogram();
            LongAdder ok = new LongAdder();
            long t0 = System.nanoTime();
            long rpcs = closedLoop(c, 16, seconds * 1000L, okLatency, ok);
            long wall = System.nanoTime() - t0;
            long[] after = c.servedPerBackend();
            long[] d = new long[after.length];
            long total = 0;
            for (int i = 0; i < d.length; i++) total += (d[i] = after[i] - before[i]);
            double min = Double.MAX_VALUE, max = 0;
            for (int i = 2; i < d.length; i++) {
                double s = 100.0 * d[i] / Math.max(1, total);
                min = Math.min(min, s);
                max = Math.max(max, s);
            }
            Map<String, Object> r = new LinkedHashMap<>();
            r.put("rps", rpcs * 1e9 / wall);
            r.put("successPct", 100.0 * ok.sum() / Math.max(1, rpcs));
            r.put("p50us", okLatency.percentileMicros(50));
            r.put("p99us", okLatency.percentileMicros(99));
            r.put("shareSlow", 100.0 * d[0] / Math.max(1, total));
            r.put("shareFailing", 100.0 * d[1] / Math.max(1, total));
            r.put("healthyShareRange", String.format(Locale.ROOT, "%.1f%%..%.1f%%", min, max));
            System.out.println("RESULT " + encode(r));
        }
        System.exit(0);
    }

    // ------------------------------------------------------------------ workload

    private static long closedLoop(BenchCluster c, int threads, long millis, Histogram h)
            throws InterruptedException {
        return closedLoop(c, threads, millis, h, null);
    }

    /** Each thread issues blocking unary RPCs back to back until the deadline. */
    private static long closedLoop(
            BenchCluster c, int threads, long millis, Histogram okLatency, LongAdder okCount)
            throws InterruptedException {
        LongAdder count = new LongAdder();
        AtomicBoolean stop = new AtomicBoolean();
        CountDownLatch done = new CountDownLatch(threads);
        Histogram[] local = new Histogram[threads];
        for (int t = 0; t < threads; t++) {
            Histogram mine = local[t] = new Histogram();
            Thread th =
                    new Thread(
                            () -> {
                                long n = 0;
                                while (!stop.get()) {
                                    long s = System.nanoTime();
                                    boolean ok = c.call(5_000);
                                    if (ok) {
                                        mine.record(System.nanoTime() - s);
                                        if (okCount != null) okCount.increment();
                                    }
                                    n++;
                                }
                                count.add(n);
                                done.countDown();
                            },
                            "bench-caller-" + t);
            th.setDaemon(true);
            th.start();
        }
        Thread.sleep(millis);
        stop.set(true);
        done.await();
        if (okLatency != null) for (Histogram l : local) okLatency.merge(l);
        return count.sum();
    }

    private static long settledHeap() throws InterruptedException {
        var mem = ManagementFactory.getMemoryMXBean();
        long best = Long.MAX_VALUE;
        for (int i = 0; i < 4; i++) {
            System.gc();
            Thread.sleep(150);
            best = Math.min(best, mem.getHeapMemoryUsage().getUsed());
        }
        return best;
    }

    private static long gcMillis() {
        long t = 0;
        for (GarbageCollectorMXBean g : ManagementFactory.getGarbageCollectorMXBeans()) {
            t += Math.max(0, g.getCollectionTime());
        }
        return t;
    }

    /** Log-linear latency histogram (~2% resolution), single-writer per instance. */
    static final class Histogram {
        private static final double BASE = Math.log(1.02);
        private final long[] buckets = new long[1400];

        void record(long nanos) {
            int i = (int) (Math.log(Math.max(1, nanos)) / BASE);
            buckets[Math.min(buckets.length - 1, i)]++;
        }

        void merge(Histogram o) {
            for (int i = 0; i < buckets.length; i++) buckets[i] += o.buckets[i];
        }

        double percentileMicros(double p) {
            long total = 0;
            for (long b : buckets) total += b;
            if (total == 0) return 0;
            long target = (long) Math.ceil(total * p / 100.0);
            long seen = 0;
            for (int i = 0; i < buckets.length; i++) {
                seen += buckets[i];
                if (seen >= target) return Math.exp(i * BASE) / 1000.0;
            }
            return Math.exp((buckets.length - 1) * BASE) / 1000.0;
        }
    }

    // ------------------------------------------------------------------ tiny helpers

    private static Map<String, String> parse(String[] argv) {
        Map<String, String> m = new LinkedHashMap<>();
        for (int i = 0; i < argv.length; i++) {
            if (!argv[i].startsWith("--")) continue;
            String k = argv[i].substring(2);
            String v = (i + 1 < argv.length && !argv[i + 1].startsWith("--")) ? argv[++i] : "";
            m.put(k, v);
        }
        return m;
    }

    private static String encode(Map<String, Object> m) {
        StringBuilder sb = new StringBuilder();
        m.forEach((k, v) -> sb.append(k).append('=').append(v).append(';'));
        return sb.toString();
    }

    private static Map<String, String> decode(String s) {
        Map<String, String> m = new LinkedHashMap<>();
        for (String kv : s.split(";")) {
            int i = kv.indexOf('=');
            if (i > 0) m.put(kv.substring(0, i), kv.substring(i + 1));
        }
        return m;
    }

    private static double d(Map<String, String> m, String k) {
        return Double.parseDouble(m.getOrDefault(k, "0"));
    }

    private static String pct(double v, double base) {
        if (base <= 0) return "";
        return String.format(Locale.ROOT, "%+.0f%%", 100.0 * (v - base) / base);
    }
}
