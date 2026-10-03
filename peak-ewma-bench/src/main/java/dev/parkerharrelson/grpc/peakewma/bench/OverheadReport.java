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
 *     [--repeats 1] [--out target/overhead-report.md] [--skip-quality]
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
        int repeats = Math.max(1, Integer.parseInt(a.getOrDefault("repeats", "1")));
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
                                        + " + %ds measured per run, fresh JVM per run, in-process"
                                        + " transport, zero-latency backends. %s%n%n",
                                System.getProperty("java.version"),
                                Runtime.getRuntime().availableProcessors(),
                                System.getProperty("os.name"),
                                System.getProperty("os.arch"),
                                threads,
                                warmup,
                                seconds,
                                repeats == 1
                                        ? "Single run per row (no error bars)."
                                        : repeats
                                                + " runs per row, interleaved across policies;"
                                                + " values are mean ± 95% CI. A Δ marked"
                                                + " \"n.s.\" is not significant (Welch t-test,"
                                                + " p≥0.05)."));

        for (int n : sizes) {
            Map<String, List<Map<String, String>>> runs = new LinkedHashMap<>();
            // Interleave: every policy once per repeat, so drift (thermal, background load)
            // affects all policies equally instead of biasing whichever ran last.
            for (int r = 1; r <= repeats; r++) {
                for (String p : policies) {
                    System.err.printf(
                            "[overhead] %s backends=%d run %d/%d ...%n", p, n, r, repeats);
                    runs.computeIfAbsent(p, k -> new ArrayList<>())
                            .add(
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
                                                    "" + threads)));
                }
            }
            String table = overheadTable(n, runs);
            md.append(table).append('\n');
            System.out.println(table);
        }

        if (!a.containsKey("skip-quality")) {
            Map<String, List<Map<String, String>>> runs = new LinkedHashMap<>();
            for (int r = 1; r <= repeats; r++) {
                for (String p : policies) {
                    System.err.printf("[quality] %s run %d/%d ...%n", p, r, repeats);
                    runs.computeIfAbsent(p, k -> new ArrayList<>())
                            .add(
                                    runChild(
                                            List.of(
                                                    "--quality-child",
                                                    "",
                                                    "--policy",
                                                    p,
                                                    "--seconds",
                                                    "" + Math.max(seconds, 15))));
                }
            }
            String table = qualityTable(runs);
            md.append(table);
            System.out.println(table);
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

    private static String overheadTable(int n, Map<String, List<Map<String, String>>> runs) {
        List<Map<String, String>> rr = runs.get("round_robin");
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("## %d backends%n%n", n));
        sb.append(
                "| policy | RPC/s | CPU µs/RPC | Δ CPU vs RR | alloc B/RPC | Δ alloc vs RR"
                        + " | retained heap (KB) | GC ms/s | p50 µs | p99 µs |\n");
        sb.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n");
        for (var e : runs.entrySet()) {
            List<Map<String, String>> r = e.getValue();
            if (r.stream().anyMatch(m -> m.containsKey("error"))) {
                sb.append(String.format("| %s | child failed |||||||||%n", e.getKey()));
                continue;
            }
            Stat cpu = Stat.of(r, "cpuNsPerRpc", 1e-3);
            Stat alloc = Stat.of(r, "allocBytesPerRpc", 1);
            boolean haveRr =
                    rr != null && r != rr && rr.stream().noneMatch(m -> m.containsKey("error"));
            sb.append(
                    String.format(
                            Locale.ROOT,
                            "| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s |%n",
                            e.getKey(),
                            Stat.of(r, "rps", 1).fmt("%,.0f"),
                            cpu.fmt("%.2f"),
                            haveRr ? cpu.delta(Stat.of(rr, "cpuNsPerRpc", 1e-3)) : "baseline",
                            alloc.fmt("%,.0f"),
                            haveRr ? alloc.delta(Stat.of(rr, "allocBytesPerRpc", 1)) : "baseline",
                            Stat.of(r, "retainedHeapBytes", 1.0 / 1024).fmt("%,.0f"),
                            Stat.of(r, "gcMsPerSec", 1).fmt("%.1f"),
                            Stat.of(r, "p50us", 1).fmt("%.0f"),
                            Stat.of(r, "p99us", 1).fmt("%.0f")));
        }
        return sb.toString();
    }

    private static String qualityTable(Map<String, List<Map<String, String>>> runs) {
        StringBuilder sb = new StringBuilder();
        sb.append("## Routing quality: 10 backends @ 2 ms, b0 slow (20 ms), b1 fails instantly\n\n")
                .append(
                        "Closed loop, 16 threads. Lower share for b0/b1 and higher success are"
                                + " better.\n\n")
                .append(
                        "| policy | RPC/s | success % | p50 µs (ok) | p99 µs (ok) | share b0 (slow)"
                                + " % | share b1 (failing) % | share min..max of healthy |\n")
                .append("|---|---:|---:|---:|---:|---:|---:|---|\n");
        for (var e : runs.entrySet()) {
            List<Map<String, String>> r = e.getValue();
            if (r.stream().anyMatch(m -> m.containsKey("error"))) {
                sb.append(String.format("| %s | child failed |||||||%n", e.getKey()));
                continue;
            }
            sb.append(
                    String.format(
                            Locale.ROOT,
                            "| %s | %s | %s | %s | %s | %s | %s | %s |%n",
                            e.getKey(),
                            Stat.of(r, "rps", 1).fmt("%,.0f"),
                            Stat.of(r, "successPct", 1).fmt("%.1f"),
                            Stat.of(r, "p50us", 1).fmt("%.0f"),
                            Stat.of(r, "p99us", 1).fmt("%.0f"),
                            Stat.of(r, "shareSlow", 1).fmt("%.1f"),
                            Stat.of(r, "shareFailing", 1).fmt("%.1f"),
                            r.get(r.size() - 1).get("healthyShareRange")));
        }
        return sb.toString();
    }

    /** Mean, sample SD and 95% CI of one metric across repeated runs. */
    record Stat(double mean, double sd, int n) {
        // Two-sided 97.5% Student t quantiles for df = 1..30.
        private static final double[] T975 = {
            12.706, 4.303, 3.182, 2.776, 2.571, 2.447, 2.365, 2.306, 2.262, 2.228, 2.201, 2.179,
            2.160, 2.145, 2.131, 2.120, 2.110, 2.101, 2.093, 2.086, 2.080, 2.074, 2.069, 2.064,
            2.060, 2.056, 2.052, 2.048, 2.045, 2.042
        };

        static Stat of(List<Map<String, String>> runs, String key, double scale) {
            double[] v = runs.stream().mapToDouble(m -> d(m, key) * scale).toArray();
            double mean = Arrays.stream(v).average().orElse(0);
            double ss = Arrays.stream(v).map(x -> (x - mean) * (x - mean)).sum();
            return new Stat(mean, v.length > 1 ? Math.sqrt(ss / (v.length - 1)) : 0, v.length);
        }

        static double t(int df) {
            return df <= 0 ? Double.NaN : df <= T975.length ? T975[df - 1] : 1.96;
        }

        double halfWidth() {
            return n > 1 ? t(n - 1) * sd / Math.sqrt(n) : Double.NaN;
        }

        String fmt(String f) {
            String m = String.format(Locale.ROOT, f, mean);
            return n > 1 ? m + " ± " + String.format(Locale.ROOT, f, halfWidth()) : m;
        }

        /**
         * Relative difference vs {@code base}, marked n.s. if a Welch t-test can't separate them.
         */
        String delta(Stat base) {
            if (base.mean <= 0) return "";
            String pct =
                    String.format(Locale.ROOT, "%+.0f%%", 100.0 * (mean - base.mean) / base.mean);
            if (n < 2 || base.n < 2) return pct;
            double va = sd * sd / n, vb = base.sd * base.sd / base.n;
            double se = Math.sqrt(va + vb);
            if (se == 0) return pct;
            double df = (va + vb) * (va + vb) / (va * va / (n - 1) + vb * vb / (base.n - 1));
            boolean significant = Math.abs(mean - base.mean) / se > t((int) Math.floor(df));
            return significant ? pct : pct + " (n.s.)";
        }
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
}
