package dev.parkerharrelson.grpc.peakewma.loadtest;

import com.sun.management.GarbageCollectionNotificationInfo;
import dev.parkerharrelson.grpc.peakewma.PeakEwmaP2CProvider;
import io.grpc.Attributes;
import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.ClientStreamTracer;
import io.grpc.Grpc;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.io.BufferedWriter;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;
import javax.management.NotificationEmitter;
import javax.management.openmbean.CompositeData;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;

/**
 * Load generator: one client "pod" running one LB policy against a target.
 *
 * <p>Open-loop at a fixed rate (latency measured from each call's <i>intended</i> start, so a
 * stalled client or channel can't hide queueing: no coordinated omission), or closed-loop with a
 * fixed number of outstanding calls per thread ({@code --rps 0}) to find the saturation rate.
 *
 * <p>Writes one JSON line per interval (latency per method, status counts, calls per backend, CPU,
 * allocation, GC, LB internals) and a summary line over the measurement window, so per-RPC cost is
 * computed here rather than from scrape timing. See the module README for every flag and field.
 */
public final class LoadGen {

    /** Calls of one stats group (a method, or all of a kind when there are many). */
    private static final class Group {
        final String key;
        final Recorder latencyMicros = new Recorder(60_000_000L, 3);
        final ConcurrentHashMap<String, LongAdder> status = new ConcurrentHashMap<>();
        final Histogram total = new Histogram(60_000_000L, 3);
        final Map<String, Histogram> byPhase = new LinkedHashMap<>();

        Group(String key) {
            this.key = key;
        }

        void record(Status.Code code, long micros) {
            String c = code.name();
            LongAdder a = status.get(c);
            (a != null ? a : status.computeIfAbsent(c, k -> new LongAdder())).increment();
            if (code == Status.Code.OK) latencyMicros.recordValue(Math.max(1, micros));
        }
    }

    private record MethodSpec(
            String bare,
            MethodDescriptor<byte[], byte[]> descriptor,
            long deadlineMillis,
            Group group,
            double cumulativeWeight) {}

    private record Phase(String name, double startS, double endS) {}

    // ---- configuration ----
    private final String target;
    private final String policy;
    private final int rps;
    private final int threads;
    private final int outstandingPerThread;
    private final double warmupS;
    private final double durationS;
    private final double intervalS;
    private final byte[] payload;
    private final List<MethodSpec> methods = new ArrayList<>();
    private final Map<String, Group> groups = new LinkedHashMap<>();
    private final int churnActive;
    private final double churnPerSec;
    private final MethodSpec churnTemplate;
    private final int watches;
    private final double restartAtS;
    private final double heapGcEveryS;
    private final List<Phase> phases = new ArrayList<>();
    private final String jfrOut;
    private final String jfrSettings;
    private final boolean costs;
    private final Args args;

    // ---- state ----
    private volatile ManagedChannel channel;
    private final RecordingLbMetrics lbMetrics;
    private final ConcurrentHashMap<String, LongAdder> perBackend = new ConcurrentHashMap<>();
    private final LongAdder completed = new LongAdder();
    private final AtomicInteger inflight = new AtomicInteger();
    private final AtomicLong maxLagNanos = new AtomicLong();
    private final LongAdder watchMessages = new LongAdder();
    private final LongAdder watchRestarts = new LongAdder();
    private final Recorder gcPauseMicros = new Recorder(3);
    private final Histogram gcPauseTotal = new Histogram(3);
    private volatile boolean running = true;
    private long startNanos;

    private LoadGen(Args a) {
        this.args = a;
        this.target = a.get("target", "file:///tmp/addrs.txt");
        this.policy = a.get("policy", "peak_ewma_p2c");
        this.rps = Integer.parseInt(a.get("rps", "1000"));
        this.threads = Integer.parseInt(a.get("threads", "8"));
        this.outstandingPerThread = Integer.parseInt(a.get("outstanding", "32"));
        this.warmupS = Double.parseDouble(a.get("warmup-s", "10"));
        this.durationS = Double.parseDouble(a.get("duration-s", "30"));
        this.intervalS = Double.parseDouble(a.get("interval-s", "1"));
        this.payload = new byte[Integer.parseInt(a.get("payload", "100"))];
        ThreadLocalRandom.current().nextBytes(payload);
        this.watches = Integer.parseInt(a.get("watches", "0"));
        this.restartAtS = Double.parseDouble(a.get("restart-at-s", "-1"));
        this.heapGcEveryS = Double.parseDouble(a.get("heap-gc-every-s", "0"));
        this.jfrOut = a.get("jfr", null);
        this.jfrSettings = a.get("jfr-settings", "profile");
        this.costs = Boolean.parseBoolean(a.get("costs", "false"));
        this.lbMetrics = new RecordingLbMetrics(costs);
        for (String p : a.all("phase")) {
            String[] f = p.split(":");
            phases.add(new Phase(f[0], Double.parseDouble(f[1]), Double.parseDouble(f[2])));
        }

        // Methods: an explicit weighted list ("U10_get:70,U50_search:30"), or N identical ones.
        double streamFraction = Double.parseDouble(a.get("stream-fraction", "0"));
        String explicit = a.get("methods", null);
        List<String[]> specs = new ArrayList<>();
        if (explicit != null) {
            for (String m : explicit.split(",")) {
                String[] f = m.split(":");
                specs.add(new String[] {f[0], f.length > 1 ? f[1] : "1"});
            }
        } else {
            int n = Integer.parseInt(a.get("method-count", "1"));
            String lat = a.get("method-latency-ms", "0");
            for (int i = 0; i < n; i++) specs.add(new String[] {"U" + lat + "_m" + i, "1"});
        }
        boolean perMethodGroups = specs.size() <= 10;
        double unaryWeight = 0;
        for (String[] s : specs) unaryWeight += Double.parseDouble(s[1]);
        double cum = 0;
        for (String[] s : specs) {
            double w = Double.parseDouble(s[1]) / unaryWeight * (1 - streamFraction);
            cum += w;
            methods.add(spec(s[0], perMethodGroups ? s[0] : "unary", cum));
        }
        if (streamFraction > 0) {
            String lat = a.get("method-latency-ms", "0");
            methods.add(spec("S" + lat + "_stream", "stream", 1.0));
        }
        this.churnActive = Integer.parseInt(a.get("churn", "0"));
        this.churnPerSec = Double.parseDouble(a.get("churn-per-s", "10"));
        this.churnTemplate =
                churnActive > 0
                        ? spec("U" + a.get("method-latency-ms", "0") + "_c0", "churn", 1)
                        : null;
    }

    private MethodSpec spec(String bare, String groupKey, double cumulativeWeight) {
        double lat = Probe.methodLatencyMillis(bare);
        long auto = (long) Math.max(500, 5 * lat);
        long deadline = Long.parseLong(args.get("deadline-ms", "0"));
        Group g = groups.computeIfAbsent(groupKey, Group::new);
        return new MethodSpec(
                bare, Probe.descriptor(bare), deadline > 0 ? deadline : auto, g, cumulativeWeight);
    }

    public static void main(String[] argv) throws Exception {
        LoadGen g = new LoadGen(new Args(argv));
        g.run(Path.of(g.args.get("out", "loadgen.jsonl")));
        System.exit(0);
    }

    // ---- channel ----

    private ManagedChannel newChannel() {
        String timed;
        Map<String, Object> childConfig;
        switch (policy) {
            case "lr_od", "least_request+outlier_detection" -> {
                timed = TimedPolicyProvider.register("outlier_detection_experimental");
                // grpc defaults (10 s interval, 30 s base ejection, success-rate ejection), plus
                // failure-percentage ejection and the same 20% ejection cap as peak_ewma_p2c.
                Map<String, Object> od = new LinkedHashMap<>();
                od.put("interval", "10s");
                od.put("baseEjectionTime", "30s");
                od.put("maxEjectionTime", "300s");
                od.put("maxEjectionPercent", 20.0);
                od.put(
                        "successRateEjection",
                        Map.of(
                                "stdevFactor",
                                1900.0,
                                "enforcementPercentage",
                                100.0,
                                "minimumHosts",
                                5.0,
                                "requestVolume",
                                100.0));
                od.put(
                        "failurePercentageEjection",
                        Map.of(
                                "threshold",
                                50.0,
                                "enforcementPercentage",
                                100.0,
                                "minimumHosts",
                                5.0,
                                "requestVolume",
                                50.0));
                od.put(
                        "childPolicy",
                        List.of(Map.of("least_request_experimental", Map.of("choiceCount", 2.0))));
                childConfig = od;
            }
            case "control" -> {
                timed = TimedPolicyProvider.register("pick_first");
                childConfig = Map.of();
            }
            case "least_request_experimental" -> {
                timed = TimedPolicyProvider.register(policy);
                childConfig = Map.of("choiceCount", 2.0);
            }
            default -> {
                timed = TimedPolicyProvider.register(policy);
                childConfig = Map.of();
            }
        }
        Map<String, Object> serviceConfig =
                Map.of("loadBalancingConfig", List.of(Map.of(timed, childConfig)));
        String t = target;
        if (policy.equals("control")) {
            // The floor: calls go straight to one backend through a trivial picker.
            t = args.get("control-target", target);
        }
        return NettyChannelBuilder.forTarget(t)
                .usePlaintext()
                .directExecutor()
                .defaultServiceConfig(serviceConfig)
                .disableServiceConfigLookUp()
                .build();
    }

    // ---- traffic ----

    private MethodSpec pickMethod(ThreadLocalRandom rnd) {
        if (churnActive > 0) {
            // A window of churnActive names sliding forward churnPerSec names per second: old
            // names stop being called (and must be pruned), new ones keep appearing.
            double elapsed = (System.nanoTime() - startNanos) / 1e9;
            long base = (long) (elapsed * churnPerSec);
            String bare =
                    "U"
                            + args.get("method-latency-ms", "0")
                            + "_c"
                            + (base + rnd.nextInt(churnActive));
            return new MethodSpec(
                    bare,
                    Probe.descriptor(bare),
                    churnTemplate.deadlineMillis(),
                    churnTemplate.group(),
                    1);
        }
        if (methods.size() == 1) return methods.get(0);
        double r = rnd.nextDouble();
        for (MethodSpec m : methods) if (r < m.cumulativeWeight()) return m;
        return methods.get(methods.size() - 1);
    }

    /**
     * Captures the backend a call was sent to when its stream is created. The call's own attributes
     * are empty for calls that never got a response (deadline exceeded, black hole), so reading
     * them at close would hide exactly the calls sent to a bad backend.
     */
    private static final class RemoteCapture extends ClientStreamTracer.Factory {
        volatile SocketAddress remote;

        @Override
        public ClientStreamTracer newClientStreamTracer(
                ClientStreamTracer.StreamInfo info, Metadata headers) {
            return new ClientStreamTracer() {
                @Override
                public void streamCreated(Attributes transportAttrs, Metadata headers) {
                    remote = transportAttrs.get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR);
                }
            };
        }
    }

    private void issue(MethodSpec m, long intendedNanos, Semaphore permits) {
        ManagedChannel ch = channel;
        RemoteCapture capture = new RemoteCapture();
        ClientCall<byte[], byte[]> call =
                ch.newCall(
                        m.descriptor(),
                        CallOptions.DEFAULT
                                .withDeadlineAfter(m.deadlineMillis(), TimeUnit.MILLISECONDS)
                                .withStreamTracerFactory(capture));
        inflight.incrementAndGet();
        call.start(
                new ClientCall.Listener<>() {
                    @Override
                    public void onClose(Status status, Metadata trailers) {
                        long micros = (System.nanoTime() - intendedNanos) / 1000;
                        m.group().record(status.getCode(), micros);
                        completed.increment();
                        SocketAddress remote = capture.remote;
                        String be =
                                remote instanceof InetSocketAddress isa
                                        ? isa.getAddress().getHostAddress() + ":" + isa.getPort()
                                        : "none";
                        LongAdder a = perBackend.get(be);
                        (a != null ? a : perBackend.computeIfAbsent(be, k -> new LongAdder()))
                                .increment();
                        inflight.decrementAndGet();
                        if (permits != null) permits.release();
                    }
                },
                new Metadata());
        call.request(m.descriptor().getType() == MethodDescriptor.MethodType.UNARY ? 1 : 16);
        call.sendMessage(payload);
        call.halfClose();
    }

    private void openLoop(int threadIndex) {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        long period = (long) (threads * 1e9 / rps);
        long next = startNanos + (period * threadIndex) / threads;
        while (running) {
            long now = System.nanoTime();
            if (now < next) {
                long wait = next - now;
                if (wait > 50_000) LockSupport.parkNanos(wait - 20_000);
                else Thread.onSpinWait();
                continue;
            }
            long lag = now - next;
            if (lag > maxLagNanos.get()) maxLagNanos.accumulateAndGet(lag, Math::max);
            issue(pickMethod(rnd), next, null);
            next += period;
        }
    }

    private void closedLoop() {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        Semaphore permits = new Semaphore(outstandingPerThread);
        while (running) {
            try {
                if (!permits.tryAcquire(100, TimeUnit.MILLISECONDS)) continue;
            } catch (InterruptedException e) {
                return;
            }
            issue(pickMethod(rnd), System.nanoTime(), permits);
        }
    }

    private void openWatch(int i) {
        if (!running) return;
        MethodDescriptor<byte[], byte[]> d = Probe.descriptor("W_watch");
        ClientCall<byte[], byte[]> call = channel.newCall(d, CallOptions.DEFAULT);
        call.start(
                new ClientCall.Listener<>() {
                    @Override
                    public void onMessage(byte[] message) {
                        watchMessages.increment();
                        call.request(1);
                    }

                    @Override
                    public void onClose(Status status, Metadata trailers) {
                        // GOAWAY, backend restart, ...: reopen, as a real watcher would.
                        watchRestarts.increment();
                        Thread.ofVirtual()
                                .start(
                                        () -> {
                                            LockSupport.parkNanos(100_000_000L);
                                            openWatch(i);
                                        });
                    }
                },
                new Metadata());
        call.request(1);
        call.sendMessage(new byte[] {1});
        call.halfClose();
    }

    // ---- run + reporting ----

    private void run(Path out) throws Exception {
        PeakEwmaP2CProvider.register(lbMetrics);
        installGcListener();
        channel = newChannel();
        Files.createDirectories(out.toAbsolutePath().getParent());
        try (BufferedWriter w = Files.newBufferedWriter(out, StandardCharsets.UTF_8)) {
            w.write(metaJson());
            w.newLine();
            w.flush();

            // Connect before the clock starts, so warmup isn't mostly connection setup.
            channel.getState(true);
            startNanos = System.nanoTime();
            for (int i = 0; i < watches; i++) openWatch(i);
            List<Thread> gens = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                final int idx = i;
                Thread t =
                        new Thread(
                                rps > 0 ? () -> openLoop(idx) : this::closedLoop, "loadgen-" + i);
                t.setDaemon(true);
                gens.add(t);
                t.start();
            }
            measure(w);
            running = false;
            for (Thread t : gens) t.join(2_000);
            channel.shutdown();
            channel.awaitTermination(3, TimeUnit.SECONDS);
        }
    }

    private void measure(BufferedWriter w) throws Exception {
        var os =
                (com.sun.management.OperatingSystemMXBean)
                        ManagementFactory.getOperatingSystemMXBean();
        var tmx = (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
        var mem = ManagementFactory.getMemoryMXBean();

        long measureStart = startNanos + (long) (warmupS * 1e9);
        long measureEnd = measureStart + (long) (durationS * 1e9);
        long intervalNanos = (long) (intervalS * 1e9);

        long prevT = System.nanoTime();
        long prevCpu = os.getProcessCpuTime();
        long prevAlloc = tmx.getTotalThreadAllocatedBytes();
        long[] prevGc = gcTotals();
        long sumCpu = 0, sumAlloc = 0, sumGcCount = 0, sumGcMs = 0, sumCompleted = 0;
        long sumTickBusy = 0;
        Map<String, Long> sumStatus = new TreeMap<>();
        Map<String, Long> sumPicks = new TreeMap<>();
        Map<String, Long> sumEjections = new TreeMap<>();
        Histogram pickTotal = new Histogram(3);
        Histogram tickTotal = new Histogram(3);
        Map<String, Map<String, Long>> phaseStatus = new LinkedHashMap<>();
        Map<String, Long> phaseCompleted = new LinkedHashMap<>();
        Long lastHeapAfterGc = null;
        double nextHeapGc = heapGcEveryS > 0 ? heapGcEveryS : Double.MAX_VALUE;
        boolean restarted = false;
        boolean jfrStarted = false;
        Object jfr = null;
        double measuredS = 0;

        long next = startNanos + intervalNanos;
        while (true) {
            long sleep = next - System.nanoTime();
            if (sleep > 0) Thread.sleep(sleep / 1_000_000L, (int) (sleep % 1_000_000L));
            long now = System.nanoTime();
            double t = (now - measureStart) / 1e9; // seconds since measurement start
            boolean inWindow = now > measureStart && prevT >= measureStart - intervalNanos / 2;

            if (!jfrStarted && jfrOut != null && now >= measureStart) {
                jfr = JfrSupport.start(jfrSettings);
                jfrStarted = true;
            }
            if (!restarted && restartAtS >= 0 && t >= restartAtS) {
                // Client restart: a brand-new channel (cold LB state) against the warm fleet.
                ManagedChannel old = channel;
                channel = newChannel();
                channel.getState(true);
                old.shutdown();
                restarted = true;
            }

            long cpu = os.getProcessCpuTime();
            long alloc = tmx.getTotalThreadAllocatedBytes();
            long[] gc = gcTotals();
            long dCpu = cpu - prevCpu, dAlloc = alloc - prevAlloc;
            long dGcCount = gc[0] - prevGc[0], dGcMs = gc[1] - prevGc[1];
            double dt = (now - prevT) / 1e9;
            long done = completed.sumThenReset();

            StringBuilder sb = new StringBuilder(4096);
            sb.append("{\"type\":\"interval\"");
            num(sb, "t", t);
            sb.append(",\"warm\":").append(!inWindow);
            num(sb, "dur_s", dt);
            num(sb, "cpu_ns", dCpu);
            num(sb, "alloc_bytes", dAlloc);
            num(sb, "gc_count", dGcCount);
            num(sb, "gc_ms", dGcMs);
            num(sb, "heap_used", mem.getHeapMemoryUsage().getUsed());
            num(sb, "completed", done);
            num(sb, "inflight", inflight.get());
            num(sb, "max_lag_ms", maxLagNanos.getAndSet(0) / 1e6);
            if (watches > 0) {
                num(sb, "watch_msgs", watchMessages.sumThenReset());
                num(sb, "watch_restarts", watchRestarts.sumThenReset());
            }

            List<String> activePhases = new ArrayList<>();
            for (Phase p : phases)
                if (inWindow && t > p.startS && t <= p.endS) activePhases.add(p.name);

            // Latency + status per group
            sb.append(",\"methods\":{");
            boolean first = true;
            Map<String, Long> intervalStatus = new TreeMap<>();
            for (Group g : groups.values()) {
                Histogram h = g.latencyMicros.getIntervalHistogram();
                Map<String, Long> st = new TreeMap<>();
                g.status.forEach((k, v) -> st.put(k, v.sumThenReset()));
                long n = st.values().stream().mapToLong(Long::longValue).sum();
                if (!first) sb.append(',');
                first = false;
                sb.append('"').append(g.key).append("\":{");
                sb.append("\"n\":").append(n);
                sb.append(",\"ok\":").append(st.getOrDefault("OK", 0L));
                appendPercentiles(sb, h, 1);
                sb.append(",\"status\":");
                mapJson(sb, st);
                sb.append('}');
                st.forEach((k, v) -> intervalStatus.merge(k, v, Long::sum));
                if (inWindow) {
                    g.total.add(h);
                    for (String p : activePhases) {
                        g.byPhase.computeIfAbsent(p, k -> new Histogram(60_000_000L, 3)).add(h);
                    }
                }
            }
            sb.append('}');
            sb.append(",\"status\":");
            mapJson(sb, intervalStatus);

            Map<String, Long> be = new TreeMap<>();
            perBackend.forEach(
                    (k, v) -> {
                        long c = v.sumThenReset();
                        if (c > 0) be.put(k, c);
                    });
            sb.append(",\"backends\":");
            mapJson(sb, be);

            // LB internals
            Map<String, Long> picks = lbMetrics.drainPicks();
            picks.values().removeIf(v -> v == 0);
            if (!picks.isEmpty()) {
                sb.append(",\"picks\":");
                mapJson(sb, picks);
            }
            List<RecordingLbMetrics.Ejection> ej = new ArrayList<>();
            for (RecordingLbMetrics.Ejection e; (e = lbMetrics.ejections.poll()) != null; )
                ej.add(e);
            if (!ej.isEmpty()) {
                sb.append(",\"ejections\":[");
                for (int i = 0; i < ej.size(); i++) {
                    var e = ej.get(i);
                    if (i > 0) sb.append(',');
                    sb.append("{\"sc\":\"")
                            .append(e.subchannel())
                            .append("\",\"reason\":\"")
                            .append(e.reason())
                            .append('"');
                    num(sb, "error_rate", e.errorRate());
                    num(sb, "latency_ratio", e.latencyRatio());
                    sb.append('}');
                }
                sb.append(']');
            }
            num(sb, "ejected_now", lbMetrics.ejected);
            if (!lbMetrics.scales.isEmpty() && lbMetrics.scales.size() <= 20) {
                sb.append(",\"scales\":{");
                boolean f = true;
                for (var e : new TreeMap<>(lbMetrics.scales).entrySet()) {
                    if (!f) sb.append(',');
                    f = false;
                    double[] v = e.getValue();
                    sb.append('"')
                            .append(shortMethod(e.getKey()))
                            .append("\":[")
                            .append(fmt(v[0]))
                            .append(',')
                            .append(fmt(v[1]))
                            .append(',')
                            .append(fmt(v[2]))
                            .append(']');
                }
                sb.append('}');
            }
            if (costs && !lbMetrics.costs.isEmpty()) {
                sb.append(",\"costs\":{");
                boolean f = true;
                for (var e : new TreeMap<>(lbMetrics.costs).entrySet()) {
                    if (!f) sb.append(',');
                    f = false;
                    String[] k = e.getKey().split("\\|", 2);
                    sb.append('"')
                            .append(k[0])
                            .append('|')
                            .append(shortMethod(k[1]))
                            .append("\":")
                            .append(fmt(e.getValue()));
                }
                sb.append('}');
            }
            Histogram pickH = TimedPolicyProvider.PICK_NANOS.getIntervalHistogram();
            Histogram tickH = lbMetrics.tickNanos.getIntervalHistogram();
            long tickBusy = lbMetrics.tickBusyNanos.sumThenReset();
            if (tickH.getTotalCount() > 0) {
                sb.append(",\"tick\":{");
                sb.append("\"n\":").append(tickH.getTotalCount());
                num(sb, "p50_ms", tickH.getValueAtPercentile(50) / 1e6);
                num(sb, "max_ms", tickH.getMaxValue() / 1e6);
                num(sb, "busy_ms", tickBusy / 1e6);
                sb.append('}');
            }

            if (heapGcEveryS > 0 && inWindow && t >= nextHeapGc) {
                System.gc();
                lastHeapAfterGc = mem.getHeapMemoryUsage().getUsed();
                num(sb, "heap_after_gc", lastHeapAfterGc);
                nextHeapGc += heapGcEveryS;
            }
            sb.append('}');
            w.write(sb.toString());
            w.newLine();
            w.flush();

            if (inWindow) {
                measuredS += dt;
                sumCpu += dCpu;
                sumAlloc += dAlloc;
                sumGcCount += dGcCount;
                sumGcMs += dGcMs;
                sumCompleted += done;
                sumTickBusy += tickBusy;
                intervalStatus.forEach((k, v) -> sumStatus.merge(k, v, Long::sum));
                picks.forEach((k, v) -> sumPicks.merge(k, v, Long::sum));
                for (var e : ej) sumEjections.merge(e.reason(), 1L, Long::sum);
                pickTotal.add(pickH);
                tickTotal.add(tickH);
                gcPauseTotal.add(gcPauseMicros.getIntervalHistogram());
                for (String p : activePhases) {
                    var ps = phaseStatus.computeIfAbsent(p, k -> new TreeMap<>());
                    intervalStatus.forEach((k, v) -> ps.merge(k, v, Long::sum));
                    phaseCompleted.merge(p, done, Long::sum);
                }
            } else {
                gcPauseMicros.getIntervalHistogram(); // discard warmup pauses
            }

            prevT = now;
            prevCpu = cpu;
            prevAlloc = alloc;
            prevGc = gc;
            next += intervalNanos;
            if (now >= measureEnd) break;
        }

        String jfrPath = null;
        if (jfr != null) jfrPath = JfrSupport.stop(jfr, Path.of(jfrOut));

        // Drain: let in-flight calls finish so they are not silently lost.
        running = false;
        long drainUntil = System.nanoTime() + TimeUnit.SECONDS.toNanos(6);
        while (inflight.get() > 0 && System.nanoTime() < drainUntil) Thread.sleep(20);

        StringBuilder sb = new StringBuilder(8192);
        sb.append("{\"type\":\"summary\"");
        num(sb, "measure_s", measuredS);
        num(sb, "completed", sumCompleted);
        num(sb, "ok", sumStatus.getOrDefault("OK", 0L));
        num(sb, "cpu_ns", sumCpu);
        num(sb, "alloc_bytes", sumAlloc);
        num(sb, "gc_count", sumGcCount);
        num(sb, "gc_ms", sumGcMs);
        num(sb, "gc_pause_p99_ms", gcPauseTotal.getValueAtPercentile(99) / 1000.0);
        num(sb, "gc_pause_max_ms", gcPauseTotal.getMaxValue() / 1000.0);
        num(sb, "rps", measuredS > 0 ? sumCompleted / measuredS : 0);
        num(sb, "cores_used", measuredS > 0 ? sumCpu / 1e9 / measuredS : 0);
        num(sb, "cpu_us_per_rpc", sumCompleted > 0 ? sumCpu / 1e3 / sumCompleted : 0);
        num(sb, "alloc_b_per_rpc", sumCompleted > 0 ? (double) sumAlloc / sumCompleted : 0);
        num(sb, "alloc_mb_per_s", measuredS > 0 ? sumAlloc / 1e6 / measuredS : 0);
        num(sb, "gc_per_s", measuredS > 0 ? sumGcCount / measuredS : 0);
        num(sb, "inflight_at_end", inflight.get());
        sb.append(",\"status\":");
        mapJson(sb, sumStatus);
        sb.append(",\"latency\":{");
        Histogram all = new Histogram(60_000_000L, 3);
        boolean first = true;
        for (Group g : groups.values()) {
            all.add(g.total);
            if (!first) sb.append(',');
            first = false;
            sb.append('"').append(g.key).append("\":{\"n\":").append(g.total.getTotalCount());
            appendPercentiles(sb, g.total, 1);
            sb.append('}');
        }
        sb.append(",\"all\":{\"n\":").append(all.getTotalCount());
        appendPercentiles(sb, all, 1);
        sb.append("}}");
        sb.append(",\"phases\":{");
        first = true;
        for (Phase p : phases) {
            if (!first) sb.append(',');
            first = false;
            sb.append('"').append(p.name).append("\":{");
            sb.append("\"completed\":").append(phaseCompleted.getOrDefault(p.name, 0L));
            num(sb, "start_s", p.startS);
            num(sb, "end_s", p.endS);
            sb.append(",\"status\":");
            mapJson(sb, phaseStatus.getOrDefault(p.name, Map.of()));
            sb.append(",\"latency\":{");
            Histogram pa = new Histogram(60_000_000L, 3);
            boolean f2 = true;
            for (Group g : groups.values()) {
                Histogram h = g.byPhase.get(p.name);
                if (h == null) continue;
                pa.add(h);
                if (!f2) sb.append(',');
                f2 = false;
                sb.append('"').append(g.key).append("\":{\"n\":").append(h.getTotalCount());
                appendPercentiles(sb, h, 1);
                sb.append('}');
            }
            if (!f2) sb.append(',');
            sb.append("\"all\":{\"n\":").append(pa.getTotalCount());
            appendPercentiles(sb, pa, 1);
            sb.append("}}}");
        }
        sb.append('}');
        sb.append(",\"pick_ns\":{\"n\":").append(pickTotal.getTotalCount());
        appendPercentiles(sb, pickTotal, 1, "");
        sb.append('}');
        sb.append(",\"tick_ms\":{\"n\":").append(tickTotal.getTotalCount());
        num(sb, "p50", tickTotal.getValueAtPercentile(50) / 1e6);
        num(sb, "p99", tickTotal.getValueAtPercentile(99) / 1e6);
        num(sb, "max", tickTotal.getMaxValue() / 1e6);
        num(sb, "busy_ms_per_s", measuredS > 0 ? sumTickBusy / 1e6 / measuredS : 0);
        sb.append('}');
        sb.append(",\"pick_outcomes\":");
        mapJson(sb, sumPicks);
        sb.append(",\"ejections\":");
        mapJson(sb, sumEjections);
        if (lastHeapAfterGc != null) num(sb, "heap_after_gc", lastHeapAfterGc);
        if (jfrPath != null) sb.append(",\"jfr\":\"").append(jfrPath).append('"');
        sb.append('}');
        w.write(sb.toString());
        w.newLine();
        w.flush();
    }

    private String metaJson() {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"type\":\"meta\",\"policy\":\"").append(policy).append('"');
        sb.append(",\"client\":\"").append(args.get("client-id", "c0")).append('"');
        sb.append(",\"pid\":").append(ProcessHandle.current().pid());
        sb.append(",\"jdk\":\"").append(System.getProperty("java.vm.version")).append('"');
        sb.append(",\"grpc\":\"").append(grpcVersion()).append('"');
        sb.append(",\"cpus\":").append(Runtime.getRuntime().availableProcessors());
        sb.append(",\"start_epoch_ms\":").append(System.currentTimeMillis());
        sb.append(",\"args\":{");
        boolean first = true;
        for (var e : args.asMap().entrySet()) {
            if (!first) sb.append(',');
            first = false;
            sb.append('"')
                    .append(e.getKey())
                    .append("\":\"")
                    .append(e.getValue().replace("\\", "\\\\").replace("\"", "\\\""))
                    .append('"');
        }
        return sb.append("}}").toString();
    }

    private static String grpcVersion() {
        Package p = io.grpc.ManagedChannel.class.getPackage();
        String v = p != null ? p.getImplementationVersion() : null;
        return v != null ? v : "1.78.0";
    }

    private static String shortMethod(String full) {
        int i = full.lastIndexOf('/');
        return i >= 0 ? full.substring(i + 1) : full;
    }

    private void installGcListener() {
        for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {
            if (!(gc instanceof NotificationEmitter ne)) continue;
            ne.addNotificationListener(
                    (n, h) -> {
                        if (!GarbageCollectionNotificationInfo.GARBAGE_COLLECTION_NOTIFICATION
                                .equals(n.getType())) return;
                        var info =
                                GarbageCollectionNotificationInfo.from(
                                        (CompositeData) n.getUserData());
                        // Concurrent cycles are not stop-the-world pauses.
                        if (info.getGcName().contains("Concurrent")) return;
                        gcPauseMicros.recordValue(
                                Math.max(1, info.getGcInfo().getDuration() * 1000));
                    },
                    null,
                    null);
        }
    }

    private static long[] gcTotals() {
        long count = 0, ms = 0;
        for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {
            if (gc.getName().contains("Concurrent")) continue;
            count += Math.max(0, gc.getCollectionCount());
            ms += Math.max(0, gc.getCollectionTime());
        }
        return new long[] {count, ms};
    }

    // ---- JSON helpers ----

    private static void appendPercentiles(StringBuilder sb, Histogram h, double scale) {
        appendPercentiles(sb, h, scale, "_us");
    }

    private static void appendPercentiles(
            StringBuilder sb, Histogram h, double scale, String suffix) {
        if (h.getTotalCount() == 0) return;
        num(sb, "p50" + suffix, h.getValueAtPercentile(50) / scale);
        num(sb, "p90" + suffix, h.getValueAtPercentile(90) / scale);
        num(sb, "p99" + suffix, h.getValueAtPercentile(99) / scale);
        num(sb, "p999" + suffix, h.getValueAtPercentile(99.9) / scale);
        num(sb, "max" + suffix, h.getMaxValue() / scale);
        num(sb, "mean" + suffix, h.getMean() / scale);
    }

    private static void num(StringBuilder sb, String k, double v) {
        sb.append(",\"").append(k).append("\":").append(fmt(v));
    }

    private static String fmt(double v) {
        if (Double.isNaN(v) || Double.isInfinite(v)) return "null";
        if (v == Math.rint(v) && Math.abs(v) < 1e15) return Long.toString((long) v);
        return String.format(Locale.ROOT, "%.4f", v);
    }

    private static void mapJson(StringBuilder sb, Map<String, Long> m) {
        sb.append('{');
        boolean first = true;
        for (var e : m.entrySet()) {
            if (!first) sb.append(',');
            first = false;
            sb.append('"').append(e.getKey()).append("\":").append(e.getValue());
        }
        sb.append('}');
    }
}
