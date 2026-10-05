package dev.parkerharrelson.grpc.peakewma.loadtest;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.grpc.HandlerRegistry;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerMethodDefinition;
import io.grpc.Status;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.ServerCalls;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

/**
 * Load-test backend: one or more gRPC ports (one per simulated pod), each with its own {@link
 * Behaviour}, plus an HTTP admin endpoint.
 *
 * <pre>
 * java -cp loadtest.jar dev.parkerharrelson.grpc.peakewma.loadtest.Backend \
 *     --ports 9001,9002 --admin-port 8081 [--bind 127.0.0.1] [--max-connection-age-ms 10000] \
 *     [--set slots=8 --set sigma=0.3 ...]
 * </pre>
 *
 * Admin endpoint: {@code GET /set?ports=all&latencyFactor=5} (or a comma list of ports) changes
 * behaviour at runtime; {@code GET /stats} returns per-port counters and process CPU as JSON;
 * {@code GET /metrics} is Prometheus text; {@code GET /health}.
 */
public final class Backend {

    private static final double[] LATENCY_BUCKETS_MS = {
        0.5, 1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000
    };
    private static final int MAX_TRACKED_METHODS = 64;

    private final Map<Integer, PortState> ports = new LinkedHashMap<>();
    private final List<Server> servers = new ArrayList<>();
    private final ScheduledExecutorService scheduler;

    private Backend(int schedulerThreads) {
        ScheduledThreadPoolExecutor s =
                new ScheduledThreadPoolExecutor(
                        schedulerThreads,
                        r -> {
                            Thread t = new Thread(r, "backend-scheduler");
                            t.setDaemon(true);
                            return t;
                        });
        s.setRemoveOnCancelPolicy(true);
        this.scheduler = s;
    }

    public static void main(String[] args) throws Exception {
        Args a = new Args(args);
        List<Integer> portList = new ArrayList<>();
        for (String p : a.get("ports", "9000").split(",")) portList.add(Integer.parseInt(p.trim()));
        String bind = a.get("bind", "127.0.0.1");
        long maxAge = Long.parseLong(a.get("max-connection-age-ms", "0"));
        Map<String, String> initial = new LinkedHashMap<>();
        for (String kv : a.all("set")) {
            int eq = kv.indexOf('=');
            initial.put(kv.substring(0, eq), kv.substring(eq + 1));
        }
        Behaviour start = Behaviour.DEFAULT.with(initial);

        Backend b = new Backend(Integer.parseInt(a.get("scheduler-threads", "2")));
        for (int port : portList) {
            PortState ps = new PortState(port, start);
            b.ports.put(port, ps);
            NettyServerBuilder sb =
                    NettyServerBuilder.forAddress(new InetSocketAddress(bind, port))
                            .directExecutor()
                            .fallbackHandlerRegistry(b.registryFor(ps));
            if (maxAge > 0) {
                sb.maxConnectionAge(maxAge, TimeUnit.MILLISECONDS)
                        .maxConnectionAgeGrace(5, TimeUnit.SECONDS);
            }
            b.servers.add(sb.build().start());
        }
        int adminPort = Integer.parseInt(a.get("admin-port", "8080"));
        HttpServer http = HttpServer.create(new InetSocketAddress(bind, adminPort), 64);
        http.createContext("/set", b::handleSet);
        http.createContext("/stats", ex -> respond(ex, 200, b.statsJson(), "application/json"));
        http.createContext("/metrics", ex -> respond(ex, 200, b.prometheus(), "text/plain"));
        http.createContext("/health", ex -> respond(ex, 200, "ok", "text/plain"));
        http.setExecutor(Executors.newFixedThreadPool(2));
        http.start();

        Runtime.getRuntime()
                .addShutdownHook(
                        new Thread(
                                () -> {
                                    // Graceful: GOAWAY, let in-flight calls finish, like a pod
                                    // receiving SIGTERM.
                                    for (Server s : b.servers) s.shutdown();
                                    for (Server s : b.servers) {
                                        try {
                                            s.awaitTermination(5, TimeUnit.SECONDS);
                                        } catch (InterruptedException e) {
                                            Thread.currentThread().interrupt();
                                        }
                                    }
                                    http.stop(0);
                                }));
        System.out.println("backend ready ports=" + portList + " admin=" + adminPort);
        System.out.flush();
        for (Server s : b.servers) s.awaitTermination();
    }

    // ---- request handling ----

    private HandlerRegistry registryFor(PortState ps) {
        ConcurrentHashMap<String, ServerMethodDefinition<?, ?>> cache = new ConcurrentHashMap<>();
        return new HandlerRegistry() {
            @Override
            public ServerMethodDefinition<?, ?> lookupMethod(String fullName, String authority) {
                ServerMethodDefinition<?, ?> d = cache.get(fullName);
                if (d != null) return d;
                String svc = MethodDescriptor.extractFullServiceName(fullName);
                if (!Probe.SERVICE.equals(svc)) return null;
                String bare = fullName.substring(svc.length() + 1);
                if (bare.isEmpty() || "USW".indexOf(bare.charAt(0)) < 0) return null;
                d = definition(ps, bare);
                if (cache.size() > 20_000) cache.clear(); // churning method names
                cache.put(fullName, d);
                return d;
            }
        };
    }

    private ServerMethodDefinition<byte[], byte[]> definition(PortState ps, String bare) {
        MethodDescriptor<byte[], byte[]> md = Probe.descriptor(bare);
        double nominalMs = Probe.methodLatencyMillis(bare);
        String tracked = ps.trackedName(bare);
        return switch (bare.charAt(0)) {
            case 'U' ->
                    ServerMethodDefinition.create(
                            md,
                            ServerCalls.asyncUnaryCall(
                                    (req, obs) -> unary(ps, tracked, nominalMs, req, obs)));
            case 'S' ->
                    ServerMethodDefinition.create(
                            md,
                            ServerCalls.asyncServerStreamingCall(
                                    (req, obs) -> stream(ps, tracked, nominalMs, req, obs)));
            default ->
                    ServerMethodDefinition.create(
                            md,
                            ServerCalls.asyncServerStreamingCall(
                                    (req, obs) -> watch(ps, tracked, req, obs)));
        };
    }

    /** Returns false (and has already answered) if the call must not be served normally. */
    private static boolean admit(PortState ps, Behaviour b, String method, StreamObserver<?> obs) {
        ps.calls.increment();
        ps.methodCounter(method).increment();
        switch (b.mode()) {
            case "unavailable" -> {
                ps.errors.increment();
                obs.onError(Status.UNAVAILABLE.withDescription("crash-looping").asException());
                return false;
            }
            case "blackhole" -> {
                ps.blackholed.increment();
                return false; // never answered; the client's deadline ends it
            }
            default -> {
                if (b.errorRate() > 0 && ThreadLocalRandom.current().nextDouble() < b.errorRate()) {
                    ps.errors.increment();
                    obs.onError(Status.UNAVAILABLE.withDescription("flaky").asException());
                    return false;
                }
                return true;
            }
        }
    }

    private void unary(
            PortState ps, String method, double nominalMs, byte[] req, StreamObserver<byte[]> obs) {
        Behaviour b = ps.behaviour.get();
        if (!admit(ps, b, method, obs)) return;
        if (nominalMs <= 0
                && b.slots() <= 0
                && b.extraDelayMillis() <= 0
                && b.gcPeriodMillis() <= 0) {
            obs.onNext(req);
            obs.onCompleted();
            ps.ok.increment();
            return;
        }
        ServerCallStreamObserver<byte[]> sobs = (ServerCallStreamObserver<byte[]>) obs;
        ps.submit(
                new Job(
                        serviceNanos(b, nominalMs),
                        () -> {
                            if (sobs.isCancelled()) return;
                            obs.onNext(req);
                            obs.onCompleted();
                            ps.ok.increment();
                        },
                        sobs::isCancelled),
                this);
    }

    private void stream(
            PortState ps, String method, double nominalMs, byte[] req, StreamObserver<byte[]> obs) {
        Behaviour b = ps.behaviour.get();
        if (!admit(ps, b, method, obs)) return;
        ServerCallStreamObserver<byte[]> sobs = (ServerCallStreamObserver<byte[]>) obs;
        ps.submit(
                new Job(
                        serviceNanos(b, nominalMs),
                        () -> {
                            if (sobs.isCancelled()) return;
                            for (int i = 0; i < 5; i++) obs.onNext(req);
                            obs.onCompleted();
                            ps.ok.increment();
                        },
                        sobs::isCancelled),
                this);
    }

    private void watch(PortState ps, String method, byte[] req, StreamObserver<byte[]> obs) {
        Behaviour b = ps.behaviour.get();
        if (!admit(ps, b, method, obs)) return;
        ServerCallStreamObserver<byte[]> sobs = (ServerCallStreamObserver<byte[]>) obs;
        ps.watches.incrementAndGet();
        AtomicReference<ScheduledFuture<?>> task = new AtomicReference<>();
        sobs.setOnCancelHandler(
                () -> {
                    ScheduledFuture<?> f = task.get();
                    if (f != null) f.cancel(false);
                    ps.watches.decrementAndGet();
                });
        task.set(
                scheduler.scheduleAtFixedRate(
                        () -> {
                            if (!sobs.isCancelled() && sobs.isReady()) sobs.onNext(req);
                        },
                        1,
                        1,
                        TimeUnit.SECONDS));
    }

    private static long serviceNanos(Behaviour b, double nominalMs) {
        double median = nominalMs * b.latencyFactor();
        if (median <= 0) return 0L;
        double z = ThreadLocalRandom.current().nextGaussian();
        double ms = median * Math.exp(b.sigma() * z);
        return (long) (Math.min(ms, 50 * median) * 1_000_000L);
    }

    /**
     * A queued request. {@code cancelled} is checked when it reaches a slot: a real server skips
     * work whose caller already gave up (deadline), instead of serving a backlog nobody awaits.
     */
    private record Job(
            long serviceNanos, Runnable complete, java.util.function.BooleanSupplier cancelled) {}

    /** Delays {@code r} until any stop-the-world pause in progress at that moment has ended. */
    private void runAfterPause(PortState ps, Runnable r) {
        Behaviour b = ps.behaviour.get();
        long wait = ps.pauseRemainingMillis(b);
        if (wait > 0) {
            scheduler.schedule(r, wait, TimeUnit.MILLISECONDS);
        } else {
            r.run();
        }
    }

    // ---- per-port state ----

    private static final class PortState {
        final int port;
        final AtomicReference<Behaviour> behaviour;
        final LongAdder calls = new LongAdder();
        final LongAdder ok = new LongAdder();
        final LongAdder errors = new LongAdder();
        final LongAdder blackholed = new LongAdder();
        final LongAdder cancelledInQueue = new LongAdder();
        final LongAdder busyNanos = new LongAdder();
        final AtomicInteger watches = new AtomicInteger();
        final AtomicInteger busy = new AtomicInteger();
        final ConcurrentLinkedQueue<Job> queue = new ConcurrentLinkedQueue<>();
        final AtomicInteger queued = new AtomicInteger();
        final ConcurrentHashMap<String, LongAdder> byMethod = new ConcurrentHashMap<>();
        final long[] latencyBuckets = new long[LATENCY_BUCKETS_MS.length + 1];
        final long gcPhaseOffset = ThreadLocalRandom.current().nextLong(10_000);

        PortState(int port, Behaviour b) {
            this.port = port;
            this.behaviour = new AtomicReference<>(b);
        }

        String trackedName(String bare) {
            return byMethod.size() < MAX_TRACKED_METHODS || byMethod.containsKey(bare)
                    ? bare
                    : "other";
        }

        LongAdder methodCounter(String m) {
            LongAdder a = byMethod.get(m);
            return a != null ? a : byMethod.computeIfAbsent(m, k -> new LongAdder());
        }

        long pauseRemainingMillis(Behaviour b) {
            if (b.gcPeriodMillis() <= 0 || b.gcPauseMillis() <= 0) return 0;
            long phase = (System.currentTimeMillis() + gcPhaseOffset) % b.gcPeriodMillis();
            return phase < b.gcPauseMillis() ? b.gcPauseMillis() - phase : 0;
        }

        void submit(Job j, Backend owner) {
            queue.add(j);
            queued.incrementAndGet();
            drain(owner);
        }

        /** Starts queued jobs while slots are free. Lock-free; no lost wakeups. */
        void drain(Backend owner) {
            while (true) {
                int slots = behaviour.get().slots();
                int b = busy.get();
                if (slots > 0 && b >= slots) return;
                if (queue.isEmpty()) return;
                if (!busy.compareAndSet(b, b + 1)) continue;
                Job j = queue.poll();
                if (j == null) {
                    busy.decrementAndGet();
                    if (queue.isEmpty()) return;
                    continue;
                }
                queued.decrementAndGet();
                if (j.cancelled().getAsBoolean()) {
                    busy.decrementAndGet();
                    cancelledInQueue.increment();
                    continue;
                }
                start(j, owner);
            }
        }

        private void start(Job j, Backend owner) {
            long startNanos = System.nanoTime();
            Runnable finish =
                    () ->
                            owner.runAfterPause(
                                    this,
                                    () -> {
                                        long took = System.nanoTime() - startNanos;
                                        busyNanos.add(took);
                                        recordLatency(took);
                                        busy.decrementAndGet();
                                        drain(owner);
                                        double extra = behaviour.get().extraDelayMillis();
                                        if (extra > 0) {
                                            owner.scheduler.schedule(
                                                    j.complete(),
                                                    (long) (extra * 1_000_000L),
                                                    TimeUnit.NANOSECONDS);
                                        } else {
                                            j.complete().run();
                                        }
                                    });
            if (j.serviceNanos() <= 0) {
                finish.run();
            } else {
                owner.scheduler.schedule(finish, j.serviceNanos(), TimeUnit.NANOSECONDS);
            }
        }

        private void recordLatency(long nanos) {
            double ms = nanos / 1e6;
            int i = 0;
            while (i < LATENCY_BUCKETS_MS.length && ms > LATENCY_BUCKETS_MS[i]) i++;
            synchronized (latencyBuckets) {
                latencyBuckets[i]++;
            }
        }
    }

    // ---- admin ----

    private void handleSet(HttpExchange ex) throws IOException {
        try {
            Map<String, String> q = query(ex.getRequestURI().getRawQuery());
            String sel = q.getOrDefault("ports", q.getOrDefault("port", "all"));
            List<PortState> targets = new ArrayList<>();
            if (sel.equals("all")) {
                targets.addAll(ports.values());
            } else {
                for (String p : sel.split(",")) {
                    PortState ps = ports.get(Integer.parseInt(p.trim()));
                    if (ps == null) throw new IllegalArgumentException("no such port " + p);
                    targets.add(ps);
                }
            }
            for (PortState ps : targets) {
                ps.behaviour.updateAndGet(b -> b.with(q));
                ps.drain(this); // slots may have grown
            }
            respond(ex, 200, statsJson(), "application/json");
        } catch (RuntimeException e) {
            respond(ex, 400, String.valueOf(e.getMessage()), "text/plain");
        }
    }

    private String statsJson() {
        StringBuilder sb = new StringBuilder();
        long cpu =
                ((com.sun.management.OperatingSystemMXBean)
                                ManagementFactory.getOperatingSystemMXBean())
                        .getProcessCpuTime();
        sb.append("{\"pid\":")
                .append(ProcessHandle.current().pid())
                .append(",\"cpu_ns\":")
                .append(cpu)
                .append(",\"time_ms\":")
                .append(System.currentTimeMillis())
                .append(",\"ports\":{");
        boolean first = true;
        for (PortState ps : ports.values()) {
            if (!first) sb.append(',');
            first = false;
            sb.append('"').append(ps.port).append("\":{");
            sb.append("\"calls\":").append(ps.calls.sum());
            sb.append(",\"ok\":").append(ps.ok.sum());
            sb.append(",\"errors\":").append(ps.errors.sum());
            sb.append(",\"blackholed\":").append(ps.blackholed.sum());
            sb.append(",\"cancelled_in_queue\":").append(ps.cancelledInQueue.sum());
            sb.append(",\"busy_ns\":").append(ps.busyNanos.sum());
            sb.append(",\"busy\":").append(ps.busy.get());
            sb.append(",\"queued\":").append(ps.queued.get());
            sb.append(",\"watches\":").append(ps.watches.get());
            sb.append(",\"behaviour\":").append(ps.behaviour.get().toJson());
            sb.append(",\"methods\":{");
            boolean f2 = true;
            for (Map.Entry<String, LongAdder> e : ps.byMethod.entrySet()) {
                if (!f2) sb.append(',');
                f2 = false;
                sb.append('"').append(e.getKey()).append("\":").append(e.getValue().sum());
            }
            sb.append("}}");
        }
        return sb.append("}}").toString();
    }

    private String prometheus() {
        StringBuilder sb = new StringBuilder();
        sb.append("# TYPE loadtest_backend_calls_total counter\n");
        for (PortState ps : ports.values()) {
            for (Map.Entry<String, LongAdder> e : ps.byMethod.entrySet()) {
                sb.append("loadtest_backend_calls_total{port=\"")
                        .append(ps.port)
                        .append("\",method=\"")
                        .append(e.getKey())
                        .append("\"} ")
                        .append(e.getValue().sum())
                        .append('\n');
            }
            sb.append("loadtest_backend_errors_total{port=\"")
                    .append(ps.port)
                    .append("\"} ")
                    .append(ps.errors.sum())
                    .append('\n');
        }
        sb.append("# TYPE loadtest_backend_service_seconds histogram\n");
        for (PortState ps : ports.values()) {
            long[] snap;
            synchronized (ps.latencyBuckets) {
                snap = ps.latencyBuckets.clone();
            }
            long cum = 0;
            for (int i = 0; i < snap.length; i++) {
                cum += snap[i];
                String le =
                        i < LATENCY_BUCKETS_MS.length
                                ? Double.toString(LATENCY_BUCKETS_MS[i] / 1000.0)
                                : "+Inf";
                sb.append("loadtest_backend_service_seconds_bucket{port=\"")
                        .append(ps.port)
                        .append("\",le=\"")
                        .append(le)
                        .append("\"} ")
                        .append(cum)
                        .append('\n');
            }
            sb.append("loadtest_backend_service_seconds_count{port=\"")
                    .append(ps.port)
                    .append("\"} ")
                    .append(cum)
                    .append('\n');
        }
        return sb.toString();
    }

    private static Map<String, String> query(String raw) {
        Map<String, String> m = new LinkedHashMap<>();
        if (raw == null || raw.isEmpty()) return m;
        for (String kv : raw.split("&")) {
            int eq = kv.indexOf('=');
            if (eq < 0) continue;
            m.put(
                    URLDecoder.decode(kv.substring(0, eq), StandardCharsets.UTF_8),
                    URLDecoder.decode(kv.substring(eq + 1), StandardCharsets.UTF_8));
        }
        return m;
    }

    private static void respond(HttpExchange ex, int code, String body, String type)
            throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", type);
        ex.sendResponseHeaders(code, bytes.length);
        try (OutputStream os = ex.getResponseBody()) {
            os.write(bytes);
        }
    }
}
