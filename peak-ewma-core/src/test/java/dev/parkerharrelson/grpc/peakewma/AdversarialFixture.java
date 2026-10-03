package dev.parkerharrelson.grpc.peakewma;

import dev.parkerharrelson.grpc.peakewma.metrics.LbMetrics;
import io.grpc.Attributes;
import io.grpc.CallOptions;
import io.grpc.ChannelLogger;
import io.grpc.ClientStreamTracer;
import io.grpc.ConnectivityState;
import io.grpc.ConnectivityStateInfo;
import io.grpc.EquivalentAddressGroup;
import io.grpc.LoadBalancer;
import io.grpc.LoadBalancer.PickResult;
import io.grpc.LoadBalancer.PickSubchannelArgs;
import io.grpc.LoadBalancer.ResolvedAddresses;
import io.grpc.LoadBalancer.Subchannel;
import io.grpc.LoadBalancer.SubchannelPicker;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.SynchronizationContext;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Delayed;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Test rig for the adversarial suite. Wraps the real {@link PeakEwmaP2CBalancer} with:
 *
 * <ul>
 *   <li>a controllable fake clock injected into the balancer (so 60 s of traffic runs in ms),
 *   <li>a real {@link SynchronizationContext} and fake subchannels that, like grpc-java's
 *       ManagedChannelImpl, throw when {@code requestConnection}/{@code shutdown} are called off
 *       the sync context,
 *   <li>a discrete-event simulator that drives open-loop traffic through the published picker and
 *       the real stream tracers, with per-backend latency/status models.
 * </ul>
 */
final class AdversarialFixture {

    static final long T0 = 1_000_000_000L;
    static final long MS = 1_000_000L;

    static final MethodDescriptor<byte[], byte[]> METHOD_A = method("svc/A");
    static final MethodDescriptor<byte[], byte[]> METHOD_B = method("svc/B");

    final AtomicLong now = new AtomicLong(T0);
    final EwmaClocks clocks = new EwmaClocks(now::get);
    final RecordingMetrics metrics = new RecordingMetrics();
    final Helper helper = new Helper();
    final PeakEwmaP2CBalancer balancer;
    final Map<Integer, FakeSubchannel> byPort = new ConcurrentHashMap<>();
    final List<Throwable> syncCtxErrors = new CopyOnWriteArrayList<>();

    AdversarialFixture(PeakEwmaConfig cfg) {
        this(cfg, null);
    }

    AdversarialFixture(PeakEwmaConfig cfg, LbMetrics metricsOverride) {
        balancer =
                new PeakEwmaP2CBalancer(
                        helper, cfg, metricsOverride != null ? metricsOverride : metrics);
        setField(balancer, "clocks", clocks);
    }

    // ------------------------------------------------------------------ balancer driving

    /** Resolves {@code ports} on 127.0.0.1, on the sync context like a real channel would. */
    void resolve(int... ports) {
        List<EquivalentAddressGroup> eags = new ArrayList<>();
        for (int p : ports)
            eags.add(new EquivalentAddressGroup(new InetSocketAddress("127.0.0.1", p)));
        helper.syncCtx.execute(
                () ->
                        balancer.handleResolvedAddresses(
                                ResolvedAddresses.newBuilder().setAddresses(eags).build()));
    }

    void resolveAndReady(int... ports) {
        resolve(ports);
        for (int p : ports) drive(p, ConnectivityState.READY);
    }

    void drive(int port, ConnectivityState state) {
        FakeSubchannel sc = byPort.get(port);
        ConnectivityStateInfo info =
                state == ConnectivityState.TRANSIENT_FAILURE
                        ? ConnectivityStateInfo.forTransientFailure(Status.UNAVAILABLE)
                        : ConnectivityStateInfo.forNonError(state);
        helper.syncCtx.execute(() -> sc.deliver(info));
    }

    void nameResolutionError(Status s) {
        helper.syncCtx.execute(() -> balancer.handleNameResolutionError(s));
    }

    /** Runs one outlier tick on the calling thread, exactly as the shared scheduler would. */
    void tick() {
        Runnable t = helper.scheduler.task;
        if (t == null) throw new IllegalStateException("outlier ticker not scheduled");
        IN_TICK.set(true);
        try {
            t.run();
        } finally {
            IN_TICK.set(false);
        }
    }

    static final ThreadLocal<Boolean> IN_TICK = ThreadLocal.withInitial(() -> false);

    /**
     * One-shot hook run the next time the outlier tick (on the scheduler thread) calls
     * helper.getSynchronizationContext() — which publishPicker does right after writing
     * lastReadyHash and right before enqueueing the new picker. Lets a test interleave sync-context
     * work into exactly that window.
     */
    volatile Runnable onTickGetsSyncContext;

    void advanceMs(long ms) {
        now.addAndGet(ms * MS);
    }

    Update latest() {
        return helper.last;
    }

    PickResult pick(MethodDescriptor<?, ?> md) {
        return latest().picker.pickSubchannel(args(md));
    }

    @SuppressWarnings("unchecked")
    static List<Subchannel> readyPoolOf(SubchannelPicker p) {
        if (!(p instanceof P2CPicker)) return List.of();
        return (List<Subchannel>) getField(p, "readyPool");
    }

    // ------------------------------------------------------------------ discrete-event sim

    /** Latency + status model for one backend. */
    interface BackendModel {
        /** Returns the response for a call arriving at {@code nowNanos}. */
        Response respond(long nowNanos, String method);
    }

    record Response(long latencyNanos, Status status) {
        static Response ok(long latencyNanos) {
            return new Response(latencyNanos, Status.OK);
        }
    }

    static BackendModel constant(double latencyMs) {
        return (t, m) -> Response.ok((long) (latencyMs * MS));
    }

    static BackendModel jittered(double latencyMs, double jitterFrac, long seed) {
        java.util.Random r = new java.util.Random(seed);
        return (t, m) -> {
            double f = 1.0 + (r.nextDouble() * 2 - 1) * jitterFrac;
            return Response.ok((long) (latencyMs * f * MS));
        };
    }

    final Map<Integer, BackendModel> models = new HashMap<>();
    final Map<Integer, LongAdder> served = new ConcurrentHashMap<>();
    final Map<Integer, LongAdder> servedOk = new ConcurrentHashMap<>();
    final LongAdder noPick = new LongAdder();

    private record Pending(long at, ClientStreamTracer tracer, Status status, long seq)
            implements Comparable<Pending> {
        @Override
        public int compareTo(Pending o) {
            int c = Long.compare(at, o.at);
            return c != 0 ? c : Long.compare(seq, o.seq);
        }
    }

    private final PriorityQueue<Pending> pending = new PriorityQueue<>();
    private long seq;
    private long nextTickAt = Long.MIN_VALUE;

    /**
     * Open-loop traffic at {@code rps} for {@code durationMs} of simulated time. Every arrival is
     * picked through the live picker; the chosen backend's model decides latency/status; the real
     * tracer from the pick result receives streamCreated/streamClosed at the simulated times. The
     * outlier ticker fires every {@code tickMs}.
     */
    void run(double rps, long durationMs, MethodDescriptor<?, ?>... methods) {
        long end = now.get() + durationMs * MS;
        long gap = (long) (1e9 / rps);
        long nextArrival = now.get();
        long tickMs = 1000;
        if (nextTickAt == Long.MIN_VALUE) nextTickAt = now.get() + tickMs * MS;
        int rr = 0;
        while (true) {
            long nextCompletion = pending.isEmpty() ? Long.MAX_VALUE : pending.peek().at;
            long next = Math.min(Math.min(nextArrival, nextCompletion), nextTickAt);
            if (next >= end) break;
            now.set(next);
            if (next == nextCompletion) {
                Pending p = pending.poll();
                p.tracer.streamClosed(p.status);
            } else if (next == nextTickAt) {
                tick();
                nextTickAt += tickMs * MS;
            } else {
                MethodDescriptor<?, ?> md = methods[rr++ % methods.length];
                issue(md);
                nextArrival += gap;
            }
        }
        now.set(end);
    }

    /** Issues a single call now and schedules its completion. */
    void issue(MethodDescriptor<?, ?> md) {
        PickResult pr = pick(md);
        Subchannel sc = pr.getSubchannel();
        if (sc == null) {
            noPick.increment();
            return;
        }
        int port = portOf(sc);
        Response resp = models.get(port).respond(now.get(), md.getFullMethodName());
        served.computeIfAbsent(port, k -> new LongAdder()).increment();
        if (resp.status.isOk()) servedOk.computeIfAbsent(port, k -> new LongAdder()).increment();
        ClientStreamTracer tracer =
                pr.getStreamTracerFactory()
                        .newClientStreamTracer(
                                ClientStreamTracer.StreamInfo.newBuilder().build(), new Metadata());
        tracer.streamCreated(Attributes.EMPTY, new Metadata());
        pending.add(new Pending(now.get() + resp.latencyNanos, tracer, resp.status, seq++));
    }

    /** Drains every outstanding call (advancing the clock) without new arrivals or ticks. */
    void drain() {
        while (!pending.isEmpty()) {
            Pending p = pending.poll();
            now.set(Math.max(now.get(), p.at));
            p.tracer.streamClosed(p.status);
        }
    }

    void resetCounters() {
        served.clear();
        servedOk.clear();
        noPick.reset();
    }

    long served(int port) {
        LongAdder a = served.get(port);
        return a == null ? 0 : a.sum();
    }

    long totalServed() {
        return served.values().stream().mapToLong(LongAdder::sum).sum();
    }

    long totalOk() {
        return servedOk.values().stream().mapToLong(LongAdder::sum).sum();
    }

    double share(int port) {
        long t = totalServed();
        return t == 0 ? 0.0 : (double) served(port) / t;
    }

    static int portOf(Subchannel sc) {
        return ((InetSocketAddress) sc.getAddresses().getAddresses().get(0)).getPort();
    }

    // ------------------------------------------------------------------ gRPC fakes

    record Update(ConnectivityState state, SubchannelPicker picker) {}

    final class Helper extends LoadBalancer.Helper {
        final List<Update> updates = java.util.Collections.synchronizedList(new ArrayList<>());
        volatile Update last;
        final CapturingScheduler scheduler = new CapturingScheduler();
        final SynchronizationContext syncCtx =
                new SynchronizationContext((t, e) -> syncCtxErrors.add(e));

        @Override
        public Subchannel createSubchannel(LoadBalancer.CreateSubchannelArgs args) {
            syncCtx.throwIfNotInThisSynchronizationContext();
            FakeSubchannel sc = new FakeSubchannel(args.getAddresses(), syncCtx);
            byPort.put(portOf(sc), sc);
            return sc;
        }

        @Override
        public io.grpc.ManagedChannel createOobChannel(EquivalentAddressGroup eag, String a) {
            return new PeakEwmaP2CBalancerTest.NoopManagedChannel(a);
        }

        @Override
        public void updateBalancingState(ConnectivityState s, SubchannelPicker p) {
            syncCtx.throwIfNotInThisSynchronizationContext();
            Update u = new Update(s, p);
            updates.add(u);
            last = u;
        }

        @Override
        public SynchronizationContext getSynchronizationContext() {
            Runnable hook = onTickGetsSyncContext;
            if (hook != null && IN_TICK.get()) {
                onTickGetsSyncContext = null;
                hook.run();
            }
            return syncCtx;
        }

        @Override
        public ScheduledExecutorService getScheduledExecutorService() {
            return scheduler;
        }

        @Override
        public String getAuthority() {
            return "adversarial";
        }

        @Override
        public ChannelLogger getChannelLogger() {
            return new ChannelLogger() {
                @Override
                public void log(ChannelLogLevel level, String message) {}

                @Override
                public void log(ChannelLogLevel level, String message, Object... args) {}
            };
        }
    }

    /**
     * Mirrors ManagedChannelImpl.SubchannelImpl: requestConnection/shutdown must be called from the
     * synchronization context. Violations are counted rather than only thrown so tests can assert
     * on them even when the balancer swallows the exception.
     */
    static final class FakeSubchannel extends Subchannel {
        final EquivalentAddressGroup eag;
        final SynchronizationContext syncCtx;
        final AtomicInteger requestConnections = new AtomicInteger();
        final AtomicInteger syncCtxViolations = new AtomicInteger();
        volatile boolean shutdown;
        volatile LoadBalancer.SubchannelStateListener listener;

        FakeSubchannel(List<EquivalentAddressGroup> eags, SynchronizationContext syncCtx) {
            this.eag = eags.get(0);
            this.syncCtx = syncCtx;
        }

        private void checkCtx() {
            try {
                syncCtx.throwIfNotInThisSynchronizationContext();
            } catch (IllegalStateException e) {
                syncCtxViolations.incrementAndGet();
                throw e;
            }
        }

        @Override
        public void start(LoadBalancer.SubchannelStateListener l) {
            checkCtx();
            this.listener = l;
        }

        @Override
        public void requestConnection() {
            checkCtx();
            requestConnections.incrementAndGet();
        }

        @Override
        public void shutdown() {
            checkCtx();
            shutdown = true;
        }

        void deliver(ConnectivityStateInfo info) {
            listener.onSubchannelState(info);
        }

        @Override
        public List<EquivalentAddressGroup> getAllAddresses() {
            return List.of(eag);
        }

        @Override
        public Attributes getAttributes() {
            return Attributes.EMPTY;
        }
    }

    /** Captures the fixed-rate outlier task so tests can fire it deterministically. */
    static final class CapturingScheduler implements ScheduledExecutorService {
        volatile Runnable task;
        final AtomicInteger scheduleCount = new AtomicInteger();

        @Override
        public ScheduledFuture<?> scheduleAtFixedRate(
                Runnable command, long initialDelay, long period, TimeUnit unit) {
            task = command;
            scheduleCount.incrementAndGet();
            return new DoneFuture();
        }

        @Override
        public ScheduledFuture<?> scheduleWithFixedDelay(Runnable c, long i, long d, TimeUnit u) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ScheduledFuture<?> schedule(Runnable c, long d, TimeUnit u) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <V> ScheduledFuture<V> schedule(Callable<V> c, long d, TimeUnit u) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void shutdown() {}

        @Override
        public List<Runnable> shutdownNow() {
            return List.of();
        }

        @Override
        public boolean isShutdown() {
            return false;
        }

        @Override
        public boolean isTerminated() {
            return false;
        }

        @Override
        public boolean awaitTermination(long t, TimeUnit u) {
            return true;
        }

        @Override
        public <T> Future<T> submit(Callable<T> t) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> Future<T> submit(Runnable t, T r) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Future<?> submit(Runnable t) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> t) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> List<Future<T>> invokeAll(
                Collection<? extends Callable<T>> t, long l, TimeUnit u) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> T invokeAny(Collection<? extends Callable<T>> t) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> T invokeAny(Collection<? extends Callable<T>> t, long l, TimeUnit u) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void execute(Runnable c) {
            c.run();
        }
    }

    static final class DoneFuture implements ScheduledFuture<Object> {
        volatile boolean cancelled;

        @Override
        public long getDelay(TimeUnit unit) {
            return 0;
        }

        @Override
        public int compareTo(Delayed o) {
            return 0;
        }

        @Override
        public boolean cancel(boolean b) {
            cancelled = true;
            return true;
        }

        @Override
        public boolean isCancelled() {
            return cancelled;
        }

        @Override
        public boolean isDone() {
            return cancelled;
        }

        @Override
        public Object get() {
            return null;
        }

        @Override
        public Object get(long t, TimeUnit u) {
            return null;
        }
    }

    /** Counts ejections and keeps the last value of each gauge. */
    static class RecordingMetrics implements LbMetrics {
        final List<String> ejections = new CopyOnWriteArrayList<>();
        final Map<String, Double> tuning = new ConcurrentHashMap<>();

        @Override
        public void recordPick(String outcome) {}

        @Override
        public void setInflight(String subchannelId, int inflight) {}

        @Override
        public void setCost(String subchannelId, String method, double cost) {}

        @Override
        public void recordOutlierEjection(String sc, String reason, double er, double ratio) {
            ejections.add(
                    sc + " reason=" + reason + String.format(" err=%.2f ratio=%.2f", er, ratio));
        }

        @Override
        public void setReadySubchannelCount(int readyCount) {}

        @Override
        public void setEjectedSubchannelCount(int ejectedCount) {}

        @Override
        public void setAdaptiveTuning(String key, double value) {
            tuning.put(key, value);
        }

        @Override
        public void setMethodLatencyEwma(String method, double slow, double fast) {}

        @Override
        public void removeSubchannel(String subchannelId) {}

        @Override
        public void setMethodRate(String method, double ratePerSec) {}

        @Override
        public void setMethodErrorRate(String method, double errorRate) {}
    }

    // ------------------------------------------------------------------ misc helpers

    static PickSubchannelArgs args(MethodDescriptor<?, ?> md) {
        return new PickSubchannelArgs() {
            @Override
            public CallOptions getCallOptions() {
                return CallOptions.DEFAULT;
            }

            @Override
            public Metadata getHeaders() {
                return new Metadata();
            }

            @Override
            public MethodDescriptor<?, ?> getMethodDescriptor() {
                return md;
            }
        };
    }

    static MethodDescriptor<byte[], byte[]> method(String fullName) {
        MethodDescriptor.Marshaller<byte[]> m =
                new MethodDescriptor.Marshaller<>() {
                    @Override
                    public InputStream stream(byte[] value) {
                        return new ByteArrayInputStream(value);
                    }

                    @Override
                    public byte[] parse(InputStream stream) {
                        return new byte[0];
                    }
                };
        return MethodDescriptor.<byte[], byte[]>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(fullName)
                .setRequestMarshaller(m)
                .setResponseMarshaller(m)
                .build();
    }

    static void setField(Object target, String name, Object value) {
        try {
            Field f = findField(target.getClass(), name);
            f.setAccessible(true);
            f.set(target, value);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    static Object getField(Object target, String name) {
        try {
            Field f = findField(target.getClass(), name);
            f.setAccessible(true);
            return f.get(target);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private static Field findField(Class<?> c, String name) throws NoSuchFieldException {
        for (Class<?> k = c; k != null; k = k.getSuperclass()) {
            try {
                return k.getDeclaredField(name);
            } catch (NoSuchFieldException ignored) {
                // keep walking
            }
        }
        throw new NoSuchFieldException(name);
    }
}
