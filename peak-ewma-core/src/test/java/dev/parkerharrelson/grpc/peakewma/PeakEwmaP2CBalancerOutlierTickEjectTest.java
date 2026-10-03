package dev.parkerharrelson.grpc.peakewma;

import static org.junit.jupiter.api.Assertions.*;

import dev.parkerharrelson.grpc.peakewma.metrics.LbMetrics;
import dev.parkerharrelson.grpc.peakewma.outlier.ErrorWindow;
import io.grpc.Attributes;
import io.grpc.CallOptions;
import io.grpc.ChannelLogger;
import io.grpc.ClientCall;
import io.grpc.ConnectivityState;
import io.grpc.ConnectivityStateInfo;
import io.grpc.EquivalentAddressGroup;
import io.grpc.LoadBalancer;
import io.grpc.LoadBalancer.Subchannel;
import io.grpc.LoadBalancer.SubchannelPicker;
import io.grpc.ManagedChannel;
import io.grpc.MethodDescriptor;
import io.grpc.SynchronizationContext;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Delayed;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nonnull;
import org.junit.jupiter.api.Test;

class PeakEwmaP2CBalancerOutlierTickEjectTest {

    static final class FakeHelper extends LoadBalancer.Helper {
        final List<Update> updates = new ArrayList<>();
        final FakeScheduler scheduler = new FakeScheduler();
        final SynchronizationContext syncCtx =
                new SynchronizationContext(
                        (t, e) -> {
                            throw new AssertionError(e);
                        });
        final List<FakeSubchannel> created = new ArrayList<>();

        @Override
        public Subchannel createSubchannel(LoadBalancer.CreateSubchannelArgs args) {
            FakeSubchannel sc = new FakeSubchannel(args.getAddresses());
            created.add(sc);
            return sc;
        }

        @Override
        public ManagedChannel createOobChannel(EquivalentAddressGroup eag, String authority) {
            return new NoopManagedChannel(authority);
        }

        @Override
        public void updateBalancingState(
                @Nonnull ConnectivityState newState, @Nonnull SubchannelPicker newPicker) {
            updates.add(new Update(newState, newPicker));
        }

        @Override
        public SynchronizationContext getSynchronizationContext() {
            return syncCtx;
        }

        @Override
        public ScheduledExecutorService getScheduledExecutorService() {
            return scheduler;
        }

        @Override
        public String getAuthority() {
            return "";
        }

        @Override
        public ChannelLogger getChannelLogger() {
            return new ChannelLogger() {
                @Override
                public void log(ChannelLogLevel level, String message) {}

                @Override
                public void log(ChannelLogLevel level, String message, Object... params) {}
            };
        }
    }

    static final class NoopManagedChannel extends ManagedChannel {
        private final String authority;

        NoopManagedChannel(String authority) {
            this.authority = authority == null ? "" : authority;
        }

        @Override
        public String authority() {
            return authority;
        }

        @Override
        public ManagedChannel shutdown() {
            return this;
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
        public ManagedChannel shutdownNow() {
            return this;
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) {
            return true;
        }

        @Override
        public <ReqT, RespT> ClientCall<ReqT, RespT> newCall(
                MethodDescriptor<ReqT, RespT> methodDescriptor, CallOptions callOptions) {
            throw new UnsupportedOperationException("noop");
        }

        @Override
        public ConnectivityState getState(boolean requestConnection) {
            return ConnectivityState.CONNECTING;
        }

        @Override
        public void notifyWhenStateChanged(ConnectivityState source, Runnable callback) {}

        @Override
        public void resetConnectBackoff() {}

        @Override
        public void enterIdle() {}
    }

    static final class FakeScheduler implements ScheduledExecutorService {
        Runnable lastFixedRateTask;
        long lastInitialDelayMs;
        long lastPeriodMs;
        FakeFuture lastFuture = new FakeFuture();

        @Override
        public ScheduledFuture<?> scheduleAtFixedRate(
                Runnable command, long initialDelay, long period, TimeUnit unit) {
            lastFixedRateTask = command;
            lastInitialDelayMs = unit.toMillis(initialDelay);
            lastPeriodMs = unit.toMillis(period);
            lastFuture = new FakeFuture();
            return lastFuture;
        }

        @Override
        public ScheduledFuture<?> scheduleWithFixedDelay(Runnable c, long a, long b, TimeUnit u) {
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
        public <T> Future<T> submit(Callable<T> task) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> Future<T> submit(Runnable task, T r) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Future<?> submit(Runnable task) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> List<Future<T>> invokeAll(
                Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> T invokeAny(Collection<? extends Callable<T>> tasks) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> T invokeAny(
                Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void execute(Runnable command) {
            command.run();
        }
    }

    static final class FakeFuture implements ScheduledFuture<Object> {
        boolean cancelled = false;

        @Override
        public long getDelay(TimeUnit unit) {
            return 0;
        }

        @Override
        public int compareTo(Delayed o) {
            return 0;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
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
        public Object get(long timeout, TimeUnit unit) {
            return null;
        }
    }

    static final class FakeSubchannel extends Subchannel {
        final List<EquivalentAddressGroup> eags;
        LoadBalancer.SubchannelStateListener listener;
        final AtomicInteger reqConn = new AtomicInteger();
        final AtomicInteger shutdowns = new AtomicInteger();

        FakeSubchannel(List<EquivalentAddressGroup> eags) {
            this.eags = List.copyOf(eags);
        }

        @Override
        public List<EquivalentAddressGroup> getAllAddresses() {
            return eags;
        }

        @Override
        public Attributes getAttributes() {
            return Attributes.EMPTY;
        }

        @Override
        public void requestConnection() {
            reqConn.incrementAndGet();
        }

        @Override
        public void shutdown() {
            shutdowns.incrementAndGet();
        }

        @Override
        public void start(LoadBalancer.SubchannelStateListener listener) {
            this.listener = listener;
        }

        void drive() {
            assertNotNull(listener, "listener not set");
            listener.onSubchannelState(ConnectivityStateInfo.forNonError(ConnectivityState.READY));
        }
    }

    static final class Update {
        final ConnectivityState state;
        final SubchannelPicker picker;

        Update(ConnectivityState s, SubchannelPicker p) {
            this.state = s;
            this.picker = p;
        }
    }

    static final class CapturingMetrics implements LbMetrics {
        String lastOutlierSubchannel;
        String lastOutlierReason;
        double lastOutlierErrRate;
        double lastOutlierLatencyRatio;

        int readyCount;
        int ejectedCount;

        @Override
        public void recordPick(String outcome) {}

        @Override
        public void setInflight(String subchannelId, int inflightVal) {}

        @Override
        public void setCost(String subchannelId, String method, double c) {}

        @Override
        public void recordOutlierEjection(
                String subchannelId, String reason, double errorRate, double latencyRatio) {
            lastOutlierSubchannel = subchannelId;
            lastOutlierReason = reason;
            lastOutlierErrRate = errorRate;
            lastOutlierLatencyRatio = latencyRatio;
        }

        @Override
        public void setReadySubchannelCount(int readyCount) {
            this.readyCount = readyCount;
        }

        @Override
        public void setEjectedSubchannelCount(int ejectedCount) {
            this.ejectedCount = ejectedCount;
        }

        @Override
        public void setAdaptiveTuning(String key, double value) {}

        @Override
        public void setMethodLatencyEwma(
                String method, double slowEwmaMicros, double fastEwmaMicros) {}

        @Override
        public void removeSubchannel(String subchannelId) {}

        @Override
        public void setMethodRate(String method, double ratePerSec) {}

        @Override
        public void setMethodErrorRate(String method, double errorRate) {}
    }

    private static void setAtomicLongFieldValue(Object target, long value) throws Exception {
        Field f = MethodStats.class.getDeclaredField("firstSampleNanos");
        f.setAccessible(true);
        Object obj = f.get(target);
        AtomicLong al;
        if (obj == null) {
            al = new AtomicLong();
            f.set(target, al);
        } else {
            al = (AtomicLong) obj;
        }
        al.set(value);
    }

    private static void setAtomicIntFieldValue(Object target) throws Exception {
        Field f = MethodStats.class.getDeclaredField("samples");
        f.setAccessible(true);
        Object obj = f.get(target);
        AtomicInteger ai;
        if (obj == null) {
            ai = new AtomicInteger();
            f.set(target, ai);
        } else {
            ai = (AtomicInteger) obj;
        }
        ai.set(5000);
    }

    private static void setVolatileLongField(Object target, long value) throws Exception {
        Field f = MethodStats.class.getDeclaredField("lastUpdateNanos");
        f.setAccessible(true);
        f.setLong(target, value);
    }

    private static void setVolatileDoubleField(Object target, String fieldName, double value)
            throws Exception {
        Field f = MethodStats.class.getDeclaredField(fieldName);
        f.setAccessible(true);
        f.setDouble(target, value);
    }

    @SuppressWarnings("unchecked")
    private static <K, V> Map<K, V> getPrivateMap(Object target, String field) throws Exception {
        Field f = target.getClass().getDeclaredField(field);
        f.setAccessible(true);
        return (Map<K, V>) f.get(target);
    }

    @SuppressWarnings("unchecked")
    private static <T> T invokePrivate(Object target, String name, Class<?>[] sig, Object... args)
            throws Exception {
        Method m = target.getClass().getDeclaredMethod(name, sig);
        m.setAccessible(true);
        return (T) m.invoke(target, args);
    }

    @Test
    void outlierTick_processesReadySubchannel_andUpdatesMetrics_withoutThrowing() throws Exception {
        FakeHelper helper = new FakeHelper();
        CapturingMetrics metrics = new CapturingMetrics();

        PeakEwmaConfig cfg =
                PeakEwmaConfig.builder()
                        .outlierEnabled(true)
                        .outlierWindowMillis(1_000)
                        .outlierErrorRate(0.10)
                        .outlierEjectMillis(5_000)
                        .outlierReentryCooldownMillis(0)
                        .staleMillisForRatio(60_000)
                        .build();

        PeakEwmaP2CBalancer balancer = new PeakEwmaP2CBalancer(helper, cfg, metrics);

        var addr = new EquivalentAddressGroup(new InetSocketAddress("10.0.0.1", 1234));
        LoadBalancer.ResolvedAddresses ra =
                LoadBalancer.ResolvedAddresses.newBuilder().setAddresses(List.of(addr)).build();
        balancer.handleResolvedAddresses(ra);

        assertEquals(1, helper.created.size(), "expected exactly one subchannel created");
        FakeSubchannel sc = helper.created.get(0);

        sc.drive();

        Map<Subchannel, MethodTable> tables = getPrivateMap(balancer, "tables");
        Map<Subchannel, SubchannelState> states = getPrivateMap(balancer, "states");
        Map<Subchannel, String> subIds = getPrivateMap(balancer, "subchannelIds");

        MethodTable mt = tables.get(sc);
        SubchannelState st = states.get(sc);
        String scId = subIds.get(sc);

        assertNotNull(mt, "method table should exist");
        assertNotNull(st, "subchannel state should exist");
        assertNotNull(scId, "subchannel id should exist");

        long nowNanos = System.nanoTime();
        String methodName = "svc/Foo";
        MethodStats ms = mt.statsFor(methodName);
        mt.windowFor(methodName);

        setAtomicLongFieldValue(ms, nowNanos - TimeUnit.SECONDS.toNanos(30));
        setVolatileLongField(ms, nowNanos - TimeUnit.MILLISECONDS.toNanos(10));
        setAtomicIntFieldValue(ms);
        setVolatileDoubleField(ms, "ewmaFastMicros", 200.0);
        setVolatileDoubleField(ms, "ewmaSlowMicros", 400.0);

        ErrorWindow ew = new ErrorWindow(1_000L);
        for (int i = 0; i < 1000; i++) {
            boolean ok = (i % 3) != 0;
            ew.recordResult(ok, nowNanos);
        }
        Field winsField = MethodTable.class.getDeclaredField("methodWindows");
        winsField.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<String, ErrorWindow> winMap = (Map<String, ErrorWindow>) winsField.get(mt);
        winMap.put(methodName, ew);

        assertFalse(st.isEjected(nowNanos), "precondition: subchannel should start non-ejected");

        assertDoesNotThrow(
                () -> invokePrivate(balancer, "outlierTick", new Class<?>[] {}),
                "outlierTick should not throw");

        assertTrue(
                metrics.readyCount >= 1,
                "metrics.readyCount should reflect that at least one subchannel was READY");

        boolean nowEjected = st.isEjected(System.nanoTime());

        if (nowEjected) {
            assertEquals(
                    scId,
                    metrics.lastOutlierSubchannel,
                    "if ejected, metrics should capture subchannel id");
            assertNotNull(
                    metrics.lastOutlierReason, "if ejected, metrics.reason should be populated");
        } else {
            // If we didn't eject:
            assertNull(
                    metrics.lastOutlierSubchannel,
                    "if not ejected, metrics.lastOutlierSubchannel should still be null");
        }
    }
}
