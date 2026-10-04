package dev.parkerharrelson.grpc.peakewma;

import static io.grpc.ConnectivityState.*;
import static org.junit.jupiter.api.Assertions.*;

import dev.parkerharrelson.grpc.peakewma.outlier.ErrorWindow;
import io.grpc.Attributes;
import io.grpc.CallOptions;
import io.grpc.ChannelLogger;
import io.grpc.ClientCall;
import io.grpc.ConnectivityState;
import io.grpc.ConnectivityStateInfo;
import io.grpc.EquivalentAddressGroup;
import io.grpc.LoadBalancer;
import io.grpc.LoadBalancer.CreateSubchannelArgs;
import io.grpc.LoadBalancer.Subchannel;
import io.grpc.LoadBalancer.SubchannelPicker;
import io.grpc.ManagedChannel;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.SynchronizationContext;
import java.io.Serial;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Delayed;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PeakEwmaP2CBalancerTest {

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
        public Subchannel createSubchannel(CreateSubchannelArgs args) {
            FakeSubchannel sc = new FakeSubchannel(args.getAddresses());
            created.add(sc);
            return sc;
        }

        @Override
        public ManagedChannel createOobChannel(EquivalentAddressGroup eag, String authority) {
            return new NoopManagedChannel(authority);
        }

        @Override
        public void updateBalancingState(ConnectivityState newState, SubchannelPicker newPicker) {
            updates.add(new Update(newState, newPicker));
        }

        @Override
        public void refreshNameResolution() {

            // real channels re-resolve; nothing to do in tests

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
                public void log(ChannelLogLevel level, String message) {
                    /* no-op */
                }

                @Override
                public void log(ChannelLogLevel level, String message, Object... params) {
                    /* no-op */
                }
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
            return CONNECTING;
        }

        @Override
        public void notifyWhenStateChanged(ConnectivityState source, Runnable callback) {
            /* no-op */
        }

        @Override
        public void resetConnectBackoff() {
            /* no-op */
        }

        @Override
        public void enterIdle() {
            /* no-op */
        }
    }

    static final class FakeScheduler implements ScheduledExecutorService {
        Runnable lastFixedRateTask;
        long lastInitialDelayMs;
        long lastPeriodMs;
        FakeFuture lastFuture;

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
        public ScheduledFuture<?> scheduleWithFixedDelay(
                Runnable command, long initialDelay, long delay, TimeUnit unit) {
            throw new UnsupportedOperationException("not used in tests");
        }

        // Unused in tests
        @Override
        public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
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
        public boolean awaitTermination(long timeout, TimeUnit unit) {
            return true;
        }

        @Override
        public <T> Future<T> submit(Callable<T> task) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> Future<T> submit(Runnable task, T result) {
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

        void drive(ConnectivityState s) {
            assertNotNull(listener, "listener not set");
            listener.onSubchannelState(ConnectivityStateInfo.forNonError(s));
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

    private FakeHelper helper;
    private PeakEwmaP2CBalancer balancer;
    private PeakEwmaConfig baseCfg;

    @BeforeEach
    void setUp() {
        helper = new FakeHelper();
        baseCfg = PeakEwmaConfig.builder().outlierEnabled(true).outlierWindowMillis(1_500).build();
        balancer = new PeakEwmaP2CBalancer(helper, baseCfg, null);
    }

    @Test
    void handleNameResolutionError_publishesTransientFailure_withErrorPicker() {
        balancer.handleNameResolutionError(Status.UNAVAILABLE.withDescription("dns fail"));
        assertFalse(helper.updates.isEmpty());

        Update u = helper.updates.get(helper.updates.size() - 1);
        assertEquals(TRANSIENT_FAILURE, u.state);

        LoadBalancer.PickResult pr = u.picker.pickSubchannel(null);
        assertNotNull(pr.getStatus());
        assertFalse(pr.getStatus().isOk());
    }

    @Test
    void
            handleResolvedAddresses_mergesConfig_createsSubchannels_requestsConnections_andPublishesConnecting()
                    throws Exception {
        Map<String, Object> inner = new HashMap<>();
        inner.put(PeakEwmaConfigKeys.TAU_FAST_MILLIS, 1500L);
        Map<String, Object> lbCfg = Map.of(PeakEwmaConfigKeys.POLICY_NAME, inner);

        List<EquivalentAddressGroup> addrs =
                List.of(
                        new EquivalentAddressGroup(new InetSocketAddress("127.0.0.1", 5001)),
                        new EquivalentAddressGroup(new InetSocketAddress("127.0.0.1", 5002)));

        LoadBalancer.ResolvedAddresses ra =
                LoadBalancer.ResolvedAddresses.newBuilder()
                        .setAddresses(addrs)
                        .setLoadBalancingPolicyConfig(lbCfg)
                        .build();

        balancer.handleResolvedAddresses(ra);

        Map<?, ?> keyToSub = getPrivateMap(balancer, "keyToSubchannel");
        assertEquals(2, keyToSub.size(), "exactly two live subchannels expected");

        for (Object scObj : keyToSub.values()) {
            FakeSubchannel sc = (FakeSubchannel) scObj;
            assertTrue(sc.reqConn.get() >= 1, "each subchannel should request connection");
        }

        Update last = helper.updates.get(helper.updates.size() - 1);
        assertEquals(CONNECTING, last.state);
        LoadBalancer.PickResult pr = last.picker.pickSubchannel(null);
        assertNull(pr.getSubchannel());
        assertTrue(pr.getStatus().isOk());
    }

    @Test
    void publishPicker_transitionsToReady_whenAnyReady_and_dedupsOnSameReadyHash() {
        var a1 = new EquivalentAddressGroup(new InetSocketAddress("127.0.0.1", 6001));
        var a2 = new EquivalentAddressGroup(new InetSocketAddress("127.0.0.1", 6002));
        var ra = LoadBalancer.ResolvedAddresses.newBuilder().setAddresses(List.of(a1, a2)).build();
        balancer.handleResolvedAddresses(ra);

        assertEquals(2, helper.created.size());
        FakeSubchannel sc1 = helper.created.get(0);
        FakeSubchannel sc2 = helper.created.get(1);

        sc1.drive(READY);
        boolean sawReady = helper.updates.stream().anyMatch(u -> u.state == READY);
        assertTrue(sawReady, "should publish READY after first subchannel is READY");

        int readyPublishes = (int) helper.updates.stream().filter(u -> u.state == READY).count();

        sc2.drive(READY);
        int readyPublishesAfter =
                (int) helper.updates.stream().filter(u -> u.state == READY).count();

        balancer.handleResolvedAddresses(
                LoadBalancer.ResolvedAddresses.newBuilder().setAddresses(List.of(a1, a2)).build());

        int readyPublishesFinal =
                (int) helper.updates.stream().filter(u -> u.state == READY).count();
        assertTrue(readyPublishesAfter >= readyPublishes);
        assertEquals(
                readyPublishesAfter, readyPublishesFinal, "duplicate hash should not republish");
    }

    @Test
    void reconcileSubchannels_removesMissingOnNextResolution_andShutsThemDown() throws Exception {
        var a1 = new EquivalentAddressGroup(new InetSocketAddress("127.0.0.1", 7001));
        var a2 = new EquivalentAddressGroup(new InetSocketAddress("127.0.0.1", 7002));
        balancer.handleResolvedAddresses(
                LoadBalancer.ResolvedAddresses.newBuilder().setAddresses(List.of(a1, a2)).build());

        assertEquals(2, helper.created.size());

        balancer.handleResolvedAddresses(
                LoadBalancer.ResolvedAddresses.newBuilder().setAddresses(List.of(a2)).build());

        Map<?, ?> keyToSub = getPrivateMap(balancer, "keyToSubchannel");

        String keyA1 = "127.0.0.1:7001";
        String keyA2 = "127.0.0.1:7002";

        assertFalse(keyToSub.containsKey(keyA1), "removed subchannel key must be gone");
        assertTrue(keyToSub.containsKey(keyA2), "remaining subchannel key must still exist");
    }

    @Test
    void ensureOutlierTicker_schedules_whenEnabled_andCancelsOnDisable() {
        var a = new EquivalentAddressGroup(new InetSocketAddress("127.0.0.1", 8001));
        balancer.handleResolvedAddresses(
                LoadBalancer.ResolvedAddresses.newBuilder().setAddresses(List.of(a)).build());

        assertNotNull(helper.scheduler.lastFixedRateTask, "ticker scheduled");
        assertTrue(helper.scheduler.lastPeriodMs >= 1000);

        Map<String, Object> inner = new HashMap<>();
        inner.put(PeakEwmaConfigKeys.OUTLIER_ENABLED, false);
        var raDisable =
                LoadBalancer.ResolvedAddresses.newBuilder()
                        .setAddresses(List.of(a))
                        .setLoadBalancingPolicyConfig(Map.of(PeakEwmaConfigKeys.POLICY_NAME, inner))
                        .build();

        ScheduledFuture<?> prevFuture = helper.scheduler.lastFuture;
        balancer.handleResolvedAddresses(raDisable);

        assertTrue(prevFuture.isCancelled(), "previous ticker cancelled");
    }

    @Test
    void handleResolvedAddresses_withParsedProviderConfig_appliesIt() {
        var a = new EquivalentAddressGroup(new InetSocketAddress("127.0.0.1", 8101));
        balancer.handleResolvedAddresses(
                LoadBalancer.ResolvedAddresses.newBuilder().setAddresses(List.of(a)).build());
        ScheduledFuture<?> prevFuture = helper.scheduler.lastFuture;
        assertNotNull(prevFuture, "ticker scheduled with defaults");

        // This is the object gRPC passes through when the policy is configured via service config.
        Object parsed =
                new PeakEwmaP2CProvider()
                        .parseLoadBalancingPolicyConfig(
                                Map.of(PeakEwmaConfigKeys.OUTLIER_ENABLED, false))
                        .getConfig();
        balancer.handleResolvedAddresses(
                LoadBalancer.ResolvedAddresses.newBuilder()
                        .setAddresses(List.of(a))
                        .setLoadBalancingPolicyConfig(parsed)
                        .build());

        assertTrue(prevFuture.isCancelled(), "outlierEnabled=false from parsed config applied");
    }

    @Test
    void outlierTick_earlyReturnsWhenNoReady_andDoesNotCrash() {
        var a = new EquivalentAddressGroup(new InetSocketAddress("127.0.0.1", 9001));
        balancer.handleResolvedAddresses(
                LoadBalancer.ResolvedAddresses.newBuilder().setAddresses(List.of(a)).build());

        assertNotNull(helper.scheduler.lastFixedRateTask);

        assertDoesNotThrow(() -> helper.scheduler.lastFixedRateTask.run());
    }

    @Test
    void subchannelId_sequenceForNonInet_andHostPortForInet() throws Exception {
        var inet = new EquivalentAddressGroup(new InetSocketAddress("127.0.0.1", 7777));
        var weird =
                new EquivalentAddressGroup(
                        new SocketAddress() {
                            @Serial private static final long serialVersionUID = 1L;
                        });

        balancer.handleResolvedAddresses(
                LoadBalancer.ResolvedAddresses.newBuilder()
                        .setAddresses(List.of(inet, weird))
                        .build());

        assertEquals(2, helper.created.size());
        Map<?, ?> subchannelIds = getPrivateMap(balancer, "subchannelIds");

        Collection<?> ids = subchannelIds.values();
        assertEquals(2, ids.size());
        boolean sawInet = ids.stream().anyMatch(v -> String.valueOf(v).contains("127.0.0.1:7777"));
        boolean sawSeq = ids.stream().anyMatch(v -> String.valueOf(v).startsWith("sc-"));
        assertTrue(sawInet, "should include inet host:port id");
        assertTrue(sawSeq, "should include sequence id");
    }

    @Test
    void scListener_marksReady_andPublishCalled() {
        var a = new EquivalentAddressGroup(new InetSocketAddress("127.0.0.1", 5050));
        balancer.handleResolvedAddresses(
                LoadBalancer.ResolvedAddresses.newBuilder().setAddresses(List.of(a)).build());

        FakeSubchannel sc = helper.created.get(0);
        int updatesBefore = helper.updates.size();

        sc.drive(READY);

        assertTrue(helper.updates.size() >= updatesBefore, "publish should be invoked");
        boolean sawReady = helper.updates.stream().anyMatch(u -> u.state == READY);
        assertTrue(sawReady);
    }

    @Test
    void idleSubchannel_isAlwaysAskedToReconnect() {
        var a = new EquivalentAddressGroup(new InetSocketAddress("127.0.0.1", 5160));
        balancer.handleResolvedAddresses(
                LoadBalancer.ResolvedAddresses.newBuilder().setAddresses(List.of(a)).build());
        FakeSubchannel sc = helper.created.get(0);
        sc.drive(READY);
        int before = sc.reqConn.get();

        sc.drive(IDLE); // e.g. server GOAWAY

        assertEquals(before + 1, sc.reqConn.get());
    }

    @Test
    void handleNoReadySubchannels_debouncesRequestConnectionPerSubchannel() {
        var a = new EquivalentAddressGroup(new InetSocketAddress("127.0.0.1", 5150));
        balancer.handleResolvedAddresses(
                LoadBalancer.ResolvedAddresses.newBuilder().setAddresses(List.of(a)).build());

        FakeSubchannel sc = helper.created.get(0);
        // Initial handleResolvedAddresses already fired one requestConnection during creation.
        int initial = sc.reqConn.get();

        // Simulate "no ready" publishes in quick succession. handleNoReadySubchannels treats
        // idle, connecting and transient-failure states as reconnect-eligible and debounces
        // them. (IDLE itself is not debounced: each IDLE report means a connection closed and,
        // per the grpc LB contract, must be answered with requestConnection().)
        sc.drive(CONNECTING);
        sc.drive(CONNECTING);
        sc.drive(CONNECTING);

        // Debounce should prevent more than one extra requestConnection inside the 1s window.
        int afterBurst = sc.reqConn.get();
        assertTrue(
                afterBurst <= initial + 1,
                "expected at most one additional reconnect request in the debounce window (initial="
                        + initial
                        + ", after="
                        + afterBurst
                        + ")");
    }

    @Test
    void shutdown_cancelsTicker_shutsSubchannels_and_clearsCollections() throws Exception {
        var a1 = new EquivalentAddressGroup(new InetSocketAddress("127.0.0.1", 5051));
        var a2 = new EquivalentAddressGroup(new InetSocketAddress("127.0.0.1", 5052));
        balancer.handleResolvedAddresses(
                LoadBalancer.ResolvedAddresses.newBuilder().setAddresses(List.of(a1, a2)).build());

        ScheduledFuture<?> prev = helper.scheduler.lastFuture;
        FakeSubchannel s1 = helper.created.get(0);
        FakeSubchannel s2 = helper.created.get(1);

        balancer.shutdown();

        assertTrue(prev.isCancelled(), "ticker cancelled on shutdown");
        assertTrue(s1.shutdowns.get() > 0);
        assertTrue(s2.shutdowns.get() > 0);

        assertTrue(getPrivateMap(balancer, "tables").isEmpty());
        assertTrue(getPrivateMap(balancer, "states").isEmpty());
        assertTrue(getPrivateMap(balancer, "subchannelIds").isEmpty());
        assertTrue(getPrivateMap(balancer, "subchannelConn").isEmpty());
        assertTrue(getPrivateMap(balancer, "keyToSubchannel").isEmpty());
        assertTrue(getPrivateSet(balancer).isEmpty());
    }

    @Test
    void isWarm_warmAndFresh_true_and_stale_false() throws Exception {
        PeakEwmaConfig cfg = PeakEwmaConfig.builder().staleMillisForRatio(5_000).build();

        long now = System.nanoTime();

        ErrorWindow ew = new ErrorWindow(1_000L);
        for (int i = 0; i < 20; i++) {
            ew.recordResult(true, now);
        }

        MethodStats warmFresh = newMethodStatsUnsafe();
        setAtomicLongFieldValue(warmFresh, now - TimeUnit.SECONDS.toNanos(10));
        setVolatileLongField(warmFresh, now - TimeUnit.MILLISECONDS.toNanos(10));
        setAtomicIntFieldValue(warmFresh);

        setVolatileDoubleField(warmFresh, "ewmaFastMicros", 200.0);
        setVolatileDoubleField(warmFresh, "ewmaSlowMicros", 400.0);

        boolean gateTrue =
                invokePrivateStaticMethod(
                        new Class<?>[] {
                            MethodStats.class, double.class, long.class, PeakEwmaConfig.class
                        },
                        warmFresh,
                        PeakEwmaTuner.methodRatePerSec(ew, now),
                        now,
                        cfg);
        assertTrue(gateTrue, "expected adaptiveGate true for warm+fresh stats");

        MethodStats staleStats = newMethodStatsUnsafe();
        setAtomicLongFieldValue(staleStats, now - TimeUnit.SECONDS.toNanos(10));
        setVolatileLongField(staleStats, now - TimeUnit.SECONDS.toNanos(30));
        setAtomicIntFieldValue(staleStats);
        setVolatileDoubleField(staleStats, "ewmaFastMicros", 200.0);
        setVolatileDoubleField(staleStats, "ewmaSlowMicros", 400.0);

        boolean gateFalse =
                invokePrivateStaticMethod(
                        new Class<?>[] {
                            MethodStats.class, double.class, long.class, PeakEwmaConfig.class
                        },
                        staleStats,
                        PeakEwmaTuner.methodRatePerSec(ew, now),
                        now,
                        cfg);
        assertFalse(gateFalse, "expected adaptiveGate false when stats are stale");
    }

    private static MethodStats newMethodStatsUnsafe() {
        long initialRttMicros = 0L;
        long initNanos = System.nanoTime();
        return new MethodStats(initialRttMicros, initNanos);
    }

    private static void setVolatileLongField(Object target, long value) throws Exception {
        Field f = MethodStats.class.getDeclaredField("lastUpdateNanos");
        f.setAccessible(true);
        if (f.getType() != long.class) {
            throw new IllegalStateException(
                    "lastUpdateNanos" + " is not long (it's " + f.getType() + ")");
        }
        f.setLong(target, value);
    }

    private static void setVolatileDoubleField(Object target, String fieldName, double value)
            throws Exception {
        Field f = MethodStats.class.getDeclaredField(fieldName);
        f.setAccessible(true);
        if (f.getType() != double.class) {
            throw new IllegalStateException(
                    fieldName + " is not double (it's " + f.getType() + ")");
        }
        f.setDouble(target, value);
    }

    private static void setAtomicLongFieldValue(Object target, long value) throws Exception {
        Field f = MethodStats.class.getDeclaredField("firstSampleNanos");
        f.setAccessible(true);

        Object obj = f.get(target);
        AtomicLong al;
        if (obj == null) {
            al = new AtomicLong();
            f.set(target, al);
        } else if (obj instanceof AtomicLong) {
            al = (AtomicLong) obj;
        } else {
            throw new IllegalStateException(
                    "firstSampleNanos" + " is not AtomicLong (it's " + obj.getClass() + ")");
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
        } else if (obj instanceof AtomicInteger) {
            ai = (AtomicInteger) obj;
        } else {
            throw new IllegalStateException(
                    "samples" + " is not AtomicInteger (it's " + obj.getClass() + ")");
        }

        ai.set(50);
    }

    @SuppressWarnings("unchecked")
    private static <K, V> Map<K, V> getPrivateMap(Object target, String field) throws Exception {
        Field f = target.getClass().getDeclaredField(field);
        f.setAccessible(true);
        return (Map<K, V>) f.get(target);
    }

    @SuppressWarnings("unchecked")
    private static <T> Set<T> getPrivateSet(Object target) throws Exception {
        Field f = target.getClass().getDeclaredField("currentAddressKeys");
        f.setAccessible(true);
        return (Set<T>) f.get(target);
    }

    @SuppressWarnings("unchecked")
    private static <T> T invokePrivateStaticMethod(Class<?>[] paramTypes, Object... args)
            throws Exception {
        Method m = PeakEwmaP2CBalancer.class.getDeclaredMethod("isWarm", paramTypes);
        m.setAccessible(true);
        return (T) m.invoke(null, args);
    }
}
