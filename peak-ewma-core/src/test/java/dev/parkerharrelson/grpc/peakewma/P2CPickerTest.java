package dev.parkerharrelson.grpc.peakewma;

import static org.junit.jupiter.api.Assertions.*;

import dev.parkerharrelson.grpc.peakewma.metrics.NoopLbMetrics;
import dev.parkerharrelson.grpc.peakewma.tracing.EwmaClientStreamTracerFactory;
import io.grpc.Attributes;
import io.grpc.CallOptions;
import io.grpc.ClientStreamTracer;
import io.grpc.EquivalentAddressGroup;
import io.grpc.LoadBalancer;
import io.grpc.LoadBalancer.PickResult;
import io.grpc.LoadBalancer.Subchannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class P2CPickerTest {

    static final class FakeSubchannel extends Subchannel {
        private final List<EquivalentAddressGroup> eags;

        FakeSubchannel(int port) {
            this.eags =
                    List.of(new EquivalentAddressGroup(new InetSocketAddress("127.0.0.1", port)));
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
        public void requestConnection() {}

        @Override
        public void shutdown() {}

        @Override
        public void start(LoadBalancer.SubchannelStateListener listener) {}

        @Override
        public String toString() {
            return "sc:" + eags.get(0).getAddresses().get(0);
        }
    }

    static final class TimeHarness {
        final AtomicLong now = new AtomicLong(TimeUnit.MILLISECONDS.toNanos(1_000));
        final EwmaClocks clocks = new EwmaClocks(now::get);

        long nowNanos() {
            return now.get();
        }
    }

    static final class ByteMarshaller implements MethodDescriptor.Marshaller<byte[]> {
        @Override
        public InputStream stream(byte[] value) {
            return new java.io.ByteArrayInputStream(value == null ? new byte[0] : value);
        }

        @Override
        public byte[] parse(InputStream stream) {
            try {
                return stream.readAllBytes();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private TimeHarness t;
    private PeakEwmaConfig cfg;

    @BeforeEach
    void setUp() {
        t = new TimeHarness();
        cfg =
                PeakEwmaConfig.builder()
                        .tauFastMillis(1_000)
                        .tauSlowMillis(30_000)
                        .initialRttMicros(50_000)
                        .inflightWeight(0.1)
                        .staleMillisForRatio(30_000)
                        .outlierEnabled(true)
                        .outlierWindowMillis(15_000)
                        .outlierErrorRate(0.20)
                        .outlierEjectMillis(10_000)
                        .outlierLatencyMultiplier(2.5)
                        .build();
    }

    private static MethodDescriptor<byte[], byte[]> md(String fullName) {
        return MethodDescriptor.<byte[], byte[]>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(fullName)
                .setRequestMarshaller(new ByteMarshaller())
                .setResponseMarshaller(new ByteMarshaller())
                .build();
    }

    private static LoadBalancer.PickSubchannelArgs args(MethodDescriptor<?, ?> md) {
        return new LoadBalancer.PickSubchannelArgs() {
            @Override
            public MethodDescriptor<?, ?> getMethodDescriptor() {
                return md;
            }

            @Override
            public Metadata getHeaders() {
                return new Metadata();
            }

            @Override
            public CallOptions getCallOptions() {
                return CallOptions.DEFAULT;
            }
        };
    }

    private static void sample(
            TimeHarness t, MethodTable table, String method, PeakEwmaConfig cfg, long rttMicros) {
        MethodStats ms = table.statsFor(method);
        long start = t.nowNanos();
        long rttNanos = TimeUnit.MICROSECONDS.toNanos(rttMicros);
        ms.update(start + rttNanos, rttNanos, cfg);
    }

    @Test
    void emptyPool_returnsNoResult() {
        P2CPicker picker =
                new P2CPicker(List.of(), Map.of(), Map.of(), cfg, t.clocks, NoopLbMetrics.INSTANCE);
        PickResult pr = picker.pickSubchannel(args(md("svc/Method")));
        assertNull(pr.getSubchannel());
        assertTrue(pr.getStatus().isOk()); // withNoResult
    }

    @Test
    void singleSubchannel_withoutTable_returnsNoResult_fromBuildPickResultGuard() {
        FakeSubchannel sc = new FakeSubchannel(5001);
        P2CPicker picker =
                new P2CPicker(
                        List.of(sc), Map.of(), Map.of(), cfg, t.clocks, NoopLbMetrics.INSTANCE);
        PickResult pr = picker.pickSubchannel(args(md("svc/A")));
        assertNull(pr.getSubchannel());
        assertTrue(pr.getStatus().isOk());
    }

    @Test
    void singleSubchannel_withTableAndState_returnsSubchannel_andTracerFactoryIncrementsInflight() {
        FakeSubchannel sc = new FakeSubchannel(5002);
        MethodTable table = new MethodTable(cfg, t.clocks);
        SubchannelState st = new SubchannelState();
        st.markReady(t.nowNanos());

        Map<Subchannel, MethodTable> tables = new HashMap<>();
        tables.put(sc, table);
        Map<Subchannel, SubchannelState> states = Map.of(sc, st);

        sample(t, table, "svc/A", cfg, 10_000);

        P2CPicker picker =
                new P2CPicker(List.of(sc), tables, states, cfg, t.clocks, NoopLbMetrics.INSTANCE);

        PickResult pr = picker.pickSubchannel(args(md("svc/A")));
        assertNotNull(pr.getSubchannel());
        assertSame(sc, pr.getSubchannel());
        assertNotNull(pr.getStreamTracerFactory());
        assertInstanceOf(EwmaClientStreamTracerFactory.class, pr.getStreamTracerFactory());

        ClientStreamTracer.Factory f = pr.getStreamTracerFactory();
        ClientStreamTracer tracer = f.newClientStreamTracer(null, new Metadata());
        assertEquals(
                0,
                table.getInflight(),
                "inflight should not increment until the stream is actually created");

        tracer.streamCreated(Attributes.EMPTY, new Metadata());
        assertEquals(1, table.getInflight(), "streamCreated should bump inflight to 1");

        tracer.streamClosed(Status.OK);
        assertEquals(0, table.getInflight(), "streamClosed should return inflight to 0");
    }

    @Test
    void allCostsInfinite_fallsBackToBaseline_bestCostWins_thenLeastRecentlyEjectedBreaksTie() {
        String method = "svc/A";

        FakeSubchannel sc1 = new FakeSubchannel(6001);
        FakeSubchannel sc2 = new FakeSubchannel(6002);

        MethodTable t1 = new MethodTable(cfg, t.clocks);
        MethodTable t2 = new MethodTable(cfg, t.clocks);
        SubchannelState s1 = new SubchannelState();
        SubchannelState s2 = new SubchannelState();

        s1.markReady(t.nowNanos());
        s2.markReady(t.nowNanos());

        sample(t, t1, method, cfg, 30_000);
        sample(t, t2, method, cfg, 10_000);

        long until = t.nowNanos() + TimeUnit.SECONDS.toNanos(30);
        s1.ejectUntil(until);
        s2.ejectUntil(until);

        Map<Subchannel, MethodTable> tables = Map.of(sc1, t1, sc2, t2);
        Map<Subchannel, SubchannelState> states = Map.of(sc1, s1, sc2, s2);

        P2CPicker picker =
                new P2CPicker(
                        List.of(sc1, sc2), tables, states, cfg, t.clocks, NoopLbMetrics.INSTANCE);

        double inflightWeightEff = PeakEwmaTuner.inflightWeightEff(2, 1, cfg);
        java.util.function.Function<MethodTable, Double> baselineCost =
                mt -> {
                    MethodStats ms = mt.statsFor(method);
                    double fast = Math.max(1e-6, ms.getEwmaFastMicros());
                    double busy = 1.0 + inflightWeightEff * Math.max(0, mt.getInflight());
                    return fast * busy;
                };

        double c1 = baselineCost.apply(t1);
        double c2 = baselineCost.apply(t2);

        PickResult pr = picker.pickSubchannel(args(md(method)));
        Subchannel chosen = pr.getSubchannel();

        Subchannel expected = (c1 <= c2) ? sc1 : sc2;
        assertSame(expected, chosen, "baseline fallback must pick strictly lower baseline cost");

        MethodTable t3 = new MethodTable(cfg, t.clocks);
        MethodTable t4 = new MethodTable(cfg, t.clocks);
        SubchannelState s3 = new SubchannelState();
        SubchannelState s4 = new SubchannelState();
        s3.markReady(t.nowNanos());
        s4.markReady(t.nowNanos());

        sample(t, t3, method, cfg, 15_000);
        sample(t, t4, method, cfg, 15_000);

        long base = t.nowNanos();
        s3.ejectUntil(base + TimeUnit.SECONDS.toNanos(20));
        s4.ejectUntil(base + TimeUnit.SECONDS.toNanos(25));

        FakeSubchannel sc3 = new FakeSubchannel(6003);
        FakeSubchannel sc4 = new FakeSubchannel(6004);

        P2CPicker picker2 =
                new P2CPicker(
                        List.of(sc3, sc4),
                        Map.of(sc3, t3, sc4, t4),
                        Map.of(sc3, s3, sc4, s4),
                        cfg,
                        t.clocks,
                        NoopLbMetrics.INSTANCE);

        PickResult pr2 = picker2.pickSubchannel(args(md(method)));
        assertSame(sc3, pr2.getSubchannel(), "older lastEjectEnd should win baseline tie");
    }

    @Test
    void someFinite_someInfinite_drawsTournamentFromFinitePeers_onlyFiniteCanWin() {
        String method = "svc/B";

        FakeSubchannel finite = new FakeSubchannel(6101);
        FakeSubchannel infinite = new FakeSubchannel(6102);

        MethodTable tFinite = new MethodTable(cfg, t.clocks);
        MethodTable tInfinite = new MethodTable(cfg, t.clocks);
        SubchannelState sFinite = new SubchannelState();
        SubchannelState sInfinite = new SubchannelState();

        sFinite.markReady(t.nowNanos());
        sInfinite.markReady(t.nowNanos());

        sample(t, tFinite, method, cfg, 8_000);

        long until = t.nowNanos() + TimeUnit.SECONDS.toNanos(60);
        tInfinite.ejectMethodUntil(method, until);

        P2CPicker picker =
                new P2CPicker(
                        List.of(finite, infinite),
                        Map.of(finite, tFinite, infinite, tInfinite),
                        Map.of(finite, sFinite, infinite, sInfinite),
                        cfg,
                        t.clocks,
                        NoopLbMetrics.INSTANCE);

        int winsFinite = 0;
        for (int i = 0; i < 20; i++) {
            PickResult pr = picker.pickSubchannel(args(md(method)));
            if (pr.getSubchannel() == finite) winsFinite++;
        }
        assertEquals(20, winsFinite, "only finite peer should be selectable");
    }

    @Test
    void methodEjection_allFiniteBecomesInfinite_thenBaselineFallbackChoosesBest() {
        String method = "svc/C";

        FakeSubchannel a = new FakeSubchannel(6201);
        FakeSubchannel b = new FakeSubchannel(6202);

        MethodTable ta = new MethodTable(cfg, t.clocks);
        MethodTable tb = new MethodTable(cfg, t.clocks);
        SubchannelState sa = new SubchannelState();
        SubchannelState sb = new SubchannelState();
        sa.markReady(t.nowNanos());
        sb.markReady(t.nowNanos());

        sample(t, ta, method, cfg, 20_000);
        sample(t, tb, method, cfg, 10_000);

        long until = t.nowNanos() + TimeUnit.SECONDS.toNanos(30);
        ta.ejectMethodUntil(method, until);
        tb.ejectMethodUntil(method, until);

        P2CPicker picker =
                new P2CPicker(
                        List.of(a, b),
                        Map.of(a, ta, b, tb),
                        Map.of(a, sa, b, sb),
                        cfg,
                        t.clocks,
                        NoopLbMetrics.INSTANCE);

        double inflightWeightEff = PeakEwmaTuner.inflightWeightEff(2, 1, cfg);
        java.util.function.Function<MethodTable, Double> baselineCost =
                mt -> {
                    MethodStats ms = mt.statsFor(method);
                    double fast = Math.max(1e-6, ms.getEwmaFastMicros());
                    double busy = 1.0 + inflightWeightEff * Math.max(0, mt.getInflight());
                    return fast * busy;
                };

        double ca = baselineCost.apply(ta);
        double cb = baselineCost.apply(tb);

        PickResult pr = picker.pickSubchannel(args(md(method)));
        Subchannel chosen = pr.getSubchannel();
        Subchannel expected = (ca <= cb) ? a : b;

        assertSame(
                expected, chosen, "baseline fallback must pick argmin by computed baseline cost");
    }

    @Test
    void warmupBias_increasesCostForNewlyReadyPeer_soOlderPeerWinsTournament() {
        String method = "svc/D";

        FakeSubchannel young = new FakeSubchannel(6301);
        FakeSubchannel old = new FakeSubchannel(6302);

        MethodTable tYoung = new MethodTable(cfg, t.clocks);
        MethodTable tOld = new MethodTable(cfg, t.clocks);
        SubchannelState sYoung = new SubchannelState();
        SubchannelState sOld = new SubchannelState();

        sample(t, tYoung, method, cfg, 12_000);
        sample(t, tOld, method, cfg, 12_000);

        sOld.markReady(t.nowNanos() - TimeUnit.SECONDS.toNanos(60));
        sYoung.markReady(t.nowNanos());

        P2CPicker picker =
                new P2CPicker(
                        List.of(young, old),
                        Map.of(young, tYoung, old, tOld),
                        Map.of(young, sYoung, old, sOld),
                        cfg,
                        t.clocks,
                        NoopLbMetrics.INSTANCE);

        int oldWins = 0;
        for (int i = 0; i < 25; i++) {
            PickResult pr = picker.pickSubchannel(args(md(method)));
            if (pr.getSubchannel() == old) oldWins++;
        }
        assertTrue(
                oldWins >= 20, "old (warmed) peer should overwhelmingly win due to warmup penalty");
    }

    @Test
    void withTwoPeers_costsEqual_rngTiesResolvedByBetterMethodDeterministically() {
        String method = "svc/E";
        FakeSubchannel s1 = new FakeSubchannel(6401);
        FakeSubchannel s2 = new FakeSubchannel(6402);

        MethodTable t1 = new MethodTable(cfg, t.clocks);
        MethodTable t2 = new MethodTable(cfg, t.clocks);
        SubchannelState st1 = new SubchannelState();
        SubchannelState st2 = new SubchannelState();
        st1.markReady(t.nowNanos());
        st2.markReady(t.nowNanos());

        sample(t, t1, method, cfg, 11_000);
        sample(t, t2, method, cfg, 11_000);

        P2CPicker picker =
                new P2CPicker(
                        List.of(s1, s2),
                        Map.of(s1, t1, s2, t2),
                        Map.of(s1, st1, s2, st2),
                        cfg,
                        t.clocks,
                        NoopLbMetrics.INSTANCE);

        int s1Wins = 0;
        for (int i = 0; i < 200; i++) {
            if (picker.pickSubchannel(args(md(method))).getSubchannel() == s1) s1Wins++;
        }
        assertTrue(s1Wins >= 70 && s1Wins <= 130);
    }
}
