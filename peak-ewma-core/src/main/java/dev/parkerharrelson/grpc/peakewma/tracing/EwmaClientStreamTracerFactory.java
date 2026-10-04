package dev.parkerharrelson.grpc.peakewma.tracing;

import dev.parkerharrelson.grpc.peakewma.EwmaClocks;
import dev.parkerharrelson.grpc.peakewma.MethodTable;
import dev.parkerharrelson.grpc.peakewma.PeakEwmaConfig;
import dev.parkerharrelson.grpc.peakewma.metrics.LbMetrics;
import dev.parkerharrelson.grpc.peakewma.metrics.NoopLbMetrics;
import io.grpc.ClientStreamTracer;
import io.grpc.Metadata;

/**
 * Factory for {@link EwmaClientStreamTracer}, constructed per-pick so the resulting tracer is bound
 * to a specific method key and carries the inflight inc/dec callbacks for its subchannel.
 */
public final class EwmaClientStreamTracerFactory extends ClientStreamTracer.Factory {
    private final MethodTable table;
    private final PeakEwmaConfig cfg;
    private final EwmaClocks clocks;
    private final String method;
    private final Runnable onInc;
    private final Runnable onDec;
    private final LbMetrics metrics;
    private final boolean recordLatency;

    /** Delegates to the full constructor with a no-op metrics sink. */
    public EwmaClientStreamTracerFactory(
            MethodTable table,
            PeakEwmaConfig cfg,
            EwmaClocks clocks,
            String method,
            Runnable onInc,
            Runnable onDec) {
        this(table, cfg, clocks, method, onInc, onDec, NoopLbMetrics.INSTANCE);
    }

    /**
     * @param table the subchannel's method table
     * @param cfg current Peak-EWMA config (used by the tracer to update tau on each sample)
     * @param clocks shared clock
     * @param method full gRPC method name; null → ""
     * @param onInc callback run in {@code streamCreated}
     * @param onDec callback run in {@code streamClosed} only if the stream was actually created
     * @param metrics metrics sink for the observed RTT histogram (null → no-op)
     */
    public EwmaClientStreamTracerFactory(
            MethodTable table,
            PeakEwmaConfig cfg,
            EwmaClocks clocks,
            String method,
            Runnable onInc,
            Runnable onDec,
            LbMetrics metrics) {
        this(table, cfg, clocks, method, onInc, onDec, metrics, true);
    }

    /**
     * @param recordLatency whether this call's duration is a latency sample. False for streaming
     *     RPCs, whose duration is the stream's lifetime (seconds to hours) and would poison the
     *     method's EWMAs; their outcome still feeds the error window and penalty.
     */
    public EwmaClientStreamTracerFactory(
            MethodTable table,
            PeakEwmaConfig cfg,
            EwmaClocks clocks,
            String method,
            Runnable onInc,
            Runnable onDec,
            LbMetrics metrics,
            boolean recordLatency) {
        this.recordLatency = recordLatency;
        this.table = table;
        this.cfg = cfg;
        this.clocks = clocks;
        this.method = method != null ? method : "";
        this.onInc = onInc != null ? onInc : () -> {};
        this.onDec = onDec != null ? onDec : () -> {};
        this.metrics = metrics != null ? metrics : NoopLbMetrics.INSTANCE;
    }

    @Override
    public ClientStreamTracer newClientStreamTracer(
            ClientStreamTracer.StreamInfo info, Metadata headers) {
        var stats = table.statsFor(method);
        var window = table.windowFor(method);
        return new EwmaClientStreamTracer(
                stats, cfg, clocks, onInc, onDec, window, metrics, method, recordLatency);
    }
}
