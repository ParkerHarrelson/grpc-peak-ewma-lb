package dev.parkerharrelson.grpc.peakewma.tracing;

import dev.parkerharrelson.grpc.peakewma.EwmaClocks;
import dev.parkerharrelson.grpc.peakewma.MethodStats;
import dev.parkerharrelson.grpc.peakewma.MethodTable;
import dev.parkerharrelson.grpc.peakewma.PeakEwmaConfig;
import dev.parkerharrelson.grpc.peakewma.metrics.LbMetrics;
import dev.parkerharrelson.grpc.peakewma.metrics.NoopLbMetrics;
import dev.parkerharrelson.grpc.peakewma.metrics.SampledTimers;
import dev.parkerharrelson.grpc.peakewma.outlier.ErrorWindow;
import io.grpc.Attributes;
import io.grpc.ClientStreamTracer;
import io.grpc.Metadata;
import io.grpc.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Per-stream tracer that records RTT and outcome into the {@link MethodStats} + {@link ErrorWindow}
 * for the subchannel that was picked, and (optionally) publishes the observed RTT to an {@link
 * LbMetrics} sink as a histogram sample.
 *
 * <p>Inflight accounting is performed against the actual stream lifecycle: {@code onInc} runs in
 * {@link #streamCreated(Attributes, Metadata)} and {@code onDec} runs in {@link
 * #streamClosed(Status)} only if the stream was actually created. gRPC guarantees {@code
 * streamClosed} is always called, but {@code streamCreated} may be skipped when stream setup fails
 * before a transport stream is established — in that case there is no real in-flight call to count.
 */
final class EwmaClientStreamTracer extends ClientStreamTracer {
    private static final Logger logger = LoggerFactory.getLogger(EwmaClientStreamTracer.class);

    // Either the table (production: stats and window are resolved when the call ends) or fixed
    // stats/window (tests). Resolving at the end matters for long-lived streams: the table prunes
    // methods with no recent samples, and a stream opened before the prune would otherwise report
    // its failure into a detached window the outlier tick never reads.
    private final MethodTable table;
    private final MethodStats stats;
    private final PeakEwmaConfig cfg;
    private final EwmaClocks clocks;
    private final Runnable onInc;
    private final Runnable onDec;
    private final ErrorWindow window;
    private final LbMetrics metrics;
    private final String method;
    private final boolean recordLatency;

    // volatile so streamClosed observes the writes made in streamCreated even when gRPC
    // dispatches the two callbacks from different threads.
    private volatile long startNanos;
    private volatile boolean streamCreated;

    @SuppressWarnings("java:S107")
    EwmaClientStreamTracer(
            MethodStats stats,
            PeakEwmaConfig cfg,
            EwmaClocks clocks,
            Runnable onInc,
            Runnable onDec,
            ErrorWindow window,
            LbMetrics metrics,
            String method) {
        this(stats, cfg, clocks, onInc, onDec, window, metrics, method, true);
    }

    @SuppressWarnings("java:S107")
    EwmaClientStreamTracer(
            MethodStats stats,
            PeakEwmaConfig cfg,
            EwmaClocks clocks,
            Runnable onInc,
            Runnable onDec,
            ErrorWindow window,
            LbMetrics metrics,
            String method,
            boolean recordLatency) {
        this(null, stats, cfg, clocks, onInc, onDec, window, metrics, method, recordLatency);
    }

    /** Production constructor: stats and window are looked up in {@code table} at close time. */
    @SuppressWarnings("java:S107")
    EwmaClientStreamTracer(
            MethodTable table,
            PeakEwmaConfig cfg,
            EwmaClocks clocks,
            Runnable onInc,
            Runnable onDec,
            LbMetrics metrics,
            String method,
            boolean recordLatency) {
        this(table, null, cfg, clocks, onInc, onDec, null, metrics, method, recordLatency);
    }

    @SuppressWarnings("java:S107")
    private EwmaClientStreamTracer(
            MethodTable table,
            MethodStats stats,
            PeakEwmaConfig cfg,
            EwmaClocks clocks,
            Runnable onInc,
            Runnable onDec,
            ErrorWindow window,
            LbMetrics metrics,
            String method,
            boolean recordLatency) {
        this.recordLatency = recordLatency;
        this.table = table;
        this.stats = stats;
        this.cfg = cfg;
        this.clocks = clocks;
        this.onInc = onInc != null ? onInc : () -> {};
        this.onDec = onDec != null ? onDec : () -> {};
        this.window = window;
        this.metrics = metrics != null ? metrics : NoopLbMetrics.INSTANCE;
        this.method = method != null ? method : "";
    }

    /**
     * Statuses that say the backend (not the request) is unhealthy. Application-level outcomes
     * (NOT_FOUND, INVALID_ARGUMENT, PERMISSION_DENIED, ...) and client cancellation come from a
     * healthy server and are scored on their latency like successes. DEADLINE_EXCEEDED needs no
     * penalty: its RTT is already the full deadline.
     */
    static boolean isServerFailure(Status status) {
        return switch (status.getCode()) {
            case UNAVAILABLE, INTERNAL, UNKNOWN, DATA_LOSS, RESOURCE_EXHAUSTED, UNIMPLEMENTED ->
                    true;
            default -> false;
        };
    }

    @Override
    public void streamCreated(Attributes transportAttrs, Metadata headers) {
        if (SampledTimers.ENABLED && SampledTimers.sample()) {
            long start = System.nanoTime();
            onStreamCreated();
            metrics.recordTracerNanos("streamCreated", System.nanoTime() - start);
            return;
        }
        onStreamCreated();
    }

    private void onStreamCreated() {
        startNanos = clocks.nanoTime();
        streamCreated = true;
        // Only unary calls count as load: a long-lived stream (watch, subscription) occupies a
        // slot without queueing work, and latency x (inflight + 1) would make a backend holding
        // many idle streams look many times slower for unary traffic.
        if (recordLatency) onInc.run();
    }

    @Override
    public void streamClosed(Status status) {
        if (SampledTimers.ENABLED && SampledTimers.sample()) {
            long start = System.nanoTime();
            onStreamClosed(status);
            metrics.recordTracerNanos("streamClosed", System.nanoTime() - start);
            return;
        }
        onStreamClosed(status);
    }

    private void onStreamClosed(Status status) {
        try {
            long end = clocks.nanoTime();
            long rtt = (startNanos == 0L) ? 0L : Math.max(0L, end - startNanos);

            boolean serverFailure = isServerFailure(status);
            if (serverFailure || (recordLatency && rtt > 0L)) {
                // A streaming call's duration is its lifetime, not a latency: it must not set
                // the size of the failure penalty (an hour-long stream reset by GOAWAY would
                // write the 10 s cap into the peak). Pass 0 so only the penalty applies.
                MethodStats s = table != null ? table.statsFor(method) : stats;
                s.update(end, recordLatency ? rtt : 0L, cfg, serverFailure);
            }
            if (recordLatency && rtt > 0L) {
                metrics.recordObservedRtt(method, rtt);
            }
            // Outlier detection counts backend-health failures only: application outcomes
            // (NOT_FOUND, INVALID_ARGUMENT, ...) and client cancellations come from a healthy
            // server. DEADLINE_EXCEEDED counts: a backend timing out every call is unhealthy.
            ErrorWindow w = table != null ? table.windowFor(method) : window;
            w.recordResult(
                    !(serverFailure || status.getCode() == Status.Code.DEADLINE_EXCEEDED), end);
        } catch (Exception e) {
            logger.error("EWMA stream tracer update failed on streamClosed", e);
        } finally {
            if (streamCreated && recordLatency) {
                onDec.run();
            }
        }
    }
}
