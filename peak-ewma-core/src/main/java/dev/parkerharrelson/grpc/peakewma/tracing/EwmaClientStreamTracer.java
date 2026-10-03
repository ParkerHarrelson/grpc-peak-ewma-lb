package dev.parkerharrelson.grpc.peakewma.tracing;

import dev.parkerharrelson.grpc.peakewma.EwmaClocks;
import dev.parkerharrelson.grpc.peakewma.MethodStats;
import dev.parkerharrelson.grpc.peakewma.PeakEwmaConfig;
import dev.parkerharrelson.grpc.peakewma.metrics.LbMetrics;
import dev.parkerharrelson.grpc.peakewma.metrics.NoopLbMetrics;
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

    private final MethodStats stats;
    private final PeakEwmaConfig cfg;
    private final EwmaClocks clocks;
    private final Runnable onInc;
    private final Runnable onDec;
    private final ErrorWindow window;
    private final LbMetrics metrics;
    private final String method;

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
        this.stats = stats;
        this.cfg = cfg;
        this.clocks = clocks;
        this.onInc = onInc != null ? onInc : () -> {};
        this.onDec = onDec != null ? onDec : () -> {};
        this.window = window;
        this.metrics = metrics != null ? metrics : NoopLbMetrics.INSTANCE;
        this.method = method != null ? method : "";
    }

    @Override
    public void streamCreated(Attributes transportAttrs, Metadata headers) {
        startNanos = clocks.nanoTime();
        streamCreated = true;
        onInc.run();
    }

    @Override
    public void streamClosed(Status status) {
        try {
            long end = clocks.nanoTime();
            long rtt = (startNanos == 0L) ? 0L : Math.max(0L, end - startNanos);

            if (rtt > 0L) {
                stats.update(end, rtt, cfg);
                metrics.recordObservedRtt(method, rtt);
            }
            window.recordResult(status.isOk(), end);
        } catch (Exception e) {
            logger.error("EWMA stream tracer update failed on streamClosed", e);
        } finally {
            if (streamCreated) {
                onDec.run();
            }
        }
    }
}
