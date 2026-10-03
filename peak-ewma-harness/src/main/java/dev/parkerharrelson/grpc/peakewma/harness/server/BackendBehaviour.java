package dev.parkerharrelson.grpc.peakewma.harness.server;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Mutable per-backend behaviour controls used by {@link InjectableService} to simulate real-world
 * response profiles during load balancer tests.
 *
 * <p>All fields are exposed via a single immutable snapshot that the service reads per-request, so
 * scenario code can flip a backend from "healthy" to "slow" to "outage" atomically without holding
 * any lock.
 */
public final class BackendBehaviour {

    /** Parameters of one call's behaviour, read once per request. */
    public record Snapshot(
            Duration baseLatency,
            double latencyJitter,
            double errorRate,
            boolean outage,
            Duration tailLatency,
            double tailProbability) {

        /** Returns the effective latency for this call, including jitter and potential tail. */
        public Duration sampleLatency() {
            long baseNanos = baseLatency.toNanos();
            double jitter = latencyJitter;
            double spread = 1.0 + (ThreadLocalRandom.current().nextDouble() * 2 - 1) * jitter;
            long withJitter = (long) (baseNanos * Math.max(0.0, spread));

            if (tailProbability > 0.0
                    && ThreadLocalRandom.current().nextDouble() < tailProbability) {
                withJitter += tailLatency.toNanos();
            }
            return Duration.ofNanos(Math.max(0L, withJitter));
        }

        /**
         * @return true if this call should fail before doing real work
         */
        public boolean shouldError() {
            return errorRate > 0.0 && ThreadLocalRandom.current().nextDouble() < errorRate;
        }
    }

    public static final Snapshot HEALTHY =
            new Snapshot(Duration.ofMillis(10), 0.2, 0.0, false, Duration.ZERO, 0.0);

    private final AtomicReference<Snapshot> state = new AtomicReference<>(HEALTHY);

    /** Returns the current snapshot for use in the service handler. */
    public Snapshot snapshot() {
        return state.get();
    }

    /** Atomically replaces the snapshot. */
    public void set(Snapshot next) {
        state.set(next);
    }

    /** Convenience: fix base latency and jitter, zero errors, no outage, no tail. */
    public void setHealthy(Duration base, double jitter) {
        state.set(new Snapshot(base, jitter, 0.0, false, Duration.ZERO, 0.0));
    }

    /** Convenience: turn this backend into an errors-only sink (for testing ejection). */
    public void setAllErrors() {
        state.set(new Snapshot(Duration.ofMillis(1), 0.0, 1.0, false, Duration.ZERO, 0.0));
    }

    /** Convenience: full outage (the service returns {@code UNAVAILABLE} on every request). */
    public void setOutage() {
        Snapshot cur = state.get();
        state.set(
                new Snapshot(
                        cur.baseLatency,
                        cur.latencyJitter,
                        cur.errorRate,
                        true,
                        cur.tailLatency,
                        cur.tailProbability));
    }

    /** Convenience: slow brownout — base latency is multiplied by {@code factor}. */
    public void setSlow(Duration newBase, double jitter) {
        state.set(new Snapshot(newBase, jitter, 0.0, false, Duration.ZERO, 0.0));
    }
}
