package dev.parkerharrelson.grpc.peakewma;

/**
 * Ejection duration with backoff, as in grpc's outlier_detection and Envoy: each repeat ejection of
 * the same target lasts one more base period ({@code base x level}, capped at 5 minutes), and each
 * base period of healthy time after an ejection ends forgives one level.
 *
 * <p>A backend that was briefly bad comes back after one short base period; a backend that keeps
 * failing stays out progressively longer instead of flapping in and out every few seconds. Accessed
 * only from the outlier tick (synchronization context), so no synchronization.
 */
final class EjectionBackoff {

    static final int MAX_LEVEL = 10;
    static final long MAX_EJECTION_NANOS = 300_000_000_000L;

    private int level;
    private long forgiveAtNanos;

    /** Registers an ejection now and returns when it ends. */
    long nextEjectionEnd(long now, long baseNanos) {
        level = Math.min(level + 1, MAX_LEVEL);
        long duration = Math.min(baseNanos * level, Math.max(baseNanos, MAX_EJECTION_NANOS));
        long end = now + duration;
        forgiveAtNanos = end + baseNanos;
        return end;
    }

    /** Called every tick: one base period of healthy, non-ejected time forgives one level. */
    void maybeForgive(long now, long baseNanos, boolean currentlyEjected) {
        if (level > 0 && !currentlyEjected && now >= forgiveAtNanos) {
            level--;
            forgiveAtNanos = now + baseNanos;
        }
    }

    int level() {
        return level;
    }
}
