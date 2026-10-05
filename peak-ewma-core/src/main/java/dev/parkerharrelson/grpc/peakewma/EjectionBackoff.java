package dev.parkerharrelson.grpc.peakewma;

/**
 * Ejection duration with backoff, as in grpc's outlier_detection and Envoy: each repeat ejection of
 * the same target lasts one more base period ({@code base x level}, capped at 5 minutes or one base
 * period if that is longer), and each base period of healthy time after an ejection ends forgives
 * one level. The level stops growing once the cap is reached, so forgiveness never has to work
 * through levels that no longer lengthen the ejection.
 *
 * <p>A backend that was briefly bad comes back after one short base period; a backend that keeps
 * failing stays out progressively longer instead of flapping in and out every few seconds. Accessed
 * only from the outlier tick (synchronization context), so no synchronization.
 */
final class EjectionBackoff {

    static final long MAX_EJECTION_NANOS = 300_000_000_000L;

    private int level;
    private long forgiveAtNanos;

    /** Registers an ejection now and returns when it ends. */
    long nextEjectionEnd(long now, long baseNanos) {
        long base = Math.max(1L, baseNanos);
        long cap = Math.max(base, MAX_EJECTION_NANOS);
        long maxLevel = (cap + base - 1) / base; // first level whose duration reaches the cap
        level = (int) Math.min(level + 1L, maxLevel);
        long duration = Math.min(base * level, cap);
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
