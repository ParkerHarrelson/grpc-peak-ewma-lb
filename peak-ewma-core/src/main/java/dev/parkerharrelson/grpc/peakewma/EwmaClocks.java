package dev.parkerharrelson.grpc.peakewma;

import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * Nano-time facade used by all Peak-EWMA components so decay math can be unit-tested
 * deterministically via a fake {@link LongSupplier} clock.
 *
 * <p>Production code uses {@link System#nanoTime()}; tests pass a controllable supplier.
 */
public final class EwmaClocks {
    private final LongSupplier nanoTime;

    /** Creates an EwmaClocks backed by {@link System#nanoTime()}. */
    public EwmaClocks() {
        this(System::nanoTime);
    }

    /**
     * Creates an EwmaClocks backed by the given supplier. Intended for tests.
     *
     * @param nanoTime nano-time source, must not return a decreasing value for correctness
     */
    public EwmaClocks(LongSupplier nanoTime) {
        this.nanoTime = nanoTime;
    }

    /**
     * @return the current nano-time from the underlying supplier
     */
    public long nanoTime() {
        return nanoTime.getAsLong();
    }

    /**
     * Computes an exponential decay factor for a half-life-style EWMA update.
     *
     * <p>Returns {@code exp(-deltaNanos / tauNanos)} where {@code tauNanos} is derived from {@code
     * halfLifeMillis} so that one half-life elapses after exactly {@code halfLifeMillis}
     * milliseconds. Values at/below 0 or an uninitialized last-update return 0 so the first sample
     * overrides the seed cleanly.
     *
     * @param nowNanos end-of-call timestamp
     * @param lastUpdateNanos previous EWMA update timestamp; 0 means "never"
     * @param halfLifeMillis desired half-life in milliseconds
     * @return the decay factor, in [0.0, 1.0]
     */
    public static double decayFactor(long nowNanos, long lastUpdateNanos, long halfLifeMillis) {
        if (lastUpdateNanos == 0L) {
            return 0.0;
        }

        long deltaNanos = Math.max(0L, nowNanos - lastUpdateNanos);
        if (halfLifeMillis <= 0) {
            return 0.0;
        }

        double tauNanos = halfLifeMillis / Math.log(2.0) * 1_000_000.0;
        return Math.exp(-deltaNanos / tauNanos);
    }

    /** Converts milliseconds to nanoseconds. */
    public static long millisToNanos(long millis) {
        return TimeUnit.MILLISECONDS.toNanos(millis);
    }

    /** Converts nanoseconds to microseconds as a double (keeps sub-microsecond precision). */
    public static double nanosToMicros(long nanos) {
        return nanos / 1_000.0;
    }
}
