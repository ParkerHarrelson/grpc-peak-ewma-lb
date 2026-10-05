package dev.parkerharrelson.grpc.peakewma.metrics;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Gate for the sampled hot-path timers ({@link LbMetrics#recordPickNanos}, {@link
 * LbMetrics#recordTracerNanos}). Off by default: {@link #ENABLED} is a static final read once at
 * class load, so with the property unset the JIT removes the timing branches entirely and
 * production pays nothing.
 *
 * <ul>
 *   <li>{@code -Dpeakewma.sampledTimers=true} enables them;
 *   <li>{@code -Dpeakewma.sampledTimers.every=N} times about one call in N (default 1000).
 * </ul>
 */
public final class SampledTimers {

    public static final boolean ENABLED = Boolean.getBoolean("peakewma.sampledTimers");

    public static final int EVERY =
            Math.max(1, Integer.getInteger("peakewma.sampledTimers.every", 1000));

    private SampledTimers() {}

    /** Whether to time this call; only meaningful when {@link #ENABLED}. */
    public static boolean sample() {
        return ThreadLocalRandom.current().nextInt(EVERY) == 0;
    }
}
