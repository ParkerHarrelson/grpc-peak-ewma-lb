package dev.parkerharrelson.grpc.peakewma;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Per-subchannel bookkeeping that is not metric-specific: readiness timestamp and active/past
 * ejection windows.
 *
 * <p>Readiness is stamped by the balancer's subchannel listener each time gRPC reports {@code
 * READY}. (There is no warmup ramp any more: a peer that hasn't served a method yet is scored at
 * the fleet's typical latency for it.)
 *
 * <p>Ejection is represented as an "until" nano-timestamp; writes use {@link #ejectUntil(long)};
 * reads use {@link #isEjected(long)}. {@link #lastEjectEndNanos()} tracks the tail so the outlier
 * ticker can enforce a reentry cooldown without remembering every past ejection.
 */
public final class SubchannelState {
    private volatile long readySinceNanos = 0L;
    private volatile long ejectedUntilNanos = 0L;
    private final AtomicLong lastEjectEndNanos = new AtomicLong(0L);
    private final EjectionBackoff backoff = new EjectionBackoff();
    private volatile boolean removed;

    /** The balancer dropped this backend; pickers built before that must stop choosing it. */
    void markRemoved() {
        removed = true;
    }

    boolean isRemoved() {
        return removed;
    }

    /** Ejection backoff for this backend (outlier tick only). */
    EjectionBackoff backoff() {
        return backoff;
    }

    /** Stamps the moment this subchannel last transitioned to READY. */
    public void markReady(long nowNanos) {
        this.readySinceNanos = nowNanos;
    }

    /**
     * @return the nano-time of the most recent READY transition, or 0 if not yet ready
     */
    public long readySinceNanos() {
        return readySinceNanos;
    }

    /**
     * Marks this subchannel ejected until {@code untilNanos}. Also moves the recorded
     * end-of-last-ejection forward only, so cooldown checks see the true most-recent expiry.
     */
    public void ejectUntil(long untilNanos) {
        this.ejectedUntilNanos = untilNanos;
        lastEjectEndNanos.getAndUpdate(prev -> Math.max(prev, untilNanos));
    }

    /**
     * @return true if the subchannel is currently ejected (i.e. {@code now < ejectedUntil})
     */
    public boolean isEjected(long nowNanos) {
        return nowNanos < ejectedUntilNanos;
    }

    /**
     * @return nano-timestamp at which the most recent ejection was set to expire
     */
    public long lastEjectEndNanos() {
        return lastEjectEndNanos.get();
    }
}
