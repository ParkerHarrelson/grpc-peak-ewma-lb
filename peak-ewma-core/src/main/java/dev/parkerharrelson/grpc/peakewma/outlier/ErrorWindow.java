package dev.parkerharrelson.grpc.peakewma.outlier;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Sliding success/error window implemented as a fixed-size ring of time buckets.
 *
 * <p>Writers use {@link #recordResult(boolean, long)} on every completed call. A snapshot sums
 * across all buckets at read time and computes an error rate. The window length is adjustable at
 * runtime via {@link #setWindowMillis(long)} so the Peak-EWMA tuner can widen or tighten it.
 *
 * <p>Rotation and recording are serialized through a single ring lock, but the lock is only
 * acquired on the rotation path. The hot path (no rotation needed) is lock-free after a small
 * fast-path check, so steady-state write throughput is bounded only by the underlying {@link
 * LongAdder} counters.
 */
public class ErrorWindow {

    private static final int DEFAULT_BUCKETS = 10;

    private final int buckets;
    private final LongAdder[] success;
    private final LongAdder[] err;

    private final AtomicLong windowNanos = new AtomicLong();
    private final AtomicLong bucketWidthNanos = new AtomicLong();
    private final AtomicLong startNanos = new AtomicLong(0L);

    private final AtomicInteger headIndex = new AtomicInteger(0);

    private final ReentrantLock ringLock = new ReentrantLock();

    public ErrorWindow(long initialWindowMillis) {
        this(DEFAULT_BUCKETS, initialWindowMillis);
    }

    public ErrorWindow(int buckets, long initialWindowMillis) {
        if (buckets < 2) throw new IllegalArgumentException("buckets >= 2");
        this.buckets = buckets;
        this.success = new LongAdder[buckets];
        this.err = new LongAdder[buckets];
        for (int i = 0; i < buckets; i++) {
            success[i] = new LongAdder();
            err[i] = new LongAdder();
        }
        setWindowMillis(initialWindowMillis);
    }

    /**
     * Resizes the effective window. Buckets are not resampled — the new width takes effect for
     * subsequent rotations only.
     *
     * @param windowMillis new total window width in milliseconds; clamped to at least 1 ms
     */
    public void setWindowMillis(long windowMillis) {
        long w = Math.max(1L, windowMillis);
        windowNanos.set(w * 1_000_000L);
        bucketWidthNanos.set(Math.max(1L, windowNanos.get() / buckets));
    }

    /**
     * Records a single call outcome.
     *
     * @param ok true for success, false for error
     * @param nowNanos current nano-time from the shared clock
     */
    public void recordResult(boolean ok, long nowNanos) {
        int idx = rotateAndGetHead(nowNanos);
        if (ok) {
            success[idx].increment();
        } else {
            err[idx].increment();
        }
    }

    /**
     * Advances the head pointer through any buckets whose time has fully elapsed, resetting each as
     * it rolls off, and returns the current head bucket index.
     *
     * <p>Fast path is lock-free when no rotation is due and the window has already been
     * initialized. Slow path (first write, or time to rotate) acquires the ring lock to make
     * reset+head-advance+write all observe the same ring state.
     *
     * @param nowNanos current nano-time
     * @return the bucket index that should receive the write
     */
    private int rotateAndGetHead(long nowNanos) {
        long bw = bucketWidthNanos.get();
        long start = startNanos.get();

        if (start != 0L && nowNanos - start < bw) {
            return headIndex.get();
        }

        ringLock.lock();
        try {
            long curStart = startNanos.get();
            if (curStart == 0L) {
                startNanos.set(nowNanos);
                return headIndex.get();
            }

            long elapsed = nowNanos - curStart;
            long curBw = bucketWidthNanos.get();
            if (elapsed < curBw) {
                return headIndex.get();
            }

            int steps = (int) Math.clamp(elapsed / curBw, 0L, buckets);
            for (int i = 0; i < steps; i++) {
                int next = (headIndex.get() + 1) % buckets;
                headIndex.set(next);
                success[next].reset();
                err[next].reset();
            }
            startNanos.addAndGet(steps * curBw);
            return headIndex.get();
        } finally {
            ringLock.unlock();
        }
    }

    /** Immutable snapshot of the current ring state. */
    public static final class Snapshot {
        public final long successes;
        public final long errors;
        public final long total;
        public final double errorRate;

        Snapshot(long s, long e) {
            this.successes = s;
            this.errors = e;
            this.total = s + e;
            this.errorRate = (total == 0L) ? 0.0 : ((double) e / (double) total);
        }
    }

    /**
     * Returns a summed snapshot across all live buckets, rotating the ring first so stale buckets
     * roll off.
     *
     * @param nowNanos current nano-time
     * @return snapshot of counts and error rate
     */
    public Snapshot snapshot(long nowNanos) {
        rotateAndGetHead(nowNanos);
        long s = 0L;
        long e = 0L;
        for (int i = 0; i < buckets; i++) {
            s += success[i].sum();
            e += err[i].sum();
        }
        return new Snapshot(s, e);
    }

    /** Clears all counters and resets the window origin so the next write re-seeds it. */
    public void reset() {
        ringLock.lock();
        try {
            for (int i = 0; i < buckets; i++) {
                success[i].reset();
                err[i].reset();
            }
            startNanos.set(0L);
            headIndex.set(0);
        } finally {
            ringLock.unlock();
        }
    }

    /**
     * @return the currently configured window width in milliseconds
     */
    public long currentWindowMillis() {
        return windowNanos.get() / 1_000_000L;
    }
}
