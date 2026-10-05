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

    // Width each closed bucket was opened with, and how many closed buckets (behind the head)
    // hold live data. Written under ringLock; read racily for the rate estimate. Needed because
    // setWindowMillis changes the width of future buckets only: dividing by the CURRENT window
    // inflated the rate ~5x right after a 45 s -> 8 s resize.
    private final long[] closedWidthNanos;
    private volatile int closedBuckets;

    private final ReentrantLock ringLock = new ReentrantLock();

    public ErrorWindow(long initialWindowMillis) {
        this(DEFAULT_BUCKETS, initialWindowMillis);
    }

    public ErrorWindow(int buckets, long initialWindowMillis) {
        if (buckets < 2) throw new IllegalArgumentException("buckets >= 2");
        this.buckets = buckets;
        this.success = new LongAdder[buckets];
        this.err = new LongAdder[buckets];
        this.closedWidthNanos = new long[buckets];
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

            if (elapsed >= curBw * buckets) {
                // Idle for at least a whole window: everything is stale. Start over from now.
                // (Advancing by at most one window per call left startNanos far behind, so
                // every following call wiped the ring again, freshly recorded results included.)
                for (int i = 0; i < buckets; i++) {
                    success[i].reset();
                    err[i].reset();
                }
                closedBuckets = 0;
                startNanos.set(nowNanos);
                return headIndex.get();
            }
            long steps = elapsed / curBw;
            for (long i = 0; i < steps; i++) {
                int head = headIndex.get();
                closedWidthNanos[head] = curBw;
                int next = (head + 1) % buckets;
                // Clear the bucket BEFORE publishing it as the head: fast-path writers read
                // headIndex without the lock, and one that saw the new head before the reset
                // would have its write wiped (lost counts under concurrency).
                success[next].reset();
                err[next].reset();
                headIndex.set(next);
                closedBuckets = Math.min(buckets - 1, closedBuckets + 1);
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
            closedBuckets = 0;
        } finally {
            ringLock.unlock();
        }
    }

    /**
     * Wall time actually covered by the live buckets: the closed buckets at the widths they were
     * opened with, plus the elapsed part of the head bucket. Never less than one bucket width, so a
     * window that just started doesn't report an absurd rate from a handful of calls.
     *
     * @param nowNanos current nano-time
     * @return covered time in nanoseconds
     */
    public long coveredNanos(long nowNanos) {
        rotateAndGetHead(nowNanos);
        long start = startNanos.get();
        long covered = start == 0L ? 0L : Math.max(0L, nowNanos - start);
        int head = headIndex.get();
        int closed = closedBuckets;
        for (int i = 1; i <= closed; i++) {
            covered += closedWidthNanos[Math.floorMod(head - i, buckets)];
        }
        return Math.max(covered, bucketWidthNanos.get());
    }

    /**
     * @return the currently configured window width in milliseconds
     */
    public long currentWindowMillis() {
        return windowNanos.get() / 1_000_000L;
    }
}
