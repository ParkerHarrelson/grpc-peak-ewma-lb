package dev.parkerharrelson.grpc.peakewma.outlier;

import static org.junit.jupiter.api.Assertions.*;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class ErrorWindowTest {

    @Test
    void ctor_rejectsBucketsLessThanTwo() {
        IllegalArgumentException ex =
                assertThrows(IllegalArgumentException.class, () -> new ErrorWindow(1, 1000));
        assertTrue(ex.getMessage().contains("buckets >= 2"));
    }

    @Test
    void currentWindowMillis_tracksSetWindowMillis_andClampsToAtLeast1ms() {
        ErrorWindow win = new ErrorWindow(10, 1234);
        assertEquals(1234, win.currentWindowMillis());

        win.setWindowMillis(0);
        assertEquals(1, win.currentWindowMillis());
    }

    @Test
    void record_Result_and_snapshot_countsSuccessesErrors_andErrorRate() {
        ErrorWindow win = new ErrorWindow(10, 1000);
        long t0 = 1_000_000_000L;

        win.recordResult(true, t0);
        win.recordResult(false, t0 + 1);
        win.recordResult(true, t0 + 2);

        ErrorWindow.Snapshot s = win.snapshot(t0 + 3);
        assertEquals(2, s.successes);
        assertEquals(1, s.errors);
        assertEquals(3, s.total);
        assertEquals(1.0 / 3.0, s.errorRate, 1e-12);
    }

    @Test
    void rotation_advancesHeadAndKeepsTotalsAcrossBuckets() {
        ErrorWindow win = new ErrorWindow(10, 1000);
        long base = 10_000_000_000L;
        long bucketWidthNanos = 100_000_000L;

        win.recordResult(true, base);
        win.recordResult(true, base + 1);
        win.recordResult(false, base + 2);

        win.recordResult(true, base + bucketWidthNanos - 1);

        win.recordResult(false, base + bucketWidthNanos + 10);

        ErrorWindow.Snapshot s1 = win.snapshot(base + bucketWidthNanos + 20);
        assertEquals(3, s1.successes);
        assertEquals(2, s1.errors);
        assertEquals(5, s1.total);
        assertEquals(2.0 / 5.0, s1.errorRate, 1e-12);
    }

    @Test
    void largeElapsedClearsRing_minStepsIsCappedAtBucketCount() {
        ErrorWindow win = new ErrorWindow(5, 1000);
        long base = 1_000_000_000L;

        win.recordResult(true, base);
        win.recordResult(false, base + 1);
        win.recordResult(true, base + 2);

        long bigJump = base + 1_500_000_000L;
        ErrorWindow.Snapshot s1 = win.snapshot(bigJump);

        assertEquals(0, s1.total);
        assertEquals(0, s1.successes);
        assertEquals(0, s1.errors);
        assertEquals(0.0, s1.errorRate, 0.0);

        win.recordResult(true, bigJump + 1);
        ErrorWindow.Snapshot s2 = win.snapshot(bigJump + 2);
        assertEquals(1, s2.successes);
        assertEquals(0, s2.errors);
        assertEquals(1, s2.total);
        assertEquals(0.0, s2.errorRate, 0.0);
    }

    @Test
    void reset_clearsCountsAndResetsHead() {
        ErrorWindow win = new ErrorWindow(8, 800);
        long t0 = 5_000_000_000L;

        win.recordResult(true, t0);
        win.recordResult(false, t0 + 1);
        assertEquals(2, win.snapshot(t0 + 2).total);

        win.reset();

        ErrorWindow.Snapshot s = win.snapshot(t0 + 3);
        assertEquals(0, s.total);

        win.recordResult(true, t0 + 4);
        assertEquals(1, win.snapshot(t0 + 5).total);
    }

    @Test
    void recordResult_underHighConcurrency_doesNotDropWrites() throws Exception {
        // Regression guard for the rotation race the PR fixes. Pin the timestamp and use a wide
        // window so no rotation can occur — every recordResult must land in some bucket and the
        // sum across buckets must equal the number of recordResult calls.
        final int threads = 8;
        final int callsPerThread = 5_000;
        final long fixedNanos = 1_000_000_000L;

        ErrorWindow win = new ErrorWindow(10, 60_000);

        ExecutorService exec = Executors.newFixedThreadPool(threads);
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);

        try {
            for (int t = 0; t < threads; t++) {
                final boolean ok = (t & 1) == 0;
                exec.submit(
                        () -> {
                            ready.countDown();
                            try {
                                start.await();
                                for (int i = 0; i < callsPerThread; i++) {
                                    win.recordResult(ok, fixedNanos);
                                }
                            } catch (InterruptedException ie) {
                                Thread.currentThread().interrupt();
                            } finally {
                                done.countDown();
                            }
                        });
            }

            ready.await();
            start.countDown();
            assertTrue(
                    done.await(30, TimeUnit.SECONDS),
                    "concurrent recordResult did not complete within timeout");
        } finally {
            exec.shutdownNow();
        }

        long totalCalls = (long) threads * callsPerThread;
        long expectedSuccesses = (long) (threads / 2) * callsPerThread;
        long expectedErrors = totalCalls - expectedSuccesses;

        ErrorWindow.Snapshot s = win.snapshot(fixedNanos);
        assertEquals(
                totalCalls,
                s.total,
                "every recordResult call must be counted; concurrent rotation must not drop"
                        + " writes");
        assertEquals(expectedSuccesses, s.successes);
        assertEquals(expectedErrors, s.errors);
    }

    @Test
    void snapshot_alsoTriggersRotationEvenWithoutRecordResult() {
        ErrorWindow win = new ErrorWindow(10, 1000);
        long base = 1_000_000_000L;
        long bucketWidthNanos = 100_000_000L;

        win.recordResult(true, base);
        win.recordResult(true, base + 1);

        ErrorWindow.Snapshot before = win.snapshot(base + 20 * bucketWidthNanos);
        assertEquals(0, before.total);

        win.recordResult(false, base + 20 * bucketWidthNanos + 1);
        ErrorWindow.Snapshot after = win.snapshot(base + 20 * bucketWidthNanos + 2);
        assertEquals(1, after.total);
        assertEquals(1, after.errors);
        assertEquals(0, after.successes);
        assertEquals(1.0, after.errorRate, 0.0);
    }
}
