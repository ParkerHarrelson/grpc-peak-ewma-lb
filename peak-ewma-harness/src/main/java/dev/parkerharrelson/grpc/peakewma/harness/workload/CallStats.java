package dev.parkerharrelson.grpc.peakewma.harness.workload;

import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.LongAdder;

/**
 * Thread-safe record of per-backend + aggregate call outcomes, including raw RTTs for later
 * percentile computation. Lightweight enough to stay on the hot path during the workload.
 *
 * <p>Backend identity is the server port (since the harness runs everything on localhost);
 * alternatively an arbitrary tag can be supplied if the caller wants to classify by attribute.
 */
public final class CallStats {

    public record BackendSummary(
            String id,
            long requests,
            long successes,
            long errors,
            long p50Micros,
            long p95Micros,
            long p99Micros) {}

    public record AggregateSummary(
            long totalRequests,
            long totalSuccesses,
            long totalErrors,
            long p50Micros,
            long p95Micros,
            long p99Micros) {}

    private final LongAdder totalRequests = new LongAdder();
    private final LongAdder totalSuccesses = new LongAdder();
    private final LongAdder totalErrors = new LongAdder();
    private final ConcurrentHashMap<String, LongAdder> requestsByBackend =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, LongAdder> successesByBackend =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, LongAdder> errorsByBackend = new ConcurrentHashMap<>();

    private final ConcurrentLinkedQueue<Long> rttMicrosGlobal = new ConcurrentLinkedQueue<>();
    private final ConcurrentHashMap<String, ConcurrentLinkedQueue<Long>> rttMicrosByBackend =
            new ConcurrentHashMap<>();

    /**
     * Records one completed call.
     *
     * <p>Named {@code recordCall} rather than {@code record} to avoid shadowing Java's restricted
     * {@code record} keyword.
     */
    public void recordCall(String backendId, boolean success, long rttNanos) {
        totalRequests.increment();
        requestsByBackend.computeIfAbsent(backendId, k -> new LongAdder()).increment();

        if (success) {
            totalSuccesses.increment();
            successesByBackend.computeIfAbsent(backendId, k -> new LongAdder()).increment();
        } else {
            totalErrors.increment();
            errorsByBackend.computeIfAbsent(backendId, k -> new LongAdder()).increment();
        }

        long rttMicros = Math.max(0L, rttNanos / 1000L);
        rttMicrosGlobal.offer(rttMicros);
        rttMicrosByBackend
                .computeIfAbsent(backendId, k -> new ConcurrentLinkedQueue<>())
                .offer(rttMicros);
    }

    /**
     * @return summary per backend seen so far
     */
    public java.util.Map<String, BackendSummary> perBackend() {
        java.util.TreeMap<String, BackendSummary> out = new java.util.TreeMap<>();
        for (String id : requestsByBackend.keySet()) {
            long[] sorted = sortedRtts(rttMicrosByBackend.get(id));
            out.put(
                    id,
                    new BackendSummary(
                            id,
                            requestsByBackend.getOrDefault(id, new LongAdder()).sum(),
                            successesByBackend.getOrDefault(id, new LongAdder()).sum(),
                            errorsByBackend.getOrDefault(id, new LongAdder()).sum(),
                            percentile(sorted, 50),
                            percentile(sorted, 95),
                            percentile(sorted, 99)));
        }
        return out;
    }

    /**
     * @return overall summary across all backends
     */
    public AggregateSummary aggregate() {
        long[] sorted = sortedRtts(rttMicrosGlobal);
        return new AggregateSummary(
                totalRequests.sum(),
                totalSuccesses.sum(),
                totalErrors.sum(),
                percentile(sorted, 50),
                percentile(sorted, 95),
                percentile(sorted, 99));
    }

    private static long[] sortedRtts(ConcurrentLinkedQueue<Long> q) {
        if (q == null || q.isEmpty()) return new long[0];
        long[] arr = q.stream().mapToLong(Long::longValue).toArray();
        Arrays.sort(arr);
        return arr;
    }

    private static long percentile(long[] sorted, int percentile) {
        if (sorted.length == 0) return 0L;
        int idx = (int) Math.ceil((percentile / 100.0) * sorted.length) - 1;
        idx = Math.clamp(idx, 0, sorted.length - 1);
        return sorted[idx];
    }
}
