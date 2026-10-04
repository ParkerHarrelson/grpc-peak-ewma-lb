package dev.parkerharrelson.grpc.peakewma;

import dev.parkerharrelson.grpc.peakewma.outlier.ErrorWindow;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Per-subchannel state for the Peak-EWMA load balancer.
 *
 * <p>Tracks one {@link MethodStats} (latency EWMAs) and one {@link ErrorWindow} (success/error
 * sliding window) per method name, plus a single inflight counter for the whole subchannel.
 *
 * <p>New methods are seeded from {@link #cachedSeedMicros} which is derived from the median slow
 * EWMA of existing methods; this means a freshly-seen method starts at roughly the same expected
 * latency as the subchannel's other traffic, rather than at a flat default. The seed is refreshed
 * during {@link #pruneStale(long, long, int)} rather than on every new-method insertion, so {@link
 * #statsFor(String)} stays O(1).
 */
public final class MethodTable {
    private static final long MIN_SEED_MICROS = 2_000L;
    // Starting size only: the outlier tick resizes each window to ~200 calls of its method.
    private static final long INITIAL_ERROR_WINDOW_MILLIS = 15_000L;

    private final ConcurrentHashMap<String, MethodStats> methods = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ErrorWindow> methodWindows = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Long> methodEjectedUntilNanos =
            new ConcurrentHashMap<>();
    // A single atomic: every pick READS it for two peers, so it must be one cache line (a
    // striped LongAdder made each read touch every cell). Writes are one getAndAdd each, no CAS
    // retry loop; the value is floored at zero when read.
    private final AtomicInteger inflight = new AtomicInteger();

    private final PeakEwmaConfig peakEwmaConfig;
    private final EwmaClocks clocks;

    private volatile long cachedSeedMicros;
    private final ConcurrentHashMap<String, MethodScale> fleetScales;

    public MethodTable(PeakEwmaConfig peakEwmaConfig, EwmaClocks clocks) {
        this(peakEwmaConfig, clocks, new ConcurrentHashMap<>());
    }

    /**
     * @param fleetScales per-method view of the whole fleet, shared by every backend's table and
     *     maintained by the outlier tick; supplies half-lives and the seed for new methods
     */
    MethodTable(
            PeakEwmaConfig peakEwmaConfig,
            EwmaClocks clocks,
            ConcurrentHashMap<String, MethodScale> fleetScales) {
        this.peakEwmaConfig = peakEwmaConfig;
        this.clocks = clocks;
        this.fleetScales = fleetScales;
        this.cachedSeedMicros = Math.max(MIN_SEED_MICROS, peakEwmaConfig.initialRttMicros);
    }

    /**
     * Prior latency for a method this backend hasn't served: the fleet median for the method if
     * known, else this backend's typical latency across methods, else {@code initialRttMicros}.
     */
    double seedMicros(String method) {
        MethodScale sc = fleetScales.get(method);
        return (sc != null && !Double.isNaN(sc.seedMicros())) ? sc.seedMicros() : cachedSeedMicros;
    }

    /**
     * @return the current per-method latency stats, creating a new entry seeded from the cached
     *     median if the method has not been seen before
     */
    public MethodStats statsFor(String method) {
        return methods.computeIfAbsent(
                method,
                k -> {
                    MethodStats ms = new MethodStats((long) seedMicros(k), clocks.nanoTime());
                    MethodScale sc = fleetScales.get(k);
                    if (sc != null) ms.setScale(sc);
                    return ms;
                });
    }

    /**
     * @return the stats for {@code method}, or {@code null} if this subchannel has never served it.
     *     Read-only: unlike {@link #statsFor(String)} it never creates an entry.
     */
    public MethodStats peekStats(String method) {
        return methods.get(method);
    }

    /**
     * @return the sliding error window for the given method, creating one if needed
     */
    public ErrorWindow windowFor(String method) {
        return methodWindows.computeIfAbsent(
                method, k -> new ErrorWindow(INITIAL_ERROR_WINDOW_MILLIS));
    }

    private final ConcurrentHashMap<String, EjectionBackoff> methodBackoff =
            new ConcurrentHashMap<>();

    /**
     * Clears every method's error window. Called when the backend is ejected for errors, so that
     * after it returns it is judged only on fresh evidence, never re-ejected for the same errors.
     */
    void resetErrorWindows() {
        methodWindows.values().forEach(ErrorWindow::reset);
    }

    /** Ejection backoff for {@code method} on this backend (outlier tick only). */
    EjectionBackoff backoffFor(String method) {
        return methodBackoff.computeIfAbsent(method, k -> new EjectionBackoff());
    }

    /** Marks {@code method} as ejected until the given nano-timestamp. */
    public void ejectMethodUntil(String method, long untilNanos) {
        methodEjectedUntilNanos.put(method, untilNanos);
    }

    /**
     * @return when {@code method}'s most recent ejection ends (nano-time), or {@link
     *     Long#MIN_VALUE} if it was never ejected; used for the re-entry cooldown
     */
    public long methodEjectedUntilNanos(String method) {
        Long until = methodEjectedUntilNanos.get(method);
        return until == null ? Long.MIN_VALUE / 2 : until;
    }

    /**
     * @return true if {@code method} was recently ejected and the ejection has not yet expired
     */
    public boolean isMethodEjected(String method, long nowNanos) {
        Long until = methodEjectedUntilNanos.get(method);
        return until != null && nowNanos < until;
    }

    /**
     * @return an immutable snapshot of the current method keys
     */
    public Set<String> methodKeys() {
        return Set.copyOf(methods.keySet());
    }

    /** Increments the subchannel's inflight counter. Called when a stream is created. */
    public void incrementInflight() {
        inflight.getAndIncrement();
    }

    /** Decrements the subchannel's inflight counter, clamped at zero to guard against underflow. */
    public void decrementInflight() {
        inflight.getAndDecrement(); // paired with incrementInflight by the tracer
    }

    /**
     * Removes methods that have not been updated for {@code pruneAfterMillis} ms and trims the
     * table to at most {@code maxEntries}, oldest-last-update first. Also refreshes the cached seed
     * latency used to initialize new methods, so newly added methods inherit the current fleet's
     * observed latency rather than a stale default.
     */
    public void pruneStale(long nowNanos, long pruneAfterMillis, int maxEntries) {
        final long pruneAfterNanos = EwmaClocks.millisToNanos(pruneAfterMillis);

        for (var it = methods.entrySet().iterator(); it.hasNext(); ) {
            var e = it.next();
            MethodStats methodStats = e.getValue();
            long last = methodStats.getLastUpdateNanos();
            if (last == 0L || (nowNanos - last) >= pruneAfterNanos) {
                String key = e.getKey();
                it.remove();
                methodWindows.remove(key);
                methodEjectedUntilNanos.remove(key);
                methodBackoff.remove(key);
            }
        }

        int size = methods.size();
        if (size > maxEntries) {
            var list = new ArrayList<>(methods.entrySet());
            list.sort(Comparator.comparingLong(a -> a.getValue().getLastUpdateNanos()));
            int toDrop = size - maxEntries;
            for (int i = 0; i < toDrop; i++) {
                String key = list.get(i).getKey();
                methods.remove(key);
                methodWindows.remove(key);
                methodEjectedUntilNanos.remove(key);
                methodBackoff.remove(key);
            }
        }

        refreshCachedSeedMicros();
    }

    private void refreshCachedSeedMicros() {
        if (methods.isEmpty()) {
            cachedSeedMicros = Math.max(MIN_SEED_MICROS, peakEwmaConfig.initialRttMicros);
            return;
        }
        ArrayList<Double> vals = new ArrayList<>(methods.size());
        for (MethodStats m : methods.values()) {
            vals.add(m.getEwmaSlowMicros());
        }
        vals.sort(Double::compareTo);
        int n = vals.size();
        double median =
                (n & 1) == 1 ? vals.get(n / 2) : 0.5 * (vals.get(n / 2 - 1) + vals.get(n / 2));
        cachedSeedMicros = Math.max(MIN_SEED_MICROS, (long) Math.rint(median));
    }

    /**
     * @return the subchannel's current inflight stream count, never negative
     */
    public int getInflight() {
        return Math.max(0, inflight.get());
    }

    /**
     * @return the cached seed latency (microseconds) used for newly-observed methods
     */
    long cachedSeedMicros() {
        return cachedSeedMicros;
    }
}
