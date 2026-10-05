package dev.parkerharrelson.grpc.peakewma.loadtest;

import java.util.Map;

/**
 * How one backend port responds, swapped atomically by the admin endpoint.
 *
 * <p>A method's own latency comes from its name ({@link Probe#methodLatencyMillis}); this scales it
 * and adds the faults the scenarios need:
 *
 * @param latencyFactor multiplies every method's median latency (noisy neighbour 2x, brownout 5x)
 * @param sigma lognormal shape of the service time; p99 = median x exp(2.326 sigma)
 * @param slots requests served concurrently; the rest queue (FIFO), so capacity is finite and the
 *     backend saturates like a real one. 0 = unlimited.
 * @param errorRate fraction of calls failed with UNAVAILABLE before doing work
 * @param mode {@code normal}, {@code unavailable} (every call fails instantly, as a crash-looping
 *     pod behind a proxy) or {@code blackhole} (calls are accepted and never answered)
 * @param gcPeriodMillis every this many ms the backend stops the world for {@code gcPauseMillis} (0
 *     = never): nothing completes during the pause
 * @param gcPauseMillis pause length
 * @param extraDelayMillis fixed delay added after service, outside the slots (cross-zone RTT)
 */
public record Behaviour(
        double latencyFactor,
        double sigma,
        int slots,
        double errorRate,
        String mode,
        long gcPeriodMillis,
        long gcPauseMillis,
        double extraDelayMillis) {

    public static final Behaviour DEFAULT = new Behaviour(1.0, 0.3, 0, 0.0, "normal", 0, 0, 0.0);

    /** Copy with the given fields overridden; unknown keys are rejected. */
    public Behaviour with(Map<String, String> kv) {
        double latencyFactor = this.latencyFactor;
        double sigma = this.sigma;
        int slots = this.slots;
        double errorRate = this.errorRate;
        String mode = this.mode;
        long gcPeriod = this.gcPeriodMillis;
        long gcPause = this.gcPauseMillis;
        double extra = this.extraDelayMillis;
        for (Map.Entry<String, String> e : kv.entrySet()) {
            String v = e.getValue();
            switch (e.getKey()) {
                case "latencyFactor" -> latencyFactor = Double.parseDouble(v);
                case "sigma" -> sigma = Double.parseDouble(v);
                case "slots" -> slots = Integer.parseInt(v);
                case "errorRate" -> errorRate = Double.parseDouble(v);
                case "mode" -> {
                    if (!v.equals("normal") && !v.equals("unavailable") && !v.equals("blackhole")) {
                        throw new IllegalArgumentException("mode: " + v);
                    }
                    mode = v;
                }
                case "gcPeriodMillis" -> gcPeriod = Long.parseLong(v);
                case "gcPauseMillis" -> gcPause = Long.parseLong(v);
                case "extraDelayMillis" -> extra = Double.parseDouble(v);
                case "ports", "port" -> {
                    // selector, not a behaviour field
                }
                default ->
                        throw new IllegalArgumentException("unknown behaviour key: " + e.getKey());
            }
        }
        return new Behaviour(
                latencyFactor, sigma, slots, errorRate, mode, gcPeriod, gcPause, extra);
    }

    String toJson() {
        return String.format(
                java.util.Locale.ROOT,
                "{\"latencyFactor\":%s,\"sigma\":%s,\"slots\":%d,\"errorRate\":%s,\"mode\":\"%s\","
                        + "\"gcPeriodMillis\":%d,\"gcPauseMillis\":%d,\"extraDelayMillis\":%s}",
                latencyFactor,
                sigma,
                slots,
                errorRate,
                mode,
                gcPeriodMillis,
                gcPauseMillis,
                extraDelayMillis);
    }
}
