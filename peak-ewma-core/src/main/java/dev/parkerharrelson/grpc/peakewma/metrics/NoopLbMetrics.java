package dev.parkerharrelson.grpc.peakewma.metrics;

public final class NoopLbMetrics implements LbMetrics {

    public static final NoopLbMetrics INSTANCE = new NoopLbMetrics();

    private NoopLbMetrics() {
        /* no-op */
    }

    @Override
    public void recordPick(String outcome) {
        /* no-op */
    }

    @Override
    public void setInflight(String subchannelId, int inflight) {
        /* no-op */
    }

    @Override
    public void setCost(String subchannelId, String method, double cost) {
        /* no-op */
    }

    @Override
    public void recordOutlierEjection(
            String subchannelId, String reason, double errorRate, double latencyRatio) {
        /* no-op */
    }

    @Override
    public void setReadySubchannelCount(int readyCount) {
        /* no-op */
    }

    @Override
    public void setEjectedSubchannelCount(int ejectedCount) {
        /* no-op */
    }

    @Override
    public void setAdaptiveTuning(String key, double value) {
        /* no-op */
    }

    @Override
    public void setMethodLatencyEwma(String method, double slowEwmaMicros, double fastEwmaMicros) {
        /* no-op */
    }

    @Override
    public void removeSubchannel(String subchannelId) {
        /* no-op */
    }

    @Override
    public void setMethodRate(String method, double ratePerSec) {
        /* no-op */
    }

    @Override
    public void setMethodErrorRate(String method, double errorRate) {
        /* no-op */
    }
}
