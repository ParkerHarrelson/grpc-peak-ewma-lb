package dev.parkerharrelson.grpc.peakewma.bench;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

/**
 * A full unary RPC through a real channel (in-process transport, zero-latency backend), per policy.
 * Puts the LB overhead from {@link PickerBenchmark} in context of a whole RPC.
 *
 * <pre>
 * java -jar peak-ewma-bench/target/benchmarks.jar RpcBenchmark -prof gc
 * </pre>
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(
        value = 1,
        jvmArgsAppend = {"-Xms1g", "-Xmx1g"})
public class RpcBenchmark {

    @Param({
        "pick_first",
        "round_robin",
        "least_request_experimental",
        "weighted_round_robin",
        "peak_ewma_p2c"
    })
    public String policy;

    @Param({"3", "10", "100"})
    public int backends;

    private BenchCluster cluster;

    @Setup(Level.Trial)
    public void setUp() throws Exception {
        cluster = new BenchCluster(backends, Duration.ZERO);
        cluster.connect(policy, false);
        cluster.warm(Math.max(2_000, backends * 20));
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        cluster.close();
    }

    @Benchmark
    @Threads(1)
    public boolean unary_1thread() {
        return cluster.call(5_000);
    }

    @Benchmark
    @Threads(8)
    public boolean unary_8threads() {
        return cluster.call(5_000);
    }
}
