package dev.parkerharrelson.grpc.peakewma.bench;

import dev.parkerharrelson.grpc.peakewma.harness.server.InjectableService;
import io.grpc.Attributes;
import io.grpc.CallOptions;
import io.grpc.ClientStreamTracer;
import io.grpc.LoadBalancer.PickResult;
import io.grpc.LoadBalancer.PickSubchannelArgs;
import io.grpc.LoadBalancer.SubchannelPicker;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
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
 * Cost of the LB's per-RPC work in isolation: {@code pickSubchannel} plus the stream tracer
 * lifecycle the policy attaches (streamCreated/streamClosed), on the policy's REAL picker over real
 * in-process subchannels. This is the LB overhead every RPC pays, without transport noise.
 *
 * <pre>
 * java -jar peak-ewma-bench/target/benchmarks.jar PickerBenchmark -prof gc
 * java -jar peak-ewma-bench/target/benchmarks.jar PickerBenchmark -p backends=10,100 -p policy=round_robin,peak_ewma_p2c
 * </pre>
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(
        value = 1,
        jvmArgsAppend = {"-Xms1g", "-Xmx1g"})
public class PickerBenchmark {

    @Param({
        "pick_first",
        "round_robin",
        "least_request_experimental",
        "weighted_round_robin",
        "peak_ewma_p2c"
    })
    public String policy;

    @Param({"3", "10", "100", "500"})
    public int backends;

    private BenchCluster cluster;
    private SubchannelPicker picker;

    @Setup(Level.Trial)
    public void setUp() throws Exception {
        cluster = new BenchCluster(backends, Duration.ZERO);
        cluster.connect(policy, true);
        cluster.warm(Math.max(2_000, backends * 20));
        picker = cluster.picker();
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        cluster.close();
    }

    @State(Scope.Thread)
    public static class Call {
        final Metadata headers = new Metadata();
        final ClientStreamTracer.StreamInfo info =
                ClientStreamTracer.StreamInfo.newBuilder().build();
        final PickSubchannelArgs args = args(InjectableService.fastDescriptor(), headers);
    }

    @Benchmark
    @Threads(1)
    public PickResult pick_1thread(Call c) {
        return pickAndTrace(c);
    }

    @Benchmark
    @Threads(8)
    public PickResult pick_8threads(Call c) {
        return pickAndTrace(c);
    }

    private PickResult pickAndTrace(Call c) {
        PickResult r = picker.pickSubchannel(c.args);
        ClientStreamTracer.Factory f = r.getStreamTracerFactory();
        if (f != null) {
            ClientStreamTracer t = f.newClientStreamTracer(c.info, c.headers);
            t.streamCreated(Attributes.EMPTY, c.headers);
            t.streamClosed(Status.OK);
        }
        return r;
    }

    static PickSubchannelArgs args(MethodDescriptor<?, ?> md, Metadata headers) {
        return new PickSubchannelArgs() {
            @Override
            public CallOptions getCallOptions() {
                return CallOptions.DEFAULT;
            }

            @Override
            public Metadata getHeaders() {
                return headers;
            }

            @Override
            public MethodDescriptor<?, ?> getMethodDescriptor() {
                return md;
            }
        };
    }
}
