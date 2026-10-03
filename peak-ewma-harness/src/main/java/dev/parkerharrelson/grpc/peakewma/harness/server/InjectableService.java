package dev.parkerharrelson.grpc.peakewma.harness.server;

import io.grpc.MethodDescriptor;
import io.grpc.ServerCallHandler;
import io.grpc.ServerServiceDefinition;
import io.grpc.Status;
import io.grpc.stub.ServerCalls;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

/**
 * A hand-built gRPC service with two unary byte[] methods: {@code fast} and {@code slow}. Both
 * methods consult the shared {@link BackendBehaviour} for this backend and either:
 *
 * <ul>
 *   <li>Fail immediately with {@code UNAVAILABLE} if outage is set,
 *   <li>Fail with {@code INTERNAL} if the configured error rate trips,
 *   <li>Or respond after a latency sampled from the behaviour snapshot.
 * </ul>
 *
 * <p>Latency is scheduled rather than slept on the grpc-nio thread so the harness does not exhaust
 * the Netty event loop under high QPS. Per-method counters let the reporter verify the backend
 * actually received the calls it was picked for.
 */
public final class InjectableService {

    public static final String SERVICE_NAME = "peakewma.harness.LoadBalancerProbe";
    public static final String METHOD_FAST = "Fast";
    public static final String METHOD_SLOW = "Slow";

    private static final MethodDescriptor.Marshaller<byte[]> BYTES_MARSHALLER =
            new MethodDescriptor.Marshaller<byte[]>() {
                @Override
                public InputStream stream(byte[] value) {
                    return new ByteArrayInputStream(value == null ? new byte[0] : value);
                }

                @Override
                public byte[] parse(InputStream stream) {
                    try {
                        return stream.readAllBytes();
                    } catch (java.io.IOException e) {
                        throw new IllegalStateException(
                                "failed to read harness request payload", e);
                    }
                }
            };

    private final BackendBehaviour behaviour;
    private final ScheduledExecutorService scheduler;
    private final LongAdder fastCalls = new LongAdder();
    private final LongAdder slowCalls = new LongAdder();

    public InjectableService(BackendBehaviour behaviour, ScheduledExecutorService scheduler) {
        this.behaviour = behaviour;
        this.scheduler = scheduler;
    }

    public long fastCalls() {
        return fastCalls.sum();
    }

    public long slowCalls() {
        return slowCalls.sum();
    }

    public ServerServiceDefinition bindService() {
        MethodDescriptor<byte[], byte[]> fast = methodDescriptor(METHOD_FAST);
        MethodDescriptor<byte[], byte[]> slow = methodDescriptor(METHOD_SLOW);

        return ServerServiceDefinition.builder(SERVICE_NAME)
                .addMethod(fast, handler(fastCalls))
                .addMethod(slow, handler(slowCalls))
                .build();
    }

    private static MethodDescriptor<byte[], byte[]> methodDescriptor(String methodName) {
        return MethodDescriptor.<byte[], byte[]>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(
                        MethodDescriptor.generateFullMethodName(SERVICE_NAME, methodName))
                .setRequestMarshaller(BYTES_MARSHALLER)
                .setResponseMarshaller(BYTES_MARSHALLER)
                .build();
    }

    private ServerCallHandler<byte[], byte[]> handler(LongAdder counter) {
        return ServerCalls.asyncUnaryCall(
                (req, resp) -> {
                    counter.increment();
                    BackendBehaviour.Snapshot snap = behaviour.snapshot();

                    if (snap.outage()) {
                        resp.onError(
                                Status.UNAVAILABLE
                                        .withDescription("backend outage")
                                        .asRuntimeException());
                        return;
                    }

                    if (snap.shouldError()) {
                        resp.onError(
                                Status.INTERNAL
                                        .withDescription("injected error")
                                        .asRuntimeException());
                        return;
                    }

                    long delayNanos = snap.sampleLatency().toNanos();
                    if (delayNanos <= 0L) {
                        respond(resp, req);
                        return;
                    }

                    scheduler.schedule(() -> respond(resp, req), delayNanos, TimeUnit.NANOSECONDS);
                });
    }

    private static void respond(io.grpc.stub.StreamObserver<byte[]> resp, byte[] req) {
        // Echo a fixed-size response that's independent of request size so payload doesn't skew
        // RTT measurements between scenarios.
        byte[] reply = new byte[8];
        System.arraycopy(req, 0, reply, 0, Math.min(req.length, reply.length));
        resp.onNext(reply);
        resp.onCompleted();
    }

    /**
     * @return the full method name (service/method) for {@code Fast}, used by the workload driver
     *     to construct a matching {@link MethodDescriptor} on the client side
     */
    public static String fastMethodName() {
        return MethodDescriptor.generateFullMethodName(SERVICE_NAME, METHOD_FAST);
    }

    /**
     * @return the full method name (service/method) for {@code Slow}
     */
    public static String slowMethodName() {
        return MethodDescriptor.generateFullMethodName(SERVICE_NAME, METHOD_SLOW);
    }

    /**
     * @return a client-side {@link MethodDescriptor} for the fast method
     */
    public static MethodDescriptor<byte[], byte[]> fastDescriptor() {
        return methodDescriptor(METHOD_FAST);
    }

    /**
     * @return a client-side {@link MethodDescriptor} for the slow method
     */
    public static MethodDescriptor<byte[], byte[]> slowDescriptor() {
        return methodDescriptor(METHOD_SLOW);
    }
}
