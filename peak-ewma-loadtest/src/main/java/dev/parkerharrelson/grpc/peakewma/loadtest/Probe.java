package dev.parkerharrelson.grpc.peakewma.loadtest;

import io.grpc.MethodDescriptor;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;

/**
 * The load-test wire protocol: one service whose methods are created on demand, so any number of
 * method names (including churning ones) works without a proto.
 *
 * <p>The method name carries the call's shape and nominal latency, so the backend needs no
 * per-method configuration:
 *
 * <ul>
 *   <li>{@code U<ms>_<name>}: unary, median service time {@code <ms>} (0 = respond immediately);
 *   <li>{@code S<ms>_<name>}: server streaming, 5 messages, then completes after {@code <ms>};
 *   <li>{@code W_<name>}: long-lived watch stream, one message per second until cancelled.
 * </ul>
 *
 * Payloads are raw bytes; the backend echoes the request bytes back.
 */
final class Probe {
    static final String SERVICE = "loadtest.Probe";

    static final MethodDescriptor.Marshaller<byte[]> BYTES =
            new MethodDescriptor.Marshaller<>() {
                @Override
                public InputStream stream(byte[] value) {
                    return new ByteArrayInputStream(value);
                }

                @Override
                public byte[] parse(InputStream stream) {
                    try {
                        return stream.readAllBytes();
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }
            };

    private Probe() {}

    static MethodDescriptor.MethodType typeOf(String bareMethod) {
        return switch (bareMethod.charAt(0)) {
            case 'U' -> MethodDescriptor.MethodType.UNARY;
            case 'S', 'W' -> MethodDescriptor.MethodType.SERVER_STREAMING;
            default -> throw new IllegalArgumentException("method must start with U, S or W");
        };
    }

    /** Nominal median latency encoded in the name: {@code U10_x} is 10 ms. */
    static double methodLatencyMillis(String bareMethod) {
        int us = bareMethod.indexOf('_');
        String digits = bareMethod.substring(1, us < 0 ? bareMethod.length() : us);
        return digits.isEmpty() ? 0.0 : Double.parseDouble(digits);
    }

    static MethodDescriptor<byte[], byte[]> descriptor(String bareMethod) {
        return MethodDescriptor.<byte[], byte[]>newBuilder()
                .setType(typeOf(bareMethod))
                .setFullMethodName(MethodDescriptor.generateFullMethodName(SERVICE, bareMethod))
                .setRequestMarshaller(BYTES)
                .setResponseMarshaller(BYTES)
                .build();
    }
}
