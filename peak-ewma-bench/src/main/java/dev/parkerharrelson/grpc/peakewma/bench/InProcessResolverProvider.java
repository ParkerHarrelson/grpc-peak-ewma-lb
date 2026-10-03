package dev.parkerharrelson.grpc.peakewma.bench;

import io.grpc.Attributes;
import io.grpc.EquivalentAddressGroup;
import io.grpc.NameResolver;
import io.grpc.NameResolverProvider;
import io.grpc.StatusOr;
import io.grpc.inprocess.InProcessSocketAddress;
import java.net.SocketAddress;
import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** Static resolver for {@code bench-inproc:///<name>} targets producing in-process addresses. */
final class InProcessResolverProvider extends NameResolverProvider {

    static final String SCHEME = "bench-inproc";
    private static final Map<String, List<EquivalentAddressGroup>> TARGETS =
            new ConcurrentHashMap<>();

    static void register(String name, List<EquivalentAddressGroup> eags) {
        TARGETS.put(name, List.copyOf(eags));
    }

    static void deregister(String name) {
        TARGETS.remove(name);
    }

    @Override
    protected boolean isAvailable() {
        return true;
    }

    @Override
    protected int priority() {
        return 5;
    }

    @Override
    public String getDefaultScheme() {
        return SCHEME;
    }

    @Override
    public Collection<Class<? extends SocketAddress>> getProducedSocketAddressTypes() {
        return Set.of(InProcessSocketAddress.class);
    }

    @Override
    public NameResolver newNameResolver(URI uri, NameResolver.Args args) {
        if (!SCHEME.equals(uri.getScheme())) return null;
        String name = uri.getPath().substring(1);
        return new NameResolver() {
            @Override
            public String getServiceAuthority() {
                return name;
            }

            @Override
            public void start(Listener2 listener) {
                listener.onResult(
                        ResolutionResult.newBuilder()
                                .setAddressesOrError(StatusOr.fromValue(TARGETS.get(name)))
                                .setAttributes(Attributes.EMPTY)
                                .build());
            }

            @Override
            public void shutdown() {}
        };
    }
}
