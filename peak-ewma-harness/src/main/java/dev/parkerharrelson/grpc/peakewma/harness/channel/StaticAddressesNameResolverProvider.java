package dev.parkerharrelson.grpc.peakewma.harness.channel;

import io.grpc.Attributes;
import io.grpc.EquivalentAddressGroup;
import io.grpc.NameResolver;
import io.grpc.NameResolverProvider;
import io.grpc.Status;
import io.grpc.StatusOr;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Minimal {@link NameResolverProvider} used by the load balancer test harness.
 *
 * <p>Targets of the form {@code peakewma-harness:///<name>} return whatever list of {@link
 * EquivalentAddressGroup}s was registered under {@code <name>} via {@link #register(String, List)}.
 * Scenarios can mutate the registered list and call {@link #refresh(String)} to simulate a resolver
 * churn mid-run.
 *
 * <p>Not suitable for production use — it lives in the harness module only.
 */
public final class StaticAddressesNameResolverProvider extends NameResolverProvider {

    public static final String SCHEME = "peakewma-harness";

    private static final Map<String, RegisteredTarget> TARGETS = new ConcurrentHashMap<>();

    private static final class RegisteredTarget {
        final AtomicReference<List<EquivalentAddressGroup>> addresses;
        final AtomicReference<NameResolver.Listener2> listener = new AtomicReference<>();

        RegisteredTarget(List<EquivalentAddressGroup> addresses) {
            this.addresses = new AtomicReference<>(addresses);
        }
    }

    /** Register or replace the addresses behind {@code name}. */
    public static void register(String name, List<EquivalentAddressGroup> addresses) {
        TARGETS.compute(
                name,
                (k, prev) -> {
                    if (prev == null) {
                        return new RegisteredTarget(addresses);
                    }
                    prev.addresses.set(addresses);
                    NameResolver.Listener2 l = prev.listener.get();
                    if (l != null) {
                        l.onResult(buildResolutionResult(addresses));
                    }
                    return prev;
                });
    }

    /** Removes a target so subsequent resolutions fail. */
    public static void deregister(String name) {
        TARGETS.remove(name);
    }

    /** Pushes a resolution error to the live listener, as a failing DNS lookup would. */
    public static void fail(String name, Status error) {
        RegisteredTarget t = TARGETS.get(name);
        if (t == null) return;
        NameResolver.Listener2 l = t.listener.get();
        if (l != null) l.onError(error);
    }

    /** Pokes registered listeners to re-resolve without changing the address list. */
    public static void refresh(String name) {
        RegisteredTarget t = TARGETS.get(name);
        if (t == null) return;
        NameResolver.Listener2 l = t.listener.get();
        if (l == null) return;
        l.onResult(buildResolutionResult(t.addresses.get()));
    }

    private static NameResolver.ResolutionResult buildResolutionResult(
            List<EquivalentAddressGroup> addresses) {
        return NameResolver.ResolutionResult.newBuilder()
                .setAddressesOrError(StatusOr.fromValue(addresses))
                .setAttributes(Attributes.EMPTY)
                .build();
    }

    @Override
    protected boolean isAvailable() {
        return true;
    }

    @Override
    protected int priority() {
        return 10;
    }

    @Override
    public String getDefaultScheme() {
        return SCHEME;
    }

    @Override
    public NameResolver newNameResolver(URI targetUri, NameResolver.Args args) {
        if (!SCHEME.equals(targetUri.getScheme())) {
            return null;
        }
        String path = targetUri.getPath();
        if (path == null || path.length() < 2) {
            return null;
        }
        String name = path.substring(1); // strip leading '/'
        return new StaticResolver(name);
    }

    private static final class StaticResolver extends NameResolver {
        private final String name;
        private final AtomicReference<Listener2> listener = new AtomicReference<>();

        StaticResolver(String name) {
            this.name = name;
        }

        @Override
        public String getServiceAuthority() {
            return name;
        }

        @Override
        public void start(Listener2 listener) {
            this.listener.set(listener);
            RegisteredTarget target = TARGETS.get(name);
            if (target == null) {
                listener.onError(
                        Status.UNAVAILABLE.withDescription(
                                "no harness target registered under '" + name + "'"));
                return;
            }
            target.listener.set(listener);
            listener.onResult(buildResolutionResult(target.addresses.get()));
        }

        @Override
        public void refresh() {
            StaticAddressesNameResolverProvider.refresh(name);
        }

        @Override
        public void shutdown() {
            RegisteredTarget target = TARGETS.get(name);
            if (target != null) {
                target.listener.compareAndSet(listener.get(), null);
            }
        }
    }
}
