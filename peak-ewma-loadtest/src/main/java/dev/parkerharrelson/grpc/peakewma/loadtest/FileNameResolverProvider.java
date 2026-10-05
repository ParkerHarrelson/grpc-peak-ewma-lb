package dev.parkerharrelson.grpc.peakewma.loadtest;

import io.grpc.EquivalentAddressGroup;
import io.grpc.NameResolver;
import io.grpc.NameResolverProvider;
import io.grpc.Status;
import io.grpc.StatusOr;
import io.grpc.SynchronizationContext;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * {@code file:///abs/path}: the endpoints are the {@code host:port} lines of a file, re-read every
 * 500 ms. The scenario runner rewrites the file for pod churn (rolling restart, scale up / down),
 * which is what a headless Service's DNS answer does in Kubernetes, without DNS caching delays.
 */
public final class FileNameResolverProvider extends NameResolverProvider {

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
        return "file";
    }

    @Override
    public Collection<Class<? extends SocketAddress>> getProducedSocketAddressTypes() {
        return List.of(InetSocketAddress.class);
    }

    @Override
    public NameResolver newNameResolver(URI targetUri, NameResolver.Args args) {
        if (!"file".equals(targetUri.getScheme())) return null;
        return new FileResolver(Path.of(targetUri.getPath()), args.getSynchronizationContext());
    }

    private static final class FileResolver extends NameResolver {
        private final Path path;
        private final SynchronizationContext syncContext;
        private ScheduledExecutorService poller;
        private Listener2 listener;
        private List<String> last;

        FileResolver(Path path, SynchronizationContext syncContext) {
            this.path = path;
            this.syncContext = syncContext;
        }

        @Override
        public String getServiceAuthority() {
            return "loadtest";
        }

        @Override
        public void start(Listener2 listener) {
            this.listener = listener;
            poller =
                    Executors.newSingleThreadScheduledExecutor(
                            r -> {
                                Thread t = new Thread(r, "file-resolver");
                                t.setDaemon(true);
                                return t;
                            });
            poller.scheduleWithFixedDelay(this::poll, 0, 500, TimeUnit.MILLISECONDS);
        }

        private synchronized void poll() {
            List<String> lines;
            try {
                lines =
                        Files.readAllLines(path).stream()
                                .map(String::trim)
                                .filter(s -> !s.isEmpty() && !s.startsWith("#"))
                                .sorted()
                                .toList();
            } catch (IOException e) {
                return; // mid-rewrite; next poll
            }
            if (lines.equals(last)) return;
            last = lines;
            List<EquivalentAddressGroup> eags = new ArrayList<>();
            for (String l : lines) {
                int c = l.lastIndexOf(':');
                eags.add(
                        new EquivalentAddressGroup(
                                new InetSocketAddress(
                                        l.substring(0, c), Integer.parseInt(l.substring(c + 1)))));
            }
            ResolutionResult result =
                    ResolutionResult.newBuilder()
                            .setAddressesOrError(
                                    eags.isEmpty()
                                            ? StatusOr.fromStatus(
                                                    Status.UNAVAILABLE.withDescription(
                                                            "empty address file"))
                                            : StatusOr.fromValue(eags))
                            .build();
            // Listener2 must be called on the channel's synchronization context.
            syncContext.execute(() -> listener.onResult2(result));
        }

        @Override
        public void refresh() {
            // The poller picks up changes; nothing to force.
        }

        @Override
        public void shutdown() {
            if (poller != null) poller.shutdownNow();
        }
    }
}
