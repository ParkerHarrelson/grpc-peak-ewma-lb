package dev.parkerharrelson.grpc.peakewma.loadtest;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.ParseException;
import jdk.jfr.Configuration;
import jdk.jfr.Recording;

/**
 * JFR recording of the measurement window, for the flame-graph cells. Settings are a built-in name
 * ({@code profile}), a {@code .jfc} path, or {@code loadtest}: the template shipped in this module
 * ({@code loadtest.jfc}: the JDK {@code profile} settings with CPU sampling every 5 ms, so the
 * flame-graph cells get enough samples in a short window).
 */
final class JfrSupport {
    private JfrSupport() {}

    static Object start(String settings) throws IOException, ParseException {
        Configuration c;
        if (settings.equals("loadtest")) {
            try (InputStream in = JfrSupport.class.getResourceAsStream("/loadtest.jfc")) {
                c = Configuration.create(new InputStreamReader(in, StandardCharsets.UTF_8));
            }
        } else if (settings.endsWith(".jfc")) {
            c = Configuration.create(Path.of(settings));
        } else {
            c = Configuration.getConfiguration(settings);
        }
        Recording r = new Recording(c);
        r.setName("loadtest");
        r.start();
        return r;
    }

    static String stop(Object handle, Path out) throws IOException {
        Recording r = (Recording) handle;
        r.stop();
        Files.createDirectories(out.toAbsolutePath().getParent());
        r.dump(out);
        r.close();
        return out.toAbsolutePath().toString();
    }
}
