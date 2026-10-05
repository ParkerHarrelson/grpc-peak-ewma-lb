package dev.parkerharrelson.grpc.peakewma.loadtest;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Minimal {@code --key value} parser; repeated keys accumulate. */
final class Args {
    private final Map<String, List<String>> values = new LinkedHashMap<>();

    Args(String[] argv) {
        for (int i = 0; i < argv.length; i++) {
            String a = argv[i];
            if (!a.startsWith("--")) throw new IllegalArgumentException("unexpected arg: " + a);
            String key = a.substring(2);
            String v = (i + 1 < argv.length && !argv[i + 1].startsWith("--")) ? argv[++i] : "true";
            values.computeIfAbsent(key, k -> new ArrayList<>()).add(v);
        }
    }

    String get(String key, String def) {
        List<String> v = values.get(key);
        return v == null ? def : v.get(v.size() - 1);
    }

    List<String> all(String key) {
        return values.getOrDefault(key, List.of());
    }

    Map<String, String> asMap() {
        Map<String, String> m = new LinkedHashMap<>();
        values.forEach((k, v) -> m.put(k, String.join(",", v)));
        return m;
    }
}
