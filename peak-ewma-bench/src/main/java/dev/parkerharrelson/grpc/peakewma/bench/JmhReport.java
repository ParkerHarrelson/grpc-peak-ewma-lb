package dev.parkerharrelson.grpc.peakewma.bench;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Renders a JMH CSV ({@code -rf csv}, ideally with {@code -prof gc}) as markdown: one table per
 * benchmark method, policies as rows, backend counts as columns, "time (alloc)" per cell.
 *
 * <pre>java -cp benchmarks.jar dev.parkerharrelson.grpc.peakewma.bench.JmhReport in.csv out.md
 * </pre>
 */
public final class JmhReport {

    private JmhReport() {}

    public static void main(String[] args) throws Exception {
        List<String> lines = Files.readAllLines(Path.of(args[0]));
        List<String> header = split(lines.get(0));
        int iBench = header.indexOf("Benchmark");
        int iScore = header.indexOf("Score");
        int iUnit = header.indexOf("Unit");
        int iPolicy = header.indexOf("Param: policy");
        int iBackends = header.indexOf("Param: backends");

        // method -> policy -> backends -> {time, alloc}
        Map<String, Map<String, Map<Integer, String[]>>> t = new TreeMap<>();
        Map<String, String> units = new TreeMap<>();
        TreeSet<Integer> sizes = new TreeSet<>();
        for (String line : lines.subList(1, lines.size())) {
            List<String> c = split(line);
            String bench = c.get(iBench);
            boolean alloc = bench.endsWith(":gc.alloc.rate.norm");
            if (bench.contains(":") && !alloc) continue;
            String method = bench.replaceAll(":.*", "").replaceAll(".*\\.", "");
            String cls =
                    bench.replaceAll(":.*", "").replaceAll("\\.[^.]+$", "").replaceAll(".*\\.", "");
            String key = cls + "." + method;
            int n = Integer.parseInt(c.get(iBackends));
            sizes.add(n);
            String[] cell =
                    t.computeIfAbsent(key, k -> new TreeMap<>())
                            .computeIfAbsent(c.get(iPolicy), k -> new TreeMap<>())
                            .computeIfAbsent(n, k -> new String[2]);
            double v = Double.parseDouble(c.get(iScore));
            if (alloc) cell[1] = String.format(Locale.ROOT, "%,.0f B", v);
            else {
                cell[0] = String.format(Locale.ROOT, v < 100 ? "%,.1f" : "%,.0f", v);
                units.put(key, c.get(iUnit));
            }
        }

        StringBuilder md = new StringBuilder();
        for (var e : t.entrySet()) {
            md.append("## ")
                    .append(e.getKey())
                    .append(" (")
                    .append(units.get(e.getKey()))
                    .append(", allocation per op)\n\n| policy |");
            for (int n : sizes) md.append(' ').append(n).append(" backends |");
            md.append("\n|---|");
            for (int ignored : sizes) md.append("---:|");
            md.append('\n');
            for (var p : e.getValue().entrySet()) {
                md.append("| ").append(p.getKey()).append(" |");
                for (int n : sizes) {
                    String[] cell = p.getValue().get(n);
                    md.append(' ');
                    if (cell != null && cell[0] != null) {
                        md.append(cell[0]);
                        if (cell[1] != null) md.append(" (").append(cell[1]).append(')');
                    }
                    md.append(" |");
                }
                md.append('\n');
            }
            md.append('\n');
        }
        Files.writeString(Path.of(args[1]), md);
        System.out.print(md);
    }

    private static List<String> split(String csvLine) {
        List<String> out = new ArrayList<>();
        StringBuilder cur = new StringBuilder();
        boolean q = false;
        for (char ch : csvLine.toCharArray()) {
            if (ch == '"') q = !q;
            else if (ch == ',' && !q) {
                out.add(cur.toString());
                cur.setLength(0);
            } else cur.append(ch);
        }
        out.add(cur.toString());
        return out;
    }
}
