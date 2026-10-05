#!/usr/bin/env python3
"""Report generator for the #94 load test.

    report.py <results-dir> [--out <dir>] [--no-pdf]

Reads the raw output of run.py (and kind-smoke) - JSONL per client, run.json / cell.json /
servers.jsonl / manifest.json, optionally gzipped - and regenerates everything:

    <out>/data/*.csv       tidy tables
    <out>/report.html      self-contained interactive report (plotly.js inlined once)
    <out>/report.pdf       the same charts and tables
    <out>/png/*.png        static charts
    <results-dir>/SUMMARY.md   go/no-go table, headline numbers, key charts (renders on GitHub;
                           --summary to put it elsewhere)

<out> defaults to <results-dir>/report. Every section tolerates missing data: it is skipped with
a note, never fatal.
"""
import argparse
import base64
import glob
import gzip
import html
import io
import json
import math
import os
import re
import shutil
import subprocess
import sys
import traceback
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

import matplotlib
import matplotlib.ticker

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.backends.backend_pdf import PdfPages  # noqa: E402
from matplotlib.patches import Rectangle  # noqa: E402

import plotly.graph_objects as go  # noqa: E402
from plotly.offline import get_plotlyjs  # noqa: E402

# ---------------------------------------------------------------------------------------------
# policies: fixed order, fixed colour per policy across every chart

POLICIES = ["control", "round_robin", "peak_ewma_p2c", "least_request_experimental", "lr_od"]
COLOR = {
    "control": "#8c8c8c",
    "round_robin": "#2a78d6",
    "peak_ewma_p2c": "#eb6834",
    "least_request_experimental": "#1baf7a",
    "lr_od": "#4a3aa7",
}
LABEL = {
    "control": "control (no LB)",
    "round_robin": "round_robin",
    "peak_ewma_p2c": "peak_ewma_p2c",
    "least_request_experimental": "least_request",
    "lr_od": "least_request + outlier_detection",
}
PEAK, RR, LR, LROD, CTL = POLICIES[2], POLICIES[1], POLICIES[3], POLICIES[4], POLICIES[0]

WARN = []


def warn(msg):
    WARN.append(msg)
    print("WARN:", msg, file=sys.stderr)


# ---------------------------------------------------------------------------------------------
# io


def read_jsonl(path):
    p = Path(path)
    if not p.exists() and Path(str(p) + ".gz").exists():
        p = Path(str(p) + ".gz")
    if not p.exists():
        return None
    op = gzip.open if p.suffix == ".gz" else open
    out = []
    with op(p, "rt") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError:
                warn(f"bad JSON line in {p}")
    return out


def jsonl_files(d, pattern="*.jsonl"):
    fs = set(glob.glob(str(Path(d) / pattern))) | {
        f[:-3] for f in glob.glob(str(Path(d) / (pattern + ".gz")))}
    return sorted(fs)


def load_client(path):
    lines = read_jsonl(path)
    if not lines:
        return None
    meta = next((l for l in lines if l.get("type") == "meta"), {})
    ivs = [l for l in lines if l.get("type") == "interval"]
    summ = next((l for l in reversed(lines) if l.get("type") == "summary"), None)
    return {"meta": meta, "intervals": ivs, "summary": summ, "path": str(path)}


def mean_ci(values):
    v = np.array([x for x in values if x is not None and not (isinstance(x, float) and math.isnan(x))],
                 dtype=float)
    if len(v) == 0:
        return float("nan"), float("nan"), 0
    if len(v) == 1:
        return float(v[0]), float("nan"), 1
    m = v.mean()
    h = stats.t.ppf(0.975, len(v) - 1) * v.std(ddof=1) / math.sqrt(len(v))
    return float(m), float(h), len(v)


def welch_p(a, b):
    a = [x for x in a if x is not None and not math.isnan(x)]
    b = [x for x in b if x is not None and not math.isnan(x)]
    if len(a) < 2 or len(b) < 2:
        return float("nan")
    if np.std(a) == 0 and np.std(b) == 0:
        return 0.0 if np.mean(a) != np.mean(b) else 1.0
    return float(stats.ttest_ind(a, b, equal_var=False).pvalue)


def fmt(x, nd=1, unit=""):
    if x is None or (isinstance(x, float) and (math.isnan(x) or math.isinf(x))):
        return "n/a"
    if abs(x) >= 1000 and nd <= 1:
        return f"{x:,.0f}{unit}"
    return f"{x:.{nd}f}{unit}"


def pct(x, nd=0):
    return "n/a" if x is None or math.isnan(x) else f"{x:+.{nd}f}%"


def g(d, *keys, default=float("nan")):
    for k in keys:
        if not isinstance(d, dict) or k not in d or d[k] is None:
            return default
        d = d[k]
    return d


# ---------------------------------------------------------------------------------------------
# report model: sections of items (charts, tables, text), rendered three ways


class Chart:
    """kind: lines | bars | heatmap | box | image."""

    def __init__(self, cid, title, kind, xlabel="", ylabel="", takeaway="", key=False, **kw):
        self.cid, self.title, self.kind = cid, title, kind
        self.xlabel, self.ylabel, self.takeaway, self.key = xlabel, ylabel, takeaway, key
        self.series = kw.get("series", [])  # lines/bars: dicts name,x,y,err,color,dash
        self.categories = kw.get("categories")  # bars
        self.logx = kw.get("logx", False)
        self.logy = kw.get("logy", False)
        self.shade = kw.get("shade", [])  # [(x0, x1, label)]
        self.vlines = kw.get("vlines", [])  # [(x, label)]
        self.hlines = kw.get("hlines", [])  # [(y, label)]
        self.z, self.xs, self.ys = kw.get("z"), kw.get("xs"), kw.get("ys")  # heatmap
        self.zlabel = kw.get("zlabel", "")
        self.zmin, self.zmax = kw.get("zmin"), kw.get("zmax")
        self.annot = kw.get("annot", False)
        self.cmap = kw.get("cmap", "seq")  # seq | div
        self.groups = kw.get("groups")  # box: [(name, values, color)]
        self.png_bytes = kw.get("png_bytes")  # image
        self.height = kw.get("height", 380)
        self.categorical_x = kw.get("categorical_x", False)


class Table:
    def __init__(self, tid, title, df, takeaway="", status_col=None, key=False):
        self.tid, self.title, self.df, self.takeaway = tid, title, df, takeaway
        self.status_col, self.key = status_col, key


class Text:
    def __init__(self, text, kind="p"):
        self.text, self.kind = text, kind


SECTIONS = []  # [(title, [items])]


def section(title):
    items = []
    SECTIONS.append((title, items))
    return items


# --- plotly


def _seq_scale():
    return [[0, "#f3f6fb"], [0.5, "#7fa9e0"], [1, "#0d3f86"]]


def _div_scale():
    # orange (worse) <- grey -> blue (better); midpoint neutral
    return [[0, "#2a78d6"], [0.5, "#e6e6e3"], [1, "#eb6834"]]


def to_plotly(c):
    fig = go.Figure()
    if c.kind == "lines":
        for s in c.series:
            err = s.get("err")
            ed = None
            if err is not None and any(e is not None and not math.isnan(e) for e in err):
                ed = dict(type="data", array=[0 if (e is None or math.isnan(e)) else e for e in err],
                          visible=True, thickness=1.5, width=4)
            fig.add_trace(go.Scatter(
                x=s["x"], y=s["y"], name=s["name"], mode=s.get("mode", "lines+markers"),
                line=dict(color=s.get("color"), width=2, dash=s.get("dash", "solid")),
                marker=dict(size=8), error_y=ed,
                hovertemplate="%{x}: %{y:.3g}<extra>" + s["name"] + "</extra>"))
        for x0, x1, lab in c.shade:
            fig.add_vrect(x0=x0, x1=x1, fillcolor="#e34948", opacity=0.08, line_width=0,
                          annotation_text=lab, annotation_position="top left")
        for x, lab in c.vlines:
            fig.add_vline(x=x, line_dash="dot", line_color="#52514e", annotation_text=lab)
        for y, lab in c.hlines:
            fig.add_hline(y=y, line_dash="dash", line_color="#52514e", annotation_text=lab)
    elif c.kind == "bars":
        for s in c.series:
            err = s.get("err")
            ed = None
            if err is not None and any(e is not None and not math.isnan(e) for e in err):
                ed = dict(type="data", array=[0 if (e is None or math.isnan(e)) else e for e in err],
                          visible=True, thickness=1.5, width=4)
            fig.add_trace(go.Bar(x=c.categories, y=s["y"], name=s["name"],
                                 marker_color=s.get("color"), error_y=ed,
                                 hovertemplate="%{x}: %{y:.3g}<extra>" + s["name"] + "</extra>"))
        fig.update_layout(barmode="group", bargap=0.25, bargroupgap=0.05)
        for y, lab in c.hlines:
            fig.add_hline(y=y, line_dash="dash", line_color="#52514e", annotation_text=lab)
    elif c.kind == "heatmap":
        fig.add_trace(go.Heatmap(
            z=c.z, x=c.xs, y=c.ys, zmin=c.zmin, zmax=c.zmax,
            colorscale=_div_scale() if c.cmap == "div" else _seq_scale(),
            colorbar=dict(title=c.zlabel),
            text=[[("" if v is None or (isinstance(v, float) and math.isnan(v)) else f"{v:.2f}")
                   for v in row] for row in c.z] if c.annot else None,
            texttemplate="%{text}" if c.annot else None,
            hovertemplate="%{x} / %{y}: %{z:.3g}<extra></extra>"))
        for x0, x1, lab in c.shade:
            fig.add_vrect(x0=x0, x1=x1, line_color="#e34948", line_width=1, fillcolor="rgba(0,0,0,0)")
    elif c.kind == "box":
        for name, vals, color in c.groups:
            fig.add_trace(go.Box(y=vals, name=name, marker_color=color, boxpoints="all",
                                 jitter=0.3, pointpos=0))
    fig.update_layout(
        title=dict(text=c.title, font=dict(size=15)), height=c.height,
        margin=dict(l=60, r=20, t=50, b=50), template="plotly_white",
        legend=dict(orientation="h", y=-0.2), font=dict(color="#0b0b0b"),
        xaxis_title=c.xlabel, yaxis_title=c.ylabel)
    if c.logx:
        fig.update_xaxes(type="log")
    if c.logy:
        fig.update_yaxes(type="log")
    if c.categorical_x:
        fig.update_xaxes(type="category")
    return fig


# --- matplotlib


def to_mpl(c):
    if c.kind == "image":
        fig = plt.figure(figsize=(10, 4.2))
        img = plt.imread(io.BytesIO(c.png_bytes), format="png")
        ax = fig.add_axes([0, 0, 1, 0.92])
        ax.imshow(img)
        ax.axis("off")
        fig.suptitle(c.title, fontsize=11)
        return fig
    fig, ax = plt.subplots(figsize=(10, 4.6 if c.kind != "heatmap" else max(4.0, 0.25 * len(c.ys or []) + 2)))
    ax.set_title(c.title, fontsize=11, loc="left")
    for sp in ("top", "right"):
        ax.spines[sp].set_visible(False)
    ax.grid(True, color="#e6e6e3", linewidth=0.6)
    ax.set_axisbelow(True)
    if c.kind == "lines":
        for s in c.series:
            x = s["x"]
            y = np.array([np.nan if v is None else v for v in s["y"]], dtype=float)
            ls = {"solid": "-", "dot": ":", "dash": "--"}.get(s.get("dash", "solid"), "-")
            mk = "o" if "markers" in s.get("mode", "lines+markers") else None
            if c.categorical_x:
                x = list(range(len(x)))
            ax.plot(x, y, ls, color=s.get("color"), lw=2, marker=mk, ms=5, label=s["name"])
            err = s.get("err")
            if err is not None:
                e = np.array([np.nan if v is None else v for v in err], dtype=float)
                if np.any(~np.isnan(e)):
                    ax.errorbar(x, y, yerr=np.nan_to_num(e), fmt="none", ecolor=s.get("color"),
                                elinewidth=1.2, capsize=3)
        if c.categorical_x and c.series:
            ax.set_xticks(range(len(c.series[0]["x"])))
            ax.set_xticklabels(c.series[0]["x"], rotation=20, ha="right", fontsize=8)
        for x0, x1, lab in c.shade:
            ax.axvspan(x0, x1, color="#e34948", alpha=0.08, lw=0)
            ax.text(x0, 1.0, " " + lab, transform=ax.get_xaxis_transform(), fontsize=8,
                    va="top", color="#52514e")
        for x, lab in c.vlines:
            ax.axvline(x, ls=":", color="#52514e", lw=1)
        for y, lab in c.hlines:
            ax.axhline(y, ls="--", color="#52514e", lw=1)
            ax.text(1.0, y, " " + lab, transform=ax.get_yaxis_transform(), fontsize=8, va="bottom",
                    ha="right", color="#52514e")
    elif c.kind == "bars":
        cats = c.categories
        n = max(1, len(c.series))
        w = 0.8 / n
        xs = np.arange(len(cats))
        for i, s in enumerate(c.series):
            y = np.array([np.nan if v is None else v for v in s["y"]], dtype=float)
            e = s.get("err")
            e = None if e is None else np.nan_to_num(np.array([np.nan if v is None else v for v in e],
                                                              dtype=float))
            ax.bar(xs + (i - (n - 1) / 2) * w, np.nan_to_num(y), w * 0.92, color=s.get("color"),
                   label=s["name"], yerr=e, capsize=2, error_kw=dict(elinewidth=1))
        ax.set_xticks(xs)
        ax.set_xticklabels(cats, rotation=15 if len(cats) > 4 else 0, ha="right" if len(cats) > 4 else "center",
                           fontsize=8)
        for y, lab in c.hlines:
            ax.axhline(y, ls="--", color="#52514e", lw=1)
    elif c.kind == "heatmap":
        z = np.array([[np.nan if v is None else v for v in row] for row in c.z], dtype=float)
        cmap = matplotlib.colors.LinearSegmentedColormap.from_list(
            "c", [p[1] for p in (_div_scale() if c.cmap == "div" else _seq_scale())])
        ax.grid(False)
        xs = c.xs
        numeric_x = all(isinstance(v, (int, float)) for v in xs)
        extent = None
        if numeric_x and len(xs) > 1:
            extent = [xs[0], xs[-1], len(c.ys) - 0.5, -0.5]
        im = ax.imshow(z, aspect="auto", cmap=cmap, vmin=c.zmin, vmax=c.zmax, extent=extent,
                       interpolation="nearest")
        ax.set_yticks(range(len(c.ys)))
        ax.set_yticklabels(c.ys, fontsize=7 if len(c.ys) > 12 else 9)
        if not numeric_x:
            ax.set_xticks(range(len(xs)))
            ax.set_xticklabels(xs, fontsize=9)
        cb = fig.colorbar(im, ax=ax)
        cb.set_label(c.zlabel)
        if c.annot:
            for i in range(z.shape[0]):
                for j in range(z.shape[1]):
                    if not np.isnan(z[i, j]):
                        ax.text(j, i, f"{z[i, j]:.2f}", ha="center", va="center", fontsize=9,
                                color="#0b0b0b")
        for x0, x1, lab in c.shade:
            ax.axvline(x0, color="#e34948", lw=1)
            ax.axvline(x1, color="#e34948", lw=1)
    elif c.kind == "box":
        data = [vals for _, vals, _ in c.groups]
        bp = ax.boxplot(data, patch_artist=True, widths=0.5)
        for patch, (_, _, color) in zip(bp["boxes"], c.groups):
            patch.set_facecolor(color)
            patch.set_alpha(0.35)
        ax.set_xticks(range(1, len(c.groups) + 1))
        ax.set_xticklabels([n for n, _, _ in c.groups], fontsize=9)
    ax.set_xlabel(c.xlabel)
    ax.set_ylabel(c.ylabel)
    if c.logx:
        ax.set_xscale("log")
    if c.logy:
        ax.set_yscale("log")
        ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f"{v:g}"))
        ax.yaxis.set_minor_formatter(matplotlib.ticker.NullFormatter())
    if c.logx:
        ax.xaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f"{v:g}"))
    if c.kind in ("lines", "bars") and len(c.series) >= 2:
        ax.legend(fontsize=8, frameon=False, loc="best")
    if c.takeaway:
        fig.text(0.01, 0.005, c.takeaway, fontsize=8, color="#52514e", wrap=True)
    fig.tight_layout(rect=(0, 0.04, 1, 1))
    return fig


def mpl_image(fig):
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=110)
    plt.close(fig)
    return buf.getvalue()


# ---------------------------------------------------------------------------------------------
# Tier 1


def tier1_load(root):
    rows = []
    for cdir in sorted(glob.glob(str(root / "tier1" / "*"))):
        cdir = Path(cdir)
        cj = cdir / "cell.json"
        if not cj.exists():
            continue
        cell = json.loads(cj.read_text())
        for f in jsonl_files(cdir):
            m = re.match(r"(.+)-r(\d+)\.jsonl$", Path(f).name)
            if not m:
                continue
            try:
                c = load_client(f)
            except Exception as e:
                warn(f"{f}: {e}")
                continue
            s = c and c["summary"]
            if not s:
                warn(f"tier1 {cdir.name}/{Path(f).name}: no summary (run incomplete)")
                continue
            pol, rep = m.group(1), int(m.group(2))
            rows.append({
                "cell": cell["cell"], "backends": cell["backends"], "methods": str(cell["methods"]),
                "target_rps": cell["rps"], "threads": cell["threads"], "mix": cell["mix"],
                "payload": cell["payload"], "policy": pol, "repeat": rep,
                "rps": s.get("rps"), "cpu_us_per_rpc": s.get("cpu_us_per_rpc"),
                "alloc_b_per_rpc": s.get("alloc_b_per_rpc"), "cores_used": s.get("cores_used"),
                "rps_per_core": (s.get("rps") or 0) / s["cores_used"] if s.get("cores_used") else np.nan,
                "gc_per_s": s.get("gc_per_s"), "gc_pause_p99_ms": s.get("gc_pause_p99_ms"),
                "alloc_mb_per_s": s.get("alloc_mb_per_s"),
                "pick_p50_ns": g(s, "pick_ns", "p50"), "pick_p99_ns": g(s, "pick_ns", "p99"),
                "pick_p999_ns": g(s, "pick_ns", "p999"), "pick_samples": g(s, "pick_ns", "n", default=0),
                "tick_p50_ms": g(s, "tick_ms", "p50"), "tick_p99_ms": g(s, "tick_ms", "p99"),
                "tick_busy_ms_per_s": g(s, "tick_ms", "busy_ms_per_s"),
                "lat_p50_us": g(s, "latency", "all", "p50_us"), "lat_p99_us": g(s, "latency", "all", "p99_us"),
                "ok_frac": (s.get("ok") or 0) / s["completed"] if s.get("completed") else np.nan,
                **{f"pick_{k}": v for k, v in (s.get("pick_outcomes") or {}).items()},
            })
    df = pd.DataFrame(rows)
    if not df.empty:
        # LB cost = policy - control in the same repeat (same backend JVM, adjacent in time)
        ctl = df[df.policy == CTL].set_index(["cell", "repeat"])
        for col in ("cpu_us_per_rpc", "alloc_b_per_rpc"):
            df[col + "_minus_control"] = [
                (getattr(r, col) - ctl.loc[(r.cell, r.repeat), col]) if (r.cell, r.repeat) in ctl.index else np.nan
                for r in df.itertuples()]
    return df


def agg_cell(df, cell, policy, col):
    v = df[(df.cell == cell) & (df.policy == policy)][col].tolist()
    return mean_ci(v)


SWEEPS = {
    "backends": ("backends", [("backends_3", 3), ("centre", 10), ("backends_50", 50),
                              ("backends_200", 200), ("backends_500", 500)], "backends per channel"),
    "methods": ("methods", [("methods_1", 1), ("centre", 10), ("methods_100", 100)], "distinct methods"),
    "rps": ("rps", [("rps_1k", 1000), ("centre", 10000), ("rps_50k", 50000)], "target load (RPS)"),
    "threads": ("threads", [("threads_1", 1), ("centre", 8), ("threads_32", 32)], "caller threads"),
}


def sweep_chart(df, sweep, col, ylabel, title, cid, logy=False, policies=POLICIES, key=False):
    _, cells, xlabel = SWEEPS[sweep]
    series = []
    for pol in policies:
        xs, ys, es = [], [], []
        for cell, x in cells:
            m, h, n = agg_cell(df, cell, pol, col)
            if n:
                xs.append(x)
                ys.append(m)
                es.append(h)
        if xs:
            series.append(dict(name=LABEL[pol], x=xs, y=ys, err=es, color=COLOR[pol]))
    if not series:
        return None
    # takeaway: peak vs rr at the largest x present for both
    tk = ""
    pk = next((s for s in series if s["name"] == LABEL[PEAK]), None)
    rr = next((s for s in series if s["name"] == LABEL[RR]), None)
    if pk and rr:
        common = sorted(set(pk["x"]) & set(rr["x"]))
        if common:
            x = common[-1]
            a, b = pk["y"][pk["x"].index(x)], rr["y"][rr["x"].index(x)]
            tk = (f"At {x:,} {xlabel}, peak_ewma_p2c is {fmt(a, 2)} vs round_robin {fmt(b, 2)} "
                  f"({pct(100 * (a - b) / b if b else float('nan'))}).")
    return Chart(cid, title, "lines", xlabel=xlabel, ylabel=ylabel, series=series,
                 logx=sweep in ("backends", "methods", "rps"), logy=logy, takeaway=tk, key=key)


def tier1_sections(root, df):
    items = section("Tier 1: client-side cost (overhead matrix)")
    if df.empty:
        items.append(Text("No Tier 1 data found; section skipped."))
        return {}
    df.to_csv(OUT / "data" / "tier1_runs.csv", index=False)
    items.append(Text(
        "Homogeneous backends answering immediately, one client JVM per run (4 CPUs, 1 GB heap), "
        "fresh JVM per policy and repeat, repeats interleaved with rotated policy order. "
        "CPU and allocation are whole-process per completed RPC, so they include the harness "
        "(Netty, loadgen); the <b>control</b> client (pick_first to a single backend) is the "
        "floor and <b>LB cost = policy - control</b>. Error bars are 95% CIs (Student t) over repeats."))

    # per cell x policy aggregate table
    agg = []
    for (cell, pol), grp in df.groupby(["cell", "policy"]):
        r = {"cell": cell, "policy": pol, "n": len(grp)}
        for col in ("cpu_us_per_rpc", "cpu_us_per_rpc_minus_control", "alloc_b_per_rpc",
                    "alloc_b_per_rpc_minus_control", "rps", "rps_per_core", "pick_p50_ns", "pick_p99_ns",
                    "pick_p999_ns", "gc_pause_p99_ms", "alloc_mb_per_s", "gc_per_s", "tick_p50_ms",
                    "tick_p99_ms", "tick_busy_ms_per_s", "lat_p99_us"):
            m, h, _ = mean_ci(grp[col].tolist())
            r[col] = m
            r[col + "_ci"] = h
        agg.append(r)
    agg = pd.DataFrame(agg)
    agg.to_csv(OUT / "data" / "tier1_cells.csv", index=False)

    for sweep in ("backends", "methods", "rps", "threads"):
        c = sweep_chart(df, sweep, "cpu_us_per_rpc", "client CPU per RPC (µs)",
                        f"Client CPU per RPC vs {SWEEPS[sweep][2]}", f"t1_cpu_vs_{sweep}",
                        key=sweep == "backends")
        if c:
            items.append(c)
    c = sweep_chart(df, "backends", "cpu_us_per_rpc_minus_control", "LB cost: CPU per RPC minus control (µs)",
                    "LB cost (policy - control) vs backends", "t1_lbcost_vs_backends",
                    policies=POLICIES[1:], key=True)
    if c:
        items.append(c)
    c = sweep_chart(df, "backends", "alloc_b_per_rpc", "bytes allocated per RPC",
                    "Bytes allocated per RPC vs backends", "t1_alloc_vs_backends")
    if c:
        c.takeaway += " The harness floor (control) dominates the absolute value; read the deltas."
        items.append(c)
    c = sweep_chart(df, "backends", "alloc_b_per_rpc_minus_control", "bytes per RPC minus control",
                    "Allocation attributable to the LB (policy - control) vs backends",
                    "t1_allocdelta_vs_backends", policies=POLICIES[1:])
    if c:
        items.append(c)
    # pick time
    for col, lab in (("pick_p50_ns", "p50"), ("pick_p99_ns", "p99"), ("pick_p999_ns", "p99.9")):
        c = sweep_chart(df, "backends", col, f"pickSubchannel {lab} (ns, log)",
                        f"Pick time {lab} vs backends (sampled 1 in 1,000 picks)", f"t1_pick_{lab}",
                        logy=True, key=lab == "p99")
        if c:
            if lab == "p99":
                c.hlines = [(5000, "5 µs criterion")]
            items.append(c)
    # mix / payload / churn / saturation grouped bars
    cats = [("centre", "unary, 100 B"), ("mix_unary80_stream20", "80/20 unary/stream"),
            ("mix_watch_plus_unary", "watch streams + unary"), ("payload_16k", "16 KB payload"),
            ("methods_1000_churn", "1,000 churning methods"), ("corner_small", "corner 3b×1m×1k"),
            ("corner_big", "corner 500b×100m×50k")]
    cats = [(c, l) for c, l in cats if c in set(df.cell)]
    if cats:
        for col, ylab, title, cid in (
                ("cpu_us_per_rpc", "client CPU per RPC (µs)", "CPU per RPC by call mix, payload and corner cells", "t1_cpu_mix"),
                ("cpu_us_per_rpc_minus_control", "CPU per RPC minus control (µs)", "LB cost by call mix, payload and corner cells", "t1_lbcost_mix")):
            series = []
            for pol in (POLICIES if "minus" not in col else POLICIES[1:]):
                ms = [agg_cell(df, c, pol, col) for c, _ in cats]
                series.append(dict(name=LABEL[pol], y=[m[0] for m in ms], err=[m[1] for m in ms],
                                   color=COLOR[pol]))
            pk = [agg_cell(df, c, PEAK, "cpu_us_per_rpc")[0] for c, _ in cats]
            rr = [agg_cell(df, c, RR, "cpu_us_per_rpc")[0] for c, _ in cats]
            d = [100 * (a - b) / b for a, b in zip(pk, rr) if b and not math.isnan(a) and not math.isnan(b)]
            tk = (f"peak_ewma_p2c vs round_robin CPU per RPC ranges {min(d):+.0f}% to {max(d):+.0f}% across these cells."
                  if d else "")
            items.append(Chart(cid, title, "bars", ylabel=ylab, categories=[l for _, l in cats],
                               series=series, takeaway=tk))
    # saturation
    if "rps_saturation" in set(df.cell):
        ms = [agg_cell(df, "rps_saturation", p, "rps_per_core") for p in POLICIES]
        best = max((m[0], p) for m, p in zip(ms, POLICIES) if not math.isnan(m[0]))
        pk, rr = ms[2][0], ms[1][0]
        items.append(Chart("t1_saturation", "Max throughput at saturation: RPS per client core",
                           "bars", ylabel="completed RPS per client CPU core", categories=["saturation"],
                           series=[dict(name=LABEL[p], y=[m[0]], err=[m[1]], color=COLOR[p])
                                   for p, m in zip(POLICIES, ms)],
                           takeaway=f"peak_ewma_p2c sustains {fmt(pk, 0)} RPS/core vs round_robin "
                                    f"{fmt(rr, 0)} ({pct(100 * (pk - rr) / rr if rr else float('nan'))}); "
                                    f"highest: {LABEL[best[1]]}."))
    # GC at centre
    if "centre" in set(df.cell):
        cats2 = ["GC pause p99 (ms)", "allocation rate (MB/s)", "GC collections per second"]
        series = []
        for p in POLICIES:
            ms = [agg_cell(df, "centre", p, c) for c in ("gc_pause_p99_ms", "alloc_mb_per_s", "gc_per_s")]
            series.append(dict(name=LABEL[p], y=[m[0] for m in ms], err=[m[1] for m in ms], color=COLOR[p]))
        items.append(Chart("t1_gc", "GC at the centre point (10 backends, 10 methods, 10k RPS)", "bars",
                           ylabel="value (unit per group)", categories=cats2, series=series,
                           takeaway="GC behaviour is set by the harness allocation floor; differences between policies are within the CIs unless the bars separate."))
    # LB cost table with Welch t-test vs RR
    rows = []
    order = tier1_cell_order()
    for cell in sorted(set(df.cell), key=lambda c: order.index(c) if c in order else 99):
        rr_cpu = df[(df.cell == cell) & (df.policy == RR)].cpu_us_per_rpc.tolist()
        for pol in POLICIES[1:]:
            sub = df[(df.cell == cell) & (df.policy == pol)]
            if sub.empty:
                continue
            m, h, n = mean_ci(sub.cpu_us_per_rpc.tolist())
            lc, lh, _ = mean_ci(sub.cpu_us_per_rpc_minus_control.tolist())
            am, _, _ = mean_ci(sub.alloc_b_per_rpc_minus_control.tolist())
            rrm = np.mean(rr_cpu) if rr_cpu else float("nan")
            p = welch_p(sub.cpu_us_per_rpc.tolist(), rr_cpu) if pol != RR else float("nan")
            rows.append({"cell": cell, "policy": LABEL[pol], "n": n,
                         "CPU µs/RPC": f"{fmt(m, 2)} ± {fmt(h, 2)}",
                         "LB cost µs/RPC (− control)": f"{fmt(lc, 2)} ± {fmt(lh, 2)}",
                         "Δ CPU vs RR": "baseline" if pol == RR else
                         (pct(100 * (m - rrm) / rrm) + ("" if (not math.isnan(p) and p < 0.05) else " (n.s.)")),
                         "Welch p vs RR": "" if pol == RR else fmt(p, 3),
                         "LB alloc B/RPC (− control)": fmt(am, 0),
                         "pick p99 ns": fmt(agg_cell(df, cell, pol, "pick_p99_ns")[0], 0)})
    if rows:
        t = pd.DataFrame(rows)
        t.to_csv(OUT / "data" / "tier1_lbcost_table.csv", index=False)
        items.append(Table("t1_lbcost", "LB cost per cell (mean ± 95% CI; Welch t-test vs round_robin)", t))
    # peak internals: tracer, tick, outcomes
    pk = df[df.policy == PEAK]
    if not pk.empty:
        rows = []
        for cell, grp in pk.groupby("cell"):
            rows.append({"cell": cell,
                         "outlier tick p50/p99 ms": f"{fmt(grp.tick_p50_ms.mean(), 3)} / {fmt(grp.tick_p99_ms.mean(), 3)}",
                         "sync-context busy ms/s": fmt(grp.tick_busy_ms_per_s.mean(), 3),
                         **{f"picks {k[5:]}": int(grp[k].sum()) for k in grp.columns
                            if k.startswith("pick_") and k[5:] in ("ok", "ok_fallback", "all_ejected", "no_ready", "no_table")
                            and not grp[k].isna().all()}})
        t = pd.DataFrame(rows)
        t.to_csv(OUT / "data" / "tier1_peak_internals.csv", index=False)
        items.append(Table("t1_peak_internals", "peak_ewma_p2c internals per cell (sampled timers, outlier tick, pick outcomes)", t))
    return {"agg": agg}


def tier1_cell_order():
    return ["centre", "backends_3", "backends_50", "backends_200", "backends_500", "methods_1",
            "methods_100", "methods_1000_churn", "rps_1k", "rps_50k", "rps_saturation", "threads_1",
            "threads_32", "mix_unary80_stream20", "mix_watch_plus_unary", "payload_16k", "corner_big",
            "corner_small"]


# --- Tier 1 extras: memory + JFR


def memory_section(root):
    items = section("Tier 1: memory under 1,000 churning methods")
    res = {}
    series = []
    for pol in POLICIES:
        c = None
        p = root / "tier1_extra" / f"memory_churn-{pol}.jsonl"
        try:
            c = load_client(p)
        except Exception as e:
            warn(f"{p}: {e}")
        if not c:
            continue
        pts = [(iv["t"], iv["heap_after_gc"] / 2**20) for iv in c["intervals"]
               if iv.get("heap_after_gc") is not None and not iv.get("warm")]
        if len(pts) < 2:
            continue
        xs, ys = zip(*pts)
        series.append(dict(name=LABEL[pol], x=list(xs), y=list(ys), color=COLOR[pol]))
        T = xs[-1]
        half = [(x, y) for x, y in pts if x >= T / 2]
        if len(half) >= 3:
            slope = np.polyfit([x for x, _ in half], [y for _, y in half], 1)[0]
            growth = 100 * slope * (T / 2) / np.mean([y for _, y in half])
        else:
            growth = float("nan")
        res[pol] = {"growth_pct_last_half": growth, "duration_s": T, "final_mb": ys[-1],
                    "first_mb": ys[0]}
    if not series:
        items.append(Text("No memory_churn data (tier1_extra) yet; section skipped."))
        return res
    pk = res.get(PEAK, {})
    tk = (f"peak_ewma_p2c heap after GC goes {fmt(pk.get('first_mb'), 1)} → {fmt(pk.get('final_mb'), 1)} MB over "
          f"{fmt(pk.get('duration_s'), 0)} s; growth over the last half is {pct(pk.get('growth_pct_last_half', float('nan')), 1)} "
          f"({'plateaued' if abs(pk.get('growth_pct_last_half', 99)) < 5 else 'still growing'}).") if pk else ""
    items.append(Chart("t1_heap_churn", "Process heap after GC vs time, 1,000 churning methods (10 new names/s)",
                       "lines", xlabel="time since measurement start (s)", ylabel="heap used after System.gc() (MiB)",
                       series=series, takeaway=tk, key=True))
    items.append(Text("Heap is the whole client process after a forced GC every 15 s (LB state + channel + "
                      "harness), at 10 backends. Per-method entries are pruned after 120 s idle and capped at "
                      "512 per backend, so the LB share must stop growing once the cap/prune steady state is reached."))
    pd.DataFrame([{"policy": k, **v} for k, v in res.items()]).to_csv(OUT / "data" / "tier1_memory.csv", index=False)
    return res


def jfr_tool():
    try:
        jh = subprocess.check_output(["/usr/libexec/java_home"], text=True).strip()
        p = Path(jh) / "bin" / "jfr"
        if p.exists():
            return str(p)
    except Exception:
        pass
    return shutil.which("jfr")


def jfr_parse(path, cache_dir):
    cache = cache_dir / (Path(path).name + ".stacks.json")
    if cache.exists() and cache.stat().st_mtime >= Path(path).stat().st_mtime:
        return json.loads(cache.read_text())
    tool = jfr_tool()
    if not tool:
        raise RuntimeError("jfr tool not found")
    out = subprocess.run([tool, "print", "--json", "--stack-depth", "64", "--events",
                          "jdk.ExecutionSample", str(path)], capture_output=True, text=True, timeout=600)
    if out.returncode != 0:
        raise RuntimeError(out.stderr[:300])
    data = json.loads(out.stdout)
    stacks = defaultdict(int)
    for ev in data.get("recording", {}).get("events", []):
        frames = (ev.get("values", {}).get("stackTrace") or {}).get("frames") or []
        names = []
        for fr in frames:
            m = fr.get("method") or {}
            t = (m.get("type") or {}).get("name", "?")
            names.append(f"{t}.{m.get('name', '?')}")
        if names:
            stacks[";".join(reversed(names))] += 1  # root first
    res = {"stacks": stacks}
    cache.write_text(json.dumps(res))
    return res


LT_PKG = "dev.parkerharrelson.grpc.peakewma.loadtest"


def lb_frame(f):
    return f.startswith("dev.parkerharrelson.") and not f.startswith(LT_PKG)


def flame_png(stacks, title, max_depth=28, min_frac=0.004):
    total = sum(stacks.values())
    tree = {}

    for st, n in stacks.items():
        node = tree
        for f in st.split(";")[:max_depth]:
            ch = node.setdefault(f, {"n": 0, "c": {}})
            ch["n"] += n
            node = ch["c"]
    fig, ax = plt.subplots(figsize=(12, 4.2))
    ax.set_xlim(0, 1)
    ax.set_ylim(0, max_depth)
    ax.axis("off")

    def draw(node, x0, depth):
        x = x0
        for f, ch in sorted(node.items(), key=lambda kv: -kv[1]["n"]):
            w = ch["n"] / total
            if w < min_frac:
                x += w
                continue
            col = "#eb6834" if lb_frame(f) else ("#8c8c8c" if f.startswith(LT_PKG) else "#9fc0ea")
            ax.add_patch(Rectangle((x, depth), w, 0.95, facecolor=col, edgecolor="white", lw=0.3))
            if w > 0.06:
                short = f.split(".")[-2] + "." + f.split(".")[-1] if f.count(".") >= 1 else f
                ax.text(x + 0.002, depth + 0.45, short[: int(w * 140)].replace("$", r"\$"), fontsize=5, va="center")
            draw(ch["c"], x, depth + 1)
            x += w

    draw(tree, 0, 0)
    ax.set_title(title + "  (orange = dev.parkerharrelson LB frames, grey = loadtest harness)", fontsize=9, loc="left")
    return mpl_image(fig)


def jfr_section(root):
    items = section("Tier 1: where the client CPU goes (JFR)")
    files = sorted(glob.glob(str(root / "tier1_extra" / "jfr_*.jfr")))
    if not files:
        items.append(Text("No JFR recordings (tier1_extra/jfr_*.jfr) yet; section skipped."))
        return {}
    rows, res = [], {}
    for f in files:
        m = re.match(r"jfr_(.+)-([a-z0-9_]+)\.jfr$", Path(f).name)
        if not m:
            continue
        cell, pol = m.group(1), m.group(2)
        try:
            st = jfr_parse(f, OUT / "data")["stacks"]
        except Exception as e:
            warn(f"JFR {f}: {e}")
            continue
        total = sum(st.values())
        if not total:
            continue
        incl = sum(n for s, n in st.items() if any(lb_frame(x) for x in s.split(";")))
        self_ = sum(n for s, n in st.items() if lb_frame(s.split(";")[-1]))
        harness = sum(n for s, n in st.items() if s.split(";")[-1].startswith(LT_PKG))
        rows.append({"cell": cell, "policy": pol, "samples": total, "LB inclusive %": 100 * incl / total,
                     "LB self %": 100 * self_ / total, "harness self %": 100 * harness / total})
        res[(cell, pol)] = rows[-1]
        items.append(Chart(f"t1_flame_{cell}_{pol}", f"Flame graph: {cell}, {LABEL.get(pol, pol)}", "image",
                           png_bytes=flame_png(st, f"{cell} / {pol}: {total} samples"),
                           takeaway=f"{100 * incl / total:.1f}% of CPU samples have an LB frame on the stack "
                                    f"({100 * self_ / total:.1f}% with the LB itself on top)."))
    if rows:
        t = pd.DataFrame(rows)
        t.to_csv(OUT / "data" / "tier1_jfr_share.csv", index=False)
        items.insert(0, Table("t1_jfr", "LB share of client CPU from JFR execution samples (5 ms period)",
                              t.round(2)))
    return res



# ---------------------------------------------------------------------------------------------
# Decision table: the one table to read


SCENARIO_WHAT = {
    "s01_steady_het": "steady, uneven fleet (3 noisy, 1 GC, 1 flaky 5%, 1 throttled of 20)",
    "s02_steady_homo": "steady, identical pods",
    "s03_brownout": "one pod 5x slower for 40 s",
    "s04_crashloop": "one pod fails every call for 40 s",
    "s05_blackhole": "one pod never answers for 40 s",
    "s06_gc_pause": "one pod pauses 500 ms every 10 s",
    "s07_rolling_restart": "every pod replaced by a cold one",
    "s08_scale_up_down": "+5 cold pods, then -5 pods",
    "s09_max_connection_age": "GOAWAY every 10 s",
    "s10_node_cpu_contention": "4 pods (one node) 2x slower, half capacity",
    "s11_client_restart": "client starts cold against the uneven fleet",
    "s12_cross_zone": "a third of the pods +2 ms RTT",
    "s13_steady_het_5_methods": "uneven fleet, 5 methods 1 ms..1 s",
}
CONTENDERS = ["least_request_experimental", "lr_od"]
SHORT = {"round_robin": "round_robin", "peak_ewma_p2c": "peak", "least_request_experimental": "least_request",
         "lr_od": "lr+od"}


def decision_table(runs):
    """Per scenario: worst-case p99 (fault window if any) and error rate per policy, mean of repeats,
    and a verdict naming a winner only when the margin is material:
    p99 >= 20% and >= 2 ms better; error rate >= 0.1 pp and >= 1.5x lower; recovery >= 5 s faster."""
    rows = []
    pols = ["peak_ewma_p2c"] + CONTENDERS + ["round_robin"]
    for tag in scenario_order([t for t in runs if re.match(r"s\d\d_", t)]):
        acc = {p: defaultdict(list) for p in pols}
        for run in runs[tag]:
            for p in pols:
                m = run_policy_metrics(run, p)
                if not m:
                    continue
                acc[p]["p99"].append(m.get("fault_p99_ms", m["p99_ms"]))
                acc[p]["err"].append(m.get("fault_error_rate", m["error_rate"]))
                _, rec = detect_recover(run, p)
                acc[p]["rec"].append(rec)
        mean = {p: {k: (float(np.nanmean(v)) if v and not all(np.isnan(v)) else float("nan"))
                    for k, v in acc[p].items()} for p in pols}
        # A repeat that never recovered within the window counts as "not recovered".
        never = {p: bool(acc[p]["rec"]) and any(np.isnan(x) for x in acc[p]["rec"]) for p in pols}
        if "peak_ewma_p2c" not in mean or not acc["peak_ewma_p2c"]["p99"]:
            continue
        pk = mean["peak_ewma_p2c"]
        verdict = []
        others = [p for p in CONTENDERS if acc[p]["p99"]]
        if others:
            bo = min(others, key=lambda p: mean[p]["p99"])
            a, b = pk["p99"], mean[bo]["p99"]
            if a <= 0.8 * b and b - a >= 2:
                verdict.append(f"**peak**: p99 {100 * (1 - a / b):.0f}% lower than {SHORT[bo]}")
            elif b <= 0.8 * a and a - b >= 2:
                verdict.append(f"**{SHORT[bo]}**: p99 {100 * (1 - b / a):.0f}% lower")
            be = min(others, key=lambda p: mean[p]["err"])
            ea, eb = pk["err"], mean[be]["err"]
            if eb + 0.001 <= ea and ea >= 1.5 * eb:
                verdict.append(f"**{SHORT[be]}**: fewer errors ({100 * eb:.2f}% vs {100 * ea:.2f}%)")
            elif ea + 0.001 <= eb and eb >= 1.5 * ea:
                verdict.append(f"**peak**: fewer errors ({100 * ea:.2f}% vs {100 * eb:.2f}%)")
            fast = [p for p in others if acc[p]["rec"] and not never[p]]
            if acc["peak_ewma_p2c"]["rec"] and fast:
                bf = min(fast, key=lambda p: mean[p]["rec"])
                if never["peak_ewma_p2c"] or pk["rec"] >= mean[bf]["rec"] + 5:
                    pr = "not within 30 s" if never["peak_ewma_p2c"] else f"{pk['rec']:.0f} s"
                    verdict.append(f"**{SHORT[bf]}**: recovers faster ({mean[bf]['rec']:.0f} s vs {pr})")
        row = {"scenario": tag, "what happens": SCENARIO_WHAT.get(tag, "")}
        for p in pols:
            row[f"p99 {SHORT[p]} (ms)"] = mean[p].get("p99", float("nan"))
        for p in pols:
            # ok can exceed completed by in-flight calls that finish after the window: clamp at 0
            row[f"errors {SHORT[p]}"] = max(0.0, mean[p].get("err", float("nan")))
        row["verdict (peak vs least_request / lr+od)"] = "; ".join(verdict) or "tie"
        rows.append(row)
    return pd.DataFrame(rows)


def decision_md(dt):
    if dt.empty:
        return "No Tier 2 scenarios yet."
    d = dt.copy()
    for c in d.columns:
        if c.startswith("p99"):
            d[c] = d[c].map(lambda v: "—" if pd.isna(v) else (f"{v:,.0f}" if v >= 100 else f"{v:.1f}"))
        elif c.startswith("errors"):
            d[c] = d[c].map(lambda v: "—" if pd.isna(v) else f"{100 * v:.2f}%")
    cols = list(d.columns)
    out = ["| " + " | ".join(cols) + " |", "|" + "|".join("---" for _ in cols) + "|"]
    for _, r in d.iterrows():
        out.append("| " + " | ".join(str(r[c]) for c in cols) + " |")
    return "\n".join(out)


def decision_counts(dt):
    if dt.empty:
        return ""
    v = dt["verdict (peak vs least_request / lr+od)"]
    has_peak = [("**peak**" in x) for x in v]
    has_other = [x.replace("**peak**", "").count("**") > 0 for x in v]
    peak = sum(a and not b for a, b in zip(has_peak, has_other))
    other = sum(b and not a for a, b in zip(has_peak, has_other))
    mixed = sum(a and b for a, b in zip(has_peak, has_other))
    tie = len(v) - peak - other - mixed
    return (f"Of {len(v)} scenarios: peak_ewma_p2c wins {peak}, ties {tie}, loses {other}, mixed {mixed} "
            f"(winner named only for material margins: p99 >= 20% and >= 2 ms, errors >= 0.1 pp and 1.5x, "
            f"recovery >= 5 s).")


# ---------------------------------------------------------------------------------------------
# Tier 2


class Run:
    def __init__(self, d, info, clients, servers):
        self.dir, self.info, self.clients, self.servers = d, info, clients, servers

    @property
    def tag(self):
        return self.info.get("tag") or self.info.get("scenario")


def tier2_load(root):
    runs = defaultdict(list)
    for rj in sorted(glob.glob(str(root / "tier2" / "*" / "r*" / "run.json"))):
        d = Path(rj).parent
        try:
            info = json.loads(Path(rj).read_text())
            clients = []
            for c in info.get("clients", []):
                cl = load_client(d / c["file"])
                if not cl or not cl["summary"]:
                    warn(f"{d.name}/{c['file']}: incomplete client")
                    continue
                cl["policy"] = c["policy"]
                cl["client"] = c.get("client", c["file"])
                clients.append(cl)
            servers = read_jsonl(d / "servers.jsonl") or []
            runs[info.get("tag") or info["scenario"]].append(Run(d, info, clients, servers))
        except Exception as e:
            warn(f"{rj}: {e}")
    return runs


def kind_load(root):
    d = root / "kind-smoke"
    rj = d / "run.json"
    if not rj.exists():
        return None
    info = json.loads(rj.read_text())
    clients = []
    for c in info.get("clients", []):
        cl = load_client(d / c["file"])
        if cl and cl["summary"]:
            cl["policy"] = c["policy"]
            cl["client"] = c.get("client", c["file"])
            clients.append(cl)
    info.setdefault("initial_backends", info.get("backends", []))
    info.setdefault("fleet", {b: "normal" for b in info.get("backends", [])})
    info.setdefault("duration_s", max((iv["t"] for cl in clients for iv in cl["intervals"]), default=90))
    return Run(d, info, clients, [])


def measured(iv, run):
    return not iv.get("warm") and 0 < iv["t"] <= run.info.get("duration_s", 1e9) + 1.5


def healthy_pods(run):
    fp = set((run.info.get("fault") or {}).get("pods") or [])
    fleet = run.info.get("fleet", {})
    init = run.info.get("initial_backends") or list(fleet)
    return [a for a in init if a not in fp and fleet.get(a, "normal") == "normal"]


def policy_clients(run, pol):
    return [c for c in run.clients if c["policy"] == pol]


def run_policy_metrics(run, pol):
    cl = policy_clients(run, pol)
    if not cl:
        return None
    s = [c["summary"] for c in cl]
    comp = sum(x.get("completed", 0) for x in s)
    ok = sum(x.get("ok", 0) for x in s)
    meas = np.mean([x.get("measure_s", 0) for x in s])
    st = defaultdict(int)
    for x in s:
        for k, v in (x.get("status") or {}).items():
            st[k] += v
    r = {
        "p50_ms": np.mean([g(x, "latency", "all", "p50_us") for x in s]) / 1000,
        "p90_ms": np.mean([g(x, "latency", "all", "p90_us") for x in s]) / 1000,
        "p99_ms": np.mean([g(x, "latency", "all", "p99_us") for x in s]) / 1000,
        "p999_ms": np.mean([g(x, "latency", "all", "p999_us") for x in s]) / 1000,
        "error_rate": 1 - ok / comp if comp else float("nan"),
        "deadline_exceeded": st.get("DEADLINE_EXCEEDED", 0),
        "unavailable": st.get("UNAVAILABLE", 0),
        "goodput_rps": ok / meas if meas else float("nan"),
        "completed": comp,
        "ejections_errors": sum(g(x, "ejections", "errors", default=0) for x in s),
        "ejections_latency": sum(g(x, "ejections", "latency", default=0) for x in s),
        "clients": len(cl), "measure_s": meas,
    }
    for ph in ("before", "fault", "after"):
        if all(ph in (x.get("phases") or {}) for x in s):
            pc = sum(g(x, "phases", ph, "completed", default=0) for x in s)
            pok = sum(g(x, "phases", ph, "status", "OK", default=0) for x in s)
            pst = sum(sum((g(x, "phases", ph, "status", default={}) or {}).values()) for x in s)
            r[f"{ph}_p99_ms"] = np.mean([g(x, "phases", ph, "latency", "all", "p99_us") for x in s]) / 1000
            r[f"{ph}_p50_ms"] = np.mean([g(x, "phases", ph, "latency", "all", "p50_us") for x in s]) / 1000
            r[f"{ph}_error_rate"] = 1 - pok / pst if pst else float("nan")
    # traffic balance across healthy pods over the whole window
    hp = healthy_pods(run)
    tot = defaultdict(int)
    for c in cl:
        for iv in c["intervals"]:
            if measured(iv, run):
                for a, n in (iv.get("backends") or {}).items():
                    tot[a] += n
    allc = sum(v for k, v in tot.items() if k != "none")
    live = [a for a in (run.info.get("initial_backends") or []) if a in tot] or [k for k in tot if k != "none"]
    if allc and hp and live:
        fair = 1 / len(live)
        shares = np.array([tot.get(a, 0) / allc for a in hp])
        r["healthy_share_cv"] = float(shares.std() / shares.mean()) if shares.mean() else float("nan")
        r["healthy_share_min_over_fair"] = float(shares.min() / fair)
        r["healthy_share_max_over_fair"] = float(shares.max() / fair)
    fp = (run.info.get("fault") or {}).get("pods") or []
    if allc and fp:
        r["fault_pods_share_whole"] = sum(tot.get(a, 0) for a in fp) / allc
    return r


def share_timeline(run, pol, pods):
    """t -> fraction of this policy's calls (summed over its clients) sent to `pods`."""
    num, den = defaultdict(int), defaultdict(int)
    for c in policy_clients(run, pol):
        for iv in c["intervals"]:
            if iv.get("warm"):
                continue
            t = round(iv["t"])
            b = iv.get("backends") or {}
            den[t] += sum(v for k, v in b.items() if k != "none")
            num[t] += sum(b.get(a, 0) for a in pods)
    ts = sorted(t for t in den if den[t] > 0)
    return ts, [num[t] / den[t] for t in ts]


def smooth(ys, k=3):
    out = []
    for i in range(len(ys)):
        w = ys[max(0, i - k + 1): i + 1]
        out.append(sum(w) / len(w))
    return out


def detect_recover(run, pol):
    f = run.info.get("fault")
    if not f or not f.get("pods") or run.info.get("scenario") in ("s08_scale_up_down",):
        return float("nan"), float("nan")
    fair = len(f["pods"]) / max(1, len(run.info.get("initial_backends") or []))
    ts, ys = share_timeline(run, pol, f["pods"])
    if not ts:
        return float("nan"), float("nan")
    ys = smooth(ys, 3)
    det = float("nan")
    for t, y in zip(ts, ys):
        if t > f["start_s"] and y < fair / 3:
            det = t - f["start_s"]
            break
    rec = float("nan")
    if f["end_s"] < run.info.get("duration_s", 90):
        streak = 0
        for t, y in zip(ts, ys):
            if t <= f["end_s"]:
                continue
            if abs(y - fair) <= 0.1 * fair:
                streak += 1
                if streak >= 3:
                    rec = t - 2 - f["end_s"]
                    break
            else:
                streak = 0
    return det, rec


def dominant_method(run):
    tot = defaultdict(int)
    for c in run.clients:
        for k, v in (g(c["summary"], "latency", default={}) or {}).items():
            if k != "all" and isinstance(v, dict):
                tot[k] += v.get("n", 0)
    return max(tot, key=tot.get) if tot else None


def p99_timeline(run, pol, method):
    acc = defaultdict(list)
    for c in policy_clients(run, pol):
        for iv in c["intervals"]:
            if iv.get("warm"):
                continue
            m = (iv.get("methods") or {}).get(method) or {}
            if "p99_us" in m:
                acc[round(iv["t"])].append(m["p99_us"] / 1000)
    ts = sorted(acc)
    return ts, [float(np.mean(acc[t])) for t in ts]


def err_timeline(run, pol):
    n, ok = defaultdict(int), defaultdict(int)
    for c in policy_clients(run, pol):
        for iv in c["intervals"]:
            if iv.get("warm"):
                continue
            t = round(iv["t"])
            st = iv.get("status") or {}
            n[t] += sum(st.values())
            ok[t] += st.get("OK", 0)
    ts = sorted(t for t in n if n[t])
    return ts, [100 * (1 - ok[t] / n[t]) for t in ts]


SCEN_TITLES = {
    "s01_steady_het": "Steady state, heterogeneous fleet",
    "s02_steady_homo": "Steady state, homogeneous fleet",
    "s03_brownout": "Brownout: one pod ×5 latency for 40 s",
    "s04_crashloop": "Crash-looping pod (instant UNAVAILABLE)",
    "s05_blackhole": "Black-holed pod (never answers)",
    "s06_gc_pause": "GC-pause pod (500 ms stop-the-world every 10 s)",
    "s07_rolling_restart": "Rolling restart of the backend Deployment",
    "s08_scale_up_down": "Scale up +5 cold pods, then scale down −5",
    "s09_max_connection_age": "Server maxConnectionAge 10 s (periodic GOAWAY)",
    "s10_node_cpu_contention": "Node-level CPU contention (4 pods slower, half capacity)",
    "s11_client_restart": "Client restart mid-run (cold LB, warm fleet)",
    "s12_cross_zone": "Cross-zone pods (+2 ms RTT for a third of the fleet)",
    "s13_steady_het_5_methods": "Steady heterogeneous fleet, 5 methods (1 ms … 1 s)",
}


def scenario_order(tags):
    return sorted(tags, key=lambda t: (0 if t.startswith("s") and t[1:3].isdigit() else 1, t))


def tier2_sections(root, runs):
    items = section("Tier 2: routing quality per scenario")
    scen = [t for t in scenario_order(runs) if re.match(r"s\d\d_", t)]
    if not scen:
        items.append(Text("No Tier 2 scenario runs found; section skipped."))
        return {}
    items.append(Text(
        "20 backend pods (one JVM each, 8 concurrent slots, lognormal service time with median 10 ms) "
        "and two client pods per policy, all four policies running at the same time against the same "
        "pods at 70% of fleet capacity. Fault scenarios hit pod 0 (pods 0-3 for node contention) from "
        "t = 20 s to 60 s of a 90 s measurement window (after a 20 s warmup). Latency percentiles are "
        "of successful calls, measured from each call's intended start (open loop, no coordinated "
        "omission); per repeat a policy's value is the mean of its clients' percentiles; error bars "
        "are 95% CIs over repeats."))
    rows = []
    for tag in scen:
        for run in runs[tag]:
            for pol in POLICIES[1:]:
                r = run_policy_metrics(run, pol)
                if r is None:
                    continue
                det, rec = detect_recover(run, pol)
                rows.append({"scenario": tag, "repeat": run.info.get("repeat", 0), "policy": pol,
                             "time_to_detect_s": det, "time_to_recover_s": rec, **r})
    df = pd.DataFrame(rows)
    df.to_csv(OUT / "data" / "tier2_runs.csv", index=False)
    if df.empty:
        items.append(Text("Tier 2 runs had no complete clients."))
        return {"df": df}

    summary_rows = []
    for tag in scen:
        sub = df[df.scenario == tag]
        if sub.empty:
            continue
        run0 = sorted(runs[tag], key=lambda r: r.info.get("repeat", 0))[0]
        f = run0.info.get("fault")
        title = SCEN_TITLES.get(tag, tag)
        items.append(Text(f"{tag}: {title}", kind="h3"))
        pols = [p for p in POLICIES[1:] if p in set(sub.policy)]
        # latency bars
        cats = ["p50", "p99", "p99.9"]
        series = []
        agg = {}
        for p in pols:
            ms = [mean_ci(sub[sub.policy == p][c].tolist()) for c in ("p50_ms", "p99_ms", "p999_ms")]
            agg[p] = ms
            series.append(dict(name=LABEL[p], y=[m[0] for m in ms], err=[m[1] for m in ms], color=COLOR[p]))
        tk = ""
        if PEAK in agg and RR in agg:
            a, b = agg[PEAK][1][0], agg[RR][1][0]
            best = min(pols, key=lambda p: agg[p][1][0] if not math.isnan(agg[p][1][0]) else 1e18)
            tk = (f"p99: peak_ewma_p2c {fmt(a, 1)} ms vs round_robin {fmt(b, 1)} ms "
                  f"({pct(100 * (a - b) / b if b else float('nan'))}); lowest p99: {LABEL[best]}.")
        items.append(Chart(f"t2_{tag}_lat", f"{tag}: client latency percentiles (whole window)", "bars",
                           ylabel="latency (ms)", categories=cats, series=series, takeaway=tk,
                           key=tag in ("s01_steady_het", "s03_brownout")))
        # fault-window latency / errors
        if f:
            cats = ["p99 before", "p99 during fault", "p99 after"]
            series = []
            for p in pols:
                ms = [mean_ci(sub[sub.policy == p].get(c, pd.Series(dtype=float)).tolist())
                      for c in ("before_p99_ms", "fault_p99_ms", "after_p99_ms")]
                series.append(dict(name=LABEL[p], y=[m[0] for m in ms], err=[m[1] for m in ms], color=COLOR[p]))
            fe = {p: mean_ci(sub[sub.policy == p].get("fault_error_rate", pd.Series(dtype=float)).tolist())[0]
                  for p in pols}
            tk = "Error rate during the fault: " + ", ".join(
                f"{LABEL[p]} {fmt(100 * fe[p], 2)}%" for p in pols) + "."
            items.append(Chart(f"t2_{tag}_phase", f"{tag}: p99 before / during / after the fault window", "bars",
                               ylabel="p99 latency (ms)", categories=cats, series=series, takeaway=tk))
        # timelines (r0)
        meth = dominant_method(run0)
        shade = [(f["start_s"], f["end_s"], "fault")] if f else []
        if run0.info.get("restart_at_s") is not None:
            shade = [(run0.info["restart_at_s"], run0.info["restart_at_s"] + 1, "client restart")]
        series = []
        for p in pols:
            ts, ys = p99_timeline(run0, p, meth)
            if ts:
                series.append(dict(name=LABEL[p], x=ts, y=ys, color=COLOR[p], mode="lines"))
        if series:
            peak_max = max(next((s["y"] for s in series if s["name"] == LABEL[PEAK]), [float("nan")]))
            rr_max = max(next((s["y"] for s in series if s["name"] == LABEL[RR]), [float("nan")]))
            items.append(Chart(f"t2_{tag}_p99t", f"{tag}: p99 over time ({meth}, 1 s windows, repeat 0)", "lines",
                               xlabel="time since measurement start (s)", ylabel="p99 latency (ms)",
                               series=series, shade=shade, logy=True,
                               takeaway=f"Worst 1 s p99: peak_ewma_p2c {fmt(peak_max, 1)} ms, round_robin {fmt(rr_max, 1)} ms.",
                               key=tag == "s03_brownout"))
        series = []
        for p in pols:
            ts, ys = err_timeline(run0, p)
            if ts:
                series.append(dict(name=LABEL[p], x=ts, y=ys, color=COLOR[p], mode="lines"))
        if series and any(max(s["y"]) > 0 for s in series):
            tot = {s["name"]: np.mean(s["y"]) for s in series}
            items.append(Chart(f"t2_{tag}_errt", f"{tag}: error rate over time (repeat 0)", "lines",
                               xlabel="time since measurement start (s)", ylabel="failed calls (%)",
                               series=series, shade=shade,
                               takeaway="Mean error rate: " + ", ".join(f"{k} {v:.2f}%" for k, v in tot.items()) + "."))
        # share heatmaps
        for p in pols:
            hm = share_heatmap(run0, p, tag, shade)
            if hm:
                items.append(hm)
        # detect / recover bars
        if f and f.get("pods") and tag not in ("s08_scale_up_down",):
            series = []
            notes = []
            for p in pols:
                d = mean_ci(sub[sub.policy == p].time_to_detect_s.tolist())
                r = mean_ci(sub[sub.policy == p].time_to_recover_s.tolist())
                nd = sub[sub.policy == p].time_to_detect_s.isna().sum()
                nr = sub[sub.policy == p].time_to_recover_s.isna().sum()
                series.append(dict(name=LABEL[p], y=[d[0], r[0]], err=[d[1], r[1]], color=COLOR[p]))
                if nd or nr:
                    notes.append(f"{LABEL[p]}: {nd} repeat(s) never detected, {nr} never recovered within the window")
            items.append(Chart(f"t2_{tag}_ttd", f"{tag}: time to detect and to recover", "bars",
                               ylabel="seconds", categories=["time to detect (share < ⅓ fair)",
                                                             "time to recover (share within ±10% of fair)"],
                               series=series,
                               takeaway=("; ".join(notes) + ".") if notes else "All policies detected and recovered in every repeat."))
        summary_rows.append(tag)
    # balance across healthy pods
    series = []
    for p in POLICIES[1:]:
        ms = [mean_ci(df[(df.scenario == t) & (df.policy == p)].get("healthy_share_cv", pd.Series(dtype=float)).tolist())
              for t in summary_rows]
        series.append(dict(name=LABEL[p], y=[100 * m[0] for m in ms], err=[100 * m[1] for m in ms], color=COLOR[p]))
    items2 = section("Tier 2: balance across healthy pods")
    items2.append(Chart("t2_balance", "Traffic imbalance across healthy pods (coefficient of variation of per-pod share)",
                        "bars", ylabel="CV of traffic share across healthy pods (%)", categories=summary_rows,
                        series=series, takeaway="Lower is more even; P2C policies deliberately trade some evenness for latency."))
    box = server_cpu_box(runs)
    if box:
        items2.append(box)
    items2.append(Text("Server CPU is per pod and every policy's clients hit the same pods at the same time, so "
                       "server CPU cannot be split by policy; the box shows the fleet's per-pod CPU balance per "
                       "scenario (repeat 0), and the per-policy view is the traffic balance chart above."))
    return {"df": df}


def share_heatmap(run, pol, tag, shade):
    counts = defaultdict(lambda: defaultdict(int))
    tot = defaultdict(int)
    for c in policy_clients(run, pol):
        for iv in c["intervals"]:
            if iv.get("warm"):
                continue
            t = round(iv["t"])
            for a, n in (iv.get("backends") or {}).items():
                if a == "none":
                    continue
                counts[a][t] += n
                tot[t] += n
    if not tot:
        return None
    ts = sorted(tot)
    fp = (run.info.get("fault") or {}).get("pods") or []
    fleet = run.info.get("fleet", {})
    pods = sorted(counts, key=lambda a: (a not in fp, fleet.get(a, "normal") == "normal", int(a.rsplit(":", 1)[-1])
                                         if a.rsplit(":", 1)[-1].isdigit() else 0))
    live = defaultdict(int)
    for t in ts:
        live[t] = sum(1 for a in pods if counts[a].get(t, 0) > 0) or 1
    z = [[(counts[a].get(t, 0) / tot[t]) * live[t] if tot[t] else None for t in ts] for a in pods]
    ys = [f"{a} ({fleet.get(a, '?')}{', FAULT' if a in fp else ''})" for a in pods]
    return Chart(f"t2_{tag}_heat_{pol}", f"{tag}: traffic share ÷ fair share per backend over time — {LABEL[pol]}",
                 "heatmap", xlabel="time since measurement start (s)", ylabel="backend", z=z, xs=ts, ys=ys,
                 zmin=0, zmax=2, zlabel="share ÷ fair", shade=shade, cmap="div", height=max(320, 18 * len(pods) + 120),
                 takeaway="Grey = fair share (1.0); orange = more than fair, blue = less (0 = no traffic).")


def server_cpu_box(runs):
    groups = []
    for tag in scenario_order(runs):
        if not re.match(r"s\d\d_", tag):
            continue
        run = sorted(runs[tag], key=lambda r: r.info.get("repeat", 0))[0]
        sv = run.servers
        if len(sv) < 3:
            continue
        t0 = run.info.get("measure_start_epoch_ms", sv[0]["epoch_ms"])
        win = [s for s in sv if s["epoch_ms"] >= t0]
        if len(win) < 2:
            continue
        a, b = win[0]["pods"], win[-1]["pods"]
        dt = (win[-1]["epoch_ms"] - win[0]["epoch_ms"]) / 1000
        vals = [100 * (b[p]["cpu_ns"] - a[p]["cpu_ns"]) / 1e9 / dt for p in b if p in a and dt > 0]
        if vals:
            groups.append((tag, vals, "#7fa9e0"))
    if not groups:
        return None
    return Chart("t2_server_cpu", "Server CPU per pod over the measurement window (fleet, repeat 0)", "box",
                 ylabel="pod CPU (% of one core)", groups=groups,
                 takeaway="Each point is one backend pod; spread shows how evenly the combined traffic of all policies loaded the fleet.")


def herd_section(runs):
    items = section("Tier 2: herd effect (many clients of one policy)")
    tags = [t for t in runs if t.startswith("herd_")]
    if not tags:
        items.append(Text("No herd runs found; section skipped."))
        return {}
    res = {}
    series, rows = [], []
    for tag in sorted(tags, key=lambda t: (t.rsplit("_", 1)[0], int(t.rsplit("_", 1)[1]))):
        run = runs[tag][0]
        pol = run.info["policies"][0]
        n = run.info.get("clients_per_policy")
        hp = healthy_pods(run)
        live = len(run.info.get("initial_backends") or [])
        fair = 1 / live if live else float("nan")
        agg = defaultdict(lambda: defaultdict(int))
        per_client = defaultdict(lambda: defaultdict(dict))
        for c in run.clients:
            for iv in c["intervals"]:
                if not measured(iv, run):
                    continue
                t = round(iv["t"])
                b = iv.get("backends") or {}
                tot = sum(v for k, v in b.items() if k != "none")
                for a in hp:
                    agg[t][a] += b.get(a, 0)
                agg[t]["__tot"] += tot
                if tot:
                    per_client[t][c["client"]] = {a: b.get(a, 0) / tot for a in hp}
        ts = sorted(agg)
        ratio = [max(agg[t][a] for a in hp) / agg[t]["__tot"] / fair if agg[t]["__tot"] else float("nan") for t in ts]
        var = []
        for t in ts:
            pcs = per_client[t]
            if len(pcs) >= 2:
                var.append(float(np.mean([np.std([pcs[c][a] for c in pcs]) / fair for a in hp])))
        res[tag] = {"policy": pol, "clients": n, "max_share_over_fair_p95": float(np.nanpercentile(ratio, 95)) if ratio else float("nan"),
                    "max_share_over_fair_max": float(np.nanmax(ratio)) if ratio else float("nan"),
                    "client_share_std_over_fair": float(np.mean(var)) if var else float("nan"),
                    "p99_ms": run_policy_metrics(run, pol)["p99_ms"]}
        rows.append({"run": tag, **res[tag]})
        series.append(dict(name=f"{LABEL[pol]} × {n} clients", x=ts, y=ratio, mode="lines",
                           color=COLOR[pol], dash={1: "dot", 4: "dash", 16: "solid"}.get(n, "solid")))
    t = pd.DataFrame(rows)
    t.to_csv(OUT / "data" / "tier2_herd.csv", index=False)
    h16 = res.get("herd_peak_ewma_p2c_16", {})
    items.append(Chart("t2_herd", "Herd: busiest healthy backend's combined share ÷ fair share, 1 s windows",
                       "lines", xlabel="time since measurement start (s)", ylabel="max healthy share ÷ fair",
                       series=series, hlines=[(1.5, "1.5× criterion")], key=True,
                       takeaway=f"With 16 peak_ewma_p2c clients the busiest healthy backend gets "
                                f"{fmt(h16.get('max_share_over_fair_p95'), 2)}× its fair share (p95 over windows)."))
    items.append(Table("t2_herd_t", "Herd metrics (healthy pods only; share variance = mean over pods of the std across clients, ÷ fair)",
                       t.round(3)))
    return res


def sensitivity_section(runs):
    items = section("Sensitivity: improvement vs round_robin over fleet load × backend count")
    tags = [t for t in runs if t.startswith("sens_")]
    if not tags:
        items.append(Text("No sensitivity runs found; section skipped."))
        return {}
    cells = {}
    for t in tags:
        m = re.match(r"sens_b(\d+)_load(\d+)", t)
        if not m:
            continue
        run = runs[t][0]
        mets = {p: run_policy_metrics(run, p) for p in POLICIES[1:]}
        cells[(int(m.group(1)), int(m.group(2)))] = mets
    bs = sorted({k[0] for k in cells})
    ls = sorted({k[1] for k in cells})
    rows = []
    for pol in (PEAK, LROD, LR):
        z = []
        for b in bs:
            row = []
            for l in ls:
                mets = cells.get((b, l))
                if mets and mets.get(pol) and mets.get(RR) and mets[RR]["p99_ms"]:
                    v = mets[pol]["p99_ms"] / mets[RR]["p99_ms"]
                    rows.append({"backends": b, "load_pct": l, "policy": pol, "p99_ratio_vs_rr": v,
                                 "p99_ms": mets[pol]["p99_ms"], "rr_p99_ms": mets[RR]["p99_ms"]})
                else:
                    v = None
                row.append(v)
            z.append(row)
        vals = [v for r in z for v in r if v is not None]
        if not vals:
            continue
        items.append(Chart(f"sens_{pol}", f"p99 ratio {LABEL[pol]} ÷ round_robin (steady heterogeneous fleet)",
                           "heatmap", xlabel="fleet load (% of capacity)", ylabel="backends",
                           z=z, xs=[f"{l}%" for l in ls], ys=[str(b) for b in bs], zmin=0, zmax=2, cmap="div",
                           zlabel="p99 ratio (<1 better)", annot=True, key=pol == PEAK,
                           takeaway=f"{LABEL[pol]} p99 ranges from {min(vals):.2f}× to {max(vals):.2f}× round_robin's across the grid."))
    pd.DataFrame(rows).to_csv(OUT / "data" / "sensitivity.csv", index=False)
    return cells


def internals_section(runs):
    items = section("LB internals (peak_ewma_p2c)")
    # ejections over time by reason
    any_ = False
    for tag in [t for t in scenario_order(runs) if re.match(r"s\d\d_", t)]:
        run = sorted(runs[tag], key=lambda r: r.info.get("repeat", 0))[0]
        bucket = defaultdict(lambda: defaultdict(int))
        for c in policy_clients(run, PEAK):
            for iv in c["intervals"]:
                if iv.get("warm"):
                    continue
                for e in iv.get("ejections") or []:
                    bucket[e["reason"]][5 * math.floor(iv["t"] / 5)] += 1
        if not bucket:
            continue
        any_ = True
        f = run.info.get("fault")
        series = []
        for reason, col in (("errors", "#e34948"), ("latency", "#eda100")):
            if reason in bucket:
                xs = sorted(bucket[reason])
                series.append(dict(name=f"{reason} ejections", x=xs, y=[bucket[reason][x] for x in xs], color=col,
                                   mode="markers+lines"))
        tot = {r: sum(v.values()) for r, v in bucket.items()}
        items.append(Chart(f"int_ej_{tag}", f"{tag}: ejections over time by reason (all peak clients, repeat 0, 5 s buckets)",
                           "lines", xlabel="time since measurement start (s)", ylabel="ejections per 5 s",
                           series=series, shade=[(f["start_s"], f["end_s"], "fault")] if f else [],
                           takeaway="Total: " + ", ".join(f"{k} {v}" for k, v in tot.items()) + "."))
    if not any_:
        items.append(Text("No peak_ewma_p2c ejections recorded in any scenario."))
    # half-lives and seed per method
    for tag in ("s13_steady_het_5_methods", "s01_steady_het"):
        if tag not in runs:
            continue
        run = sorted(runs[tag], key=lambda r: r.info.get("repeat", 0))[0]
        scales = None
        for c in policy_clients(run, PEAK):
            for iv in c["intervals"]:
                if iv.get("scales"):
                    scales = iv["scales"]
        if not scales:
            continue
        rows = [{"method": m, "peak half-life (ms)": v[0], "baseline half-life (ms)": v[1],
                 "seed (ms)": None if v[2] is None else v[2] / 1000} for m, v in sorted(scales.items(), key=lambda kv: Probe_ms(kv[0]))]
        t = pd.DataFrame(rows)
        t.to_csv(OUT / "data" / f"scales_{tag}.csv", index=False)
        ms = [r["method"] for r in rows]
        items.append(Chart(f"int_scales_{tag}", f"{tag}: half-lives and seed chosen per method (end of run)", "bars",
                           ylabel="milliseconds (log)", categories=ms, logy=True,
                           series=[dict(name="peak half-life", y=[r["peak half-life (ms)"] for r in rows], color="#2a78d6"),
                                   dict(name="baseline half-life", y=[r["baseline half-life (ms)"] for r in rows], color="#1baf7a"),
                                   dict(name="seed latency", y=[r["seed (ms)"] for r in rows], color="#eb6834")],
                           takeaway="Half-lives scale with each method's own rate and latency (sample-based), not one fixed time constant."))
        items.append(Table(f"int_scales_t_{tag}", f"{tag}: MethodScale per method", t.round(2)))
        break
    # cost gauge: faulty pod vs fleet median (brownout)
    for tag in ("s03_brownout", "s10_node_cpu_contention"):
        if tag not in runs:
            continue
        run = sorted(runs[tag], key=lambda r: r.info.get("repeat", 0))[0]
        f = run.info.get("fault") or {}
        cl = policy_clients(run, PEAK)
        if not cl or not f.get("pods"):
            continue
        xs, bad, med = [], [], []
        for iv in cl[0]["intervals"]:
            if iv.get("warm") or not iv.get("costs"):
                continue
            costs = iv["costs"]
            b = [v for k, v in costs.items() if k.split("|")[0] in f["pods"] and v is not None]
            o = [v for k, v in costs.items() if k.split("|")[0] not in f["pods"] and v is not None]
            if b and o:
                xs.append(iv["t"])
                bad.append(np.mean(b) / 1000)
                med.append(float(np.median(o)) / 1000)
        if xs:
            items.append(Chart(f"int_cost_{tag}", f"{tag}: picker cost gauge, faulty pod vs fleet median (client 0, repeat 0)",
                               "lines", xlabel="time since measurement start (s)", ylabel="cost (ms-equivalent, log)",
                               series=[dict(name="faulty pod", x=xs, y=bad, color="#e34948", mode="lines"),
                                       dict(name="fleet median", x=xs, y=med, color="#2a78d6", mode="lines")],
                               shade=[(f["start_s"], f["end_s"], "fault")], logy=True,
                               takeaway="Cost = decayed peak latency × (inflight + 1), published once per tick; the faulty pod's cost rises above the fleet's during the fault."))
            break


def Probe_ms(bare):
    m = re.match(r"[USW](\d*)_", bare)
    return int(m.group(1)) if m and m.group(1) else 0


def kind_section(run):
    items = section("Kubernetes smoke run (kind)")
    if run is None or not run.clients:
        items.append(Text("No kind-smoke data; section skipped."))
        return
    f = run.info.get("fault") or {}
    env = run.info.get("env", {})
    items.append(Text(f"Single-node kind cluster, {len(run.info.get('backends', []))} backend pods behind a "
                      f"headless Service (dns:///), one loadgen Job per policy; brownout ({f.get('kind')}) on "
                      f"{', '.join(f.get('pods', []))} from t={f.get('start_s')} to {f.get('end_s')} s. "
                      f"Absolute latencies include CPU throttling on the shared node; compare policies. "
                      f"Env: {html.escape(json.dumps(env))[:400]}"))
    rows = []
    for c in run.clients:
        s = c["summary"]
        ts, ys = share_timeline(run, c["policy"], f.get("pods", []))
        dur = [y for t, y in zip(ts, ys) if f.get("start_s", 0) < t <= f.get("end_s", 0)]
        rows.append({"policy": LABEL.get(c["policy"], c["policy"]), "rps": round(s.get("rps", 0)),
                     "ok %": round(100 * s.get("ok", 0) / max(1, s.get("completed", 1)), 3),
                     "p99 ms": round(g(s, "latency", "all", "p99_us") / 1000, 1),
                     "p99 during brownout ms": round(g(s, "phases", "fault", "latency", "all", "p99_us") / 1000, 1),
                     "faulty pod share during brownout %": round(100 * np.mean(dur), 1) if dur else None,
                     "ejections": json.dumps(s.get("ejections") or {})})
    t = pd.DataFrame(rows)
    t.to_csv(OUT / "data" / "kind_smoke.csv", index=False)
    items.append(Table("kind", "kind smoke: per-policy results", t))
    series = []
    for c in run.clients:
        ts, ys = p99_timeline(run, c["policy"], dominant_method(run))
        series.append(dict(name=LABEL.get(c["policy"], c["policy"]), x=ts, y=ys, color=COLOR.get(c["policy"]), mode="lines"))
    items.append(Chart("kind_p99t", "kind smoke: p99 over time", "lines", xlabel="time since measurement start (s)",
                       ylabel="p99 (ms)", series=series, shade=[(f.get("start_s", 0), f.get("end_s", 0), "brownout")],
                       logy=True))


# ---------------------------------------------------------------------------------------------
# go / no-go


def gonogo(t1, t1agg, mem, t2, herd, runs):
    rows = []

    def add(area, criterion, measured, status, note=""):
        rows.append({"area": area, "criterion": criterion, "measured": measured, "result": status, "note": note})

    def cpu_cell(cell, label):
        if t1.empty or cell not in set(t1.cell):
            add("CPU overhead", f"client CPU/RPC ≤ +25% vs round_robin ({label})", "no data", "N/A")
            return None
        pk = agg_cell(t1, cell, PEAK, "cpu_us_per_rpc")[0]
        rr = agg_cell(t1, cell, RR, "cpu_us_per_rpc")[0]
        lc = agg_cell(t1, cell, PEAK, "cpu_us_per_rpc_minus_control")[0]
        lrr = agg_cell(t1, cell, RR, "cpu_us_per_rpc_minus_control")[0]
        d = 100 * (pk - rr) / rr if rr else float("nan")
        p = welch_p(t1[(t1.cell == cell) & (t1.policy == PEAK)].cpu_us_per_rpc.tolist(),
                    t1[(t1.cell == cell) & (t1.policy == RR)].cpu_us_per_rpc.tolist())
        add("CPU overhead", f"client CPU/RPC ≤ +25% vs round_robin ({label})",
            f"{fmt(pk, 2)} vs {fmt(rr, 2)} µs/RPC ({pct(d, 1)}{', n.s.' if not math.isnan(p) and p >= 0.05 else ''}); "
            f"LB cost (− control) {fmt(lc, 2)} vs {fmt(lrr, 2)} µs",
            "N/A" if math.isnan(d) else ("PASS" if d <= 25 else "FAIL"),
            "whole-process CPU incl. Netty + harness; LB cost = policy − control")
        return d

    d10 = cpu_cell("centre", "10 backends")
    d50 = cpu_cell("backends_50", "50 backends")
    if not t1.empty and "centre" in set(t1.cell):
        pk = agg_cell(t1, "centre", PEAK, "alloc_b_per_rpc")[0]
        rr = agg_cell(t1, "centre", RR, "alloc_b_per_rpc")[0]
        d = 100 * (pk - rr) / rr if rr else float("nan")
        add("Allocation", "bytes/RPC ≤ +10% vs round_robin (10 backends)",
            f"{fmt(pk, 0)} vs {fmt(rr, 0)} B/RPC ({pct(d, 1)}; Δ {fmt(pk - rr, 0)} B)",
            "N/A" if math.isnan(d) else ("PASS" if d <= 10 else "FAIL"),
            "harness floor ≈ control's B/RPC dilutes the %; the absolute Δ is the LB's")
    else:
        add("Allocation", "bytes/RPC ≤ +10% vs round_robin", "no data", "N/A")
    m = mem.get(PEAK)
    if m and not math.isnan(m["growth_pct_last_half"]):
        add("Memory", "LB heap plateaus under 1,000 churning methods",
            f"growth over last half of {fmt(m['duration_s'], 0)} s: {pct(m['growth_pct_last_half'], 1)} "
            f"({fmt(m['first_mb'], 1)} → {fmt(m['final_mb'], 1)} MiB)",
            "PASS" if abs(m["growth_pct_last_half"]) < 5 else "FAIL", "run is shorter than the 1 h in the issue")
    else:
        add("Memory", "LB heap plateaus under 1,000 churning methods", "no data", "N/A")
    if not t1.empty and "backends_500" in set(t1.cell):
        v = agg_cell(t1, "backends_500", PEAK, "pick_p99_ns")[0]
        add("Pick time", "p99 pick < 5 µs at 500 backends", f"{fmt(v, 0)} ns (sampled 1/1000)",
            "N/A" if math.isnan(v) else ("PASS" if v < 5000 else "FAIL"),
            "wall time of pickSubchannel incl. the timer itself")
    else:
        add("Pick time", "p99 pick < 5 µs at 500 backends", "no data", "N/A")

    df = t2.get("df", pd.DataFrame()) if t2 else pd.DataFrame()

    def sm(tag, pol, col):
        if df.empty or col not in df:
            return float("nan")
        return mean_ci(df[(df.scenario == tag) & (df.policy == pol)][col].tolist())[0]

    if not df.empty and "s01_steady_het" in set(df.scenario):
        a, b = sm("s01_steady_het", PEAK, "p99_ms"), sm("s01_steady_het", RR, "p99_ms")
        ea, eb = sm("s01_steady_het", PEAK, "error_rate"), sm("s01_steady_het", RR, "error_rate")
        add("Steady heterogeneous", "p99 better than round_robin; error rate ≤ round_robin",
            f"p99 {fmt(a, 1)} vs {fmt(b, 1)} ms; errors {fmt(100 * ea, 3)}% vs {fmt(100 * eb, 3)}%",
            "PASS" if a < b and ea <= eb * 1.0000001 else "FAIL")
    else:
        add("Steady heterogeneous", "p99 better than round_robin; error rate ≤ round_robin", "no data", "N/A")
    if not df.empty and "s03_brownout" in set(df.scenario):
        det = sm("s03_brownout", PEAK, "time_to_detect_s")
        a, b = sm("s03_brownout", PEAK, "fault_p99_ms"), sm("s03_brownout", RR, "fault_p99_ms")
        add("Brownout", "detected within 10 s; p99 during brownout ≤ ½ round_robin's",
            f"detect {fmt(det, 1)} s; p99 during {fmt(a, 1)} vs {fmt(b, 1)} ms ({fmt(a / b if b else float('nan'), 2)}×)",
            "N/A" if math.isnan(b) else ("PASS" if (not math.isnan(det) and det <= 10 and a <= b / 2) else "FAIL"),
            f"p99 is of successful calls; errors during the fault {fmt(100 * sm('s03_brownout', PEAK, 'fault_error_rate'), 2)}% vs "
            f"{fmt(100 * sm('s03_brownout', RR, 'fault_error_rate'), 2)}% (round_robin's slowest calls hit the deadline and count as errors)")
    else:
        add("Brownout", "detected within 10 s; p99 ≤ ½ round_robin's", "no data", "N/A")
    for tag, lab in (("s04_crashloop", "Crash-loop"), ("s05_blackhole", "Black hole")):
        if not df.empty and tag in set(df.scenario):
            a, b = sm(tag, PEAK, "fault_error_rate"), sm(tag, RR, "fault_error_rate")
            add(lab, "client error rate ≤ 20% of round_robin's (fault window)",
                f"{fmt(100 * a, 3)}% vs {fmt(100 * b, 3)}% ({fmt(a / b if b else float('nan'), 2)}×)",
                "N/A" if math.isnan(b) or b == 0 else ("PASS" if a <= 0.2 * b else "FAIL"))
        else:
            add(lab, "client error rate ≤ 20% of round_robin's", "no data", "N/A")
    if not df.empty and "s02_steady_homo" in set(df.scenario):
        sub = df[(df.scenario == "s02_steady_homo") & (df.policy == PEAK)]
        ej = (sub.ejections_errors + sub.ejections_latency).sum()
        hours = (sub.measure_s * sub.clients).sum() / 3600
        lo = sub.healthy_share_min_over_fair.min()
        hi = sub.healthy_share_max_over_fair.max()
        ok = ej == 0 and lo >= 0.9 and hi <= 1.1
        add("Homogeneous steady", "0 false ejections/hour; every share within ±10% of fair",
            f"{int(ej)} ejections in {fmt(hours * 60, 1)} client-minutes ({fmt(ej / hours if hours else float('nan'), 0)}/client-hour); "
            f"shares {fmt(lo, 3)}–{fmt(hi, 3)}× fair", "PASS" if ok else "FAIL")
    else:
        add("Homogeneous steady", "0 false ejections/hour; shares within ±10%", "no data", "N/A")
    h = herd.get("herd_peak_ewma_p2c_16")
    if h:
        add("Herd (16 clients)", "max healthy-backend share ≤ 1.5× fair",
            f"{fmt(h['max_share_over_fair_p95'], 2)}× (p95 of 1 s windows; max {fmt(h['max_share_over_fair_max'], 2)}×)",
            "PASS" if h["max_share_over_fair_p95"] <= 1.5 else "FAIL")
    else:
        add("Herd (16 clients)", "max healthy-backend share ≤ 1.5× fair", "no data", "N/A")
    # vs lr_od: where each wins
    if not df.empty:
        wins = []
        for tag in scenario_order(set(df.scenario)):
            a, b = sm(tag, PEAK, "p99_ms"), sm(tag, LROD, "p99_ms")
            ea, eb = sm(tag, PEAK, "error_rate"), sm(tag, LROD, "error_rate")
            if math.isnan(a) or math.isnan(b):
                continue
            wins.append(f"{tag.split('_', 1)[0]}: p99 {'peak' if a < b else 'lr_od'} ({fmt(a, 1)}/{fmt(b, 1)} ms), "
                        f"errors {'peak' if ea < eb else ('tie' if ea == eb else 'lr_od')}")
        add("vs least_request + outlier_detection", "document where each wins", "; ".join(wins) or "no data", "INFO")
    t = pd.DataFrame(rows)
    t.to_csv(OUT / "data" / "gonogo.csv", index=False)
    return t


def headlines(t1, t2, runs):
    out = []
    df = t2.get("df", pd.DataFrame()) if t2 else pd.DataFrame()
    if not t1.empty and "centre" in set(t1.cell):
        pk, rr = agg_cell(t1, "centre", PEAK, "cpu_us_per_rpc")[0], agg_cell(t1, "centre", RR, "cpu_us_per_rpc")[0]
        lc = agg_cell(t1, "centre", PEAK, "cpu_us_per_rpc_minus_control")[0]
        lrr = agg_cell(t1, "centre", RR, "cpu_us_per_rpc_minus_control")[0]
        out.append(f"CPU per RPC at the centre point: peak_ewma_p2c {fmt(pk, 2)} µs vs round_robin {fmt(rr, 2)} µs "
                   f"({pct(100 * (pk - rr) / rr if rr else float('nan'), 1)}); LB cost above the no-LB control: "
                   f"{fmt(lc, 2)} µs vs {fmt(lrr, 2)} µs.")
        a, b = agg_cell(t1, "centre", PEAK, "alloc_b_per_rpc")[0], agg_cell(t1, "centre", RR, "alloc_b_per_rpc")[0]
        out.append(f"Allocation: {fmt(a - b, 0)} B/RPC more than round_robin ({pct(100 * (a - b) / b if b else float('nan'), 1)}).")

    def sm(tag, pol, col):
        if df.empty or col not in df:
            return float("nan")
        return mean_ci(df[(df.scenario == tag) & (df.policy == pol)][col].tolist())[0]

    for tag, col, lab in (("s03_brownout", "fault_p99_ms", "Brownout p99 during the fault"),
                          ("s01_steady_het", "p99_ms", "Steady heterogeneous p99")):
        a, b = sm(tag, PEAK, col), sm(tag, RR, col)
        if not math.isnan(a) and not math.isnan(b):
            out.append(f"{lab}: {fmt(a, 1)} ms vs round_robin {fmt(b, 1)} ms (ratio {fmt(a / b if b else float('nan'), 2)}×).")
    for tag, lab in (("s04_crashloop", "Crash-loop"), ("s05_blackhole", "Black hole")):
        a, b = sm(tag, PEAK, "fault_error_rate"), sm(tag, RR, "fault_error_rate")
        if not math.isnan(a) and not math.isnan(b):
            out.append(f"{lab} error rate during the fault: {fmt(100 * a, 2)}% vs round_robin {fmt(100 * b, 2)}%.")
    return out[:6]


# ---------------------------------------------------------------------------------------------
# rendering


CSS = """
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Helvetica,Arial,sans-serif;margin:0;background:#fcfcfb;color:#0b0b0b}
main{max-width:1180px;margin:0 auto;padding:24px}
h1{font-size:26px} h2{font-size:21px;border-bottom:1px solid #e6e6e3;padding-bottom:6px;margin-top:40px}
h3{font-size:16px;margin-top:28px;color:#2a2a28}
p.takeaway{color:#52514e;font-size:13px;margin:2px 0 22px 6px}
table{border-collapse:collapse;font-size:12.5px;margin:8px 0 22px}
th,td{border:1px solid #e6e6e3;padding:4px 8px;text-align:left;vertical-align:top}
th{background:#f3f3f1}
td.PASS{background:#e3f4e3;font-weight:600} td.FAIL{background:#fbe2e1;font-weight:600}
td.N\\/A, td.NA{background:#f3f3f1;color:#52514e} td.INFO{background:#eaf1fb}
.legend{font-size:12px;color:#52514e}
nav a{margin-right:10px;font-size:13px}
img.flame{max-width:100%;border:1px solid #e6e6e3}
ul.head li{margin:4px 0}
"""


def table_html(t):
    df = t.df
    cols = list(df.columns)
    h = ["<table><tr>" + "".join(f"<th>{html.escape(str(c))}</th>" for c in cols) + "</tr>"]
    for _, r in df.iterrows():
        cells = []
        for c in cols:
            v = r[c]
            s = "" if v is None or (isinstance(v, float) and math.isnan(v)) else (
                f"{v:.4g}" if isinstance(v, float) else str(v))
            cls = f' class="{s.replace("/", "")}"' if c == t.status_col else ""
            cells.append(f"<td{cls}>{html.escape(s)}</td>")
        h.append("<tr>" + "".join(cells) + "</tr>")
    h.append("</table>")
    return "".join(h)


def table_md(df, status_col=None):
    cols = list(df.columns)
    icon = {"PASS": "✅ PASS", "FAIL": "❌ FAIL", "N/A": "➖ N/A", "INFO": "ℹ️ INFO"}
    out = ["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
    for _, r in df.iterrows():
        vals = []
        for c in cols:
            v = r[c]
            s = "" if v is None or (isinstance(v, float) and math.isnan(v)) else (
                f"{v:.4g}" if isinstance(v, float) else str(v))
            if c == status_col:
                s = icon.get(s, s)
            vals.append(s.replace("|", "\\|").replace("\n", " "))
        out.append("| " + " | ".join(vals) + " |")
    return "\n".join(out)


def text_pages(pdf, title, df, status_col=None):
    """A table as PDF page(s)."""
    rows_per = 22
    cols = list(df.columns)
    for start in range(0, max(1, len(df)), rows_per):
        chunk = df.iloc[start:start + rows_per]
        fig, ax = plt.subplots(figsize=(11, 8.5))
        ax.axis("off")
        ax.set_title(title, loc="left", fontsize=11)
        cell = [[("" if v is None or (isinstance(v, float) and math.isnan(v)) else
                  (f"{v:.4g}" if isinstance(v, float) else str(v)))[:70] for v in r] for r in chunk.values]
        if not cell:
            plt.close(fig)
            continue
        tb = ax.table(cellText=cell, colLabels=[str(c)[:28] for c in cols], loc="upper left", cellLoc="left")
        tb.auto_set_font_size(False)
        tb.set_fontsize(6.5 if len(cols) > 6 else 7.5)
        tb.scale(1, 1.35)
        if status_col in cols:
            j = cols.index(status_col)
            for i, r in enumerate(cell):
                col = {"PASS": "#e3f4e3", "FAIL": "#fbe2e1", "INFO": "#eaf1fb"}.get(r[j], "#f3f3f1")
                tb[(i + 1, j)].set_facecolor(col)
        pdf.savefig(fig)
        plt.close(fig)


def render(root, gn, heads, manifest, no_pdf=False, full_pdf=False, decision=None):
    (OUT / "png").mkdir(parents=True, exist_ok=True)
    pdf = None if no_pdf else PdfPages(OUT / "report.pdf")
    body = []
    toc = []
    pngs = {}
    if pdf:
        fig = plt.figure(figsize=(11, 8.5))
        fig.text(0.05, 0.9, "peak_ewma_p2c load test (#94)", fontsize=22)
        fig.text(0.05, 0.84, f"{root.name} · commit {manifest.get('commit')} · {manifest.get('os')} · {manifest.get('cpus')} CPUs",
                 fontsize=11, color="#52514e")
        y = 0.75
        for h in heads:
            fig.text(0.05, y, "• " + h, fontsize=10, wrap=True)
            y -= 0.06
        pdf.savefig(fig)
        plt.close(fig)
        if decision is not None and not decision.empty:
            d = decision.copy()
            for c in d.columns:
                if c.startswith("errors"):
                    d[c] = d[c].map(lambda v: "" if pd.isna(v) else f"{100 * v:.2f}%")
                elif c.startswith("p99"):
                    d[c] = d[c].map(lambda v: "" if pd.isna(v) else f"{v:.1f}")
            d = d.drop(columns=["what happens"])
            d["verdict (peak vs least_request / lr+od)"] = d["verdict (peak vs least_request / lr+od)"].str.replace("**", "", regex=False)
            text_pages(pdf, "Decision table (worst-case p99 / error rate per scenario)", d)
        text_pages(pdf, "Go / no-go", gn, "result")
    for si, (title, items) in enumerate(SECTIONS):
        sid = f"sec{si}"
        toc.append(f'<a href="#{sid}">{html.escape(title)}</a>')
        body.append(f'<h2 id="{sid}">{html.escape(title)}</h2>')
        for it in items:
            try:
                if isinstance(it, Text):
                    if it.kind == "h3":
                        body.append(f"<h3>{html.escape(it.text)}</h3>")
                    else:
                        body.append(f"<p>{it.text}</p>")
                elif isinstance(it, Table):
                    body.append(f"<h4>{html.escape(it.title)}</h4>" + table_html(it))
                    if it.takeaway:
                        body.append(f'<p class="takeaway">{html.escape(it.takeaway)}</p>')
                    if pdf and full_pdf:
                        text_pages(pdf, it.title, it.df, it.status_col)
                elif isinstance(it, Chart):
                    if it.kind == "image":
                        b64 = base64.b64encode(it.png_bytes).decode()
                        body.append(f"<h4>{html.escape(it.title)}</h4><img class='flame' src='data:image/png;base64,{b64}'/>")
                        (OUT / "png" / f"{it.cid}.png").write_bytes(it.png_bytes)
                        pngs[it.cid] = it
                    else:
                        fig = to_plotly(it)
                        body.append(fig.to_html(full_html=False, include_plotlyjs=False,
                                                config={"displaylogo": False}))
                    if it.takeaway:
                        body.append(f'<p class="takeaway">{html.escape(it.takeaway)}</p>')
                    if it.kind != "image":
                        mf = to_mpl(it)
                        if pdf and (full_pdf or it.key):
                            pdf.savefig(mf)
                        png = mpl_image(mf)
                        (OUT / "png" / f"{it.cid}.png").write_bytes(png)
                        pngs[it.cid] = it
                    elif pdf and full_pdf:
                        mf = to_mpl(it)
                        pdf.savefig(mf)
                        plt.close(mf)
            except Exception as e:
                warn(f"render {getattr(it, 'cid', getattr(it, 'tid', type(it).__name__))}: {e}")
                traceback.print_exc()
    if pdf:
        pdf.close()

    head_html = "<ul class='head'>" + "".join(f"<li>{html.escape(h)}</li>" for h in heads) + "</ul>"
    gn_html = table_html(Table("gn", "", gn, status_col="result"))
    env_rows = "".join(f"<tr><th>{html.escape(str(k))}</th><td>{html.escape(json.dumps(v) if isinstance(v, (dict, list)) else str(v))}</td></tr>"
                       for k, v in manifest.items())
    legend = " ".join(f"<span style='color:{COLOR[p]}'>■</span> {LABEL[p]}" for p in POLICIES)
    doc = f"""<!doctype html><html><head><meta charset="utf-8"><title>peak_ewma_p2c load test — {html.escape(root.name)}</title>
<style>{CSS}</style><script type="text/javascript">{get_plotlyjs()}</script></head><body><main>
<h1>peak_ewma_p2c load test (#94) — {html.escape(root.name)}</h1>
<p class="legend">Policy colours (same in every chart): {legend}</p>
<nav>{' '.join(toc)} <a href="#env">Environment</a></nav>
<h2>Executive summary</h2>{head_html}
<h4>Decision table</h4><p>{html.escape(decision_counts(decision) if decision is not None else "")}</p>
{table_html(Table("dt", "", decision)) if decision is not None and not decision.empty else ""}
<h4>Go / no-go</h4>{gn_html}
{''.join(body)}
<h2 id="env">Environment appendix</h2><table>{env_rows}</table>
{caveats_html()}
{('<h4>Warnings while generating</h4><ul>' + ''.join(f'<li>{html.escape(w)}</li>' for w in WARN) + '</ul>') if WARN else ''}
</main></body></html>"""
    (OUT / "report.html").write_text(doc)
    return pngs


CAVEATS = [
    "Local multi-process run on one 18-core Mac, not a real cluster: one JVM per backend pod and per client pod, "
    "real gRPC over loopback TCP, pod churn via a re-read address file instead of DNS. No real network RTT, no "
    "node boundaries; all pods share the same CPUs.",
    "Backends model capacity with a fixed number of concurrent slots (8) and a FIFO queue, and lognormal service "
    "times; faults (latency factor, UNAVAILABLE, black hole, stop-the-world pauses, +RTT) are injected through the "
    "admin endpoint.",
    "All policies share the fleet concurrently, so one policy's routing changes the load the others see (e.g. "
    "round_robin keeps loading a slow pod, which makes it slower for everyone).",
    "Fault scenarios use a homogeneous base fleet so the fault is the only difference; s01, s11 and s13 use the "
    "heterogeneous fleet (14 normal, 3 noisy, 1 GC-pausing, 1 flaky, 1 CPU-throttled at 20 pods).",
    "Time-compressed: brownout lasts 40 s instead of 2 min, scenario windows are 90 s instead of 5–10 min, "
    "sensitivity/herd cells 45 s with one repeat, and the memory-churn run is minutes, not 1 h.",
    "Tier 1 CPU and allocation are whole-process figures that include Netty and the loadgen; the harness floor "
    "(control) is large, so percentage deltas are diluted and the policy − control figures are the LB's own cost.",
    "lr_od = grpc outlier_detection_experimental (interval 10 s, base ejection 30 s, max 20% ejected, success-rate "
    "and failure-percentage ejection) wrapping least_request_experimental (choiceCount 2).",
    "Latency percentiles are of successful calls; failed calls (including deadline exceeded) are counted in the "
    "error rate. Per repeat, a policy's percentile is the mean of its clients' percentiles (not a merged histogram).",
]


def caveats_html():
    return "<h4>Caveats</h4><ul>" + "".join(f"<li>{html.escape(c)}</li>" for c in CAVEATS) + "</ul>"


def write_summary(root, gn, heads, manifest, pngs, repeats_note, path, decision=None):
    """SUMMARY.md: the decision table first, then go/no-go and the key charts. Key charts are copied to
    <results>/charts/ so the summary can be committed without the full (large) report."""
    rel = os.path.relpath(OUT, path.parent)
    charts = path.parent / "charts"
    charts.mkdir(exist_ok=True)
    lines = [f"# peak_ewma_p2c load test (#94): {root.name}", "",
             f"Commit `{manifest.get('commit')}`{' (dirty)' if manifest.get('dirty') else ''} · {manifest.get('jdk')} · "
             f"gRPC {manifest.get('grpc')} · {manifest.get('os')}, {manifest.get('cpus')} CPUs, {manifest.get('mem_gb')} GB · "
             f"{manifest.get('mode')}", "",
             f"Generated from the raw run data with `report.py`. PDF brief: [`{rel}/report.pdf`]({rel}/report.pdf) · "
             f"tables: [`{rel}/data/`]({rel}/data/) · full interactive report: `{rel}/report.html` (local only: "
             f"regenerate it with `report.py`).", "",
             "## Decision table", "",
             "Worst-case p99 (the fault window, when there is one) and client error rate, mean of repeats. "
             + decision_counts(decision if decision is not None else pd.DataFrame()), "",
             decision_md(decision if decision is not None else pd.DataFrame()), "",
             "## Headline numbers", ""]
    lines += [f"- {h}" for h in heads] or ["- (no data yet)"]
    lines += ["", "## Go / no-go", "", table_md(gn, "result"), ""]
    keys = [cid for cid, c in pngs.items() if c.key]
    if keys:
        lines += ["## Key charts", ""]
        for cid in keys:
            c = pngs[cid]
            shutil.copyfile(OUT / "png" / f"{cid}.png", charts / f"{cid}.png")
            lines += [f"### {c.title}", "", f"![{c.title}](charts/{cid}.png)", ""]
            if c.takeaway:
                lines += [f"_{c.takeaway}_", ""]
    lines += ["## Repeats", "", repeats_note, "", "## Caveats", ""] + [f"- {c}" for c in CAVEATS]
    if WARN:
        lines += ["", "## Generation warnings", ""] + [f"- {w}" for w in WARN[:30]]
    path.write_text("\n".join(lines) + "\n")


def repeats_summary(t1, runs):
    parts = []
    if not t1.empty:
        n = t1.groupby(["cell", "policy"]).size()
        parts.append(f"Tier 1: {t1.cell.nunique()} cells, {n.min()}–{n.max()} repeats per cell × policy "
                     f"({len(t1)} runs).")
    sc = {t: len(v) for t, v in runs.items() if re.match(r"s\d\d_", t)}
    if sc:
        parts.append(f"Tier 2: {len(sc)} scenarios, {min(sc.values())}–{max(sc.values())} repeats each; "
                     f"{sum(1 for t in runs if t.startswith('sens_'))} sensitivity cells and "
                     f"{sum(1 for t in runs if t.startswith('herd_'))} herd cells (1 repeat each).")
    return " ".join(parts) or "No runs found."


# ---------------------------------------------------------------------------------------------


def main():
    global OUT
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("results")
    ap.add_argument("--out", default=None)
    ap.add_argument("--no-pdf", action="store_true")
    ap.add_argument("--full-pdf", action="store_true",
                    help="every chart and table in the PDF (default: a brief with the decision table, "
                         "go/no-go and key charts)")
    ap.add_argument("--summary", default=None, help="where to write SUMMARY.md (default <results>/SUMMARY.md)")
    a = ap.parse_args()
    root = Path(a.results).resolve()
    OUT = Path(a.out).resolve() if a.out else root / "report"
    (OUT / "data").mkdir(parents=True, exist_ok=True)
    manifest = {}
    if (root / "manifest.json").exists():
        manifest = json.loads((root / "manifest.json").read_text())
    else:
        warn("no manifest.json")

    def guard(fn, *args, default=None):
        try:
            return fn(*args)
        except Exception as e:
            warn(f"{fn.__name__} failed: {e}")
            traceback.print_exc()
            return default

    t1 = guard(tier1_load, root, default=pd.DataFrame())
    runs = guard(tier2_load, root, default={}) or {}
    t1res = guard(tier1_sections, root, t1, default={}) or {}
    mem = guard(memory_section, root, default={}) or {}
    guard(jfr_section, root)
    t2 = guard(tier2_sections, root, runs, default={}) or {}
    herd = guard(herd_section, runs, default={}) or {}
    guard(sensitivity_section, runs)
    guard(internals_section, runs)
    guard(kind_section, guard(kind_load, root))
    gn = guard(gonogo, t1, t1res.get("agg"), mem, t2, herd, runs, default=pd.DataFrame())
    heads = guard(headlines, t1, t2, runs, default=[]) or []
    decision = guard(decision_table, runs, default=pd.DataFrame())
    if decision is not None and not decision.empty:
        decision.to_csv(OUT / "data" / "decision.csv", index=False)
    pngs = render(root, gn, heads, manifest, a.no_pdf, a.full_pdf, decision)
    summary_path = Path(a.summary).resolve() if a.summary else root / "SUMMARY.md"
    write_summary(root, gn, heads, manifest, pngs, repeats_summary(t1, runs), summary_path, decision)
    print(f"report: {OUT / 'report.html'}")
    if not a.no_pdf:
        print(f"pdf:    {OUT / 'report.pdf'}")
    print(f"summary: {summary_path}")
    print(f"{len(WARN)} warnings")


OUT = None

if __name__ == "__main__":
    main()
