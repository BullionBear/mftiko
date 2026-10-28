#!/usr/bin/env python3
"""Summarise drill strategy logs since a time (drill.sh orders): one line per log file.

    orders.py SINCE FILE...      SINCE is RFC 3339 UTC, e.g. 2026-10-26T02:00:00Z

Counts submits and the ACCEPTED, CANCELLED and REJECTED order events, OnStart lines (a new
incarnation) and heartbeats, and finds the longest gap between two order events. With cycle_s 10 a
gap is normally about 10 s; one much longer is a stall.
"""
import json
import re
import sys
from datetime import datetime, timezone


def ts(s):
    # Go writes up to 9 fractional digits; fromisoformat takes 6.
    s = re.sub(r"(\.\d{6})\d*", r"\1", s.replace("Z", "+00:00"))
    return datetime.fromisoformat(s)


def summarise(since, path):
    c = {"submit": 0, "ACCEPTED": 0, "CANCELLED": 0, "REJECTED": 0, "OnStart": 0, "heartbeat": 0}
    last, gap, gap_at = None, 0.0, "-"
    with open(path) as f:
        for raw in f:
            if not raw.startswith("{"):
                continue
            try:
                line = json.loads(raw)
                stamp = line.get("time") or line.get("ts")
                if not stamp:
                    continue
                t = ts(stamp)
            except (ValueError, KeyError):
                continue
            if t < since:
                continue
            hook = line.get("hook", "")
            if hook in c:
                c[hook] += 1
            status = line.get("status", "").replace("ORDER_STATUS_", "")
            if hook == "OnOrder" and status in c:
                c[status] += 1
            if hook in ("OnOrder", "submit"):
                if last is not None and (t - last).total_seconds() > gap:
                    gap, gap_at = (t - last).total_seconds(), last.strftime("%H:%M:%S")
                last = t
    age = (datetime.now(timezone.utc) - last).total_seconds() if last else -1
    return (f"submit={c['submit']} accepted={c['ACCEPTED']} cancelled={c['CANCELLED']} "
            f"rejected={c['REJECTED']} starts={c['OnStart']} heartbeats={c['heartbeat']} "
            f"max_gap={gap:.1f}s after {gap_at} last_event_age={age:.1f}s")


def main():
    since = ts(sys.argv[1])
    for path in sys.argv[2:]:
        print(f"{path}: {summarise(since, path)}")


main()
