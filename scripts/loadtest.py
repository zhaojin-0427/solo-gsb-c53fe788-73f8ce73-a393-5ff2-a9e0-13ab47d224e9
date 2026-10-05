#!/usr/bin/env python3
"""Scale benchmark for the incremental-build adjudicator.

Imports 100k tasks / 200k dependencies (10k independent groups of 10, so a
single report never affects more than 10 tasks), then performs 1000 rounds of
claim -> report -> incremental plan query and checks the total time.

Usage:  python3 scripts/loadtest.py [--port 8000] [--reports 1000]
"""

import argparse
import http.client
import json
import time

GROUP = 10


def build_payload(groups):
    """10k groups x 10 tasks; exactly 20 intra-group deps per group."""
    tasks = []

    def tid(g, i):
        return f"t{g:05d}_{i}"

    for g in range(groups):
        for i in range(GROUP):
            static, dynamic = [], []
            edges = []
            if i >= 1:
                edges.append(i - 1)
            if i >= 2:
                edges.append(i - 2)
            if i in (4, 7, 9):
                edges.append(i - 3)
            for n, j in enumerate(edges):
                dep = {"upstream": tid(g, j), "read_digest": f"art:{tid(g, j)}"}
                (static if n % 2 == 0 else dynamic).append(dep)
            dirty = i == 0  # group head got new sources this round
            tasks.append({
                "id": tid(g, i),
                "definition": f"def:{tid(g, i)}:v1",
                "prev_definition": f"def:{tid(g, i)}:v1",
                "source_digest": f"src:{tid(g, i)}:v{2 if dirty else 1}",
                "prev_source_digest": f"src:{tid(g, i)}:v1",
                "artifact_digest": f"art:{tid(g, i)}",
                "static_deps": static,
                "dynamic_deps": dynamic,
            })
    return {"tasks": tasks}


class Client:
    def __init__(self, host, port):
        self.conn = http.client.HTTPConnection(host, port, timeout=120)

    def call(self, method, path, payload=None):
        body = json.dumps(payload).encode() if payload is not None else None
        headers = {"Content-Type": "application/json"} if body else {}
        self.conn.request(method, path, body=body, headers=headers)
        resp = self.conn.getresponse()
        data = resp.read()
        if resp.status >= 400:
            raise RuntimeError(f"{method} {path} -> {resp.status}: {data[:300]}")
        return json.loads(data) if data else {}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8000)
    ap.add_argument("--groups", type=int, default=10000)
    ap.add_argument("--reports", type=int, default=1000)
    args = ap.parse_args()

    c = Client(args.host, args.port)

    n_tasks = args.groups * GROUP
    print(f"== importing {n_tasks} tasks / {args.groups * 20} deps ==")
    t0 = time.perf_counter()
    result = c.call("POST", "/api/import", build_payload(args.groups))
    t_import = time.perf_counter() - t0
    print(f"import ok in {t_import:.2f}s -> {result['counts']}")
    assert result["tasks"] == n_tasks

    plan = c.call("GET", "/api/plan?since=0")
    seq = plan["seq"]
    executable = [ch["task_id"] for ch in plan["changes"]
                  if ch["state"] == "executable"]
    assert len(executable) == args.groups, len(executable)
    print(f"initial plan: seq={seq} executable={len(executable)}")

    print(f"== {args.reports} rounds of claim+report+plan ==")
    t_claim = t_report = t_plan = 0.0
    t_start = time.perf_counter()
    for n in range(args.reports):
        tid = executable[n]

        t = time.perf_counter()
        claim = c.call("POST", f"/api/tasks/{tid}/claim")
        t_claim += time.perf_counter() - t

        # alternate: even rounds keep the artifact digest (downstream reusable),
        # odd rounds produce a new digest (downstream invalidated)
        artifact = f"art:{tid}" if n % 2 == 0 else f"art:{tid}:r1"
        t = time.perf_counter()
        rep = c.call("POST", "/api/results", {
            "task_id": tid,
            "input_fingerprint": claim["input_fingerprint"],
            "reads": [],
            "artifact_digest": artifact,
        })
        t_report += time.perf_counter() - t
        assert rep["status"] == "accepted", rep

        t = time.perf_counter()
        plan = c.call("GET", f"/api/plan?since={seq}")
        t_plan += time.perf_counter() - t
        seq = plan["seq"]

    total = time.perf_counter() - t_start
    print(f"total {total:.2f}s  (claim {t_claim:.2f}s, report {t_report:.2f}s, "
          f"plan {t_plan:.2f}s)  -> {args.reports / total:.0f} rounds/s")
    print(f"final counts: {plan['counts']}")
    assert total < 10.0, f"too slow: {total:.2f}s >= 10s"
    print("PASS: 1000 report+plan rounds under 10s")


if __name__ == "__main__":
    main()
