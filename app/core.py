"""Incremental-build adjudication core.

Verdict states
--------------
reusable    definition, source digest and every upstream artifact digest read
            last time all match the current graph -> previous result reusable.
waiting     some upstream is not settled yet (it is itself waiting or
            executable) -> must wait for upstream before running.
executable  inputs changed (or an upstream artifact digest changed) and every
            upstream is settled -> may be claimed and executed now.

Every non-reusable task carries the *shortest invalidation reason chain*:
``reason`` describes this task's own link and ``reason_task_id`` points to
the upstream task responsible (NULL for root causes such as a definition or
source change).  ``depth`` is the chain length to the root cause, used to
pick the shortest chain when several upstreams are to blame.
"""

import hashlib
import json
from collections import Counter, deque

from . import db

REUSABLE = "reusable"
WAITING = "waiting"
EXECUTABLE = "executable"

REASON_DEFINITION = "definition_changed"
REASON_SOURCE = "source_changed"
REASON_UPSTREAM_WAITING = "upstream_waiting"
REASON_UPSTREAM_CHANGED = "upstream_changed"


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def canonical(value):
    """Canonical text for a definition (any JSON value or raw string)."""
    if isinstance(value, str):
        return value
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def sha(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def input_fingerprint(def_hash, source_digest, deps):
    """Fingerprint of everything a task is allowed to read.

    ``deps`` is an iterable of ``(upstream_id, upstream_artifact_digest)``.
    """
    h = hashlib.sha256()
    h.update(def_hash.encode())
    h.update(b"\n")
    h.update(source_digest.encode())
    h.update(b"\n")
    for upstream_id, digest in sorted(deps):
        h.update(upstream_id.encode())
        h.update(b":")
        h.update(digest.encode())
        h.update(b"\n")
    return h.hexdigest()


def _decide(self_reason, upstreams):
    """Pure adjudication for one task.

    ``self_reason`` is REASON_DEFINITION / REASON_SOURCE / None.
    ``upstreams`` is a list of dicts with keys id, state, artifact, read, depth.
    Returns ``(state, reason, reason_task_id, depth)``.
    """
    bad = [u for u in upstreams
           if u["state"] != REUSABLE or u["artifact"] != u["read"]]
    if self_reason is None and not bad:
        return REUSABLE, None, None, 0

    # shortest invalidation chain: own change (depth 0) beats any upstream.
    if self_reason is not None:
        reason, reason_task, depth = self_reason, None, 0
    else:
        culprit = min(bad, key=lambda u: (u["depth"], u["id"]))
        reason = (REASON_UPSTREAM_WAITING if culprit["state"] != REUSABLE
                  else REASON_UPSTREAM_CHANGED)
        reason_task, depth = culprit["id"], culprit["depth"] + 1

    settled = all(u["state"] == REUSABLE for u in upstreams)
    state = EXECUTABLE if settled else WAITING
    return state, reason, reason_task, depth


def _self_reason(row):
    """Root-cause reason if the task's own definition/sources changed."""
    if row["def_hash"] != row["prev_def_hash"]:
        return REASON_DEFINITION
    if row["source_digest"] != row["prev_source_digest"]:
        return REASON_SOURCE
    return None


def _fetch_upstreams(conn, task_id):
    rows = conn.execute(
        "SELECT d.upstream_id AS id, d.read_digest AS read, "
        "       t.artifact_digest AS artifact, t.state AS state, t.depth AS depth "
        "FROM deps d JOIN tasks t ON t.task_id = d.upstream_id "
        "WHERE d.task_id = ?", (task_id,))
    return [dict(r) for r in rows]


class _ChangeLog:
    """Accumulates state transitions inside one transaction."""

    def __init__(self, conn):
        self.conn = conn
        self.counts = Counter()
        self.rows = []

    def record(self, task_id, old_state, state, reason, reason_task, depth):
        self.conn.execute(
            "UPDATE tasks SET state=?, reason=?, reason_task_id=?, depth=? "
            "WHERE task_id=?",
            (state, reason, reason_task, depth, task_id))
        self.rows.append((task_id, state, reason, reason_task, depth))
        self.counts[old_state] -= 1
        self.counts[state] += 1

    def flush(self):
        if self.rows:
            self.conn.executemany(
                "INSERT INTO changes(task_id, state, reason, reason_task_id, depth) "
                "VALUES (?,?,?,?,?)", self.rows)
        for state, delta in self.counts.items():
            if delta:
                self.conn.execute(
                    "INSERT INTO state_counts(state, n) VALUES (?, ?) "
                    "ON CONFLICT(state) DO UPDATE SET n = n + excluded.n",
                    (state, delta))


# ---------------------------------------------------------------------------
# import
# ---------------------------------------------------------------------------

def import_graph(conn, tasks):
    """Replace the whole graph and adjudicate every task.

    ``tasks`` is a list of dicts with keys:
    id, definition, prev_definition, source_digest, prev_source_digest,
    artifact_digest, static_deps, dynamic_deps
    where each dep is ``{"upstream": id, "read_digest": digest}``.
    """
    # ---- validate & normalise ---------------------------------------------
    norm = []
    ids = set()
    for t in tasks:
        tid = t["id"]
        if not isinstance(tid, str) or not tid:
            raise ValueError(f"invalid task id: {tid!r}")
        if tid in ids:
            raise ValueError(f"duplicate task id: {tid}")
        ids.add(tid)
        definition = canonical(t.get("definition", ""))
        prev_definition = canonical(t.get("prev_definition", ""))
        norm.append({
            "id": tid,
            "definition": definition,
            "def_hash": sha(definition),
            "prev_def_hash": sha(prev_definition),
            "source_digest": str(t.get("source_digest", "")),
            "prev_source_digest": str(t.get("prev_source_digest", "")),
            "artifact_digest": str(t.get("artifact_digest", "")),
            "deps": [(str(d["upstream"]), str(d.get("read_digest", "")), "static")
                     for d in t.get("static_deps", [])] +
                    [(str(d["upstream"]), str(d.get("read_digest", "")), "dynamic")
                     for d in t.get("dynamic_deps", [])],
        })

    dep_rows = []
    for t in norm:
        seen = set()
        for upstream, read_digest, kind in t["deps"]:
            if upstream not in ids:
                raise ValueError(
                    f"task {t['id']} references missing task {upstream}")
            if upstream == t["id"]:
                raise ValueError(f"task {t['id']} depends on itself")
            if upstream in seen:
                continue
            seen.add(upstream)
            dep_rows.append((t["id"], upstream, kind, read_digest))

    # ---- topological order (cycle check) -----------------------------------
    indegree = {t["id"]: 0 for t in norm}
    downstream = {}
    for tid, upstream, _kind, _digest in dep_rows:
        indegree[tid] += 1
        downstream.setdefault(upstream, []).append(tid)
    order = []
    queue = deque([tid for tid, deg in indegree.items() if deg == 0])
    while queue:
        node = queue.popleft()
        order.append(node)
        for nxt in downstream.get(node, ()):
            indegree[nxt] -= 1
            if indegree[nxt] == 0:
                queue.append(nxt)
    if len(order) != len(norm):
        cyclic = sorted(tid for tid, deg in indegree.items() if deg > 0)
        raise ValueError(
            f"dependency cycle detected involving {len(cyclic)} tasks, "
            f"e.g. {cyclic[:5]}")

    # ---- adjudicate in topological order ------------------------------------
    by_id = {t["id"]: t for t in norm}
    deps_of = {}
    for tid, upstream, _kind, digest in dep_rows:
        deps_of.setdefault(tid, []).append((upstream, digest))

    state = {}
    info = {}  # id -> (state, reason, reason_task, depth)
    for tid in order:
        t = by_id[tid]
        self_reason = (REASON_DEFINITION if t["def_hash"] != t["prev_def_hash"]
                       else REASON_SOURCE
                       if t["source_digest"] != t["prev_source_digest"] else None)
        upstreams = []
        for upstream, read_digest in deps_of.get(tid, ()):
            ustate, _ureason, _utask, udepth = info[upstream]
            upstreams.append({
                "id": upstream, "state": ustate,
                "artifact": by_id[upstream]["artifact_digest"],
                "read": read_digest, "depth": udepth,
            })
        st, reason, reason_task, depth = _decide(self_reason, upstreams)
        info[tid] = (st, reason, reason_task, depth)
        state[tid] = st

    # ---- persist atomically --------------------------------------------------
    conn.execute("BEGIN IMMEDIATE")
    try:
        conn.execute("DELETE FROM deps")
        conn.execute("DELETE FROM tasks")
        conn.execute("DELETE FROM changes")
        conn.execute("DELETE FROM state_counts")
        conn.executemany(
            "INSERT INTO tasks(task_id, definition, def_hash, prev_def_hash, "
            "                  source_digest, prev_source_digest, artifact_digest, "
            "                  state, reason, reason_task_id, depth) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            [(t["id"], t["definition"], t["def_hash"], t["prev_def_hash"],
              t["source_digest"], t["prev_source_digest"], t["artifact_digest"],
              *info[t["id"]]) for t in norm])
        conn.executemany(
            "INSERT INTO deps(task_id, upstream_id, kind, read_digest) "
            "VALUES (?,?,?,?)", dep_rows)
        conn.executemany(
            "INSERT INTO changes(task_id, state, reason, reason_task_id, depth) "
            "VALUES (?,?,?,?,?)",
            [(tid, *info[tid]) for tid in order])
        counts = Counter(state.values())
        conn.executemany(
            "INSERT INTO state_counts(state, n) VALUES (?,?)",
            list(counts.items()))
        epoch = int(get_meta(conn, "epoch", "0")) + 1
        set_meta(conn, "epoch", str(epoch))
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    return {
        "tasks": len(norm),
        "deps": len(dep_rows),
        "counts": dict(counts),
        "epoch": epoch,
    }


# ---------------------------------------------------------------------------
# claim & report
# ---------------------------------------------------------------------------

def claim(conn, task_id):
    row = conn.execute("SELECT * FROM tasks WHERE task_id=?", (task_id,)).fetchone()
    if row is None:
        return None, "unknown_task"
    if row["state"] != EXECUTABLE:
        return {"task_id": task_id, "state": row["state"]}, "not_executable"
    deps = conn.execute(
        "SELECT d.upstream_id, d.kind, d.read_digest, t.artifact_digest "
        "FROM deps d JOIN tasks t ON t.task_id = d.upstream_id "
        "WHERE d.task_id=? ORDER BY d.upstream_id", (task_id,)).fetchall()
    fp = input_fingerprint(row["def_hash"], row["source_digest"],
                           [(d["upstream_id"], d["artifact_digest"]) for d in deps])
    return {
        "task_id": task_id,
        "state": row["state"],
        "input_fingerprint": fp,
        "inputs": {
            "definition_hash": row["def_hash"],
            "source_digest": row["source_digest"],
            "deps": [{"upstream": d["upstream_id"],
                      "kind": d["kind"],
                      "artifact_digest": d["artifact_digest"]} for d in deps],
        },
    }, None


def _creates_cycle(conn, start):
    """True if ``start`` can reach itself (its dep edges were just rewritten)."""
    visited = set()
    stack = [start]
    while stack:
        node = stack.pop()
        for (up,) in conn.execute(
                "SELECT upstream_id FROM deps WHERE task_id=?", (node,)):
            if up == start:
                return True
            if up not in visited:
                visited.add(up)
                stack.append(up)
    return False


def report_result(conn, task_id, fingerprint, reads, artifact_digest):
    """Apply one execution report atomically.

    Returns ``(status, detail)`` where status is "accepted", "stale" or one of
    the error codes.  Any failure rolls the whole report back.
    """
    with db.write_lock:
        conn.execute("BEGIN IMMEDIATE")
        try:
            row = conn.execute(
                "SELECT * FROM tasks WHERE task_id=?", (task_id,)).fetchone()
            if row is None:
                conn.rollback()
                return "unknown_task", {"task_id": task_id}

            deps = conn.execute(
                "SELECT d.upstream_id, t.artifact_digest FROM deps d "
                "JOIN tasks t ON t.task_id = d.upstream_id WHERE d.task_id=?",
                (task_id,)).fetchall()
            current_fp = input_fingerprint(
                row["def_hash"], row["source_digest"],
                [(d["upstream_id"], d["artifact_digest"]) for d in deps])
            if current_fp != fingerprint:
                conn.rollback()
                return "stale", {"task_id": task_id,
                                 "current_fingerprint": current_fp}

            # validate newly discovered dynamic dependencies
            new_reads = {}
            for r in reads:
                up = str(r["upstream"])
                new_reads[up] = str(r.get("digest", ""))
            if new_reads:
                marks = ",".join("?" * len(new_reads))
                found = {r[0] for r in conn.execute(
                    f"SELECT task_id FROM tasks WHERE task_id IN ({marks})",
                    tuple(new_reads))}
                missing = sorted(set(new_reads) - found)
                if missing:
                    conn.rollback()
                    return "missing_dependency", {"missing": missing}

            # swap dynamic deps for the actually-read set
            conn.execute(
                "DELETE FROM deps WHERE task_id=? AND kind='dynamic'", (task_id,))
            conn.executemany(
                "INSERT INTO deps(task_id, upstream_id, kind, read_digest) "
                "VALUES (?,?, 'dynamic', ?)",
                [(task_id, up, digest) for up, digest in new_reads.items()])

            if new_reads and _creates_cycle(conn, task_id):
                conn.rollback()
                return "cycle", {"task_id": task_id}

            # the task itself is now up to date with what it read
            log = _ChangeLog(conn)
            conn.execute(
                "UPDATE tasks SET artifact_digest=?, prev_def_hash=def_hash, "
                "                prev_source_digest=source_digest "
                "WHERE task_id=?", (artifact_digest, task_id))
            fresh = conn.execute(
                "SELECT state FROM tasks WHERE task_id=?", (task_id,)).fetchone()
            if fresh["state"] != REUSABLE:
                log.record(task_id, fresh["state"], REUSABLE, None, None, 0)

            # propagate the verdict to downstream tasks
            _propagate(conn, log, [task_id])
            log.flush()

            seq = conn.execute("SELECT MAX(seq) FROM changes").fetchone()[0] or 0
            conn.commit()
            return "accepted", {"task_id": task_id, "seq": seq,
                                "changed": len(log.rows)}
        except Exception:
            conn.rollback()
            raise


def _propagate(conn, log, seeds):
    queue = deque(seeds)
    seen = set(seeds)
    while queue:
        upstream = queue.popleft()
        rows = conn.execute(
            "SELECT task_id FROM deps WHERE upstream_id=?", (upstream,)).fetchall()
        for (tid,) in rows:
            if tid in seen:
                continue
            seen.add(tid)
            row = conn.execute(
                "SELECT * FROM tasks WHERE task_id=?", (tid,)).fetchone()
            upstreams = _fetch_upstreams(conn, tid)
            state, reason, reason_task, depth = _decide(
                _self_reason(row), upstreams)
            if (state, reason, reason_task, depth) != (
                    row["state"], row["reason"], row["reason_task_id"],
                    row["depth"]):
                log.record(tid, row["state"], state, reason, reason_task, depth)
                queue.append(tid)


# ---------------------------------------------------------------------------
# queries
# ---------------------------------------------------------------------------

def get_meta(conn, key, default=None):
    row = conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
    return row["value"] if row else default


def set_meta(conn, key, value):
    conn.execute(
        "INSERT INTO meta(key, value) VALUES (?,?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, value))


def plan_since(conn, since, limit=None):
    sql = ("SELECT seq, task_id, state, reason, reason_task_id, depth "
           "FROM changes WHERE seq > ? ORDER BY seq")
    params = [since]
    if limit:
        sql += " LIMIT ?"
        params.append(limit)
    changes = [dict(r) for r in conn.execute(sql, params)]
    counts = {r["state"]: r["n"]
              for r in conn.execute("SELECT state, n FROM state_counts")}
    seq = conn.execute("SELECT MAX(seq) FROM changes").fetchone()[0] or 0
    return {
        "epoch": int(get_meta(conn, "epoch", "0")),
        "seq": seq,
        "counts": counts,
        "changes": changes,
    }


def task_detail(conn, task_id):
    row = conn.execute("SELECT * FROM tasks WHERE task_id=?", (task_id,)).fetchone()
    if row is None:
        return None
    deps = conn.execute(
        "SELECT d.upstream_id, d.kind, d.read_digest, t.artifact_digest, t.state "
        "FROM deps d JOIN tasks t ON t.task_id = d.upstream_id "
        "WHERE d.task_id=? ORDER BY d.upstream_id", (task_id,)).fetchall()
    dependents = conn.execute(
        "SELECT task_id FROM deps WHERE upstream_id=? ORDER BY task_id LIMIT 100",
        (task_id,)).fetchall()
    return {
        "task_id": row["task_id"],
        "state": row["state"],
        "reason": row["reason"],
        "reason_task_id": row["reason_task_id"],
        "depth": row["depth"],
        "def_hash": row["def_hash"],
        "prev_def_hash": row["prev_def_hash"],
        "source_digest": row["source_digest"],
        "prev_source_digest": row["prev_source_digest"],
        "artifact_digest": row["artifact_digest"],
        "deps": [dict(d) for d in deps],
        "dependents": [d["task_id"] for d in dependents],
    }


def reason_chain(conn, task_id, cap=10000):
    """Follow reason_task_id links to the root cause (shortest chain)."""
    chain = []
    seen = set()
    current = task_id
    while current and current not in seen and len(chain) < cap:
        seen.add(current)
        row = conn.execute(
            "SELECT task_id, state, reason, reason_task_id, depth "
            "FROM tasks WHERE task_id=?", (current,)).fetchone()
        if row is None:
            chain.append({"task_id": current, "state": "missing",
                          "reason": "missing_dependency", "depth": None})
            break
        chain.append({"task_id": row["task_id"], "state": row["state"],
                      "reason": row["reason"], "depth": row["depth"]})
        current = row["reason_task_id"]
    return chain
