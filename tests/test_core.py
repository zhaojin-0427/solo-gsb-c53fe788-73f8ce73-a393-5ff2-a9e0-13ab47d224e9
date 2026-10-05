"""Adjudication semantics tests, each running against a fresh SQLite file."""

import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app import core, db  # noqa: E402


def fresh_conn():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    os.unlink(path)
    return db.init_db(path), path


def task(tid, deps=(), def_v=1, src_v=1, prev_def_v=None, prev_src_v=None,
         artifact=None, dynamic=False):
    """Build an import item; deps are upstream ids whose artifacts are 'art:<id>'."""
    prev_def_v = def_v if prev_def_v is None else prev_def_v
    prev_src_v = src_v if prev_src_v is None else prev_src_v
    dep_list = [{"upstream": u, "read_digest": f"art:{u}"} for u in deps]
    return {
        "id": tid,
        "definition": f"def:{tid}:v{def_v}",
        "prev_definition": f"def:{tid}:v{prev_def_v}",
        "source_digest": f"src:{tid}:v{src_v}",
        "prev_source_digest": f"src:{tid}:v{prev_src_v}",
        "artifact_digest": artifact or f"art:{tid}",
        "static_deps": [] if dynamic else dep_list,
        "dynamic_deps": dep_list if dynamic else [],
    }


def states(conn):
    return {r["task_id"]: r["state"]
            for r in conn.execute("SELECT task_id, state FROM tasks")}


def test_import_all_matching_is_reusable():
    conn, _ = fresh_conn()
    core.import_graph(conn, [task("a"), task("b", ["a"]), task("c", ["b"])])
    assert states(conn) == {"a": "reusable", "b": "reusable", "c": "reusable"}


def test_changed_source_makes_executable_downstream_waits():
    conn, _ = fresh_conn()
    core.import_graph(conn, [
        task("a", src_v=2, prev_src_v=1),   # sources changed
        task("b", ["a"]),
        task("c", ["b"]),
    ])
    s = states(conn)
    assert s["a"] == "executable"
    assert s["b"] == "waiting"
    assert s["c"] == "waiting"
    row = conn.execute("SELECT reason, depth FROM tasks WHERE task_id='a'").fetchone()
    assert row["reason"] == "source_changed" and row["depth"] == 0


def test_upstream_rerun_same_artifact_keeps_downstream_reusable():
    conn, _ = fresh_conn()
    core.import_graph(conn, [
        task("a", src_v=2, prev_src_v=1), task("b", ["a"]), task("c", ["b"]),
    ])
    claim, err = core.claim(conn, "a")
    assert err is None
    status, _ = core.report_result(conn, "a", claim["input_fingerprint"],
                                   reads=[], artifact_digest="art:a")  # unchanged
    assert status == "accepted"
    s = states(conn)
    assert s == {"a": "reusable", "b": "reusable", "c": "reusable"}


def test_upstream_artifact_change_invalidates_downstream():
    conn, _ = fresh_conn()
    core.import_graph(conn, [
        task("a", src_v=2, prev_src_v=1), task("b", ["a"]), task("c", ["b"]),
    ])
    claim, _ = core.claim(conn, "a")
    status, _ = core.report_result(conn, "a", claim["input_fingerprint"],
                                   reads=[], artifact_digest="art:a:NEW")
    assert status == "accepted"
    s = states(conn)
    assert s["a"] == "reusable"
    assert s["b"] == "executable"   # read digest no longer matches
    assert s["c"] == "waiting"      # b not settled
    row = conn.execute(
        "SELECT reason, reason_task_id, depth FROM tasks WHERE task_id='b'").fetchone()
    assert row["reason"] == "upstream_changed"
    assert row["reason_task_id"] == "a"
    # chain c -> b -> a
    chain = core.reason_chain(conn, "c")
    assert [n["task_id"] for n in chain] == ["c", "b", "a"]


def test_stale_report_rejected_without_state_change():
    conn, _ = fresh_conn()
    core.import_graph(conn, [
        task("a", src_v=2, prev_src_v=1), task("b", ["a"], src_v=2, prev_src_v=1),
    ])
    # a completes with a new artifact -> b unblocks (own sources also changed)
    claim_a, _ = core.claim(conn, "a")
    core.report_result(conn, "a", claim_a["input_fingerprint"], [], "art:a:X")
    claim_b, err = core.claim(conn, "b")
    assert err is None                      # b's own sources changed: executable
    # a slow duplicate worker re-reports a: a is already reusable. Its carried
    # fingerprint may even be unchanged (a's first report kept the same
    # upstream set), but the replay must be rejected as stale instead of
    # accepting a different artifact and rewriting b's plan.
    before_a = conn.execute("SELECT * FROM tasks WHERE task_id='a'").fetchone()
    before_b = conn.execute("SELECT * FROM tasks WHERE task_id='b'").fetchone()
    seq_before = conn.execute("SELECT MAX(seq) FROM changes").fetchone()[0]
    status, detail = core.report_result(
        conn, "a", claim_a["input_fingerprint"], [], "art:a:Y")
    assert status == "stale"
    after_a = conn.execute("SELECT * FROM tasks WHERE task_id='a'").fetchone()
    after_b = conn.execute("SELECT * FROM tasks WHERE task_id='b'").fetchone()
    seq_after = conn.execute("SELECT MAX(seq) FROM changes").fetchone()[0]
    assert dict(before_a) == dict(after_a)      # no state change at all
    assert dict(before_b) == dict(after_b)
    assert seq_before == seq_after              # no change records appended
    assert detail["state"] == "reusable"
    # b's original claim is still valid and its report goes through
    status, _ = core.report_result(conn, "b", claim_b["input_fingerprint"],
                                   [], "art:b:NEW")
    assert status == "accepted"


def test_wrong_read_digest_rejected_without_state_change():
    conn, _ = fresh_conn()
    core.import_graph(conn, [
        task("x"),
        task("a", ["x"], src_v=2, prev_src_v=1),
    ])
    claim, _ = core.claim(conn, "a")
    before = dict(conn.execute(
        "SELECT * FROM tasks WHERE task_id='a'").fetchone())
    deps_before = conn.execute(
        "SELECT * FROM deps WHERE task_id='a'").fetchall()
    seq_before = conn.execute("SELECT MAX(seq) FROM changes").fetchone()[0]
    # x exists, but the reported digest does not match its registered artifact
    status, detail = core.report_result(
        conn, "a", claim["input_fingerprint"],
        [{"upstream": "x", "digest": "art:x:TAMPERED"}], "art:a:NEW")
    assert status == "stale"
    assert detail["mismatched_reads"] == ["x"]
    after = dict(conn.execute(
        "SELECT * FROM tasks WHERE task_id='a'").fetchone())
    assert before == after
    assert {tuple(r) for r in deps_before} == {
        tuple(r) for r in conn.execute(
            "SELECT * FROM deps WHERE task_id='a'").fetchall()}
    assert conn.execute("SELECT MAX(seq) FROM changes").fetchone()[0] == seq_before
    assert states(conn)["a"] == "executable"


def test_static_read_keeps_static_identity_and_refreshes_digest():
    conn, _ = fresh_conn()
    core.import_graph(conn, [
        task("x", artifact="art:x:OLD"),
        # a statically reads x but previously read an older artifact digest
        {**task("a", ["x"], src_v=2, prev_src_v=1),
         "static_deps": [{"upstream": "x", "read_digest": "art:x:OLD"}]},
    ])
    # x is reusable with its new artifact; a is executable via own source
    claim, _ = core.claim(conn, "a")
    status, _ = core.report_result(
        conn, "a", claim["input_fingerprint"],
        [{"upstream": "x", "digest": "art:x:OLD"}], "art:a:NEW")
    assert status == "accepted"
    rows = conn.execute(
        "SELECT upstream_id, kind, read_digest FROM deps WHERE task_id='a'"
    ).fetchall()
    # the read overlaps the static key: no dynamic duplicate row is inserted
    assert [(r["upstream_id"], r["kind"], r["read_digest"]) for r in rows] == \
        [("x", "static", "art:x:OLD")]


def test_static_and_dynamic_reads_coexist_without_unique_violation():
    conn, _ = fresh_conn()
    core.import_graph(conn, [
        task("x"), task("y"),
        task("a", ["x"], src_v=2, prev_src_v=1),
    ])
    claim, _ = core.claim(conn, "a")
    # x is both a static dep and reported in reads; y is newly discovered
    status, _ = core.report_result(
        conn, "a", claim["input_fingerprint"],
        [{"upstream": "x", "digest": "art:x"},
         {"upstream": "y", "digest": "art:y"}], "art:a:NEW")
    assert status == "accepted"
    rows = conn.execute(
        "SELECT upstream_id, kind, read_digest FROM deps WHERE task_id='a' "
        "ORDER BY upstream_id").fetchall()
    assert [(r["upstream_id"], r["kind"], r["read_digest"]) for r in rows] == \
        [("x", "static", "art:x"), ("y", "dynamic", "art:y")]
    # a later run drops y and keeps reading x: dynamic set reflects this run
    claim2 = None
    conn.execute(
        "UPDATE tasks SET source_digest='src:a:v3', state='executable', "
        "reason='source_changed', reason_task_id=NULL, depth=0 WHERE task_id='a'")
    claim2, err = core.claim(conn, "a")
    assert err is None
    status, _ = core.report_result(
        conn, "a", claim2["input_fingerprint"],
        [{"upstream": "x", "digest": "art:x"}], "art:a:NEW2")
    assert status == "accepted"
    rows = conn.execute(
        "SELECT upstream_id, kind FROM deps WHERE task_id='a' "
        "ORDER BY upstream_id").fetchall()
    assert [(r["upstream_id"], r["kind"]) for r in rows] == [("x", "static")]


def test_new_unsettled_dynamic_read_keeps_task_waiting():
    conn, _ = fresh_conn()
    core.import_graph(conn, [
        task("y", src_v=2, prev_src_v=1),                       # not settled
        task("a", src_v=2, prev_src_v=1),
    ])
    claim, _ = core.claim(conn, "a")
    # the digest is the one y has currently registered, so the read itself is
    # legal; but y is still executable -> a must wait rather than go reusable
    status, _ = core.report_result(
        conn, "a", claim["input_fingerprint"],
        [{"upstream": "y", "digest": "art:y"}], "art:a:NEW")
    assert status == "accepted"
    assert states(conn)["a"] == "waiting"
    row = conn.execute(
        "SELECT reason, reason_task_id, depth FROM tasks WHERE task_id='a'"
    ).fetchone()
    assert row["reason"] == "upstream_waiting"
    assert row["reason_task_id"] == "y"
    # once y settles with the same artifact a was built against, a is reusable
    claim_y, _ = core.claim(conn, "y")
    status, _ = core.report_result(conn, "y", claim_y["input_fingerprint"],
                                   [], "art:y")
    assert status == "accepted"
    assert states(conn)["a"] == "reusable"


def test_reporting_waiting_task_is_stale():
    conn, _ = fresh_conn()
    core.import_graph(conn, [
        task("a", src_v=2, prev_src_v=1), task("b", ["a"]),
    ])
    # b is waiting; a report arriving for it (with any fingerprint) is stale
    fp = core.input_fingerprint(
        conn.execute("SELECT def_hash FROM tasks WHERE task_id='b'").fetchone()[0],
        conn.execute(
            "SELECT source_digest FROM tasks WHERE task_id='b'").fetchone()[0],
        [("a", "art:a")])
    status, detail = core.report_result(conn, "b", fp, [], "art:b:NEW")
    assert status == "stale"
    assert detail["state"] == "waiting"
    assert states(conn)["b"] == "waiting"


def test_missing_dependency_rolls_back_with_static_overlap():
    conn, _ = fresh_conn()
    core.import_graph(conn, [
        task("x"),
        task("a", ["x"], src_v=2, prev_src_v=1),
    ])
    claim, _ = core.claim(conn, "a")
    status, detail = core.report_result(
        conn, "a", claim["input_fingerprint"],
        [{"upstream": "x", "digest": "art:x"},
         {"upstream": "ghost", "digest": "z"}], "art:a:NEW")
    assert status == "missing_dependency"
    assert detail["missing"] == ["ghost"]
    # whole report rolled back: static digest untouched, no dynamic rows
    rows = conn.execute(
        "SELECT upstream_id, kind, read_digest FROM deps WHERE task_id='a'"
    ).fetchall()
    assert [(r["upstream_id"], r["kind"], r["read_digest"]) for r in rows] == \
        [("x", "static", "art:x")]
    assert states(conn)["a"] == "executable"


def test_cycle_report_rolls_back():
    conn, _ = fresh_conn()
    core.import_graph(conn, [
        task("a", src_v=2, prev_src_v=1), task("b", ["a"]),
    ])
    claim, _ = core.claim(conn, "a")
    # a reports it dynamically read b -> cycle a -> b -> a
    status, detail = core.report_result(
        conn, "a", claim["input_fingerprint"],
        [{"upstream": "b", "digest": "art:b"}], "art:a:NEW")
    assert status == "cycle"
    # everything rolled back
    assert conn.execute(
        "SELECT COUNT(*) FROM deps WHERE task_id='a' AND kind='dynamic'"
    ).fetchone()[0] == 0
    assert conn.execute(
        "SELECT artifact_digest FROM tasks WHERE task_id='a'").fetchone()[0] == "art:a"
    assert states(conn)["a"] == "executable"


def test_missing_dependency_report_rolls_back():
    conn, _ = fresh_conn()
    core.import_graph(conn, [task("a", src_v=2, prev_src_v=1)])
    claim, _ = core.claim(conn, "a")
    status, detail = core.report_result(
        conn, "a", claim["input_fingerprint"],
        [{"upstream": "ghost", "digest": "x"}], "art:a:NEW")
    assert status == "missing_dependency"
    assert detail["missing"] == ["ghost"]
    assert states(conn)["a"] == "executable"
    assert conn.execute("SELECT COUNT(*) FROM deps").fetchone()[0] == 0


def test_dynamic_deps_replaced_by_actual_reads():
    conn, _ = fresh_conn()
    core.import_graph(conn, [
        task("x"), task("y"),
        task("a", ["x"], dynamic=True, src_v=2, prev_src_v=1),
    ])
    claim, _ = core.claim(conn, "a")
    status, _ = core.report_result(
        conn, "a", claim["input_fingerprint"],
        [{"upstream": "y", "digest": "art:y"}], "art:a:NEW")
    assert status == "accepted"
    deps = conn.execute(
        "SELECT upstream_id, kind, read_digest FROM deps WHERE task_id='a'").fetchall()
    assert [(d["upstream_id"], d["kind"], d["read_digest"]) for d in deps] == \
        [("y", "dynamic", "art:y")]
    # y's artifact later changes -> a is invalidated through its dynamic dep
    conn.execute("UPDATE tasks SET artifact_digest='art:y:2' WHERE task_id='y'")
    core._propagate(conn, core._ChangeLog(conn), ["y"])
    assert states(conn)["a"] == "executable"


def test_restart_consistency():
    conn, path = fresh_conn()
    core.import_graph(conn, [
        task("a", src_v=2, prev_src_v=1), task("b", ["a"]), task("c", ["b"]),
    ])
    claim, _ = core.claim(conn, "a")
    core.report_result(conn, "a", claim["input_fingerprint"], [], "art:a:NEW")
    plan_before = core.plan_since(conn, 0)
    conn.close()
    # reopen the same database file: verdicts must be identical
    conn2 = db.init_db(path)
    plan_after = core.plan_since(conn2, 0)
    assert plan_before == plan_after
    assert states(conn2) == {"a": "reusable", "b": "executable", "c": "waiting"}
    conn2.close()
    os.unlink(path)


def test_import_cycle_rejected():
    conn, _ = fresh_conn()
    a = task("a", ["b"])
    b = task("b", ["a"])
    try:
        core.import_graph(conn, [a, b])
        assert False, "expected ValueError"
    except ValueError as e:
        assert "cycle" in str(e)


def test_import_missing_dep_rejected():
    conn, _ = fresh_conn()
    try:
        core.import_graph(conn, [task("a", ["ghost"])])
        assert False, "expected ValueError"
    except ValueError as e:
        assert "missing" in str(e)


def test_plan_returns_only_changes_since_seq():
    conn, _ = fresh_conn()
    core.import_graph(conn, [
        task("a", src_v=2, prev_src_v=1), task("b", ["a"]),
    ])
    plan0 = core.plan_since(conn, 0)
    assert len(plan0["changes"]) == 2
    claim, _ = core.claim(conn, "a")
    core.report_result(conn, "a", claim["input_fingerprint"], [], "art:a:NEW")
    plan1 = core.plan_since(conn, plan0["seq"])
    changed_ids = {c["task_id"] for c in plan1["changes"]}
    assert changed_ids == {"a", "b"}
    assert plan1["counts"]["executable"] == 1
    plan2 = core.plan_since(conn, plan1["seq"])
    assert plan2["changes"] == []


def test_shortest_chain_prefers_shallower_upstream():
    conn, _ = fresh_conn()
    # deep chain: d1 changed -> m1 -> m2 -> t ; shallow: s1 changed -> t
    core.import_graph(conn, [
        task("d1", src_v=2, prev_src_v=1),
        task("m1", ["d1"]),
        task("m2", ["m1"]),
        task("s1", src_v=2, prev_src_v=1),
        task("t", ["m2", "s1"]),
    ])
    row = conn.execute(
        "SELECT reason_task_id, depth FROM tasks WHERE task_id='t'").fetchone()
    assert row["reason_task_id"] == "s1"   # depth 1 beats depth 3
    assert row["depth"] == 1
