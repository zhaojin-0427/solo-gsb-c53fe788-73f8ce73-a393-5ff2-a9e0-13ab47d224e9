"""SQLite storage layer.

All adjudication state lives in SQLite so that a restart yields identical
verdicts.  Connections are per-thread (FastAPI sync endpoints run in a
threadpool); writes are serialised through a module-level lock plus
``BEGIN IMMEDIATE`` transactions.
"""

import os
import sqlite3
import threading

DB_PATH = os.environ.get("ADJUDICATOR_DB", os.path.join(os.path.dirname(__file__), "..", "data", "build.db"))

_local = threading.local()
write_lock = threading.Lock()

SCHEMA = """
CREATE TABLE IF NOT EXISTS tasks (
    task_id            TEXT PRIMARY KEY,
    definition         TEXT NOT NULL,           -- canonical JSON of current definition
    def_hash           TEXT NOT NULL,
    prev_def_hash      TEXT NOT NULL,           -- definition hash at last execution
    source_digest      TEXT NOT NULL,           -- digest of source files now
    prev_source_digest TEXT NOT NULL,           -- digest of source files at last execution
    artifact_digest    TEXT NOT NULL,           -- digest of own artifact (last produced)
    state              TEXT NOT NULL,           -- reusable | waiting | executable
    reason             TEXT,                    -- invalidation reason code
    reason_task_id     TEXT,                    -- next link of shortest reason chain
    depth              INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_tasks_state ON tasks(state);

CREATE TABLE IF NOT EXISTS deps (
    task_id      TEXT NOT NULL,                 -- dependent
    upstream_id  TEXT NOT NULL,                 -- prerequisite task
    kind         TEXT NOT NULL,                 -- static | dynamic
    read_digest  TEXT NOT NULL,                 -- upstream artifact digest read at last exec
    PRIMARY KEY (task_id, upstream_id)
);
CREATE INDEX IF NOT EXISTS idx_deps_upstream ON deps(upstream_id);

CREATE TABLE IF NOT EXISTS changes (
    seq            INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id        TEXT NOT NULL,
    state          TEXT NOT NULL,
    reason         TEXT,
    reason_task_id TEXT,
    depth          INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_changes_task ON changes(task_id);

CREATE TABLE IF NOT EXISTS state_counts (
    state TEXT PRIMARY KEY,
    n     INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""


def connect(path=None):
    path = path or DB_PATH
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    conn = sqlite3.connect(path, timeout=60, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA temp_store=MEMORY")
    conn.execute("PRAGMA cache_size=-65536")  # 64 MiB page cache
    return conn


def get_conn():
    conn = getattr(_local, "conn", None)
    if conn is None:
        conn = connect()
        _local.conn = conn
    return conn


def init_db(path=None):
    conn = connect(path) if path else get_conn()
    conn.executescript(SCHEMA)
    conn.commit()
    return conn
