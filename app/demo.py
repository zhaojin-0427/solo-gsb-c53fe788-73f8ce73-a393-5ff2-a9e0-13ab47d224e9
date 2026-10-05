"""Deterministic demo-graph generator (used by POST /api/demo)."""

import hashlib


def _h(text):
    return hashlib.sha256(text.encode()).hexdigest()[:16]


def generate(layers=8, width=40, fan=3, changed=60):
    """Layered acyclic graph; ~`changed` leaf tasks got new sources."""
    tasks = []
    count = layers * width

    def tid(i):
        return f"task_{i:05d}"

    for i in range(count):
        layer = i // width
        deps = []
        if layer > 0:
            base = (layer - 1) * width
            for k in range(fan):
                up = base + (i * 7 + k * 13) % width
                deps.append({
                    "upstream": tid(up),
                    "read_digest": _h(f"artifact:{tid(up)}"),
                })
        dirty = i < changed  # first `changed` tasks (layer 0) have new sources
        src = _h(f"src:{tid(i)}:v2" if dirty else f"src:{tid(i)}:v1")
        prev = _h(f"src:{tid(i)}:v1")
        tasks.append({
            "id": tid(i),
            "definition": {"cmd": "build", "target": tid(i)},
            "prev_definition": {"cmd": "build", "target": tid(i)},
            "source_digest": src,
            "prev_source_digest": prev,
            "artifact_digest": _h(f"artifact:{tid(i)}"),
            "static_deps": deps,
            "dynamic_deps": [],
        })
    return tasks
