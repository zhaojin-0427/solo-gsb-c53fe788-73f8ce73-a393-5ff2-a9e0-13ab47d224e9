#!/usr/bin/env python3
"""Generate an import payload file (default 100k tasks / 200k deps).

Usage: python3 scripts/gen_import.py [output.json] [--groups 10000]
Then:  curl -X POST --data @import.json -H 'Content-Type: application/json' \\
            http://127.0.0.1:8000/api/import
"""

import argparse
import json

from loadtest import GROUP, build_payload


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("output", nargs="?", default="import.json")
    ap.add_argument("--groups", type=int, default=10000)
    args = ap.parse_args()
    payload = build_payload(args.groups)
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(payload, f)
    n_deps = sum(len(t["static_deps"]) + len(t["dynamic_deps"])
                 for t in payload["tasks"])
    print(f"wrote {args.output}: {len(payload['tasks'])} tasks, {n_deps} deps")


if __name__ == "__main__":
    main()
