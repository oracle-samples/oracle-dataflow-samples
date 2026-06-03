#!/usr/bin/env python3
"""Repair Spark event-log data before Spark History Server loads it.

This incorporates the useful behavior from the local fix/sanitize scripts:
- keep valid Spark event JSON lines only
- stop at the first corrupt Spark event record
- truncate very large string fields that can break Spark History Server parsing
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


def sanitize(value: Any, max_string_chars: int, stats: dict[str, Any]) -> Any:
    if isinstance(value, dict):
        repaired = {}
        for key, child in value.items():
            if key == "Event":
                repaired[key] = child
            else:
                repaired[key] = sanitize(child, max_string_chars, stats)
        return repaired
    if isinstance(value, list):
        return [sanitize(child, max_string_chars, stats) for child in value]
    if isinstance(value, str) and len(value) > max_string_chars:
        stats["truncated_strings"] += 1
        stats["truncated_chars"] += len(value) - max_string_chars
        return f"[TRUNCATED {len(value)} chars]"
    return value


def extract_event_line(raw_line: str, wrapped: str, line_number: int, stats: dict[str, Any]) -> str | None:
    line = raw_line.strip()
    if not line:
        return None

    use_wrapper = wrapped == "true"
    if wrapped == "auto":
        use_wrapper = '"log"' in line and not line.startswith('{"Event":')

    if not use_wrapper:
        if not line.startswith('{"Event":'):
            stats["skipped_non_spark_lines"] += 1
            return None
        return line

    try:
        envelope = json.loads(line)
    except json.JSONDecodeError as exc:
        stats.update(
            {
                "corruption_detected": True,
                "stopped_at_line": line_number,
                "corruption_message": f"invalid wrapper JSON: {exc.msg}",
            }
        )
        return None

    if isinstance(envelope, dict) and "log" in envelope:
        event_line = str(envelope["log"]).strip()
    else:
        event_line = json.dumps(envelope, separators=(",", ":"), ensure_ascii=False)

    if not event_line.startswith('{"Event":'):
        stats["skipped_non_spark_lines"] += 1
        return None
    return event_line


def repair_file(input_path: Path, wrapped: str, max_string_chars: int) -> dict[str, Any]:
    stats: dict[str, Any] = {
        "source_file": str(input_path),
        "wrapped": wrapped,
        "max_string_chars": max_string_chars,
        "input_lines": 0,
        "spark_candidate_lines": 0,
        "output_events": 0,
        "skipped_non_spark_lines": 0,
        "corruption_detected": False,
        "stopped_at_line": None,
        "corruption_message": "",
        "truncated_strings": 0,
        "truncated_chars": 0,
    }

    with input_path.open("r", encoding="utf-8", errors="replace") as handle:
        for line_number, raw_line in enumerate(handle, start=1):
            stats["input_lines"] = line_number
            event_line = extract_event_line(raw_line, wrapped, line_number, stats)
            if stats["corruption_detected"]:
                break
            if event_line is None:
                continue

            stats["spark_candidate_lines"] += 1
            try:
                event = json.loads(event_line)
            except json.JSONDecodeError as exc:
                stats.update(
                    {
                        "corruption_detected": True,
                        "stopped_at_line": line_number,
                        "corruption_message": f"invalid Spark event JSON: {exc.msg}",
                    }
                )
                break

            if not isinstance(event, dict) or "Event" not in event:
                stats["skipped_non_spark_lines"] += 1
                continue

            repaired = sanitize(event, max_string_chars, stats)
            print(json.dumps(repaired, separators=(",", ":"), ensure_ascii=False))
            stats["output_events"] += 1

    return stats


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Repair Spark event-log JSON lines for SHS.")
    parser.add_argument("--input", required=True, help="Decompressed event-log file to repair")
    parser.add_argument(
        "--wrapped",
        choices=("auto", "true", "false"),
        default="auto",
        help="Whether input lines are Fluent Bit JSON envelopes",
    )
    parser.add_argument(
        "--max-string-chars",
        type=int,
        default=18_000_000,
        help="Replace strings longer than this many characters with a truncation marker",
    )
    parser.add_argument("--stats-file", help="Optional JSON stats output path")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    stats = repair_file(Path(args.input), args.wrapped, args.max_string_chars)

    if args.stats_file:
        stats_path = Path(args.stats_file)
        stats_path.parent.mkdir(parents=True, exist_ok=True)
        stats_path.write_text(json.dumps(stats, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    if stats["corruption_detected"]:
        print(
            "[repair] stopped at line "
            f"{stats['stopped_at_line']}: {stats['corruption_message']}",
            file=sys.stderr,
        )
    if stats["truncated_strings"]:
        print(
            "[repair] truncated "
            f"{stats['truncated_strings']} oversized string field(s)",
            file=sys.stderr,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
