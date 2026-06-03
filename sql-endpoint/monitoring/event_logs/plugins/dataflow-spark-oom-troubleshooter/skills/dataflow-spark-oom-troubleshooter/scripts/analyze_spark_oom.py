#!/usr/bin/env python3
"""Triage Spark OOM and executor-loss artifacts.

This script intentionally uses simple regex heuristics. It flags evidence for a
human/Codex diagnosis; it does not replace reading the plan and logs.
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path


SIZE_RE = r"([0-9]+(?:\.[0-9]+)?)\s*(B|KiB|MiB|GiB|TiB|KB|MB|GB|TB)"


def main() -> int:
    parser = argparse.ArgumentParser(description="Analyze Spark OOM/log/plan text files.")
    parser.add_argument("files", nargs="+", help="Plan, Spark UI export, or log text files")
    args = parser.parse_args()

    text_parts = []
    for file_name in args.files:
        path = Path(file_name)
        text_parts.append(path.read_text(encoding="utf-8", errors="replace"))
    text = "\n".join(text_parts)

    findings: list[str] = []
    recommendations: list[str] = []

    if re.search(r"\b(exit code 137|SIGKILL|OOMKilled)\b", text, re.IGNORECASE):
        findings.append("Container OOM signature found: exit 137/SIGKILL/OOMKilled.")
        recommendations.append("Inspect executor memoryOverhead and per-executor task concurrency; Kubernetes killed the container.")

    if "FetchFailedException" in text:
        findings.append("FetchFailedException found.")
        if "ExecutorDeadException" in text:
            findings.append("Fetch failure is tied to a dead executor; the failed fetch stage is probably a victim.")
            recommendations.append("Find the executor loss reason before treating the fetch stage as root cause.")

    if re.search(r"Java heap space|GC overhead limit exceeded", text):
        findings.append("JVM heap OOM/GC signature found.")
        recommendations.append("Compare GC time to task time and inspect heap-heavy operators such as aggregates and joins.")

    if re.search(r"Direct buffer memory|Netty|OutOfDirectMemory", text, re.IGNORECASE):
        findings.append("Direct/off-heap/network memory signature found.")
        recommendations.append("Increase overhead/off-heap or reduce concurrent shuffle fetch/write pressure.")

    broadcast_sizes = extract_broadcast_sizes(text)
    for size_text, bytes_value in broadcast_sizes:
        findings.append(f"Broadcast size reported: {size_text}.")
        if bytes_value >= 512 * 1024**2:
            recommendations.append("Large broadcast can be resident during downstream tasks; verify reuse and overhead sizing.")

    reused_count = len(re.findall(r"\bReusedExchange\b", text))
    broadcast_count = len(re.findall(r"\bBroadcastExchange\b", text))
    if broadcast_count:
        findings.append(f"BroadcastExchange count: {broadcast_count}; ReusedExchange count: {reused_count}.")
        if broadcast_count > 1 and reused_count == 0:
            recommendations.append("Multiple broadcast exchanges appear without reuse; inspect whether the same build side is materialized repeatedly.")

    if re.search(r"Generate.*explode\(arrays_zip", text, re.DOTALL):
        findings.append("Plan contains Generate explode(arrays_zip(...)).")
        recommendations.append("Consider exploding/filtering the original array of structs to reduce transient array materialization.")

    partition_counts = [int(value) for value in re.findall(r"(?:hashpartitioning|RoundRobinPartitioning)\([^)]*?,\s*(\d+)\)", text)]
    if partition_counts:
        unique_counts = ", ".join(str(value) for value in sorted(set(partition_counts)))
        findings.append(f"Shuffle partition counts seen in plan: {unique_counts}.")
        if min(partition_counts) <= 500:
            recommendations.append("500 or fewer shuffle partitions may create large per-task shuffle blocks for large intermediates.")

    if re.search(r"AQEShuffleRead[^\n]*coalesced", text):
        findings.append("AQE coalesced shuffle reads found.")
        recommendations.append("If tasks are too large, lower advisory partition size or disable AQE coalescing for a diagnostic run.")

    memory_overhead = re.search(r"spark\.executor\.memoryOverhead[\"'=:\s]+([0-9]+[gGmMkK]?)", text)
    if memory_overhead:
        findings.append(f"Executor memoryOverhead setting found: {memory_overhead.group(1)}.")

    if not findings:
        findings.append("No strong Spark OOM signatures were detected by regex. Read the original artifacts manually.")

    print("Findings")
    for item in dedupe(findings):
        print(f"- {item}")

    if recommendations:
        print("\nTriage recommendations")
        for item in dedupe(recommendations):
            print(f"- {item}")

    return 0


def extract_broadcast_sizes(text: str) -> list[tuple[str, float]]:
    results: list[tuple[str, float]] = []
    for match in re.finditer(r"BroadcastQueryStage.*?Statistics\(sizeInBytes=" + SIZE_RE, text, re.DOTALL):
        value = float(match.group(1))
        unit = match.group(2)
        results.append((f"{value:g} {unit}", to_bytes(value, unit)))
    return results


def to_bytes(value: float, unit: str) -> float:
    normalized = unit.upper()
    factors = {
        "B": 1,
        "KIB": 1024,
        "MIB": 1024**2,
        "GIB": 1024**3,
        "TIB": 1024**4,
        "KB": 1000,
        "MB": 1000**2,
        "GB": 1000**3,
        "TB": 1000**4,
    }
    return value * factors[normalized]


def dedupe(values: list[str]) -> list[str]:
    seen = set()
    output = []
    for value in values:
        if value not in seen:
            seen.add(value)
            output.append(value)
    return output


if __name__ == "__main__":
    raise SystemExit(main())
