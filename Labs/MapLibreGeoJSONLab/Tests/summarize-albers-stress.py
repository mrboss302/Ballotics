"""Summarize a complete Albers stress log produced by benchmark-albers.sh."""

import collections
import json
import math
import statistics
import sys


def distribution(values):
    values = sorted(values)
    if not values:
        return {"count": 0}
    return {
        "count": len(values),
        "min": values[0],
        "median": statistics.median(values),
        "p95": values[math.ceil(0.95 * len(values)) - 1],
        "max": values[-1],
    }


def summarize(records):
    by_event = collections.defaultdict(list)
    last_request = None
    last_commit = None
    unmatched_commits = 0
    unmatched_paints = 0
    footprint_peaks = []
    for row in records:
        event = row["event"]
        by_event[event].append(row)
        if event == "election_request":
            last_request = (row["year"], row["mode"])
        elif event == "election_switch":
            last_commit = (row["year"], row["mode"])
            unmatched_commits += last_commit != last_request
        elif event == "election_albers_paint":
            unmatched_paints += (row["year"], row["mode"]) != last_commit
        for key, value in row.items():
            if key.endswith("_footprint_mib") and not key.startswith("before"):
                footprint_peaks.append((value, event, row["app_ms"], key))

    checkpoints = by_event["albers_stress_checkpoint"]
    selections = by_event["election_albers_selection"]
    commits = by_event["election_switch"]
    return {
        "terminal_event": by_event["albers_stress_done"][-1],
        "release_baseline": by_event["albers_stress_release_start"][-1],
        "release_end": by_event["albers_stress_released"][-1],
        "committed_election_requests": len(commits),
        "commits_inconsistent_with_latest_request": unmatched_commits,
        "paints_inconsistent_with_latest_commit": unmatched_paints,
        "request_to_commit_ms": distribution(row["elapsed_ms"] for row in commits),
        "year_request_to_ready_observation_ms": distribution(
            row["li_ms"] for row in by_event["albers_stress_year"]
        ),
        "election_paint_ms": distribution(row["elapsed_ms"] for row in by_event["election_albers_paint"]),
        "successful_selections": sum(row["hit"] for row in selections),
        "selection_ms": distribution(row["elapsed_ms"] for row in selections),
        "steady_footprint_mib_after_four_minutes": distribution(
            row["stress_footprint_mib"] for row in checkpoints
            if row["app_ms"] > 240_000 and row["sources"] == 50
        ),
        "sampled_peak": max(footprint_peaks),
        "navigation_transition_ms": distribution(row["elapsed_ms"] for row in by_event["albers_camera_transition"]),
    }


if __name__ == "__main__":
    with open(sys.argv[1], encoding="utf-8") as log:
        print(json.dumps(summarize(map(json.loads, log)), indent=2))
