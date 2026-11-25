"""Summarize comparable simulator windows or the 30-minute Metal route."""

import argparse
import collections
import gzip
import json
import math
import statistics
from pathlib import Path


def read(path):
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt", encoding="utf-8") as source:
        return [json.loads(line) for line in source]


def field(rows, name):
    return [row[name] for row in rows if name in row]


def distribution(values):
    ordered = sorted(values)
    if not ordered:
        return {"count": 0}
    return {
        "count": len(ordered),
        "median": statistics.median(ordered),
        "p95": ordered[math.ceil(len(ordered) * 0.95) - 1],
        "p99": ordered[math.ceil(len(ordered) * 0.99) - 1],
        "max": ordered[-1],
    }


def stress(path):
    rows = read(path)
    groups = collections.defaultdict(list)
    for row in rows:
        groups[row["event"]].append(row)
    done = groups["albers_stress_done"][-1]
    end_of_year_phase = groups["albers_stress_year"][-1]["app_ms"]
    last_requested, last_committed = None, None
    superseded_commits, stale_style_events = 0, 0
    meshes_before_sources = [row for row in groups["metal_mesh_ready"] if row["app_ms"] <= end_of_year_phase]
    for row in rows:
        name = row["event"]
        if name == "election_request":
            last_requested = row["year"], row["mode"]
        elif name == "election_switch":
            last_committed = row["year"], row["mode"]
            superseded_commits += last_committed != last_requested
        elif name == "election_albers_paint":
            stale_style_events += (row["year"], row["mode"]) != last_committed
    submissions = collections.defaultdict(list)
    for row in groups["metal_appearance_submit"]:
        submissions[row["operation"]].append(row["elapsed_ms"])
    footprints = [(value, row["event"], row["app_ms"], key)
                  for row in rows for key, value in row.items()
                  if key.endswith("_footprint_mib") and not key.startswith("before")]
    return {
        "terminal_event": done,
        "election_requests": len(groups["election_request"]),
        "election_commits": len(groups["election_switch"]),
        "superseded_commits": superseded_commits,
        "stale_style_events": stale_style_events,
        "mesh_publications_during_county_election_year_phase": len(meshes_before_sources),
        "style_uploads_during_county_election_year_phase": sum(
            row["app_ms"] <= end_of_year_phase for row in groups["metal_style_upload"]
        ),
        "mesh_publications": len(groups["metal_mesh_ready"]),
        "mesh_cancellations": len(groups["metal_mesh_cancelled"]),
        "layout_changes": len(groups["metal_layout"]),
        "geographic_preparations": len(groups["metal_mesh_preparing"]),
        "published_vertex_buffers": sum(row.get("vertex_buffers", 0) for row in groups["metal_mesh_ready"]),
        "published_index_buffers": sum(row.get("index_buffers", 0) for row in groups["metal_mesh_ready"]),
        "allocated_style_and_color_buffers": sum(row.get("allocated_buffers", 0) for row in groups["metal_style_upload"]),
        "cache_entries": distribution(field(groups["albers_stress_checkpoint"], "geographic_cache_entries")),
        "appearance_outliers_over_one_second": [
            row for row in groups["metal_appearance_submit"] if row["elapsed_ms"] > 1000
        ],
        "fallbacks": groups["metal_fallback"],
        "selection_attempts": len(groups["election_albers_selection"]),
        "selection_hits": sum(row["hit"] for row in groups["election_albers_selection"]),
        "selection_ms": distribution(field(groups["election_albers_selection"], "elapsed_ms")),
        "warm_year_commit_ms": distribution(field(groups["election_switch"][1:], "elapsed_ms")),
        "cold_year_commit_ms": groups["election_switch"][0]["elapsed_ms"],
        "main_actor_queue_ms": distribution(field(groups["election_task_enter"], "queue_ms")),
        "year_observed_ready_ms": distribution(field(groups["albers_stress_year"], "li_ms")),
        "gpu_style_copy_ms": distribution(field(groups["metal_style_upload"], "copy_ms")),
        "gpu_style_prepare_ms": distribution(field(groups["metal_style_upload"], "prepare_ms")),
        "appearance_update_to_submit_ms": {name: distribution(times) for name, times in sorted(submissions.items())},
        "source_observed_ready_ms": distribution(field(groups["albers_stress_source"], "elapsed_ms")),
        "frame_transition_ms": distribution(field(groups["albers_camera_transition"], "elapsed_ms")),
        "steady_footprint_mib": distribution(row["stress_footprint_mib"] for row in groups["albers_stress_checkpoint"]
                                            if row["app_ms"] > 240_000),
        "reopen": groups["albers_stress_reopen"],
        "background_transitions": groups["metal_scene_activity"],
        "release_phase": groups["albers_stress_release_start"] + groups["albers_stress_released"],
        "mesh_releases": len(groups["metal_release"]),
        "releases_with_commands_in_flight": sum(row["commands_in_flight"] > 0 for row in groups["metal_release"]),
        "maximum_sampled_footprint": max(footprints),
    }


def reduced_motion(path):
    rows = read(path)
    start = max(i for i, row in enumerate(rows) if row["event"] == "app_init")
    session = rows[start:]
    transitions = [{"region": row["region"], "frames": row["frames"], "elapsed_ms": row["elapsed_ms"]}
                   for row in session if row["event"] == "albers_region_transition"]
    complete = any(row["event"] == "albers_region_route_done" for row in session)
    animated = sum(row["event"] == "albers_camera_transition" for row in session)
    return {"transitions": transitions, "animated_camera_transitions": animated, "complete": complete,
            "pass": complete and animated == 0 and field(transitions, "region") == ["alaska", "hawaii", "mainland"]
            and all(row["frames"] == 1 for row in transitions)}


def cancellation(path):
    requests = []
    publications = []
    rejected = []
    terminal = None
    for row in read(path):
        event = row["event"]
        if event == "historical_request":
            requests.append(row)
        elif event in {"metal_mesh_cancelled", "metal_mesh_superseded"}:
            rejected.append(row)
        elif event == "metal_mesh_ready" and requests:
            request = min(requests, key=lambda entry: abs(entry["app_ms"] - 1000 * row["revision"]))
            publications.append({"date": request["date"], "expected": requests[-1]["date"],
                                 "features": row["features"]})
        elif event == "metal_rapid_history_done":
            terminal = row
    return {"requested_dates": field(requests, "date"), "published_meshes": publications,
            "rejected_preparations": len(rejected), "terminal": terminal,
            "pass": terminal is not None and terminal["ready"] and terminal["actual"] == terminal["expected"]
            and bool(publications) and all(item["date"] == item["expected"] for item in publications)}


def comparison(directory):
    configurations = collections.defaultdict(list)
    inputs = {path.name.removesuffix(".gz"): path for path in directory.glob("*.jsonl.gz")}
    inputs.update({path.name: path for path in directory.glob("*.jsonl")})
    for filename, path in sorted(inputs.items()):
        config, separator, trial = Path(filename).stem.rpartition("-")
        if separator and trial.isdecimal():
            configurations[config].append((int(trial), read(path)))
    results = {}
    for config, trials in configurations.items():
        if not trials:
            continue
        metrics = collections.defaultdict(list)
        zooms = collections.defaultdict(list)
        for trial, rows in sorted(trials):
            first_events = {row["event"]: row for row in reversed(rows)}
            for event, key in [
                ("election_switch", "elapsed_ms"), ("albers_geometry_ready", "projection_ms"),
                ("geography_decode_validate", "decode_ms"), ("geography_decode_validate", "validation_ms"),
                ("albers_first_draw", "geometry_request_ms"), ("metal_geometry_submit", "geometry_request_ms"),
                ("metal_mesh_ready", "prepare_ms"), ("metal_mesh_ready", "tessellation_ms"),
                ("metal_mesh_ready", "upload_ms"), ("metal_pipeline", "elapsed_ms"),
                ("metal_pipeline", "library_ms"), ("metal_pipeline", "pipeline_ms"),
                ("metal_geometry_complete", "geometry_request_ms"),
                ("metal_first_submit", "app_ms"), ("metal_first_complete", "app_ms"),
                ("metal_first_complete", "gpu_ms"),
                ("metal_first_submit", "first_submit_footprint_mib"),
                ("maplibre_geometry_decode", "decode_ms"), ("first_useful_frame", "request_ms"),
                ("idle_with_geometry", "request_ms"),
            ]:
                if event in first_events and key in first_events[event]:
                    metrics[event + "." + key].append(first_events[event][key])
            first_draw = next((first_events[name] for name in
                               ("metal_first_submit", "albers_first_draw", "first_useful_frame")
                               if name in first_events), None)
            if first_draw and "election_request" in first_events:
                metrics["election_request_to_first_render_signal_ms"].append(
                    first_draw["app_ms"] - first_events["election_request"]["app_ms"]
                )
            windows = [row for row in rows if row["event"] in {"albers_navigation", "navigation"}]
            for i, row in enumerate(windows):
                zooms[str(row["zoom_multiplier"]) + (" repeated" if i == 5 else "")].append(row)
        timing_fields = [
            "camera_updates", "frames", "callback_fps", "p50_gap_ms", "p95_gap_ms", "p99_gap_ms",
            "worst_gap_ms", "gaps_over_25_ms", "p50_draw_ms", "p95_draw_ms", "p99_draw_ms",
            "p50_cpu_ms", "p95_cpu_ms", "p99_cpu_ms", "max_cpu_ms", "p50_encode_ms", "p95_encode_ms",
            "p50_drawable_ms", "p95_drawable_ms", "max_drawable_ms",
            "p50_gpu_ms", "p95_gpu_ms", "p99_gpu_ms", "max_gpu_ms", "gpu_samples",
            "in_flight_skips", "unavailable_drawables", "draw_calls_total", "submitted_indices_total",
            "referenced_vertices_total", "submitted_segments_total", "visible_feature_candidates_last_camera",
            "sampled_peak_footprint_mib", "presented_samples",
        ]
        results[config] = {
            "trials": len(trials),
            "initial_load": {name: distribution(values) for name, values in sorted(metrics.items())},
            "zoom_windows": {level: {key: distribution(field(rows, key)) for key in timing_fields if field(rows, key)}
                             for level, rows in zooms.items()},
        }
    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["stress", "comparison", "cancellation", "reduced-motion"])
    parser.add_argument("input", type=Path)
    args = parser.parse_args()
    study = {"stress": stress, "comparison": comparison, "cancellation": cancellation, "reduced-motion": reduced_motion}
    result = study[args.mode](args.input)
    print(json.dumps(result, indent=2))
    if "pass" in result and not result["pass"]:
        raise SystemExit(1)
