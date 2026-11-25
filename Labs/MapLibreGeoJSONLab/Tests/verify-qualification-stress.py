"""Extract and check the last 0.3Q stress session in a simulator's append-only lab log."""
import argparse
import collections
import gzip
import json
from pathlib import Path
import runpy

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("log", type=Path)
parser.add_argument("output", type=Path)
args = parser.parse_args()
lines = args.log.read_bytes().splitlines(keepends=True)
records = [json.loads(line) for line in lines]
start = max(i for i, row in enumerate(records) if row["event"] == "albers_stress_start")
first = max(i for i in range(start + 1) if records[i]["event"] == "app_init")
end = next(i for i in range(start, len(records))
           if records[i]["event"] in {"albers_stress_released", "app_init"})
if records[end]["event"] != "albers_stress_released":
    raise SystemExit("The stress process restarted before the release observation.")
rows = records[first:end + 1]
args.output.mkdir(parents=True, exist_ok=True)
raw = args.output / "stress.jsonl.gz"
raw.write_bytes(gzip.compress(b"".join(lines[first:end + 1]), mtime=0))
summary = runpy.run_path(str(Path(__file__).with_name("summarize-metal.py")))["stress"](raw)
events = collections.defaultdict(list)
active_bytes = None
layout_mismatches = []
for row in rows:
    event = row["event"]
    events[event].append(row)
    if event == "metal_mesh_ready":
        active_bytes = sum(row[key] for key in ("index_bytes", "vertex_bytes", "part_bytes", "boundary_bytes"))
    elif event == "metal_release":
        active_bytes = None
    elif event == "metal_layout" and row["mesh_bytes"] != active_bytes:
        layout_mismatches.append(row)
done = summary["terminal_event"]
before, after = summary["release_phase"]
drop = before["before_release_footprint_mib"] - after["after_release_footprint_mib"]
summary["release_drop_mib"] = drop
summary["max_observed_in_flight"] = max(row["commands_in_flight"] for row in
                                        events["metal_layout"] + events["metal_release"])
summary["assertions"] = {
    "duration": done["elapsed_ms"] >= 1_800_000,
    "years_sources_selections": done["years"] >= 100 and done["sources"] >= 50 and summary["selection_hits"] >= 100,
    "current_requests": done["mismatches"] == done["timeouts"] == summary["superseded_commits"] == summary["stale_style_events"] == 0,
    "complete_sources": all(row["ready"] for row in events["albers_stress_source"]),
    "current_mesh_on_layout": not layout_mismatches and summary["layout_changes"] >= 100,
    "year_phase_reuses_one_mesh": summary["mesh_publications_during_county_election_year_phase"] == 1,
    "bounded_cache": summary["cache_entries"]["max"] <= 2,
    "active_and_inactive": {row["active"] for row in events["metal_scene_activity"]} == {True, False},
    "reopened": any(row["ready"] for row in summary["reopen"]),
    "released": active_bytes is None and drop > 0 and summary["mesh_releases"] == summary["mesh_publications"],
    "no_fallback": not summary["fallbacks"],
}
(args.output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
if not all(summary["assertions"].values()):
    raise SystemExit("Failed checks: " + ", ".join(name for name, passed in summary["assertions"].items() if not passed))
print(f"Verified {done['elapsed_ms'] / 1000:.2f} s, {summary['layout_changes']} layouts, {drop:.2f} MiB released.")
