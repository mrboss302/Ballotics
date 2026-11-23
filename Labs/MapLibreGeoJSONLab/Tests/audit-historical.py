#!/usr/bin/env python3
"""Audit the preserved AHCB FeatureCollection with an independent decoder."""
import argparse
import collections
import datetime
import hashlib
import json
import pathlib
import re


def day(timestamp):
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}T00:00:00\.000Z", timestamp):
        raise ValueError(f"Unexpected timestamp {timestamp!r}")
    return datetime.date.fromisoformat(timestamp[:10])


def audit(archive, published):
    raw = archive.read_bytes()
    text = raw.decode("utf-8")
    first = re.search(r'"features"\s*:\s*\[', text)
    if first is None:
        raise ValueError("Missing features array")
    cursor = first.end()
    decoder = json.JSONDecoder()
    versions = collections.defaultdict(list)
    same_shape = collections.defaultdict(list)
    geometry_types = collections.Counter()
    events = collections.defaultdict(lambda: [0, 0])
    required = collections.Counter()
    features = []
    one_day = datetime.timedelta(days=1)
    while True:
        while text[cursor].isspace() or text[cursor] == ",":
            cursor += 1
        if text[cursor] == "]":
            break
        feature, end = decoder.raw_decode(text, cursor)
        features.append(feature)
        feature_bytes = len(text[cursor:end].encode("utf-8"))
        cursor = end
        p = feature["properties"]
        required.update(p.keys())
        start, stop = day(p["START_DATE"]), day(p["END_DATE"])
        assert start <= stop
        assert start.year * 10000 + start.month * 100 + start.day == p["START_N"]
        assert stop.year * 10000 + stop.month * 100 + stop.day == p["END_N"]
        versions[p["ID"]].append((start, stop, p))
        geom = feature["geometry"]
        geometry_types[geom["type"]] += 1
        shape_hash = hashlib.sha256(json.dumps(geom, sort_keys=True, separators=(",", ":")).encode()).digest()
        same_shape[shape_hash].append((start, stop, p["ID_NUM"]))
        events[start][0] += 1
        events[start][1] += feature_bytes
        events[stop + one_day][0] -= 1
        events[stop + one_day][1] -= feature_bytes

    steps = collections.Counter()
    for rows in versions.values():
        ordered = sorted(rows, key=lambda row: row[0])
        for predecessor, successor in zip(ordered, ordered[1:]):
            steps[(successor[0] - predecessor[1]).days] += 1
    exact_duplicate_pairs = sum(
        s1 <= e2 and s2 <= e1 for rows in same_shape.values()
        for idx, (s1, e1, _) in enumerate(rows) for s2, e2, _ in rows[idx + 1:]
    )
    count = size = interval_count = interval_bytes = 0
    wrapper_bytes = len(b'{"type":"FeatureCollection","features":[]}')
    for _, (change_count, change_bytes) in sorted(events.items()):
        count += change_count
        size += change_bytes
        if count:
            interval_count += 1
            interval_bytes += size + count - 1 + wrapper_bytes
    properties = [f["properties"] for f in features]

    stats = {
        "source_sha256": hashlib.sha256(raw).hexdigest(),
        "earliest_start": min(events).isoformat(),
        "latest_end": (max(events) - one_day).isoformat(),
        "records": len(features),
        "entities": len(versions),
        "record_ids": len({p["ID_NUM"] for p in properties}),
        "entities_with_multiple_versions": sum(len(rows) > 1 for rows in versions.values()),
        "entity_version_pairs": len({(p["ID"], p["VERSION"]) for p in properties}),
        "identical_geometries_across_all_dates": len(features) - len(same_shape),
        "exact_geometry_pairs_active_on_a_shared_day": exact_duplicate_pairs,
        "geometries": geometry_types,
        "property_presence": required,
        "adjacent_versions": steps[1],
        "gaps_between_versions": sum(n for delta, n in steps.items() if delta > 1),
        "overlapping_versions_of_one_entity": sum(n for delta, n in steps.items() if delta <= 0),
        "one_day_versions": sum(p["START_N"] == p["END_N"] for p in properties),
        "entities_with_name_changes": sum(len({p["NAME"] for _, _, p in rows}) > 1 for rows in versions.values()),
        "entities_with_jurisdiction_changes": sum(len({p["STATE_TERR"] for _, _, p in rows}) > 1 for rows in versions.values()),
        "entities_with_fips_value_changes": sum(len({p["FIPS"] for _, _, p in rows}) > 1 for rows in versions.values()),
        "cross_reference_values": len({p["CROSS_REF"] for p in properties}),
        "blank_fips": sum(not p["FIPS"] for p in properties),
        "active_set_intervals": interval_count,
        "all_intervals_materialized_geojson_bytes": interval_bytes,
        "published_snapshots_verified": [],
    }
    if published:
        for path in sorted(published.glob("historical-*.geojson")):
            date = datetime.date.fromisoformat(path.stem.removeprefix("historical-"))
            number = date.year * 10000 + date.month * 100 + date.day
            data = path.read_bytes()
            expected = [f for f in features if f["properties"]["START_N"] <= number <= f["properties"]["END_N"]]
            assert json.loads(data)["features"] == expected
            manifest = json.loads(path.with_suffix(".manifest.json").read_bytes())
            assert manifest["sourceSHA256"] == stats["source_sha256"]
            assert hashlib.sha256(data).hexdigest() == manifest["outputSHA256"]
            assert manifest["features"] == len(expected) and manifest["bytes"] == len(data)
            stats["published_snapshots_verified"].append(date.isoformat())
    return stats


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("archive", type=pathlib.Path)
    parser.add_argument("--published", type=pathlib.Path)
    args = parser.parse_args()
    print(json.dumps(audit(args.archive, args.published), indent=2, sort_keys=True))
