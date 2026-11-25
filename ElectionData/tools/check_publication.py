#!/usr/bin/env python3
"""Check deterministic generation, source coverage, manifests and JSON Schema."""
from collections import Counter
import json
from pathlib import Path
import tempfile

from jsonschema import Draft202012Validator, FormatChecker

from normalize import ROOT, VERSION, YEARS, build, digest

SCHEMA = ROOT / "ElectionData/contracts/presidential-v1.schema.json"


def main():
    validator = Draft202012Validator(json.loads(SCHEMA.read_bytes()), format_checker=FormatChecker())
    validator.check_schema(validator.schema)
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        left, right = root / "first", root / "second"
        manifest = build(ROOT, left, root / "first.audit.json", geometry=True)
        assert manifest == build(ROOT, right, root / "second.audit.json", geometry=True)
        assert (root / "first.audit.json").read_bytes() == (root / "second.audit.json").read_bytes()
        assert (left / "county-geometry-2024.geojson").read_bytes() == (right / "county-geometry-2024.geojson").read_bytes()
        validator.validate(manifest)
        audit = json.loads((root / "first.audit.json").read_bytes())
        assert len(audit["official2024Differences"]) == 13
        assert len(audit["countyVoteDifferences"]) == 151
        assert sum(len(set(e[k] for k in ("reported", "nationalRows", "allocationRows"))) > 1
                   for e in audit["electoralComparisons"]) == 32
        county_counts = Counter()
        for entry in manifest["payloads"]:
            filename = Path(VERSION) / entry["file"]
            data = (left / filename).read_bytes()
            assert data == (right / filename).read_bytes()
            assert (digest(data), len(data)) == (entry["sha256"], entry["bytes"])
            value = json.loads(data)
            validator.validate(value)
            if entry["year"] is None:
                elections = {e["year"]: e for e in value["elections"]}
                assert sum(len(e["allocations"]) for e in elections.values()) == 3155
                assert sum(len(e["results"]) for e in elections.values()) == 173
                assert elections[1824]["winnerID"] == "john-quincy-adams"
                assert elections[1800]["rules"] == "twoUndifferentiatedVotes"
                assert len({r["partyID"] for year in (1912, 1924, 1948)
                            for r in elections[year]["results"] if (r["partyID"] or "").startswith("progressive-")}) == 3
                assert elections[1864]["results"][0]["partyID"] == "national-union"
                assert elections[2024]["geography"]["snapshotDate"] == "2000-11-07"
            else:
                year = entry["year"]
                county_counts[year] = len(value["localResults"])
                ct = [r for r in value["localResults"] if r["stateCode"] == "09" and r["geometryKey"]]
                ak = [r for r in value["localResults"] if r["stateCode"] == "02"]
                assert len(ct) == 8 and len(ak) == 41 and sum(r["geometryKey"] is not None for r in ak) == 40
                district99 = next(r for r in ak if r["sourceCode"] == "02099")
                assert district99["geometryKey"] is None
                if year >= 2016:
                    assert district99["kind"] == "federalOverseasAbsentee"
                if year == 2020:
                    assert district99["status"] == "unresolved" and district99["reportingReview"]["reportedVotes"] == 438
        assert county_counts == dict(zip(YEARS, (3152, 3153, 3153, 3153, 3153, 3155, 3154)))
    fixtures = ROOT / "ElectionData/fixtures"
    validator.validate(json.loads((fixtures / "synthetic-county-tie.json").read_bytes()))
    assert not validator.is_valid(json.loads((fixtures / "invalid-numeric-geographic-code.json").read_bytes()))
    print("PASS deterministic publication, Draft 2020-12 schema, checksums, historical cases, county joins and fixtures")


if __name__ == "__main__":
    main()
