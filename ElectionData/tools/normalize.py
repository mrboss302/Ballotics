#!/usr/bin/env python3
"""Normalize the pinned Ballotics inputs and emit deterministic research evidence."""
import argparse
from collections import Counter, defaultdict
import csv
from datetime import date, timedelta
import hashlib
import json
from pathlib import Path
import re
import unicodedata

ROOT = Path(__file__).resolve().parents[2]
TOOLS = Path(__file__).resolve().parent
YEARS = tuple(range(2000, 2025, 4))
SCHEMA = 1
VERSION = "0.1.0"
HISTORICAL_PARTIES = {
    (1864, "Abraham Lincoln"): ("national-union", "National Union"),
    (1912, "Theodore Roosevelt"): ("progressive-1912", "Progressive (1912)"),
    (1924, "Robert M. LaFollette"): ("progressive-1924", "Progressive (1924)"),
    (1948, "Henry Wallace"): ("progressive-1948", "Progressive (1948)"),
}
AGGREGATES = {"Other", "Others", "Various", "Various ***", "Not Voting"}
CT = dict(zip(("Fairfield", "Hartford", "Litchfield", "Middlesex", "New Haven",
               "New London", "Tolland", "Windham"),
              ("09001", "09003", "09005", "09007", "09009", "09011", "09013", "09015")))
# Census effective year and the 2024 geometry affected by a reporting boundary change.
WITHHOLD_BEFORE = {"08014": 2002, "08001": 2002, "08013": 2002, "08059": 2002,
                   "08123": 2002, "51019": 2014, "51005": 2002}
ALASKA_OVERSEAS = {
    2016: (342, "https://www.elections.alaska.gov/results/16GENR/data/sovc/hd99.pdf"),
    2020: (438, "https://www.elections.alaska.gov/results/20GENR/data/sovc/HD99p.pdf"),
    2024: (401, "https://www.elections.alaska.gov/results/24GENR/HD99.pdf"),
}
NON_GEOGRAPHIC = {"Statewide Writein", "Maine Uocava", "Federal Precinct"}
ALIASES = json.loads((TOOLS / "aliases.json").read_bytes())
CANDIDATES = json.loads((TOOLS / "candidate-ids.json").read_bytes())
REVIEW = json.loads((TOOLS / "historical-review.json").read_bytes())
COUNT_REVIEW = json.loads((TOOLS / "electoral-count-review.json").read_bytes())
CIVIL_WAR_NONPARTICIPANTS = {
    "Alabama", "Arkansas", "Florida", "Georgia", "Louisiana", "Mississippi",
    "North Carolina", "South Carolina", "Tennessee", "Texas", "Virginia",
}


def require(condition, reason):
    if not condition:
        raise ValueError(reason)


def digest(data):
    return hashlib.sha256(data).hexdigest()


def write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True) + "\n",
                    encoding="utf-8")
    return path


def checked_json(path):
    def unique(pairs):
        result = dict(pairs)
        require(len(result) == len(pairs), f"Duplicate object key in {path.name}")
        return result
    return json.loads(path.read_bytes(), object_pairs_hook=unique)


def stable(value):
    return re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")


def candidate(name):
    value = ALIASES.get(name.strip(), name.strip())
    if value in AGGREGATES or not value:
        return None
    require(value in CANDIDATES, f"Review candidate identity: {name!r}")
    return CANDIDATES[value]


def party(name):
    return None if name in {"Other", "Various"} else stable(name)


def snapshot(year):
    if year == 1788:
        return date(1789, 2, 4)
    start = date(min(year, 2000), 12 if year < 1848 else 11, 1 if year < 1848 else 2)
    return start + timedelta(days=((2 if year < 1848 else 1) - start.weekday()) % 7)


def electoral_allocation_label(label):
    match = re.fullmatch(r"(Maine|Nebraska) CD-([1-3])", label)
    if match:
        state, number = match.groups()
        return state, "congressionalDistrict", number
    match = re.fullmatch(r"(.+)-([2-4])", label)
    if match:
        state, number = match.groups()
        return state, "historicalSplit", number
    return label, "statewide", None


def geography_sources(root):
    sources = {}
    inputs = {}
    for key, filename, vintage in (
        ("historical-states", "US_HistStateTerr_Gen05.json", "dated records"),
        ("census-county-2024", "cb_2024_us_county_5m.json", "2024"),
        ("connecticut-traditional", "Counties_Connecticut.json", "traditional counties; source vintage unverified"),
        ("alaska-house-2024", "Legislative_Districts_Alaska.json", "2024"),
    ):
        source = root / filename
        inputs[key] = checked_json(source)["features"]
        sources[key] = {"id": key, "file": filename, "vintage": vintage, "sha256": digest(source.read_bytes())}
    return sources, inputs


def national_package(root, historical, state_codes, audit):
    national = checked_json(root / "PresElectionData.json")
    require(set(map(int, national)) == set(range(1788, 2025, 4)), "National timeline")
    by_year = defaultdict(list)
    with (root / "PresElectionStateData.csv").open(encoding="utf-8-sig", newline="") as source:
        for line, raw in enumerate(csv.DictReader(source), 2):
            by_year[int(raw["Year"])].append((line, raw))
    require(set(by_year) == set(range(1788, 2025, 4)), "Allocation timeline")
    elections = []
    names = {key: value for value, key in CANDIDATES.items()}
    party_names = {}
    for year, raw_rows in sorted(by_year.items()):
        day = snapshot(year).isoformat()
        day_number = int(day.replace("-", ""))
        active = defaultdict(list)
        for feature in historical:
            p = feature["properties"]
            if p["START_N"] <= day_number <= p["END_N"] and p["TERR_TYPE"] in {"State", "District of Columbia"}:
                active[p["NAME"]].append(str(p["ID_NUM"]))
        records = []
        notes = list(REVIEW.get(str(year), []))
        affiliations = {}
        for index, row in enumerate(national[str(year)]):
            cid = candidate(row["name"])
            pid = party(row["party"]) if cid else None
            party_name = row["party"]
            if (historical_party := HISTORICAL_PARTIES.get((year, row["name"]))) is not None:
                pid, party_name = historical_party
            if pid:
                party_names[pid] = party_name
            if cid:
                require(cid not in affiliations, f"Duplicate person in {year}")
                affiliations[cid] = pid
            popular = row.get("popularVote")
            if popular == 0:
                audit["ambiguousNationalPopularZeros"].append([year, row["name"]])
            records.append({"candidateID": cid, "name": row["name"], "partyID": pid,
                            "sourceParty": row["party"], "sourceStyle": row["partyColor"],
                            "kind": "candidate" if cid else "uncast" if row["name"] == "Not Voting" else "aggregate",
                            "electoralVotes": row["electoralVote"], "popularVotes": popular or None,
                            "sourcePopularVotes": popular, "winner": row["winner"], "sourceIndex": index})
        winners = [r["candidateID"] for r in records if r["winner"]]
        require(len(winners) == 1 and winners[0], f"Winner status in {year}")
        allocations = []
        for line, row in raw_rows:
            state, kind, part = electoral_allocation_label(row["State"])
            code = state_codes[state]
            archive_name = "District of Columbia" if code == "11" else state
            matches = active[archive_name]
            cid = candidate(row["Winner"])
            votes = int(row["ElectoralVote"])
            require(votes >= 0 and (votes == 0 or cid), f"Invalid allocation at line {line}")
            if row["Winner"] and row["Winner"] != names[cid]:
                audit["allocationAliases"].append([line, year, row["Winner"], names[cid]])
            if kind == "statewide" and state in {"Maine", "Nebraska"} and any(
                    r["State"].startswith(state + " CD-") for _, r in raw_rows):
                kind = "atLarge"
            confirmed_nonparticipant = (year == 1788 and state == "New York"
                                        or year == 1864 and state in CIVIL_WAR_NONPARTICIPANTS)
            status = ("nonparticipating" if confirmed_nonparticipant or row["PartyColor"] == "Vacant State"
                      else "unresolved" if votes == 0 else "reported")
            record = {"id": f"{year}:csv:{line}", "jurisdictionID": f"state:{code}",
                      "stateName": state, "sourceLabel": row["State"], "sourceName": row["Winner"],
                      "sourceStyle": row["PartyColor"], "candidateID": cid, "partyID": affiliations.get(cid),
                      "electoralVotes": votes, "kind": kind, "part": part, "status": status,
                      "sourceLine": line, "geometryKey": matches[0] if len(matches) == 1 else None}
            if len(matches) != 1 and status != "nonparticipating":
                audit["unmappedElectoralAllocations"].append(record)
            allocations.append(record)
        reference_differences = COUNT_REVIEW.get(str(year), {})
        for state, comparison in reference_differences.items():
            rows = [row for row in allocations if row["stateName"] == state]
            actual = sum(row["electoralVotes"] for row in rows)
            require(actual == comparison["csv"], f"Review evidence changed: {year} {state}")
            official = comparison["reference"]
            if year == 2024:
                audit["official2024Differences"].append([state, actual, official])
            label = "elector appointments" if comparison["basis"] == "electorAppointments" else "presidential EV"
            notes.append({"kind": "apportionment" if year == 2024 else "sourceDiscrepancy",
                          "jurisdiction": state, "votes": official,
                          "text": f"Archives count: {official} {label}. Ballotics CSV: {actual}.",
                          "source": f"https://www.archives.gov/electoral-college/{year}"})
            for row in rows:
                row["status"] = "unresolved"
        if year == 2016:
            for row in allocations:
                if row["stateName"] in {"Hawaii", "Washington"}:
                    row["status"] = "unresolved"
        stats = {"reported": national[str(year)][0]["evTotal"], "nationalRows": sum(r["electoralVotes"] for r in records),
                 "allocationRows": sum(r["electoralVotes"] for r in allocations)}
        plurality = Counter()
        for a in allocations:
            if a["candidateID"]:
                plurality[a["candidateID"]] += a["electoralVotes"]
        candidate_deltas = {r["candidateID"]: plurality.pop(r["candidateID"], 0) - r["electoralVotes"]
                            for r in records if r["candidateID"]}
        candidate_deltas.update(plurality)
        candidate_deltas = {key: value for key, value in candidate_deltas.items() if value}
        if len(set(stats.values())) > 1 or candidate_deltas:
            audit["electoralComparisons"].append({"year": year, **stats, "candidateAllocationDeltas": candidate_deltas})
        elections.append({"year": year, "id": f"presidential:{year}", "winnerID": winners[0],
                          "decision": "houseContingent" if year in (1800, 1824) else "electoralCollege",
                          "rules": "twoUndifferentiatedVotes" if year < 1804 else "separatePresidentialBallot",
                          "reportedMajority": national[str(year)][0]["evMajority"], "totals": stats,
                          "geography": {"sourceID": "historical-states", "snapshotDate": day,
                                        "status": "fallback" if year > 2000 else "dated"},
                          "results": records, "allocations": allocations, "reviewNotes": notes})
    return {"elections": elections, "candidates": [{"id": k, "name": v} for k, v in sorted(names.items())],
            "parties": [{"id": p, "name": name} for p, name in sorted(party_names.items())]}


def county_index(inputs, state_codes):
    index = {}
    features = []
    for source, rows in inputs.items():
        if source == "historical-states":
            continue
        for row in rows:
            p = row["properties"]
            if source == "census-county-2024":
                code, state = p["GEOID"], p["STATEFP"]
                if state not in state_codes.values() or state in {"02", "09"}:
                    continue
                if state == "11":
                    require(code == "11001", "Unexpected D.C. county equivalent")
                name, legal = p["NAME"], p["NAMELSAD"]
            elif source == "connecticut-traditional":
                state = "09"
                name = p["name"].removesuffix(" County")
                code, legal = CT[name], p["name"]
            else:
                state, code, name, legal = "02", p["GEOID"], p["NAME"], p["NAMELSAD"]
                require(p["LSY"] == "2024" and code == f"02{int(name):03}", "Alaska boundary identity")
            key = f"{source}:{code}"
            require(key not in index, f"Duplicate geographic ID {key}")
            index[key] = (state, name, legal)
            features.append({"type": "Feature", "geometry": row["geometry"], "properties":
                             {"GEOID": key, "SOURCE_GEOID": code, "STATEFP": state, "NAMELSAD": legal}})
    require(sum(k.startswith("connecticut-traditional:") for k in index) == 8, "CT polygons")
    require(sum(k.startswith("alaska-house-2024:") for k in index) == 40, "AK polygons")
    return index, features


def name_key(name):
    chars = unicodedata.normalize("NFKD", name.casefold())
    return "".join(c for c in chars if c.isalnum())


def county_join(year, state, name, code, index):
    if name in NON_GEOGRAPHIC:
        return None, "nonGeographic"
    if state == "02":
        district = re.fullmatch(r"District (\d{1,2})", name)
        require(district and code == f"02{int(district[1]):03}", f"Alaska record {name} / {code}")
        if int(district[1]) == 99:
            return None, "federalOverseasAbsentee" if year in ALASKA_OVERSEAS else "classificationUnresolved"
        key = f"alaska-house-2024:{code}"
        require(key in index, f"Alaska district {code}")
        return key, "matched" if year == 2024 else "fallback"
    if state == "09":
        require(CT[name] == code, f"Connecticut identity {name} / {code}")
        key = f"connecticut-traditional:{code}"
        require(key in index, f"Connecticut polygon {name}")
        return key, "traditional"
    # The two Jackson County election boards report separately. The county polygon covers both areas.
    if state == "29" and (name == "Kansas City" or code == "29095"):
        return None, "overlappingReportingJurisdiction"
    if year < WITHHOLD_BEFORE.get(code, 0):
        return None, "boundaryChange"
    canonical = "46102" if code == "46113" and name in {"Shannon", "Oglala Lakota"} else code
    key = f"census-county-2024:{canonical}"
    shape = index.get(key)
    if shape is None or shape[0] != state:
        return None, "missingGeometry"
    name_alias = {"Saint Louis": "St. Louis"}.get(name, name) if code == "27137" else name
    if name_key(name_alias) not in {name_key(n) for n in shape[1:]} and not (
            state == "11" and code == "11001" and name == "District Of Columbia"
            or code == "46113" and canonical == "46102"):
        return None, "nameConflict"
    return key, "matched" if year == 2024 else "fallback"


def county_package(year, states, codes, index, audit):
    results = []
    used = set()
    coverage = defaultdict(Counter)
    for state_name, rows in sorted(states.items()):
        state = codes[state_name]
        for name, raw in sorted(rows.items()):
            code = raw["fips"]
            require(isinstance(code, str) and re.fullmatch(r"[0-9]+", code), f"Reporting code {code}")
            key, match = county_join(year, state, name, code, index)
            require(key is None or key not in used, f"Duplicate join {year} {key}")
            if key:
                used.add(key)
            source = key.split(":")[0] if key else None
            votes = [{"candidateID": candidate(v["candidate"]), "name": v["candidate"],
                      "partyID": party(v["party"]) if candidate(v["candidate"]) else None,
                      "sourceParty": v["party"], "votes": v["votes"], "sourceShare": v["pct"],
                      "sourceRank": v["rank"], "sourceMargin": v["margin_pct"]} for v in raw["results"]]
            ids = [v["candidateID"] for v in votes if v["candidateID"]]
            require(len(ids) == len(set(ids)), f"Duplicate local candidate: {year} {name}")
            votes.sort(key=lambda v: (-v["votes"], v["name"]))
            total = raw["total_votes"]
            require(isinstance(total, int) and total >= 0 and all(isinstance(v["votes"], int) and v["votes"] >= 0
                    for v in votes), f"Invalid vote count: {year} {name}")
            stated = sum(v["votes"] for v in votes)
            reviewed_total, review_source = ALASKA_OVERSEAS[year] if match == "federalOverseasAbsentee" else (None, None)
            review = None
            if reviewed_total is not None:
                review = {"source": review_source, "reportedVotes": reviewed_total}
                audit["officialLocalComparisons"].append([year, state_name, name, total, reviewed_total, review_source])
            reconciles = stated == total and reviewed_total in (None, total)
            winner = None
            margin = None
            if stated != total:
                status = "unresolved"
                audit["countyVoteDifferences"].append([year, state_name, name, code, total, stated])
            elif not reconciles:
                status = "unresolved"
            elif total == 0:
                status = "noData"
            elif votes[0]["candidateID"] is None:
                status = "unresolved"
            elif len(votes) > 1 and votes[0]["votes"] == votes[1]["votes"]:
                status = "tied" if votes[1]["candidateID"] else "unresolved"
            else:
                status, winner = "reported", votes[0]["candidateID"]
                if len(votes) > 1 and votes[1]["candidateID"]:
                    margin = votes[0]["votes"] - votes[1]["votes"]
            for v in votes:
                v["share"] = v["votes"] / total if reconciles and total else None
            coverage[state_name][match] += 1
            if not key:
                audit["unmatchedCountyRecords"].append([year, state_name, name, code, total, match])
            if state in {"02", "09"}:
                audit["substitutionJoins"].append([year, state_name, name, code, total, key, match])
            kind = ("stateHouseDistrict" if state == "02" and source == "alaska-house-2024"
                    else "traditionalCounty" if source == "connecticut-traditional"
                    else "countyOrEquivalent" if source == "census-county-2024"
                    else match if match in {"nonGeographic", "federalOverseasAbsentee", "overlappingReportingJurisdiction"}
                    else "unclassified")
            if kind == "countyOrEquivalent":
                jurisdiction = key.split(":")[1]
            elif kind == "traditionalCounty":
                jurisdiction = code
            else:
                jurisdiction = f"{state}:{code}:{stable(name)}"
            results.append({"id": f"{year}:local:{state}:{code}:{stable(name)}",
                            "jurisdictionID": f"{kind}:{jurisdiction}" +
                            (f":{year}" if kind == "stateHouseDistrict" else ""),
                            "stateCode": state, "stateName": state_name, "name": name, "sourceCode": code,
                            "kind": kind, "geometryKey": key, "geographySourceID": source, "joinStatus": match,
                            "status": status, "winnerID": winner, "totalVotes": total, "listedVotes": stated,
                            "marginVotes": margin, "results": votes, "reportingReview": review})
    require(len({r["id"] for r in results}) == len(results), f"Duplicate county record in {year}")
    require(sum(coverage["Connecticut"][s] for s in ("traditional",)) == 8, f"Connecticut coverage {year}")
    require(coverage["Alaska"]["matched" if year == 2024 else "fallback"] == 40
            and coverage["Alaska"]["federalOverseasAbsentee" if year in ALASKA_OVERSEAS
                                   else "classificationUnresolved"] == 1, f"Alaska coverage {year}")
    audit["countyCoverage"][year] = {"states": coverage, "geometryWithoutResults": sorted(index.keys() - used)}
    return {"year": year, "localResults": results}


def build(root, out, evidence=None, geometry=False):
    audit = {key: [] for key in ("ambiguousNationalPopularZeros", "electoralComparisons", "allocationAliases",
                                "unmappedElectoralAllocations", "countyVoteDifferences", "unmatchedCountyRecords",
                                "substitutionJoins", "official2024Differences", "officialLocalComparisons")}
    audit["countyCoverage"] = {}
    sources, inputs = geography_sources(root)
    state_rows = inputs["census-county-2024"]
    codes = {p["STATE_NAME"]: p["STATEFP"] for f in state_rows if int((p := f["properties"])["STATEFP"]) <= 56}
    codes.update({"Washington, D.C.": "11", "District Of Columbia": "11"})
    national = national_package(root, inputs["historical-states"], codes, audit)
    index, features = county_index(inputs, codes)
    counties = checked_json(root / "pres_election_county_data.json")
    require(tuple(sorted(map(int, counties["years"]))) == YEARS, "County timeline")
    package = out / VERSION
    payloads = []
    for filename, value in [("national.json", national)] + [
        (f"county-{year}.json", county_package(year, counties["years"][str(year)], codes, index, audit)) for year in YEARS
    ]:
        path = write(package / filename, value)
        data = path.read_bytes()
        payloads.append({"file": filename, "sha256": digest(data), "bytes": len(data),
                         "records": len(value.get("elections", value.get("localResults", []))),
                         "year": value.get("year")})
    source_hashes = {s: digest((root / s).read_bytes()) for s in
                     ("PresElectionData.json", "PresElectionStateData.csv", "pres_election_county_data.json")}
    manifest = {"schemaVersion": SCHEMA, "datasetVersion": VERSION, "payloads": payloads,
                "inputHashes": source_hashes, "countySourceMetadata": counties["metadata"],
                "geographySources": sorted(sources.values(), key=lambda s: s["id"])}
    write(package / "manifest.json", manifest)
    if geometry:
        write(out / "county-geometry-2024.geojson", {"type": "FeatureCollection", "features": features})
    if evidence:
        write(evidence, audit)
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=ROOT)
    parser.add_argument("--output", type=Path, default=ROOT / "ElectionData/research-staging")
    parser.add_argument("--evidence", type=Path)
    parser.add_argument("--geometry", action="store_true")
    args = parser.parse_args()
    manifest = build(args.source, args.output, args.evidence, args.geometry)
    for item in manifest["payloads"]:
        print(item["file"], item["records"], item["sha256"])


if __name__ == "__main__":
    main()
