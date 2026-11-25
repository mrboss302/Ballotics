#!/usr/bin/env python3
"""Compare normalized election totals to saved National Archives HTML pages."""
import argparse
from collections import Counter
import html
import json
from pathlib import Path
import re

from normalize import digest, write

VOTE = re.compile(r"\d+")
ABBREVIATIONS = dict(zip(
    ("CT", "DE", "GA", "MD", "MA", "NH", "NJ", "PA", "SC", "VA"),
    ("Connecticut", "Delaware", "Georgia", "Maryland", "Massachusetts", "New Hampshire",
     "New Jersey", "Pennsylvania", "South Carolina", "Virginia")))


def clean(fragment):
    fragment = re.sub(r"<sup\b[^>]*>.*?</sup>", "", fragment, flags=re.I | re.S)
    return " ".join(html.unescape(re.sub(r"<[^>]*>", " ", fragment)).split())


def table_rows(table):
    table = re.sub(r"<!--.*?-->", "", table, flags=re.S)
    rows = re.findall(r"<tr\b[^>]*>(.*?)</tr>", table, flags=re.I | re.S)
    return [[(clean(body), attributes) for _, attributes, body in
             re.findall(r"<(td|th)\b([^>]*)>(.*?)</\1>", row, flags=re.I | re.S)] for row in rows]


def number(value):
    if value in ("", "-", "–", "—"):
        return 0
    match = VOTE.search(value)
    if match is None:
        raise ValueError(f"Unexpected vote cell: {value!r}")
    return int(match[0])


def parse_official(source, year, known_states):
    tables = re.findall(r"<table\b[^>]*>(.*?)</table>", source.decode(), flags=re.I | re.S)
    summary, raw_states = map(table_rows, tables[:2])
    electoral = next([v for v, _ in row[1:]] for row in summary if row and row[0][0].startswith("Electoral Vote"))
    totals = next(value for value in electoral if "Total/Majority" in value)
    total, majority = totals.split(":")[1].split("/")
    outcomes = next(v for v, _ in summary[0][1:])
    state_results = {}
    if year == 1788:
        names = [v for v, _ in raw_states[0][1:-1]]
        for idx, name in enumerate(names, 1):
            state_results[ABBREVIATIONS[name]] = {
                "appointed": number(raw_states[1][idx][0]),
                "votes": {row[0][0].split(",")[0]: value for row in raw_states[1:]
                          if len(row) >= len(names) + 2 and row[0][0].endswith("Esq")
                          and (value := number(row[idx][0]))},
            }
    else:
        header = next(i for i, row in enumerate(raw_states) if row and row[0][0].lower() == "state")
        column = raw_states[header][2][1]
        count = int(re.search(r'colspan="(\d+)"', column)[1])
        names = [v for v, _ in raw_states[header + 1][:count]]
        for row in raw_states[header + 2:]:
            if len(row) < count + 2:
                continue
            state = row[0][0].strip("* ")
            if state == "District of Columbia":
                state = "Washington, D.C."
            if state not in known_states:
                continue
            if state in state_results:
                raise ValueError(f"Duplicate Archives state: {year} {state}")
            counts = Counter()
            for name, (value, _) in zip(names, row[2:2 + count]):
                if value := number(value):
                    counts[name] += value
            state_results[state] = {"appointed": number(row[1][0]), "votes": dict(counts)}
    if not state_results:
        raise ValueError(f"No states parsed in {year}")
    return {"summaryFirstLabel": outcomes, "reportedTotal": number(total),
            "reportedMajority": number(majority), "stateRows": state_results,
            "sourceSHA256": digest(source)}


def compare(package, directory):
    report = []
    for election in package["elections"]:
        year = election["year"]
        archives_year = 1789 if year == 1788 else year
        known_states = {row["stateName"] for row in election["allocations"]}
        official = parse_official((directory / f"{archives_year}.html").read_bytes(), year, known_states)
        official["source"] = f"https://www.archives.gov/electoral-college/{archives_year}"
        ids = Counter()
        for row in election["allocations"]:
            ids[row["stateName"]] += row["electoralVotes"]
        official["csvStateCountDifferences"] = [
            {"state": state, "csv": ids[state], "reference": ref, "basis": basis}
            for state, result in official["stateRows"].items()
            for basis, ref in [("electorAppointments" if year < 1804 else "presidentialVotes",
                               result["appointed"] if year < 1804 else sum(result["votes"].values()))]
            if ref != ids[state]
        ]
        official["stateRowsMissingFromReferenceTable"] = sorted(
            state for state, votes in ids.items() if votes > 0 and state not in official["stateRows"])
        official["stateRowsCount"] = len(official["stateRows"])
        official["referencePresidentialVoteSum"] = sum(sum(row["votes"].values()) for row in official["stateRows"].values())
        official["referenceElectorAppointmentSum"] = sum(row["appointed"] for row in official["stateRows"].values())
        official["ballotics"] = election["totals"] | {"reportedMajority": election["reportedMajority"],
                                                     "winnerID": election["winnerID"]}
        del official["stateRows"]
        report.append({"year": year, **official})
    return {"note": "An Archives table can contain grouped or disputed votes, typographical issues or multiple total bases.",
            "elections": report}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("national", type=Path)
    parser.add_argument("archives_html", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    report = compare(json.loads(args.national.read_bytes()), args.archives_html)
    write(args.output, report)
    print(f"Compared {len(report['elections'])} elections against saved Archives HTML.")


if __name__ == "__main__":
    main()
