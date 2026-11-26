#!/bin/bash
set -euo pipefail
lab=$(cd "$(dirname "$0")/.." && pwd)
root=$(cd "$lab/../.." && pwd)
archive=${1:?Specify the local historical county archive}
derivative=${2:?Specify the built county geometry}
output=${3:?Specify a results directory}
mkdir -p "$output"
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
sources=("$root"/Packages/BalloticsGeoKit/Sources/BalloticsGeoKit/*.swift "$lab/GeoJSON.swift" "$lab/HistoricalArchive.swift" "$lab/PresidentialElection.swift")
for suite in AlbersQualification HistoricalArchiveChecks PresidentialElectionChecks; do
    xcrun swiftc -O -package-name BalloticsGeoKit -module-cache-path "$work/modules" "${sources[@]}" "$lab/Tests/$suite.swift" -o "$work/$suite"
    case "$suite" in
        AlbersQualification) args=("$root" "$archive" "$lab/Tests/Fixtures/albers-proj-wgs84.json" "$derivative");;
        HistoricalArchiveChecks) args=("$archive");;
        PresidentialElectionChecks) args=("$root/US_HistStateTerr_Gen05.json");;
    esac
    "$work/$suite" "${args[@]}" > "$output/$suite.txt"
done
