#!/bin/bash
set -euo pipefail

if [[ $# != 5 ]]; then
    echo "Usage: $0 SIMULATOR_ID RELEASE_APP ARCHIVE PUBLISHED_SNAPSHOT_DIRECTORY OUTPUT_DIRECTORY" >&2
    exit 2
fi

device=$1
app=$2
archive=$3
published=$4
output=$5
bundle=org.ballotics.MapLibreGeoJSONLab
mkdir -p "$output"
xcrun simctl terminate "$device" "$bundle" 2>/dev/null || true
xcrun simctl install "$device" "$app"
container=$(xcrun simctl get_app_container "$device" "$bundle" data)
mkdir -p "$container/Documents"
cp "$archive" "$container/Documents/US_HistCounties_Gen05.json"
for file in "$published"/historical-*.geojson "$published"/historical-*.manifest.json; do
    cp "$file" "$container/Documents/"
done
log="$container/Library/Caches/benchmark.jsonl"

run_case() {
    local name=$1 marker=$2
    shift 2
    xcrun simctl terminate "$device" "$bundle" 2>/dev/null || true
    rm -f "$log"
    xcrun simctl launch "$device" "$bundle" --measure --historical-matched "$@"
    for ((second=0; second<70; second++)); do
        if [[ -f "$log" ]] && grep -Fq "\"event\":\"$marker\"" "$log"; then
            cp "$log" "$output/$name.jsonl"
            echo "Captured $name"
            return 0
        fi
        sleep 1
    done
    if [[ -f "$log" ]]; then cp "$log" "$output/$name-incomplete.jsonl"; fi
    echo "Timed out waiting for $marker ($name)" >&2
    return 1
}

for profile in defaults narrow-buffer; do
    run_case "all-$profile" navigation_benchmark_done --historical --navigation-benchmark --source-option "$profile"
done
for year in 1788 1800 1860 1900 1950 1980 2000; do
    for profile in defaults narrow-buffer; do
        run_case "runtime-$year-$profile" navigation_benchmark_done \
            --historical-date "$year-01-01" --navigation-benchmark --source-option "$profile"
    done
done
for year in 1788 2000; do
    run_case "published-$year-defaults" navigation_benchmark_done \
        --published-date "$year-01-01" --navigation-benchmark
done
for profile in defaults narrow-buffer; do
    run_case "switches-$profile" historical_switches_done \
        --historical-date 1788-01-01 --historical-date-switches --source-option "$profile"
done
