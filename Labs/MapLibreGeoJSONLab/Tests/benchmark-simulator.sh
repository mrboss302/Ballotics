#!/bin/bash
set -euo pipefail

if [[ $# -lt 3 || $# -gt 4 ]]; then
    echo "Usage: $0 BOOTED_SIMULATOR_ID RELEASE_APP_BUNDLE OUTPUT_DIRECTORY [HISTORICAL_GEOJSON]" >&2
    exit 2
fi
device=$1
app=$2
output=$3
historical=${4:-}
bundle=org.ballotics.MapLibreGeoJSONLab

mkdir -p "$output"
xcrun simctl terminate "$device" "$bundle" 2>/dev/null || true
xcrun simctl install "$device" "$app"
data=$(xcrun simctl get_app_container "$device" "$bundle" data)
log="$data/Library/Caches/benchmark.jsonl"
if [[ -n "$historical" ]]; then
    mkdir -p "$data/Documents"
    cp "$historical" "$data/Documents/US_HistCounties_Gen05.json"
fi

run_case() {
    local name=$1 marker=$2
    shift 2
    xcrun simctl terminate "$device" "$bundle" 2>/dev/null || true
    rm -f "$log"
    xcrun simctl launch "$device" "$bundle" --measure "$@"
    for ((second=0; second<80; second++)); do
        if [[ -f "$log" ]] && /usr/bin/grep -q "\"event\":\"$marker\"" "$log"; then
            cp "$log" "$output/$name.jsonl"
            echo "Captured $name"
            return 0
        fi
        sleep 1
    done
    if [[ -f "$log" ]]; then
        cp "$log" "$output/$name-incomplete.jsonl"
    fi
    echo "Timed out waiting for $marker ($name)" >&2
    return 1
}

if [[ "${BALLOTICS_SOURCE_SWEEP:-0}" == 1 ]]; then
    if [[ -z "$historical" ]]; then
        echo "Set the historical GeoJSON path for the source option sweep." >&2
        exit 2
    fi
    for profile in defaults narrow-buffer coarse-tolerance cap-zoom; do
        run_case "historical-option-$profile" navigation_benchmark_done \
            --historical --navigation-benchmark --source-option "$profile"
    done
    exit 0
fi

for dataset in districts counties states; do
    for repetition in 1 2 3; do
        run_case "$dataset-load-$repetition" idle_with_geometry --dataset "$dataset"
    done
    run_case "$dataset-navigation" navigation_benchmark_done --dataset "$dataset" --navigation-benchmark
done
if [[ -n "$historical" ]]; then
    for repetition in 1 2 3; do
        run_case "historical-load-$repetition" idle_with_geometry --historical
    done
    run_case historical-navigation navigation_benchmark_done --historical --navigation-benchmark
fi
run_case switches switch_benchmark_done --switch-benchmark
