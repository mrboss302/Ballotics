#!/bin/bash
set -euo pipefail

if [[ $# -lt 3 || $# -gt 4 ]]; then
    echo "Usage: bash $0 BOOTED_SIMULATOR_ID RELEASE_APP_BUNDLE OUTPUT_DIRECTORY [HISTORICAL_COUNTY_ARCHIVE]" >&2
    exit 2
fi

device=$1
app=$2
output=$3
county_archive=${4:-}
bundle=org.ballotics.MapLibreGeoJSONLab
mkdir -p "$output"
xcrun simctl terminate "$device" "$bundle" 2>/dev/null || true
xcrun simctl install "$device" "$app"
data=$(xcrun simctl get_app_container "$device" "$bundle" data)
log="$data/Library/Caches/benchmark.jsonl"
mkdir -p "$data/Documents" "$(dirname "$log")"
if [[ -n "$county_archive" && ! -e "$data/Documents/US_HistCounties_Gen05.json" ]]; then
    cp "$county_archive" "$data/Documents/US_HistCounties_Gen05.json"
fi

run() {
    local name=$1 marker=$2 timeout=$3
    shift 3
    xcrun simctl terminate "$device" "$bundle" 2>/dev/null || true
    rm -f "$log"
    xcrun simctl launch "$device" "$bundle" --measure "$@"
    local tick
    for ((tick=0; tick<timeout*4; tick++)); do
        if [[ -f "$log" ]] && grep -q "\"event\":\"$marker\"" "$log"; then
            cp "$log" "$output/$name.jsonl"
            echo "Captured $name"
            return
        fi
        sleep 0.25
    done
    if [[ -f "$log" ]]; then cp "$log" "$output/$name-incomplete.jsonl"; fi
    echo "Timed out waiting for $marker ($name)" >&2
    return 1
}

if [[ ${ALBERS_STRESS_ONLY:-0} == 1 ]]; then
    seconds=${ALBERS_STRESS_SECONDS:-1800}
    run stress albers_stress_released "$((seconds + 100))" --albers --albers-stress --albers-seconds "$seconds"
    exit
fi

election=(--election-year 2024 --county-results)
run prime albers_first_draw 90 "${election[@]}"
for trial in 1 2 3; do
    run "linear-launch-$trial" albers_first_draw 45 "${election[@]}" --albers-baseline
    run "indexed-launch-$trial" albers_first_draw 45 "${election[@]}"
    run "maplibre-launch-$trial" idle_with_geometry 45 "${election[@]}" --election-maplibre
done

run linear-navigation albers_route_done 85 "${election[@]}" --albers-baseline --albers-route
run indexed-navigation albers_route_done 85 "${election[@]}" --albers-route
run maplibre-navigation navigation_benchmark_done 95 "${election[@]}" --election-maplibre --navigation-benchmark
run linear-year-switches albers_stress_released 75 --albers --albers-baseline --albers-stress --albers-seconds 45
run indexed-year-switches albers_stress_released 75 --albers --albers-stress --albers-seconds 45

if [[ -n "$county_archive" ]]; then
    for day in 1788-01-01 1860-01-01 1900-01-01 1980-01-01 2000-01-01; do
        run "historical-$day" albers_first_draw 90 --albers --historical-date "$day"
    done
fi

for geography in states districts counties; do
    run "$geography" albers_first_draw 45 --albers --dataset "$geography"
done
