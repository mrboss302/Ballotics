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
    local deadline=$((SECONDS + timeout))
    until [[ -e "$log" ]] && grep -q "\"event\":\"$marker\"" "$log"; do
        if (( SECONDS >= deadline )); then
            if [[ -e "$log" ]]; then cp "$log" "$output/$name-incomplete.jsonl"; fi
            echo "Timed out waiting for $marker ($name)" >&2
            return 1
        fi
        sleep 0.25
    done
    cp "$log" "$output/$name.jsonl"
    echo "Captured $name"
}

case ${METAL_BENCHMARK_MODE:-comparison} in
    stress)
        seconds=${METAL_STRESS_SECONDS:-1800}
        run stress albers_stress_released "$((seconds + 180))" \
            --albers-metal --albers-stress --albers-seconds "$seconds"
        ;;
    cancellation)
        run cancellation metal_rapid_history_done 70 --albers-metal --historical-date 1788-01-01 --historical-rapid-switches
        python3 "$(dirname "$0")/summarize-metal.py" cancellation "$output/cancellation.jsonl" > "$output/cancellation-summary.json"
        ;;
    comparison)
        election=(--election-year 2024 --county-results)
        for trial in $(seq 1 "${METAL_TRIALS:-3}"); do
            run "cg-$trial" albers_route_done 100 "${election[@]}" --albers-route
            run "metal-fill-$trial" albers_route_done 100 "${election[@]}" --albers-metal --albers-route --metal-fill-only --metal-samples-1
            run "metal-stroke-$trial" albers_route_done 100 "${election[@]}" --albers-metal --albers-route --metal-samples-1
            run "metal-msaa-$trial" albers_route_done 100 "${election[@]}" --albers-metal --albers-route
            run "maplibre-$trial" navigation_benchmark_done 100 "${election[@]}" --election-maplibre --metal-matched-route
        done
        ;;
    visual)
        run counties-cg albers_first_draw 45 --albers --dataset counties
        sleep 1
        xcrun simctl io "$device" screenshot "$output/counties-cg.png"
        run counties-metal metal_first_complete 45 --albers-metal --dataset counties
        sleep 1
        xcrun simctl io "$device" screenshot "$output/counties-metal.png"
        run counties-metal-1x metal_first_complete 45 --albers-metal --dataset counties --metal-samples-1
        sleep 1
        xcrun simctl io "$device" screenshot "$output/counties-metal-1x.png"
        ;;
    *)
        echo "Choose comparison, stress, cancellation, or visual." >&2
        exit 2
        ;;
esac
