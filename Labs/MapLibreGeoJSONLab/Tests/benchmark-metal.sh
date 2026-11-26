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
    ownership)
        run import-supersession metal_import_supersession_done 60 --albers-metal --dataset districts --metal-import-supersession
        ;;
    regions)
        for config in cg research thematic; do
            case "$config" in
                cg) flags=(--albers) ;;
                research) flags=(--albers-metal --equal-scale) ;;
                thematic) flags=(--albers-metal) ;;
            esac
            run "regions-$config" albers_region_route_done 60 --election-year 2024 --county-results --albers-region-route "${flags[@]}"
        done
        ;;
    qualification)
        read -r -a configurations <<< "${METAL_CONFIGS:-cg research thematic maplibre}"
        workloads=(election-counties census-counties states districts counties-1788 counties-2000 electoral-states)
        if [[ -n ${METAL_WORKLOAD:-} ]]; then workloads=("$METAL_WORKLOAD"); fi
        for trial in $(seq 1 "${METAL_TRIALS:-1}"); do
            for workload in "${workloads[@]}"; do
                case "$workload" in
                    election-counties) args=(--election-year 2024 --county-results) ;;
                    census-counties) args=(--dataset counties) ;;
                    states|districts) args=(--dataset "$workload") ;;
                    counties-1788) args=(--historical-date 1788-01-01) ;;
                    counties-2000) args=(--historical-date 2000-01-01) ;;
                    states-1788) args=(--election-year 1788) ;;
                    states-2000) args=(--election-year 2000) ;;
                    electoral-states) args=(--election-year 2024) ;;
                    *) echo "Unknown workload: $workload" >&2; exit 2 ;;
                esac
                for config in "${configurations[@]}"; do
                    marker=albers_route_done
                    case "$config" in
                        cg) flags=(--albers --albers-route) ;;
                        research) flags=(--albers-metal --albers-route --equal-scale) ;;
                        thematic) flags=(--albers-metal --albers-route) ;;
                        maplibre) flags=(--election-maplibre --metal-matched-route); marker=navigation_benchmark_done ;;
                        *) echo "Unknown configuration: $config" >&2; exit 2 ;;
                    esac
                    run "$workload-$config-$trial" "$marker" 120 "${args[@]}" "${flags[@]}"
                done
            done
        done
        ;;
    label-overlay)
        run labels albers_route_done 120 --election-year 2024 --albers-metal --albers-route --label-metrics
        run focus albers_region_route_done 60 --election-year 1976 --albers-metal --albers-region-route --label-metrics
        ;;
    quality-visual)
        run counties-cg albers_first_draw 45 --albers --dataset counties
        sleep 1
        xcrun simctl io "$device" screenshot "$output/counties-cg.png"
        for samples in 1 2 4; do
            name="counties-metal-${samples}x"
            run "$name" 'metal_\(first_complete\|fallback\)' 45 --albers-metal --dataset counties "--metal-samples-$samples"
            if grep -q '"event":"metal_fallback"' "$output/$name.jsonl"; then continue; fi
            sleep 1
            xcrun simctl io "$device" screenshot "$output/$name.png"
        done
        run research-counties metal_first_complete 45 --albers-metal --equal-scale --dataset counties
        sleep 1
        xcrun simctl io "$device" screenshot "$output/research-counties.png"
        ;;
    quality)
        for trial in $(seq 1 "${METAL_TRIALS:-3}"); do
            for mode in samples-1 samples-2 samples-4 runtime-library; do
                run "$mode-$trial" albers_route_done 120 --albers-metal --albers-route --election-year 2024 --county-results "--metal-$mode"
            done
        done
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
        echo "Choose ownership, regions, qualification, label-overlay, quality-visual, quality, comparison, stress, cancellation, or visual." >&2
        exit 2
        ;;
esac
