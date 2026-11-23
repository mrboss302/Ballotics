# Ballotics MapLibre Native GeoJSON Lab

A standalone SwiftUI iOS experiment for the original Ballotics polygon files. The app bundles a background-only style and uses MapLibre Native 6.31.0. Build and dependency resolution may require internet access. The running map uses local files and rejects HTTP(S) in MapLibre's configured session.

Open [MapLibreGeoJSONLab.xcodeproj](MapLibreGeoJSONLab.xcodeproj) from the research branch and run the shared `MapLibreGeoJSONLab` scheme. The app target requires iOS 17 or later. Select a signing team for a physical device.

The app target references the three Census files at the repository root. Keep them in place. Tap a polygon and open its inspection panel to see its identifiers and other scalar properties. The camera menu resets to a contiguous U.S. overview, provides Alaska and Hawaii shortcuts, fits the selected state when the States layer is active, and lets you change fill paint on the current source. Pinch, pan, and double-tap use MapLibre's native gestures.

Use **Import file** to select a local `.geojson` or `.json` file in iOS Files. The lab supports Polygon, MultiPolygon, and polygon-only FeatureCollections. Failed imports leave the previous dataset active. To try historical counties, copy the original `US_HistCounties_Gen05.json` from the preserved iOS repository into Files. The historical file contains overlapping vintages and the inspector exposes their start and end dates. The Census layers describe historical geography and have no election results or verified 2026 boundary status.

## Verification

From the repository root, compile and run the same validator the iOS app uses:

```sh
lab=Labs/MapLibreGeoJSONLab
checks=$(mktemp -d)
xcrun swiftc -O -module-cache-path "$checks/modules" \
  "$lab/GeoJSON.swift" "$lab/Tests/GeoJSONChecks.swift" -o "$checks/geojson-checks"
"$checks/geojson-checks" "$PWD"  # Optional second argument is the historical file path.
```

Run the shared scheme's UI tests on a bootable iOS Simulator.

```sh
xcodebuild -project "$lab/MapLibreGeoJSONLab.xcodeproj" -scheme MapLibreGeoJSONLab \
  -destination "platform=iOS Simulator,id=$SIMULATOR_ID" -parallel-testing-enabled NO \
  -only-testing:MapLibreGeoJSONLabUITests/MapLibreGeoJSONLabUITests test
```

When using the Files integration test, install and open the app on the selected simulator first, then stage `US_HistCounties_Gen05.json` and an invalid JSON file named `Lab-Malformed.json` in its Documents directory. In the picker these appear under **On My iPhone → Ballotics GeoJSON Lab**. The Files test skips when these optional inputs are absent.

## Simulator timing and memory

Build the app with the Release configuration for the same simulator, then point the runner at the built `.app`. The last argument is optional.

```sh
"$lab/Tests/benchmark-simulator.sh" "$SIMULATOR_ID" "$RELEASE_APP_BUNDLE" "$OUTPUT_DIRECTORY" \
  "/absolute/path/to/US_HistCounties_Gen05.json"
```

Set `BALLOTICS_SOURCE_SWEEP=1` and pass the historical file to compare default source options with a 32-pixel buffer, a 1.5 simplification tolerance, and a source zoom cap of 8. Apply one option at a time.

The runner records three fresh app processes per layer, one camera route per layer, and 21 consecutive switches. Transitions in the four zoom windows pan and change zoom slightly about a fixed central-U.S. route. The runner repeats the 8× window once to sample memory after navigating the same tiles again. The app records diagnostics in its Caches directory only with `--measure`; use a fresh or empty `benchmark.jsonl` per run. On-device, use `--measure --dataset districts|counties|states`, `--navigation-benchmark`, or `--switch-benchmark` in the Run/Profile scheme arguments. For the historical benchmark, place the file in the lab's Documents directory and pass `--historical` instead of `--dataset`.

For native launch-to-responsive timings, run the UI test target in Release with `-only-testing:MapLibreGeoJSONLabUITests/GeoJSONLaunchTests`. Stage the historical file in the lab’s Documents directory to include that launch case.

Use a physical-device Release build and an Instruments trace to measure process launch, frame presentation and hitches, and temporary allocations separately from resident and footprint memory. See [Benchmark results](BENCHMARK_RESULTS.md) for the measured configuration and [Technical findings](TECHNICAL_FINDINGS.md) for the decision and remaining qualification gates.
