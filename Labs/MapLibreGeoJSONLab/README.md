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

## Date-filtered historical counties

Choose **Historical Counties** and select the original historical archive in Files if the lab has no local archive yet. For simulator testing, copy `US_HistCounties_Gen05.json` to the lab's Documents directory. Choose a research preset from the date menu, or enter a custom ISO civil date and a snapshot citation. The selected date, provenance, and validated feature count appear above and below the map. The archive covers 1629-03-04 through 2000-12-31, inclusive. When testing a covered day before 1788, use **Custom date**. The standard **Import file** path remains available for the all-history stress test.

From the repository root on macOS, build the independent tests and publisher with the same engine that the app uses.

```sh
lab=Labs/MapLibreGeoJSONLab
archive="/absolute/path/to/US_HistCounties_Gen05.json"
work=$(mktemp -d)
xcrun swiftc -O -module-cache-path "$work/modules" \
  "$lab/GeoJSON.swift" "$lab/HistoricalArchive.swift" "$lab/Tests/HistoricalArchiveChecks.swift" \
  -o "$work/check-history"
"$work/check-history" "$archive"

xcrun swiftc -O -module-cache-path "$work/modules" \
  "$lab/GeoJSON.swift" "$lab/HistoricalArchive.swift" "$lab/Tests/PublishHistoricalSnapshots.swift" \
  -o "$work/publish-history"
"$work/publish-history" "$archive" "$work/published" 1788-01-01 2000-01-01
python3 "$lab/Tests/audit-historical.py" "$archive" --published "$work/published"
```

Build the shared scheme for the simulator in Release. With the selected simulator booted, run the historical matrix against the resulting application bundle.

```sh
"$lab/Tests/benchmark-historical.sh" "$SIMULATOR_ID" "$RELEASE_APP_BUNDLE" \
  "$archive" "$work/published" "$work/results"
```

The benchmark stages a lab-only copy of the source and the two generated snapshots in the simulator's Documents folder. Each case starts a fresh app process. The script also runs 21 warm date switches for each buffer. To reproduce the renderer hit probes, launch with `--measure --historical-date 1788-01-01 --historical-probes` or use `2000-01-01`, then repeat with `--source-option narrow-buffer`. Read `Library/Caches/benchmark.jsonl` from the lab's simulator container.

See the [data audit](HISTORICAL_DATA_AUDIT.md), [historical benchmarks](HISTORICAL_FILTER_BENCHMARKS.md), and [architecture recommendation](HISTORICAL_GEOJSON_ARCHITECTURE.md).

## Albers equal-area overview

Choose **Map view → Albers equal area** for the national overview. Alaska and Hawaii have separate, labeled insets at the same linear scale as the lower 48. Use the **Alaska**, **Hawaii**, or **National** menu items to focus or reset the view. Tap polygons to inspect them. The view also supports pan, pinch, and double-tap zoom.

The overview uses ellipsoidal Albers on WGS 84 with regional parameters. The lower 48 use standard parallels 29.5° and 45.5° and a central meridian of −96°. Alaska uses 55° and 65° at −154°, with longitudes wrapped relative to that meridian to keep the Aleutians together. Hawaii uses 8° and 18° at −157°. Translation into the insets keeps their relative areas comparable. [USGS Albers parameters](https://pubs.usgs.gov/of/1990/0539a/report.pdf) · [USGS Hawaii WGS 84 specification](https://d9-wret.s3.us-west-2.amazonaws.com/assets/palladium/production/s3fs-public/media/files/LSDS-2309%20Land%20Change%20Monitoring%2C%20Assessment%2C%20and%20Projection%20%28LCMAP%29%20Hawaii%20Collection%201.0%20Data%20Format%20Control%20Book%20%28DFCB%29%20-v1.0%202021_12_14.pdf).

The Albers overview supports the bundled datasets and historical date snapshots. Alaska and Hawaii insets appear when those polygons are present. A new dataset or snapshot fits its projected extent. The MapLibre view is the default and provides the full coverage of bundled territories and arbitrary imports. Use **Map view → MapLibre** to return to that renderer. Existing MapLibre benchmarks use the MapLibre view. Both views read the original local geometry or the active historical snapshot.

Run the standalone projection check from the repository root. The optional final argument is a published `2000-01-01` snapshot.

```sh
lab=Labs/MapLibreGeoJSONLab
work=$(mktemp -d)
xcrun swiftc -O -module-cache-path "$work/modules" \
  "$lab/GeoJSON.swift" "$lab/AlbersProjection.swift" "$lab/Tests/AlbersChecks.swift" \
  -o "$work/check-albers"
"$work/check-albers" "$PWD"
```
