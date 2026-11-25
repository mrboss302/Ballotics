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
  "$lab"/../../Packages/BalloticsGeoKit/Sources/BalloticsGeoKit/*.swift "$lab/GeoJSON.swift" "$lab/Tests/GeoJSONChecks.swift" -o "$checks/geojson-checks"
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
  "$lab"/../../Packages/BalloticsGeoKit/Sources/BalloticsGeoKit/*.swift "$lab/GeoJSON.swift" "$lab/HistoricalArchive.swift" "$lab/Tests/HistoricalArchiveChecks.swift" \
  -o "$work/check-history"
"$work/check-history" "$archive"

xcrun swiftc -O -module-cache-path "$work/modules" \
  "$lab"/../../Packages/BalloticsGeoKit/Sources/BalloticsGeoKit/*.swift "$lab/GeoJSON.swift" "$lab/HistoricalArchive.swift" "$lab/Tests/PublishHistoricalSnapshots.swift" \
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

The Albers overview supports the bundled datasets and historical date snapshots. Alaska and Hawaii insets appear when those polygons are present. A new dataset or snapshot fits its projected extent. The MapLibre view opens by default for the census datasets and provides the full coverage of bundled territories and arbitrary imports. Use **Map view → MapLibre** to return to that renderer. Existing MapLibre benchmarks use the MapLibre view. Both views read the original local geometry or the active historical snapshot.

Run the standalone projection check from the repository root. The optional final argument is a published `2000-01-01` snapshot.

```sh
lab=Labs/MapLibreGeoJSONLab
work=$(mktemp -d)
xcrun swiftc -O -module-cache-path "$work/modules" \
  "$lab"/../../Packages/BalloticsGeoKit/Sources/BalloticsGeoKit/*.swift "$lab/GeoJSON.swift"  "$lab/Tests/AlbersChecks.swift" \
  -o "$work/check-albers"
"$work/check-albers" "$PWD"
```

## Presidential election boundaries

Choose **Presidential Elections** from the dataset menu. The lab opens the latest election in Albers. Tap the year to browse or search presidential elections from 1788 onward. The map shows every state, territory, and other jurisdiction that the source records as active on the displayed boundary date. Tap a polygon to inspect its name, jurisdiction type, source ID, validity dates, and citation. [Ballotics state and territory archive](https://github.com/mrboss302/Ballotics/blob/master/US_HistStateTerr_Gen05.json) · [Newberry Atlas national data](https://publications.newberry.org/ahcb/pages/United_States.html).

- **1788** uses February 4, 1789, the day Congress scheduled the electors to vote in the first presidential election. **1792–1844** use the scheduled Electoral College voting day, the first Wednesday in December. [September 1788 resolution](https://tile.loc.gov/storage-services/service/rbc/bdsdcc/2410h/2410h.pdf) · [1792 election law](https://www.govinfo.gov/content/pkg/STATUTE-1/pdf/STATUTE-1-Pg239.pdf).
- **1848–2000** use federal Election Day, the first Tuesday after the first Monday in November. **2004 onward** show the chosen Election Day and reuse the November 7, 2000 geometry. [1845 law establishing a uniform federal Election Day](https://www.govinfo.gov/content/pkg/STATUTE-5/pdf/STATUTE-5-Pg721.pdf).

Verify all election snapshots against the archive and check the historical insets from the repository root.

```sh
lab=Labs/MapLibreGeoJSONLab
work=$(mktemp -d)
xcrun swiftc -O -module-cache-path "$work/modules" \
  "$lab"/../../Packages/BalloticsGeoKit/Sources/BalloticsGeoKit/*.swift "$lab/GeoJSON.swift" "$lab/HistoricalArchive.swift" "$lab/PresidentialElection.swift" \
   "$lab/Tests/PresidentialElectionChecks.swift" -o "$work/election-checks"
"$work/election-checks" US_HistStateTerr_Gen05.json
```

## Presidential results research 0.1

The **Presidential Elections** dataset opens the electoral map. The **County results** mode is available in 2000–2024. Tap a shape and open its inspector for the reported allocations or local candidate returns. **Results** opens the presidential outcome, legend, election totals, historical review and unjoined reporting units. The two renderers share the result presentation. Albers opens by default, with MapLibre available from **Map view**.

The build uses Python 3 to normalize the original Ballotics inputs and stage versioned research resources in the application bundle. SwiftData imports the national package on first election entry and county years when selected. Launch options include `--election-year 2024`, `--county-results`, `--election-maplibre` and `--measure`. Use a trusted manifest SHA-256 with `--election-package` for a versioned HTTPS research package.

See the [data audit](../../ElectionData/PRESIDENTIAL_DATA_AUDIT.md), [wire contract](../../ElectionData/ELECTION_DATA_CONTRACT.md), [geography resolver](../../ElectionData/ELECTION_GEOGRAPHY_RESOLVER.md), and [measurements](PERSISTENCE_BENCHMARKS.md).

## Albers Rendering Lab 0.2

Use **Map view → Find geography** to search by name or source identifier and focus a selected feature. Use **Zoom to selected state** to frame the state around a selected county or congressional district, or **Clear selection** to dismiss the highlight and inspector. The map offers animated national, Alaska and Hawaii framing. For county results, **Map colors → Reported margin** uses the validated reported values while retaining the result status in the inspector. The Albers and MapLibre views are both available for the research comparison.

Build the existing scheme in Release and supply an optional copy of the historical county fixture. The comparison script launches fresh processes for linear Albers, indexed Albers and MapLibre. The stress mode runs a separate 30-minute session. Use an already booted simulator and its own output directory.

```sh
lab=Labs/MapLibreGeoJSONLab
work=$(mktemp -d)
xcodebuild -project "$lab/MapLibreGeoJSONLab.xcodeproj" -scheme MapLibreGeoJSONLab \
  -configuration Release -destination "platform=iOS Simulator,id=$SIMULATOR_ID" \
  -derivedDataPath "$work/build" CODE_SIGNING_ALLOWED=NO ONLY_ACTIVE_ARCH=YES build
app="$work/build/Build/Products/Release-iphonesimulator/MapLibreGeoJSONLab.app"
bash "$lab/Tests/benchmark-albers.sh" "$SIMULATOR_ID" "$app" "$work/comparison" "$HISTORICAL_COUNTY_ARCHIVE"
ALBERS_STRESS_ONLY=1 bash "$lab/Tests/benchmark-albers.sh" \
  "$SIMULATOR_ID" "$app" "$work/long-run" "$HISTORICAL_COUNTY_ARCHIVE"
python3 "$lab/Tests/summarize-albers-stress.py" "$work/long-run/stress.jsonl"
```

To run the geographic checks from the repository root after building the lab, use the resolved county derivative from the app bundle. The build generates it from the original maps and their documented substitutes.

```sh
xcrun swiftc -O -module-cache-path "$work/modules" \
  "$lab"/../../Packages/BalloticsGeoKit/Sources/BalloticsGeoKit/*.swift "$lab/GeoJSON.swift" "$lab/HistoricalArchive.swift" "$lab/PresidentialElection.swift" \
   "$lab/Tests/AlbersQualification.swift" -o "$work/check-albers-02"
"$work/check-albers-02" "$PWD" "$HISTORICAL_COUNTY_ARCHIVE" \
  "$lab/Tests/Fixtures/albers-proj-wgs84.json" "$app/ElectionResearch/county-geometry-2024.geojson"
```

Use `python3 "$lab/Tests/albers-reference.py"` with pyproj to regenerate the projection references. Use **Time Profiler** for system CPU attribution and **Animation Hitches** on supported physical devices. Callback logs are available with `--measure`.

Run the isolated CPU bitmap experiment on macOS to compare fill and outline costs at each backing scale.

```sh
xcrun swiftc -O -module-cache-path "$work/modules" \
  "$lab"/../../Packages/BalloticsGeoKit/Sources/BalloticsGeoKit/*.swift "$lab/GeoJSON.swift"  "$lab/Tests/AlbersRasterStudy.swift" \
  -o "$work/albers-raster-study"
"$work/albers-raster-study" cb_2024_us_county_5m.json > "$work/raster.jsonl"
```

[Audit](ALBERS_RENDERER_ARCHITECTURE_AUDIT.md) · [Architecture](ALBERS_RENDERING_ARCHITECTURE.md) · [Performance](ALBERS_PERFORMANCE_BENCHMARKS.md) · [Cartography](ALBERS_CARTOGRAPHIC_VALIDATION.md) · [Android study](ALBERS_CROSS_PLATFORM_STUDY.md) · [Qualification](ALBERS_QUALIFICATION_REPORT.md)

## Albers Metal Rendering Lab 0.3

Choose **Map view → Albers Metal**. The menu retains **Albers Core Graphics** and **MapLibre** as controls. The Metal backend uses the existing regional projection and camera, geographic search, inspector, electoral and county coloring, and historical snapshot selection. It uses 4× multisampling by default. An optional renderer switch lets you inspect imported geometry with the Metal lab; MapLibre remains the default for imports from the original renderers. A Metal preparation failure displays the original projected paths and identifies the reason in the status line.

Build the same Release scheme used for 0.2. Run each measurement serially on an already booted iPhone simulator. The comparison and visual modes use the app bundle built from this branch. Supply your locally authorized historical archive to include the historical source changes in the stress route.

```sh
lab=Labs/MapLibreGeoJSONLab
bash "$lab/Tests/benchmark-metal.sh" "$SIMULATOR_ID" "$app" "$work/metal-comparison"
METAL_BENCHMARK_MODE=visual bash "$lab/Tests/benchmark-metal.sh" \
  "$SIMULATOR_ID" "$app" "$work/metal-visual"
METAL_BENCHMARK_MODE=stress bash "$lab/Tests/benchmark-metal.sh" \
  "$SIMULATOR_ID" "$app" "$work/metal-stress" "$HISTORICAL_COUNTY_ARCHIVE"
METAL_BENCHMARK_MODE=cancellation bash "$lab/Tests/benchmark-metal.sh" \
  "$SIMULATOR_ID" "$app" "$work/metal-cancellation" "$HISTORICAL_COUNTY_ARCHIVE"
python3 "$lab/Tests/summarize-metal.py" comparison "$work/metal-comparison"
python3 "$lab/Tests/summarize-metal.py" stress "$work/metal-stress/stress.jsonl"
python3 "$lab/Tests/compare-metal-coverage.py" "$work/metal-visual/counties-cg.png" \
  "$work/metal-visual/counties-metal.png" "$work/metal-visual/counties-metal-1x.png"
```

Use Pillow and NumPy for the screenshot comparison. The primary cohort defaults to three fresh processes for each of five draw configurations. `METAL_TRIALS` changes that repetition count. The stress run defaults to 1,800 seconds. The route measures callback and command timings independently of presentation and logs to the standalone app's Caches directory. On physical hardware, a Release build with `--measure --albers-metal --election-year 2024 --county-results --albers-route` records real drawable presentation timestamps when available. `--metal-fill-only` and `--metal-samples-1` isolate the boundary and antialiasing costs.

Run the host tessellation checks on a Mac from the repository root. Append locally generated historical snapshots to the arguments. Use the existing temporal publishing script to generate the dates listed in the cartographic report. To compare Earcut as well, set `METAL_EARCUT_ROOT` to a clean checkout of the revision listed in the tessellation report.

```sh
bash "$lab/Tests/check-metal-mesh.sh" cb_2024_us_county_5m.json \
  "$app/ElectionResearch/county-geometry-2024.geojson" \
  cb_2024_us_state_5m.json cb_2024_us_cd119_5m.json
```

For native simulator tests, install the Release lab first, then stage your historical archive in its Documents folder. Use the standalone `MapLibreGeoJSONLab` scheme. The bundled `--albers-metal --metal-fallback-probe` fixture checks that an identified collapsed polygon causes a whole-scene fallback and keeps another polygon selectable.

[Tessellation](METAL_TESSELLATION_STUDY.md) · [Architecture](METAL_RENDERER_ARCHITECTURE.md) · [Benchmarks](METAL_PERFORMANCE_BENCHMARKS.md) · [Cartography](METAL_CARTOGRAPHIC_VALIDATION.md) · [Lifetime](METAL_RESOURCE_LIFETIME.md) · [Android](METAL_CROSS_PLATFORM_STUDY.md) · [Qualification](METAL_QUALIFICATION_REPORT.md)

## Geographic Engine 0.3Q

**Map view → National composition** switches both Albers renderers between Thematic National and Equal-Scale Research. Electoral labels use the selected historical source and current allocation data. **Find geography**, **Alaska**, **Hawaii**, **Zoom to selected feature**, and **National** use the same geographic scene. See the [composition specification](NATIONAL_MAP_COMPOSITION.md) and [qualification decision](METAL_0.3Q_QUALIFICATION_REPORT.md).

Use Xcode with its Metal toolchain installed. The ordinary build compiles `AlbersShaders.metal` into the SDK-specific default library. `--metal-runtime-library` tests the bundled source alias, `--metal-samples-1` and `--metal-samples-2` request lower sample counts, and `--equal-scale` selects the research comparison. Four samples and visible boundaries are the full-quality Metal defaults.

From the lab directory, use an already booted simulator, a new output directory, and your locally authorized county archive. The cached-package option is optional when normal package resolution is available.

```sh
build=$(mktemp -d)
xcodebuild -project MapLibreGeoJSONLab.xcodeproj -scheme MapLibreGeoJSONLab \
  -configuration Release -destination "platform=iOS Simulator,id=$SIMULATOR_ID" \
  -derivedDataPath "$build" CODE_SIGNING_ALLOWED=NO ONLY_ACTIVE_ARCH=YES build-for-testing
app="$build/Build/Products/Release-iphonesimulator/MapLibreGeoJSONLab.app"
for mode in ownership cancellation regions qualification quality quality-visual; do
  METAL_BENCHMARK_MODE="$mode" bash Tests/benchmark-metal.sh \
    "$SIMULATOR_ID" "$app" "$build/$mode" "$HISTORICAL_COUNTY_ARCHIVE"
done
METAL_WORKLOAD=election-counties METAL_TRIALS=3 METAL_BENCHMARK_MODE=qualification \
  bash Tests/benchmark-metal.sh "$SIMULATOR_ID" "$app" "$build/dense" "$HISTORICAL_COUNTY_ARCHIVE"
python3 Tests/summarize-metal.py comparison "$build/qualification"
python3 Tests/qualify-metal-coverage.py "$build/quality-visual" evidence/albers-metal-0.3q/county-frames.json
```

The coverage command uses the checked Census county presentation bounds and the 3× iPhone portrait capture.

The qualification matrix uses one fresh process per workload and configuration unless a trial count is supplied. Capture simulator model, actual drawable dimensions, OS, text size and host activity alongside the JSONL. Keep the physical deadline field unavailable when there are no presentation timestamps. The host PROJ and selection commands in the 0.2 section still apply. To reproduce the 14-input mesh check from the lab directory, use the same Release app and locally authorized county archive as above.

```sh
xcrun swiftc -O -module-cache-path "$build/modules" ../../Packages/BalloticsGeoKit/Sources/BalloticsGeoKit/*.swift GeoJSON.swift HistoricalArchive.swift \
  Tests/PublishHistoricalSnapshots.swift -o "$build/publish-history"
"$build/publish-history" "$HISTORICAL_COUNTY_ARCHIVE" "$build/county-dates" \
  1788-01-01 1860-01-01 1900-01-01 1980-01-01 2000-01-01
"$build/publish-history" ../../US_HistStateTerr_Gen05.json "$build/state-dates" \
  1789-02-04 1860-11-06 1900-11-06 1980-11-04 2000-11-07
bash Tests/check-metal-mesh.sh ../../cb_2024_us_county_5m.json \
  "$app/ElectionResearch/county-geometry-2024.geojson" ../../cb_2024_us_state_5m.json \
  ../../cb_2024_us_cd119_5m.json "$build"/county-dates/*.geojson "$build"/state-dates/*.geojson
```

Run the opt-in 30-minute XCTest separately with an installed app and the historical archive already staged by the script. Select the same simulator and build directory. The test performs real orientation changes and a Home/foreground cycle while the in-app route drives election changes, selections, profile changes and dismissal. Ordinary test runs skip this long test. The per-process cache log also contains earlier launches when an existing app container is reused. Start the evidence slice at the `app_init` immediately before the tested `albers_stress_start` and include its `albers_stress_released`.

```sh
TEST_RUNNER_METAL_LONG_QUALIFICATION=1 xcodebuild \
  -project MapLibreGeoJSONLab.xcodeproj -scheme MapLibreGeoJSONLab -configuration Release \
  -destination "platform=iOS Simulator,id=$SIMULATOR_ID" -derivedDataPath "$build" \
  -parallel-testing-enabled NO -test-timeouts-enabled NO \
  -only-testing:MapLibreGeoJSONLabUITests/MapLibreGeoJSONLabUITests/testThirtyMinuteMetalLifecycle \
  -resultBundlePath "$build/lifecycle.xcresult" test-without-building
container=$(xcrun simctl get_app_container "$SIMULATOR_ID" org.ballotics.MapLibreGeoJSONLab data)
python3 Tests/verify-qualification-stress.py "$container/Library/Caches/benchmark.jsonl" "$build/verified-stress"
```

For system Reduce Motion on a disposable simulator, run `testSystemReducedMotionRegionalCamera` with `TEST_RUNNER_METAL_MOTION_QUALIFICATION=1` using the same XCTest command. The test enables the setting in iOS Settings, runs the regional map route and restores the prior switch value. Run `python3 Tests/summarize-metal.py reduced-motion "$container/Library/Caches/benchmark.jsonl"` before launching a different process. It checks the three one-frame transitions and the absence of timed camera animations. The other accessibility checks are `testElectoralInsetTextInspector`, `testNationalInsetPickingAcrossRenderers` and `testHistoricalOverlapOnThematicMetalMap`. Repeat search and inspection at an accessibility text size.

The visual test is `testCompositionReview`. Export its attachments locally with `xcrun xcresulttool export attachments --test-id 'MapLibreGeoJSONLabUITests/testCompositionReview()' --path RESULT.xcresult --output-path LOCAL_REVIEW_FOLDER`. XCTest landscape crops can include EXIF orientation. Use an orientation-aware viewer. Keep the owner's design and rights-unverified historical/election images local.

### Physical-device protocol

1. Pair a supported iPhone or iPad, select a development team for the isolated lab, and build **Release** for its device ID. Verify the signed app contains `default.metallib` and the libtess2 notice. Record model, OS, active refresh rate, text size, Low Power Mode and initial thermal state.
2. Install with `xcrun devicectl device install app --device "$DEVICE" "$SIGNED_APP"`. Run three fresh launches per renderer. Use the same election package and the same camera, orientation and screen settings. Run once from a cold launch, then repeat with warm package storage. Record first-use conditions separately from navigation.
3. For the full-quality route, launch with `xcrun devicectl device process launch --device "$DEVICE" --terminate-existing org.ballotics.MapLibreGeoJSONLab --measure --albers-metal --election-year 2024 --county-results --albers-route`. Repeat with `--equal-scale`, then Core Graphics and MapLibre using the flags in the matrix script. Use `--albers-region-route` to exercise both insets and return. Inspect borders and islands at 1× and 8×.
4. Collect actual drawable presentation events and command timestamps alongside Instruments Metal System Trace, Animation Hitches, memory and thermal measurements. Count presentation gaps against the measured display period. Record memory warnings, process footprint, sampled preparation peaks, any thermal transitions and post-dismissal release. Keep heavy Instruments recordings separate from the uninstrumented navigation cohort.
5. Exercise 2020/2024 recoloring, historical source supersession, repeated layout switches, search, pinch/pan, rotation, foregrounding and dismissal. Repeat on representative older hardware. Verify VoiceOver focus, both inset search results, inspector text, and actual system Reduce Motion.
6. Export `Library/Caches/benchmark.jsonl` using `xcrun devicectl device copy from --device "$DEVICE" --domain-type appDataContainer --domain-identifier org.ballotics.MapLibreGeoJSONLab --source Library/Caches/benchmark.jsonl --destination "$OUTPUT"`. Retain all observations, including timeouts, fallback events, long gaps and unavailable timestamps. Compare approximately 60 presented fps only on hardware with corresponding refresh and motion conditions.

## BalloticsGeoKit 0.4A

Run the following from the research lab directory on a Mac with Xcode, its Metal toolchain, the authorized historical county archive and the existing lab build resources. The standalone package and district viewer need no election package.

```sh
kit=../../Packages/BalloticsGeoKit
work=$(mktemp -d)
swift test --package-path "$kit" --scratch-path "$work/package" -c release
swift run --package-path "$kit" --scratch-path "$work/package" -c release DistrictAtlas ../../cb_2024_us_cd119_5m.json
# Headless national or detail capture
"$work/package/release/DistrictAtlas" ../../cb_2024_us_cd119_5m.json --district TX-18 --render "$work/tx18.png"
xcodebuild -project MapLibreGeoJSONLab.xcodeproj -scheme MapLibreGeoJSONLab -configuration Release \
  -destination "platform=iOS Simulator,id=$SIMULATOR_ID" -derivedDataPath "$work/ios" \
  CODE_SIGNING_ALLOWED=NO ONLY_ACTIVE_ARCH=YES build-for-testing
app="$work/ios/Build/Products/Release-iphonesimulator/MapLibreGeoJSONLab.app"
bash Tests/check-geokit-geography.sh "$HISTORICAL_COUNTY_ARCHIVE" "$app/ElectionResearch/county-geometry-2024.geojson" "$work/geography"
xcrun swiftc -O -module-cache-path "$work/modules" "$kit"/Sources/BalloticsGeoKit/*.swift GeoJSON.swift \
  HistoricalArchive.swift ElectoralCartography.swift Tests/GeoKitLabelStudy.swift -o "$work/study"
"$work/study" ../../US_HistStateTerr_Gen05.json "$app/ElectionResearch/0.1.0/national.json" > "$work/labels.jsonl"
for workload in districts electoral-states election-counties; do
  trials=1
  if [ "$workload" = election-counties ]; then trials=3; fi
  METAL_BENCHMARK_MODE=qualification METAL_WORKLOAD="$workload" METAL_TRIALS="$trials" \
    bash Tests/benchmark-metal.sh "$SIMULATOR_ID" "$app" "$work/$workload" "$HISTORICAL_COUNTY_ARCHIVE"
done
for mode in regions label-overlay; do
  METAL_BENCHMARK_MODE="$mode" bash Tests/benchmark-metal.sh "$SIMULATOR_ID" "$app" "$work/$mode" "$HISTORICAL_COUNTY_ARCHIVE"
done
METAL_BENCHMARK_MODE=stress METAL_STRESS_SECONDS=120 bash Tests/benchmark-metal.sh \
  "$SIMULATOR_ID" "$app" "$work/lifetime" "$HISTORICAL_COUNTY_ARCHIVE"
```

The 0.3Q section provides the ten dated inputs for the 14-input tessellation check, optional 30-minute route and signed-device protocol. An optional third argument to the compiled label study writes the 1976 geographic-anchor diagram. Review historical and electoral images locally. For native screenshots run `testGeoKitOwnerReviewAndExternalSelection` and `testGeoKitLandscapeScreen` with the existing Release scheme on each viewport. On iPad the landscape fixture simulates device rotation. Export the named attachments with xcresulttool. Run testGeoKitElectoralContentSwitch to verify a same-revision content change. The label summary command is `python3 Tests/summarize-metal.py labels PATH_TO_LABELS_JSONL`. For portable test input see the package’s `Tests/BalloticsGeoKitTests/Fixtures` directory. [0.4A architecture](GEOKIT_0.4A_ARCHITECTURE.md) · [Qualification](GEOKIT_0.4A_QUALIFICATION_REPORT.md)
