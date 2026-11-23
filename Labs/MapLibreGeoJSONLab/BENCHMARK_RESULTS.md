# Raw GeoJSON benchmark results

**Run date** — November 22, 2026

**Configuration** — Release, arm64 iPhone 17 Pro Simulator on iOS 26.3, Xcode 26.3 (17C529), macOS 15.6, MacBook Pro (Apple M4, 24 GB), MapLibre Native 6.31.0. The app used a bundled background style and its MapLibre session blocked HTTP(S). Both known physical iOS devices were unavailable.

## Input validation

Positions include closed-ring endpoints. Bounds are west, south, east, north and rounded here. The validator used the original file bytes.

| Dataset | Bytes | Polygon / MultiPolygon features | Polygon parts | Rings | Positions | Bounds | Unique selection identity |
|---|---:|---:|---:|---:|---:|---|---|
| Districts | 4,749,558 | 383 / 58 | 762 | 770 | 175,196 | −179.14734, −14.55255, 179.77847, 71.35256 | GEOID (441) |
| Counties | 10,167,949 | 3,126 / 109 | 3,557 | 3,580 | 227,438 | −179.14734, −14.55255, 179.77847, 71.35256 | GEOID (3,235) |
| States | 2,349,868 | 32 / 24 | 350 | 350 | 91,908 | −179.14734, −14.55255, 179.77847, 71.35256 | GEOID (56) |
| Historical counties | 39,904,769 | 15,790 / 1,937 | 37,017 | 37,681 | 742,895 | −179.12943, 18.91228, −66.94993, 71.38961 | ID_NUM (17,727) |

| Input | SHA-256 |
|---|---|
| `cb_2024_us_cd119_5m.json` | `ef1cb8d92c1359defca5624db21012eeb25aef95585ba0c6eea6a3ffa508e11c` |
| `cb_2024_us_county_5m.json` | `335cd6da8cba27ce285334d5a6390fc1bf37a4109a8c5da6e4cf3b7fd9e80693` |
| `cb_2024_us_state_5m.json` | `2a5b936075f5e107973d8c10754ef035e13fc291c141b53ea6425305c5761b83` |
| `US_HistCounties_Gen05.json` | `af037e36183c9274d8be905b966ba775b548b6ec5210360c5814a7b952133a7b` |

## XCTest cold process launch

XCTest recorded three Release launches per dataset using the native launch-to-responsive metric. Each launch started a new app process. The metric measures OS launch through the first responsive app presentation. The initial screen can become responsive while GeoJSON loads. Rendering timings below came from a separate run series.

| Dataset | Launch → first responsive presentation, seconds, median (range) |
|---|---:|
| Districts | 1.323 (1.238–1.590) |
| Counties | 1.266 (1.253–1.573) |
| States | 1.234 (1.085–1.310) |
| Historical counties | 1.391 (1.225–1.483) |

[XCTest launch metric](https://developer.apple.com/documentation/xctest/xctapplicationlaunchmetric).

## Strategy A — unchanged GeoJSON, default source options

Three fresh app processes per dataset. Figures are medians with min–max ranges, in milliseconds. Each run starts a new app process with the host disk cache in its ordinary state. The slowest county launch took 1.62 s to a useful frame. The historical startup used the same local import pipeline from a staged file and includes the validation and copy. Its initial fitted viewport spans the historical data. Census startup uses the contiguous-U.S. camera.

| Dataset | Validation or copy + validation | App init → useful frame | Source addition → useful frame | Source addition → idle |
|---|---:|---:|---:|---:|
| Districts | 266.6 (233.5–287.9) | 582.5 (445.6–592.8) | 98.3 (79.4–110.5) | 921.8 (690.4–939.8) |
| Counties | 476.7 (470.9–677.5) | 916.5 (855.9–1623.3) | 243.8 (208.1–680.6) | 770.4 (736.3–829.8) |
| States | 155.6 (142.2–291.9) | 386.6 (378.2–536.0) | 56.7 (47.4–64.4) | 792.4 (772.9–934.9) |
| Historical counties | 1075.6 (1042.6–1285.2) | 2058.2 (2033.4–2212.7) | 783.5 (783.1–797.4) | 948.4 (947.4–960.7) |

Source submission on the main actor took 0.2–0.7 ms in these twelve runs. The first useful frame required a successful query against the newly added fill layer. Idle was recorded after that criterion; it covers the current viewport. The public delegate callbacks combine file read, C++ parsing, index creation, tile processing, and drawing into the source-to-frame interval. Their separate elapsed times were **Not measured**.

### Memory

Process memory reported by Darwin `TASK_VM_INFO`. Each entry is resident RSS / physical footprint in MiB. Baseline, validation, useful frame, and idle are medians from the launch series. RSS includes resident mapped/shared pages. Footprint reflects memory charged to the process by the OS. Temporary allocations require allocation lifetime data. Navigation peaks are independently sampled maxima at 200 ms intervals, and can miss transient peaks.

| Dataset | Before GeoJSON, style ready | After app validation | First useful frame | Idle | Navigation sampled peak | After 8× | After repeat 8× |
|---|---:|---:|---:|---:|---:|---:|---:|
| Districts | 203.0 / 25.8 | 230.6 / 48.6 | 319.3 / 66.6 | 313.7 / 66.8 | 257.7 / 123.7 | 225.6 / 122.5 | 202.1 / 122.5 |
| Counties | 203.5 / 26.1 | 243.9 / 62.3 | 476.9 / 205.3 | 346.1 / 221.3 | 402.8 / 350.7 | 314.8 / 347.2 | 299.2 / 348.7 |
| States | 203.2 / 26.2 | 219.8 / 37.8 | 234.3 / 38.8 | 233.1 / 39.0 | 217.1 / 51.3 | 197.7 / 50.8 | 197.1 / 50.8 |
| Historical counties | 203.2 / 25.8 | 322.5 / 139.9 | 1051.6 / 625.2 | 946.2 / 641.4 | 1192.7 / 1164.6 | 1106.2 / 1140.0 | 230.8 / 1146.2 |

The peak column covers camera motion in a separate navigation run. The initial loading phase can have higher RSS. Footprint after revisiting the same 8× route differed by 0.0–6.2 MiB across the four datasets at this precision. The historical layer grew by about 500 MiB between its initial idle footprint and the end of the first 8× pass. Repeated 8× navigation in the same region then added about 6 MiB. Wider, longer tours can create additional tiles and require their own stress test.

### Navigation and switching

One sustained camera route per dataset. Each zoom window contains five 1.2-second pan/zoom transitions. A repeat 8× window measures memory after revisiting the same area. Columns show MapLibre render callback FPS and p95 time between callbacks. The route starts at the contiguous-U.S. national zoom, then adds 1, 2, and 3 MapLibre zoom levels (2×, 4×, 8×). Callback pacing is a simulator diagnostic. Presented FPS, GPU stalls, and display hitches require a presentation trace.

| Dataset | 1× FPS / p95 ms | 2× FPS / p95 ms | 4× FPS / p95 ms | 8× FPS / p95 ms | Worst gap and observable callback interruptions |
|---|---:|---:|---:|---:|---|
| Districts | 60.00 / 16.9 | 60.00 / 16.9 | 60.00 / 17.0 | 60.00 / 17.0 | 25.6 ms, 1 gap over 25 ms |
| Counties | 60.00 / 17.0 | 60.00 / 16.9 | 60.00 / 16.9 | 60.00 / 16.8 | 24.3 ms, 0 gaps over 25 ms |
| States | 59.67 / 16.9 | 60.00 / 16.9 | 60.00 / 17.0 | 60.00 / 16.9 | 40.2 ms, 3 gaps over 25 ms |
| Historical counties | 59.67 / 17.0 | 59.19 / 17.3 | 59.34 / 17.1 | 59.67 / 17.0 | 66.3 ms, 13 gaps over 25 ms |

The switch run completed **21 consecutive switches** and reached verified idle on all 22 loads (including the initial launch). The table excludes the first validation of each dataset.

| Switched to | Warm loads | Request → useful frame ms, median (range) | Request → idle ms, median (range) | Idle RSS / footprint MiB, median |
|---|---:|---:|---:|---:|
| Districts | 7 | 93.4 (83.0–197.2) | 555.1 (498.9–594.1) | 432.8 / 77.1 |
| Counties | 6 | 265.1 (231.0–414.6) | 625.8 (623.5–633.4) | 522.8 / 144.0 |
| States | 6 | 61.1 (54.1–91.3) | 686.4 (647.6–727.1) | 454.2 / 69.6 |
| Historical counties | Not measured | Not measured | Not measured | Not measured |

The sequence ended in 14.5 seconds. County idle footprint varied from 93.6 to 228.3 MiB after the first cycle. District idle footprint was 70.7 MiB initially, 92.7 MiB at its warm peak, and 78.7 MiB at the end. Ending district RSS was 441.7 MiB versus 289.8 MiB initially. Footprint varied between switches, with higher residency at the end than at launch.

A separate run began with historical counties and then switched 21 times through the bundled layers. Idle RSS / footprint was 953.4 / 650.6 MiB for historical counties, 951.5 / 176.9 MiB after the switch to modern counties, and 864.9 / 64.4 MiB after switching to states. The run ended on districts at 605.3 / 67.0 MiB. The footprint fell after source replacement. RSS recovered more slowly and remained elevated at the end. This run started switching after the initial historical viewport settled. Release after prolonged historical navigation and long-duration stabilization remain open.

## Strategy B — source options on historical counties

One Release simulator run per option on the same raw file and route. The default source uses buffer 128, simplification tolerance 0.375, and maximum source zoom 18. Each candidate changes one setting. The highest camera zoom on this route was about 5.2, including the quarter-step zoom excursions.

| Configuration | Source → useful frame (ms) | Idle RSS / footprint (MiB) | Navigation sampled peak RSS / footprint (MiB) | After repeat 8× RSS / footprint (MiB) | Callback FPS range |
|---|---:|---:|---:|---:|---:|
| Default | 986.0 | 949.7 / 654.8 | 1036.0 / 1216.6 | 402.2 / 1166.4 | 59.51–60.00 |
| Buffer 32 | 802.2 | 810.4 / 399.7 | 798.1 / 748.9 | 521.5 / 696.2 | 59.50–60.00 |
| Tolerance 1.5 | 693.5 | 1007.4 / 486.1 | 1095.0 / 944.1 | 357.5 / 931.2 | 59.18–60.00 |
| Maximum source zoom 8 | 927.1 | 974.0 / 649.1 | 624.9 / 1205.9 | 240.8 / 1160.0 | 58.86–60.00 |

The 32-pixel buffer reduced sampled peak footprint by about 38% in the matched historical runs and kept the raw input bytes intact. A smaller buffer can expose tile-edge artifacts. Higher simplification tolerance can remove small geographic detail. Edge fidelity, selection fidelity, and high overzoom for these profiles are **Not measured**. The app defaults to Strategy A.

## Instruments capture

Instruments recorded separate seven-second **App Launch** and **Allocations** traces on the Release simulator. The launch trace targeted districts. The allocation trace targeted the historical import. The saved trace bundles are `BalloticsGeoJSONAppLaunch.trace` and `BalloticsGeoJSONAllocations.trace`. The CLI export supplied lifecycle and dyld schemas for App Launch and process/log schemas for Allocations. Temporary allocations by lifetime, absolute heap peak, GPU allocation totals, and presented-frame pacing are **Not measured** in the tables. The process RSS and footprint samples above use Darwin task statistics.

## Functional verification

- Release builds succeeded for the iPhone simulator and generic iOS device. The generic build was unsigned.
- The host validator passed on the four original files and the malformed, unsupported-geometry, invalid-coordinate, ring, hole, and duplicate-ID fixtures. The three Release app bundle resources matched the repository inputs byte for byte.
- Two functional simulator UI tests and four Release XCTest launch measurements passed. The functional tests covered all bundled layers, a district’s GEOID and amber highlight at the tap, double-tap and pinch zoom, pan, national reset, selection persistence, selected-state zoom, paint changes, Alaska and Oʻahu. The Files test imported historical counties, inspected a record’s numeric `ID_NUM`, and rejected malformed JSON while retaining the active map and selection.
- Rendering, local import, and switching completed with the MapLibre HTTP(S) blocker installed. The simulator log contained no unexpected request reports during the experiment.

## Physical-device gates

| Measurement | Districts | Counties | States | Historical counties |
|---|---|---|---|---|
| OS launch → responsive, source load, useful frame, and idle | Not measured | Not measured | Not measured | Not measured |
| Presented FPS, frame pacing, visible stutter | Not measured | Not measured | Not measured | Not measured |
| Resident memory, peak and temporary allocations, long-run stabilization | Not measured | Not measured | Not measured | Not measured |

The [raw JSONL measurements](evidence/simulator-2026-11-22/) contain the per-launch, per-zoom, and per-switch observations. See [Technical findings](TECHNICAL_FINDINGS.md) for the decision.
