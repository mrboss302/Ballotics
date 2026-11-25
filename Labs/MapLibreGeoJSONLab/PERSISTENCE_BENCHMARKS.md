# Presidential persistence and renderer benchmarks

The measurements are from Release builds on November 25, 2026. The SwiftData command-line checks ran on macOS. The interactive measurements use an iPhone 17 Pro simulator with iOS 26.3 and Xcode 26.3. All durations are single-study samples and vary with machine load, import order and cache state.

| macOS SwiftData path | Observed |
| --- | ---: |
| Open empty store | 28.5 ms |
| Validate and activate national bundle | 269.9 ms |
| First county activation, seven years | 252.3–742.7 ms |
| Query and decode one county year | 122.6–325.2 ms |
| Duplicate national / county import | 0.6 / 0.5 ms |
| Reopen persisted store offline | 2.0 ms |
| Offline 2024 election and allocations query | 28.9 ms |
| Offline 2024 county query | 142.8 ms |

The macOS checks installed seven county years, exercised a test download for one year and reopened the saved store from a new container without reading a package directory. They rejected a simulated interrupted response, truncation, a same-size checksum mismatch, invalid JSON carrying its own valid hash, and an unsupported schema. A failed replacement kept the active national dataset readable. A successful replacement kept the prior version and restored its cached county packages when reactivated.

## iPhone 17 Pro simulator

On the final package, the first county launch opened and installed the national package in 264.5 ms and activated the 2024 county results in another 238.2 ms. The first Albers paint was logged 1.910 seconds after the lab’s app clock started. The offline reopen samples read persisted rows and used bundled geometry.

| Offline county launch phase | Albers | MapLibre |
| --- | ---: | ---: |
| Open existing store and check active package | 22.2 ms | 22.4 ms |
| Query and decode the election and county rows | 101.0 ms | 100.4 ms |
| Resolve local geographic source and prepare presentation | 455.2 ms | 470.4 ms |
| Project geometry | 416.8 ms | — |
| Time from app clock start to first recorded election paint | 1.176 s | 0.927 s |

The interaction route alternated 2020 and 2024 county results and moved through the national and inset views. The six year changes reused the same geographic input. SwiftData query and decode took 101.7–130.7 ms during these same-source changes. The route sampled memory at switch checkpoints and did not capture a peak.

| Measurement | Albers | MapLibre |
| --- | ---: | ---: |
| County paint on same-source year changes | 12.0–13.7 ms | 34.2–53.5 ms |
| Request to dataset ready, including query and scheduling | 695.7–5,426.9 ms | 214.9–243.7 ms |
| Hit to inspection state, one county tap | 0.3 ms | 1.3 ms |
| Physical footprint at repeated-switch checkpoints | 124.9–136.8 MiB | 196.1–218.0 MiB |

The Albers paint measurement ends with its Core Graphics draw. MapLibre reports the first fully rendered frame after the paint change. At several Albers year changes, most of the end-to-end delay occurred before the query completed. The run does not isolate that delay to the renderer. The MapLibre checkpoints follow the Albers route in the same process and are affected by prior work.

## Verification

The simulator and unsigned device Release builds succeeded. Their embedded payloads match the staged package byte for byte. The three presidential UI cases passed, covering the 1824 House outcome, early and modern year selection, and county inspection and recoloring in both renderers. The preserved geometry checks and four legacy launch tests passed. The optional Files picker test skipped when its automation did not expose the fixture, although the independent historical and malformed-data checks ran.

[Raw benchmark events and verification results](evidence/presidential-0.1/) are retained alongside the prior lab evidence. The local research staging validation directory contains full simulator logs, build logs, screenshots and XCTest result bundles.

## Reproduce

From the repository root, generate the local release and run the SwiftData check.

```sh
python3 ElectionData/tools/normalize.py --geometry
lab=Labs/MapLibreGeoJSONLab
work=$(mktemp -d)
xcrun swiftc -O -module-cache-path "$work/modules" \
  "$lab/ElectionPackage.swift" "$lab/ElectionStore.swift" "$lab/Tests/ElectionStoreChecks.swift" \
  -o "$work/check-election-store"
"$work/check-election-store" "$PWD/ElectionData/research-staging/0.1.0"
```

Launch the iOS Release lab with `--election-year 2024 --county-results --measure`. Add `--election-maplibre` to start the other renderer. For an offline reopen omit `--election-package` and relaunch without deleting the installed data. The log is `Library/Caches/benchmark.jsonl` inside the app container. The interactive suite includes a navigation route with repeated year changes, camera moves and map selection in both renderers.

Physical-device performance remains a qualification gate. An unsigned device build establishes compilation for iOS. The simulator does not measure physical-device launch, memory pressure, energy use, GPU frame pacing or worst-case navigation.
