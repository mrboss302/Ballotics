# Presidential persistence and renderer benchmarks

The measurements are from Release builds on November 25, 2026. The SwiftData command-line checks ran on macOS. The interactive measurements use an iPhone 17 Pro simulator with iOS 26.3 and Xcode 26.3. All durations are single-study samples and vary with machine load, import order and cache state.

| macOS SwiftData path | Observed |
| --- | ---: |
| Open empty store | 18.4 ms |
| Validate and activate national bundle | 181.9 ms |
| First county activation, seven years | 221.8–295.3 ms |
| Query and decode one county year | 110.6–176.0 ms |
| Duplicate national / county import | 0.9 / 0.5 ms |
| Reopen persisted store offline | 1.2 ms |
| Offline 2024 election and allocations query | 23.7 ms |
| Offline 2024 county query | 170.9 ms |

The macOS checks installed seven county years, exercised a test download for one year and reopened the saved store from a new container without reading a package directory. They rejected a simulated interrupted response, truncation, a same-size checksum mismatch, invalid JSON carrying its own valid hash, and an unsupported schema. A failed replacement kept the active national dataset readable. A successful replacement kept the prior version and restored its cached county packages when reactivated.

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
