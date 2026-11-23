# Historical county temporal-filtering benchmarks

**November 22, 2026** · iPhone 17 Pro simulator, iOS 26.3, arm64 Release, Xcode 26.3, Apple M4 host with 24 GB RAM, MapLibre Native 6.31.0. No physical iOS device was available.

The previous all-history runs remain in [the prior benchmark](BENCHMARK_RESULTS.md). Those navigation runs used a central-U.S. camera and recorded 1,164.6 MiB for the default-buffer baseline and 748.9 MiB for one reduced-buffer run. Every table below uses **new measurements**. The matched runs start at the same national viewport and follow a fixed eastern route centered near 39° N, 81° W. The eastern route gives early snapshots useful county coverage. Camera levels are national scale, 2×, 4×, and 8×. Each window has five 1.2-second pan/zoom transitions. A second 8× pass revisits the same area.

## Geometry submitted to MapLibre

| Input date | Features | Polygon parts | Coordinate positions | GeoJSON bytes |
|---|---:|---:|---:|---:|
| All history | 17,727 | 37,017 | 742,895 | 39,904,769 |
| 1788-01-01 | 332 | 640 | 10,912 | 619,230 |
| 1800-01-01 | 455 | 761 | 13,324 | 785,665 |
| 1860-01-01 | 2,181 | 2,954 | 46,174 | 3,082,933 |
| 1900-01-01 | 2,845 | 4,158 | 62,448 | 4,116,586 |
| 1950-01-01 | 3,114 | 4,165 | 62,035 | 4,264,698 |
| 1980-01-01 | 3,143 | 4,217 | 63,241 | 4,330,490 |
| 2000-01-01 | 3,139 | 4,216 | 63,038 | 4,320,538 |
| 2024-01-01 | Unsupported | — | — | — |

These counts include active county equivalents, territories, and documented overlapping jurisdictions where represented by the archive. The 2024 date lies beyond its finite 2000-12-31 cutoff.

## Matched Release runs

One fresh app process per row. Times are milliseconds. Request starts after the app issues the source or date request. Useful means the new fill layer returned a rendered polygon. Idle follows a useful frame in that viewport. Memory columns show **RSS / physical footprint** in MiB. Navigation peak is sampled every 200 ms. Callback FPS is the range across all five windows, with the worst inter-callback gap in milliseconds. Frame presentation and GPU stalls require separate traces.

| Mode/date | Buffer | Prep ms | Request → useful | Source → useful | Source → idle | Idle RSS / footprint | Navigation peak RSS / footprint | Callback FPS range / worst gap ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| All history | 128 | 1086.3 | 2019.6 | 925.4 | 938.6 | 1092.0 / 624.4 | 1445.0 / 1177.8 | 58.54–60.01 / 151.2 |
| All history | 32 | 1397.9 | 2043.9 | 641.6 | 951.6 | 810.3 / 380.8 | 796.0 / 723.2 | 59.18–60.00 / 99.2 |
| Runtime 1788 | 128 | 1365.1 + 1.9 | 1419.4 | 46.9 | 942.1 | 222.5 / 35.2 | 236.7 / 59.3 | 60.00–60.00 / 32.5 |
| Runtime 1788 | 32 | 2224.4 + 4.3 | 2294.9 | 49.0 | 938.8 | 207.2 / 34.9 | 210.8 / 48.2 | 60.00–60.00 / 21.8 |
| Runtime 1800 | 128 | 1244.5 + 3.2 | 1295.7 | 42.7 | 936.9 | 225.1 / 36.8 | 246.5 / 68.6 | 60.00–60.00 / 23.1 |
| Runtime 1800 | 32 | 1329.7 + 2.4 | 1380.5 | 41.7 | 937.0 | 223.2 / 36.0 | 226.9 / 54.0 | 60.00–60.00 / 19.1 |
| Runtime 1860 | 128 | 1305.1 + 5.7 | 1375.8 | 59.0 | 919.6 | 255.7 / 50.9 | 285.4 / 175.2 | 60.00–60.00 / 21.1 |
| Runtime 1860 | 32 | 1442.5 + 6.5 | 1528.4 | 59.7 | 945.1 | 241.7 / 49.0 | 246.4 / 121.1 | 59.84–60.00 / 33.0 |
| Runtime 1900 | 128 | 1396.7 + 6.6 | 1524.4 | 103.1 | 946.6 | 270.0 / 68.6 | 286.7 / 201.1 | 59.84–60.00 / 33.4 |
| Runtime 1900 | 32 | 1346.9 + 5.2 | 1450.9 | 90.8 | 948.4 | 257.4 / 54.5 | 303.6 / 131.3 | 60.00–60.00 / 21.4 |
| Runtime 1950 | 128 | 1305.1 + 6.0 | 1413.1 | 95.8 | 923.3 | 273.7 / 61.2 | 364.4 / 217.7 | 60.00–60.00 / 26.2 |
| Runtime 1950 | 32 | 1296.0 + 7.7 | 1395.3 | 85.6 | 946.2 | 258.7 / 50.4 | 307.2 / 148.0 | 60.00–60.00 / 22.1 |
| Runtime 1980 | 128 | 1286.0 + 7.5 | 1385.9 | 86.6 | 946.4 | 273.8 / 61.0 | 389.3 / 209.7 | 60.00–60.00 / 24.6 |
| Runtime 1980 | 32 | 1259.3 + 7.9 | 1369.3 | 94.9 | 940.7 | 258.6 / 55.1 | 283.4 / 143.9 | 59.99–60.00 / 19.9 |
| Runtime 2000 | 128 | 1457.5 + 9.4 | 1561.0 | 85.4 | 962.2 | 275.5 / 67.4 | 326.5 / 210.9 | 60.00–60.00 / 21.7 |
| Runtime 2000 | 32 | 1425.3 + 8.0 | 1622.7 | 180.9 | 940.1 | 245.0 / 63.3 | 260.9 / 146.7 | 60.00–60.00 / 24.9 |
| Published 1788 | 128 | 34.3 | 87.5 | 35.5 | 879.1 | 219.2 / 32.1 | 232.1 / 55.8 | 60.00–60.00 / 21.1 |
| Published 2000 | 128 | 170.2 | 263.4 | 84.1 | 728.3 | 275.9 / 62.2 | 304.5 / 206.7 | 59.51–60.00 / 56.9 |

Prep is copy + validation for all history, index + extraction for runtime, and manifest digest + subset validation for published snapshots. The archive is opened locally in the runtime benchmark. Files imports also incur a security-scoped staging copy.

Default-buffer filtering brought sampled navigation footprint from **1,177.8 MiB** unfiltered to **59.3–217.7 MiB** over the seven dates. At 2000 it lowered the footprint by about 82%. Buffer 32 reduced each filtered date by a further 18.7–34.7%. The 2000 subset with buffer 32 reached **146.7 MiB**. The default-buffer 2000 renderer reached useful geometry in **85.4 ms** once the filtered source was submitted.

### Preprocessing and retained memory

- Runtime index took 1244.5–2224.4 ms. Peak RSS was 247.2–253.0 MiB and peak footprint was 30.5–31.7 MiB. Footprint after the step was 29.6–30.9 MiB.
- Runtime extraction took 1.9–9.4 ms. Peak RSS was 197.5–218.5 MiB and peak footprint was 30.8–35.0 MiB. Footprint after the step was 30.8–35.0 MiB.
- Published verification took 34.3–170.2 ms. Peak RSS was 210.4–225.5 MiB and peak footprint was 28.2–37.7 MiB. Footprint after the step was 28.2–37.7 MiB.

The legacy all-history copy-and-validation ended at 318.0–321.3 MiB RSS and 140.0–140.1 MiB footprint. Its transient maximum was not sampled. The separate 10 ms temporal sampler included file access and transient feature objects. Extraction windows can be shorter than the sampling interval. Each step also samples at its start and end. The cold runtime path holds a mapped archive during construction and decodes one feature at a time. After index creation, the runtime retains byte offsets and record metadata. The extraction phase reopens the archive briefly, copies exact feature slices, hashes the output, and writes a temporary file. Source addition then starts a separate MapLibre index. Heap allocation lifetimes and absolute transient peaks are **Not measured**.

The 2000 snapshot reached useful geometry 1561.0 ms after a cold runtime request and 263.4 ms after a precomputed request. Separate memory and timing observations are in the raw evidence. The two precomputed assets had the same per-date SHA-256 as runtime output.

## Warm date switching

Each sequence requests 21 switches between widely separated eras. It waits for geometry and idle before the next request. The 22nd load returns to January 1, 1788. Index creation happened once in each sequence. The table excludes the initial application load for warm timing ranges. RSS and footprint are sampled at idle.

| Buffer | Useful / idle loads | Warm extract ms | Warm request → useful ms | Start RSS / footprint, MiB | Return RSS / footprint, MiB | Max idle RSS / footprint, MiB |
|---|---:|---:|---:|---:|---:|---:|
| 128 | 22 / 22 | 3.3–14.9 | 43.3–133.6 | 213.1 / 37.0 | 299.5 / 42.2 | 309.2 / 66.4 |
| 32 | 22 / 22 | 5.2–16.2 | 39.3–133.1 | 220.8 / 34.4 | 280.1 / 40.6 | 284.2 / 59.1 |

| Date | Warm request → useful, buffer 128 median ms | Warm request → useful, buffer 32 median ms |
|---|---:|---:|
| 1788 | 53.2 | 41.3 |
| 1800 | 63.2 | 47.8 |
| 1860 | 82.4 | 81.0 |
| 1900 | 107.8 | 108.3 |
| 1950 | 104.5 | 122.2 |
| 1980 | 118.3 | 106.0 |
| 2000 | 120.7 | 123.0 |

After returning to 1788, footprint was 5.2 MiB above the initial idle sample with buffer 128 and 6.2 MiB above it with buffer 32. RSS remained higher than the starting sample for both profiles. The switch observations cover a finite sequence and one camera. Broader touring, long sessions, system memory pressure and actual frame presentation remain device gates.

## Geographic and interaction checks

The final Release build ran **22 renderer probes with each buffer** at zoom levels 6 and 10. They covered a small colonial county, coastal land and water, an independent city inside a polygon hole, points roughly 40 m on either side of that city boundary, opposite sides of an internal tile meridian, and the same Virginia location across two historical eras.

Each buffer's raw MapLibre query returned an extra Albemarle candidate in four Charlottesville probes. Confirming the rendered IDs against the source rings removed that candidate. All **44 of 44** confirmed hit sets matched the archive reference, including both sides of the sampled seam. Observed confirmation time ranged up to 9.2 ms in these isolated checks. This is a hit-testing check. Frame-by-frame seam visibility, exhaustive coastline and narrow-feature checks, and tiny features MapLibre omits at coarse zoom remain open.

The host checks passed in Honolulu and Auckland timezones with identical output. Seven documented before/effective transitions passed, including formation, renaming, and consolidation. Independent decoding checked the two offline assets against the source features and manifests, and a second publisher run produced byte-identical files. The original archive SHA-256 matched before and after the work. The bundled HTTP blocker remained attached during historical rendering.

The existing and new simulator suite passed all eight XCUITests. A four-test functional rerun passed with source-ring confirmation enabled. It exercised the bundled viewer, Files import and invalid JSON, date and provenance controls, selection reset, unsupported dates, and a rapid date burst that ended on the latest request. Release builds succeeded for the simulator and generic iOS target. No unexpected MapLibre HTTP requests appeared in the simulator log.

**Physical device** — Not measured. Both known devices were unavailable. Presented frame rates, Instruments allocation lifetimes, memory pressure behavior and device fidelity require on-device qualification.

See the [raw simulator observations](evidence/historical-temporal-2026-11-22/), [data audit](HISTORICAL_DATA_AUDIT.md), and [architecture recommendation](HISTORICAL_GEOJSON_ARCHITECTURE.md).
