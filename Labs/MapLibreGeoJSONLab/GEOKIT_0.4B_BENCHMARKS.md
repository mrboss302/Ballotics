# BalloticsGeoKit 0.4B Release benchmarks

The same iPhone 16e simulator, iOS 26.3.1 runtime on the Apple M4 MacBook Pro (24 GB RAM), Release arm64 build, thematic layout, native viewport, full borders and 4× MSAA were used for both versions. The dense 2024 county workload used three fresh launches per version. Each other map used one launch per version. The 8× statistic includes the two 8× windows per run. Both cohorts ran alongside other qualification work, and the 0.4B cohort overlapped an iPad simulator lifecycle session. Preparation and process-memory differences are observational. Previous 0.4A published figures used a different device and session. Simulator callback rate and GPU completion are separate from physical presentation.

| Workload | 1× callbacks/s | 8× callbacks/s | 1× P95 gap (ms) | Request to GPU completion (ms) |
| --- | ---: | ---: | ---: | ---: |
| election-counties | 58.83 → 58.86 | 58.72 → 58.81 | 17.50 → 17.30 | 1,628.00 → 1,201.20 |
| census-counties | 58.80 → 58.52 | 58.37 → 58.70 | 17.20 → 18.90 | 961.90 → 1,317.90 |
| districts | 58.79 → 58.85 | 58.53 → 58.60 | 17.30 → 17.70 | 494.30 → 757.80 |
| states-1788 | 57.62 → 58.37 | 57.92 → 58.43 | 18.20 → 17.60 | 184.90 → 327.40 |
| states-2000 | 57.01 → 57.70 | 57.77 → 57.61 | 19.30 → 18.30 | 261.00 → 240.50 |
| counties-1788 | 58.87 → 58.93 | 58.67 → 58.68 | 17.20 → 17.30 | 1,407.20 → 1,551.90 |
| counties-2000 | 58.80 → 58.64 | 58.59 → 58.58 | 17.20 → 17.40 | 1,492.50 → 2,696.70 |

## Dense county preparation and first frame

Values are median / P95 / P99 / maximum in milliseconds, except the footprint rows. With three observations the two tail percentiles equal the maximum. Preparation stages can overlap.

| Measurement | 0.4A | 0.4B |
| --- | ---: | ---: |
| Decode | 137.90 / 194.70 / 194.70 / 194.70 | 130.30 / 158.60 / 158.60 / 158.60 |
| Validation | 517.70 / 644.00 / 644.00 / 644.00 | 360.80 / 499.40 / 499.40 / 499.40 |
| Projection | 469.92 / 723.44 / 723.44 / 723.44 | 352.72 / 507.84 / 507.84 / 507.84 |
| Spatial index and layout | 2.98 / 3.09 / 3.09 / 3.09 | 2.41 / 2.58 / 2.58 / 2.58 |
| CPU mesh preparation | 402.70 / 427.20 / 427.20 / 427.20 | 264.50 / 370.00 / 370.00 / 370.00 |
| Tessellation within preparation | 270.10 / 287.10 / 287.10 / 287.10 | 178.10 / 250.40 / 250.40 / 250.40 |
| Compiled shader library load | 0.90 / 1.20 / 1.20 / 1.20 | 1.50 / 1.70 / 1.70 / 1.70 |
| Pipeline initialization | 3.00 / 3.60 / 3.60 / 3.60 | 3.10 / 3.30 / 3.30 / 3.30 |
| Metal buffer allocation and copy | 5.90 / 7.40 / 7.40 / 7.40 | 4.60 / 6.10 / 6.10 / 6.10 |
| Request to GPU completion | 1,628.00 / 2,121.70 / 2,121.70 / 2,121.70 | 1,201.20 / 1,649.10 / 1,649.10 / 1,649.10 |
| First CPU submission | 4.00 / 4.00 / 4.00 / 4.00 | 3.00 / 3.00 / 3.00 / 3.00 |
| First measured GPU command | 8.10 / 9.00 / 9.00 / 9.00 | 19.40 / 23.40 / 23.40 / 23.40 |
| Temporarily sampled preparation footprint, MiB | 122.13 / 122.17 / 122.17 / 122.17 | 111.49 / 116.88 / 116.88 / 116.88 |
| Footprint during 1× navigation, MiB | 105.05 / 105.11 / 105.11 / 105.11 | 104.52 / 105.42 / 105.42 / 105.42 |

## Navigation and regional transitions

Each cell is the median of the per-window summary. The raw evidence retains every 1×, 2×, 4×, 8× and 16× window.

| Dense 1× measurement | 0.4A | 0.4B |
| --- | ---: | ---: |
| Callback gap P50 / P95 / P99 / max (ms) | 16.80 / 17.50 / 19.50 / 32.10 | 17.00 / 17.30 / 20.40 / 29.20 |
| CPU submit P50 / P95 / P99 / max (ms) | 0.10 / 0.30 / 2.10 / 2.20 | 0.10 / 0.20 / 1.10 / 1.80 |
| GPU command P50 / P95 / P99 / max (ms) | 0.10 / 0.20 / 1.00 / 8.00 | 0.10 / 0.20 / 1.00 / 6.90 |
| Camera updates per 3-second window | 177.00 | 177.00 |

| Region route | Alaska / Hawaii / mainland transition (ms), 0.4A | Same route, 0.4B |
| --- | ---: | ---: |
| thematic | 290.30 / 305.90 / 291.00 | 304.30 / 305.90 / 290.30 |
| research | 307.70 / 308.60 / 287.80 | 307.40 / 305.50 / 288.90 |
| cg | 4,827.50 / 289.80 / 4,590.80 | 5,016.40 / 288.50 / 4,849.00 |

## Label layout

The 2024 route exercises native labels during navigation. The 1976 route runs Alaska, Hawaii and mainland focus. Durations include native overlay assignments.

| Route | Layout calls | Median / P95 / P99 / max (ms) | Calls above 4 ms | Mesh publications |
| --- | ---: | ---: | ---: | ---: |
| labels | 1012 | 0.30 / 0.50 / 0.60 / 0.90 | 0 | 1 |
| focus | 396 | 0.20 / 0.30 / 0.50 / 0.50 | 0 | 1 |

[Fresh-process JSONL traces and regional comparisons](evidence/geokit-0.4b/benchmark) · [Host mesh and geography checks](GEOKIT_0.4B_MESH_PARITY.md)

## Lifecycle interaction timings

Median / P95 / P99 / maximum, in milliseconds.

| Interaction | iPad A16 · extraction build |
| --- | ---: |
| Geographic selection | 0.10 / 0.20 / 0.40 / 11.00 |
| Warm election commit | 7.45 / 16.80 / 23.10 / 599.60 |
| Style CPU preparation | 9.50 / 10.60 / 11.00 / 54.60 |
| GPU style-buffer allocation and copy | 0.30 / 0.50 / 0.70 / 20.10 |
| Requested source to observed completion | 330.25 / 686.60 / 2,194.70 / 2,194.70 |

[Full resource results and traces](GEOKIT_0.4B_RESOURCE_LIFETIME.md)
