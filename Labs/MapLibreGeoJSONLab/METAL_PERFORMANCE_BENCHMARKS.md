# Metal performance benchmarks

The principal workload is the 2024 presidential county-results derivative. It contains 3,153 supported features, including eight traditional Connecticut counties and 40 Alaska State House districts. The same county source and result data are used for all five configurations. The benchmark runs the lab's Release build in portrait on an iPhone 17 Pro simulator with iOS 26.3.1 (23D8133) on an Apple M4 Mac with 24 GiB of RAM and macOS 15.6. Xcode 26.3 build 17C529 compiled the lab.

## Method

The cohort runs three fresh app processes per configuration. The on-disk election packages are warm and projected geometry and GPU buffers start fresh in each process. All three Metal modes use the same mesh, style model and camera. Fill only uses one sample. The boundary cohort adds the original ring strokes at one sample. The full-quality cohort adds 4× MSAA. Optimized Core Graphics uses its normal path, index, stroke and antialiasing settings.

The Albers camera moves through 1×, 2×, 4×, 8×, 16× and a repeated 8× window. Each window lasts three seconds. Both Albers renderers sample the same elapsed-time geographic trajectory and use their shared projection, viewport and scale. The MapLibre control shares the geographic centers, trajectory, window duration and zoom multipliers relative to its contiguous U.S. fit. Its Mercator scale and viewport cannot reproduce the equal-scale insets. MapLibre tiles and simplifies the shape source and uses its own border rendering. The two Albers draws compare equivalent geographic coverage, with their differences in stroke opacity, joins and corner decoration quantified in the cartographic report.

The logs separate request-to-commit from source preparation and first frame callbacks. Metal measures CPU submission, command encoding, drawable/encoder acquisition, and GPU command execution from command-buffer timestamps. The CPU timing covers application submission only. It excludes downstream Core Animation composition and physical scan-out. MapLibre callback timings measure the delegate events, and Core Graphics callback timings exclude delayed raster work in Core Animation. All callback rates and gap distributions reflect that distinction.

Metal reports actual presented samples only where the drawable presentation API is available. The simulator build has no drawable presentation callback. The generator writes zero presented samples for its windows. Display deadlines, physical refresh rate and end-to-end presentation must be measured on physical hardware. [Apple drawable presentation API](https://developer.apple.com/documentation/metal/mtldrawable/presentedtime).

## Input revision

| Source | SHA-256 |
| --- | --- |
| Election county derivative | `ae1aef2e5da4d499a86875bafdf8837ef8bfb504866165e05fa4edf3282582e0` |
| Census counties | `335cd6da8cba27ce285334d5a6390fc1bf37a4109a8c5da6e4cf3b7fd9e80693` |
| Census states | `2a5b936075f5e107973d8c10754ef035e13fc291c141b53ea6425305c5761b83` |
| Census 119th congressional districts | `ef1cb8d92c1359defca5624db21012eeb25aef95585ba0c6eea6a3ffa508e11c` |
| Historical county archive | `af037e36183c9274d8be905b966ba775b548b6ec5210360c5814a7b952133a7b` |
| Historical state and territory archive | `9455ea39db4490497bad7f64b514d550166833f583b224dc727a2fb42ce42fbc` |

## Matched simulator results

Each cell is the median callback frequency across three fresh processes. These are app callbacks per second.

| Configuration | 1× | 2× | 4× | 8× | 16× | 8× repeat |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Optimized Core Graphics | 1.9 | 6.5 | 23.8 | 58.4 | 59.0 | 58.7 |
| MapLibre Native | 60.0 | 60.0 | 60.0 | 60.0 | 60.0 | 60.0 |
| Metal fills, 1× sample | 59.0 | 58.8 | 58.7 | 58.7 | 58.6 | 58.8 |
| Metal fills and strokes, 1× sample | 58.9 | 58.8 | 58.7 | 58.8 | 58.8 | 58.6 |
| Metal fills, strokes and 4× MSAA | 59.0 | 58.8 | 58.7 | 58.6 | 58.6 | 58.7 |

The 4× Metal configuration sustained 58.35–59.16 callback/s at 1×. Its median-of-run P95 callback gap was 17.4 ms and median P99 was 19.6 ms. Across those three national windows the longest callback gap was 33.3 ms, with up to three gaps above 25 ms in a run. Core Graphics produced up to 639.6 ms gaps at 1×. Its application draw callback P95 ranged from 6.8 to 9.7 ms while callback frequency remained low. The prior Core Animation rasterization findings explain why the measured drawing method omits much of the control's CPU cost.

| National draw costs | Fill only, 1× sample | Fills and strokes, 1× sample | Fills and strokes, 4× MSAA |
| --- | ---: | ---: | ---: |
| Draw calls per frame with all panels | 3 | 6 | 6 |
| Geographic segments submitted | 0 | 233,230 | 233,230 |
| Median of CPU submission P95 | 0.2 ms | 0.2 ms | 0.2 ms |
| Median of GPU execution P95 | 0.1 ms | 0.1 ms | 0.2 ms |
| Median of GPU execution P99 | 0.7 ms | 1.1 ms | 1.0 ms |
| Worst measured GPU command across national windows | 1.1 ms | 10.7 ms | 19.4 ms |
| Median first-command GPU duration | 7.3 ms | 6.7 ms | 8.9 ms |
| Maximum sampled navigation footprint, all zoom windows | 103.9 MiB | 106.7 MiB | 105.9 MiB |

Each all-panel draw referenced 334,393 vertices and 979,986 indices including the inset decorations. Exact segment deduplication reduced the full edge stream from 334,394 to 233,230 segments. Indexed bounds listed 3,147–3,149 candidate features at the last sampled 1× camera position. Camera movement reused the same mesh and its buffer allocations throughout the route. Optimized Core Graphics peaked at 105.8 MiB across navigation windows and MapLibre at 196.5 MiB in this cohort.

The 1× and 4× variants produced similar submission rates. Metal timestamp values are often at the 0.1 ms reporting resolution, and the worst GPU commands were longer with geographic strokes and 4× MSAA. Treat that tail and the resource estimates as the measurable cost signals here. The simulator offers limited evidence for pass-by-pass GPU cost. A 1,110×1,138 election drawable estimated 9.64 MiB of single-sample color and depth, or 43.37 MiB with a 4× color attachment, 4× depth and one single-sample resolve. This is 33.73 MiB in additional nominal attachment bytes. Actual physical residency, tile-memory behavior and system-owned buffering require device profiling.

### Preparation and first render signals

| Fresh-process median | Optimized CG | MapLibre | Metal fill 1× | Metal strokes 1× | Metal strokes 4× |
| --- | ---: | ---: | ---: | ---: | ---: |
| Election request to first render signal | 1,631.4 ms | 813.6 ms | 2,008.6 ms | 1,479.9 ms | 1,329.0 ms |
| GeoJSON decode | 155.5 ms | 107.5 ms | 155.0 ms | 122.3 ms | 127.9 ms |
| Validate geometry | 536.3 ms | 330.2 ms | 517.9 ms | 334.6 ms | 332.1 ms |
| Albers projection | 577.0 ms | — | 568.1 ms | 347.5 ms | 335.0 ms |
| CPU mesh preparation | — | — | 396.6 ms | 426.8 ms | 256.2 ms |
| Tessellation inside mesh preparation | — | — | 292.8 ms | 291.4 ms | 183.7 ms |
| Shared buffer allocation and upload | — | — | 5.1 ms | 8.0 ms | 4.9 ms |

The first render signal is a Core Graphics draw callback, MapLibre's first useful frame callback, or Metal submission. Full-quality Metal mesh preparation ranged from 183.9 to 447.5 ms with 4.0–6.9 ms for buffer allocation and upload. CPU preparation covers the same geometry in all Metal configurations and varies with host scheduling and caches. The numbers cannot attribute startup variations to the antialiasing setting. Runtime shader compilation contributed a 322.7 ms first-use cost in the initial smoke test, and pipeline setup had a 16.8 ms median in the full-quality cohort after the host shader cache warmed.

[All 15 raw runs and aggregate](evidence/albers-metal-0.3/comparison/summary.json).

## Instruments diagnostics

A separate eight-second Metal System Trace completed during the long simulator run. Its export provided 15,002 command completion rows without a usable app attribution table. The exported application submissions, presented handlers and allocated-size tables had zero rows. Use the app's command-buffer timestamps for its GPU metrics. System completion rows cannot recover displayed frame intervals or the lab's GPU-resident bytes.

A five-second, app-scoped Time Profiler sample of the steady stress loop had 790 one-millisecond sample rows, with 640 on the main thread. Of those, 231 contained `AlbersMetalSurface` and 92 contained a Core Animation transaction. The categories can overlap. The interactive loop still spends CPU time on preparation, encoding and transactions. This separate steady sample does not attribute cold-start SwiftData scheduling delays or physical rendering stalls. [Profiling aggregates](evidence/albers-metal-0.3/profiling/simulator-aggregate.json).

## Interaction and same-source styling

The 30-minute route first alternated county election years, then changed sources, then continued navigation and recoloring. A single county mesh served the first 100 observed years. It received 198 palette uploads during that phase, including the theme changes in the route. Ten extra requests were superseded before commit. The result-paint trace agreed with the latest committed election. The 2020 and 2024 UI test also exercised margin styling, alternate palette, border width, preserved Hawaii framing, Connecticut and Alaska inspection, and the boundary-vintage disclosures.

| Operation | Samples | Median | P95 | P99 | Maximum |
| --- | ---: | ---: | ---: | ---: | ---: |
| Geographic selection processing | 6,081 | 0.1 ms | 0.2 ms | 0.3 ms | 5.1 ms |
| Highlight update to Metal submission | 150 | 8.4 ms | 15.4 ms | 16.2 ms | 16.3 ms |
| Style buffer construction | 6,132 | 9.8 ms | 10.8 ms | 11.4 ms | 63.2 ms |
| Style buffer copy | 6,132 | 0.3 ms | 0.5 ms | 0.8 ms | 6.7 ms |
| Thematic style update to Metal submission | 5,977 | 18.0 ms | 26.6 ms | 82.8 ms | 3,207.4 ms |
| MainActor election-task scheduling | 111 | 0.5 ms | 2.1 ms | 6.4 ms | 6.6 ms |
| Warm year request to model commit | 100 | 3.6 ms | 5.3 ms | 6.9 ms | 139.6 ms |
| Year request to observed ready UI | 100 | 27.1 ms | 27.5 ms | 148.1 ms | 1,328.0 ms |
| Source switch to observed ready UI | 50 | 341.1 ms | 714.2 ms | 1,618.7 ms | 1,618.7 ms |
| Completed camera transition | 658 | 282.8 ms | 289.0 ms | 292.5 ms | 7,809.6 ms |

The first 2024 model commit took 1,070.4 ms. The 1,618.7 ms source outlier was the first historical county request and included 1,526.3 ms of archive indexing and 17.5 ms of mesh preparation. The longest transition crossed the background interval. Three multi-second thematic submission outliers occurred during the steady portion while the host also ran builds, Instruments or another simulator. The logs cannot isolate their cause. These remain in the distributions. The previous long MainActor election-entry stalls did not recur in this run. The ready-UI observation samples application state every 25 ms and can reuse the ready status for an existing same-source map. A Metal submission measures when the draw reached the command queue. Time to visible update on hardware remains an open measurement.

[Interaction source data and distributions](evidence/albers-metal-0.3/lifetime/stress-30m-summary.json).

A separate one-process-per-configuration recheck after narrowing layout invalidations produced 2.0 national callbacks/s for Core Graphics, 59.7 for MapLibre, 58.9 for Metal fills, 59.0 for Metal strokes at 1× samples and 58.3 for full-quality 4× Metal. The original three-process cohort remains the primary comparison. [Layout recheck](evidence/albers-metal-0.3/layout-recheck/summary.json).
