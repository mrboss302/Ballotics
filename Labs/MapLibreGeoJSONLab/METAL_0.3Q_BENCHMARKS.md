# Metal 0.3Q benchmarks

All rates below are simulator callbacks per second. A Metal completion timestamp describes command execution. Actual drawable presentation intervals and missed physical display deadlines require the device protocol in the README.

## Method and independent baseline

The host was an Apple M4 with 24 GiB, macOS 15.6, Xcode 26.3 (17C529), and Apple Metal compiler 32023.864. The matrix used the iPhone 17 Pro on iOS 26.3.1, portrait, with the simulator's existing Accessibility Medium text size. The election drawable was 1,110 × 1,138 pixels in the new and baseline runs. Everything used Release and the pinned MapLibre Native 6.31.0 package.

The preserved 0.3 Release app ran one fresh process per old configuration. National election counties measured 1.84 callbacks/s in Core Graphics, 59.06 in fill-only Metal, 58.63 with single-sample strokes, 49.53 with 4× strokes, and 59.67 in MapLibre. Thus the 4× rerun was below the archived three-run median of 59.0. The recreated mesh counts and containment results matched. [Independent baseline logs](evidence/albers-metal-0.3q/baseline)

Each cell in the workload matrix used a fresh app process. The camera advanced along the existing elapsed-time trajectory for three seconds each at 1×, 2×, 4×, 8×, 16× and repeated 8×. Both Metal configurations used the same projected geometry, style, original-ring strokes and four samples. Changing the presentation scale changes on-screen area and fragment coverage. The Core Graphics control used the thematic layout. MapLibre used the same source and results with its Mercator fit, tile processing and delegate callbacks. Its national viewpoint and insets differ from Albers.

The matrix and quality trials ran serially on the 17 Pro. A second simulator on the same host ran the lifecycle route. This matters for scheduling, GPU contention and first-use timing. The logs preserve the number of callbacks, gaps, timing distributions and unavailable observations per window. [All 28 workload logs and distributions](evidence/albers-metal-0.3q/qualification)

| National workload | Core Graphics | Metal research | Metal thematic | MapLibre |
| --- | ---: | ---: | ---: | ---: |
| Census counties, 3,144 Albers features | 3.1 | 56.5 | 57.0 | 59.7 |
| Election counties, 3,153 | 1 callback¹ | 58.7 | 56.8 | 60.0 |
| Modern states and DC, 51 | 4.9 | 56.6 | 57.1 | 60.0 |
| Congressional districts, 436 | 2.8 | 56.7 | 57.0 | 60.0 |
| 1788 county snapshot, 332 | 56.4 | 57.0 | 57.0 | 60.0 |
| 2000 county snapshot, 3,139 | 16.8 | 56.6 | 56.9 | 60.0 |
| 2024 electoral states using 2000 boundaries, 51 | 48.1 | 56.4 | 56.5 | 60.0 |

¹ The Core Graphics election overview observed one callback in a 3.821-second window. Its inter-callback rate is undefined. This observation remains in the raw file.

For the dense election matrix run, first Core Graphics draw occurred 793.8 ms after the geometry request. Metal's first successful completion occurred at 1,040.0 ms for research and 1,542.2 ms for thematic. MapLibre reported its first useful frame at 675.4 ms and idle-with-geometry at 1,332.7 ms. These entry points represent different stages in the respective renderers. In that thematic run decode took 100.1 ms, source validation 458.6 ms, mesh preparation 297.5 ms including 207.9 ms of tessellation, and upload 5.9 ms. Projection, temporary preparation peaks, submission, encoder and drawable acquisition fields are in the per-run logs.

## Regional camera

After a fresh national election-county load, the route animated to AK, panned and magnified over three seconds, repeated for HI and returned to national. The same data and camera operations ran through all three Albers configurations. [Regional measurements](evidence/albers-metal-0.3q/regions)

| Renderer | AK transition ms | AK navigation callbacks/s | HI transition ms | HI navigation callbacks/s | National return ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| Core Graphics thematic | 5,213 | 1.3 | 819 | 59.3 | 7,156 |
| Metal research | 289 | 58.7 | 288 | 58.7 | 289 |
| Metal thematic | 292 | 58.7 | 288 | 58.9 | 303 |

The slow Core Graphics national return and AK navigation are visible when following original paths at this density. Smooth Metal callback timing on the simulator supports continuing the architecture. It provides no physical 60 Hz guarantee.

## Shader startup and sample count

Three fresh Release processes per configuration on the 17 Pro used the same thematic county map. The OS and driver caches were allowed to warm. Numbers separated by slashes are median, P95, P99 and maximum across the three launches. With three trials the upper three quantiles coincide. Pipeline time includes both the fill and stroke states. First completion includes model and geometry preparation. [Raw quality trials](evidence/albers-metal-0.3q/quality)

| Library / samples | Library ms | Pipeline ms | Launch to first completion ms | National callbacks/s | National GPU P95 ms¹ | Attachment MiB² | First-submit footprint MiB³ |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: |
| samples-1 | 0.7 / 0.9 / 0.9 / 0.9 | 2.2 / 163.5 / 163.5 / 163.5 | 1479.2 / 1612.4 / 1612.4 / 1612.4 | 57.1 | 0.2 | 9.64 | 103.2 |
| samples-2 | The requested Metal device or sample count is unavailable. | — | — | — | — | — | — |
| samples-4 | 0.9 / 1.0 / 1.0 / 1.0 | 2.7 / 3.4 / 3.4 / 3.4 | 1384.9 / 1394.5 / 1394.5 / 1394.5 | 56.8 | 0.2 | 43.37 | 101.3 |
| runtime-library | 1.7 / 387.9 / 387.9 / 387.9 | 2.2 / 71.8 / 71.8 / 71.8 | 1453.0 / 1888.6 / 1888.6 / 1888.6 | 57.1 | 0.2 | 43.37 | 100.7 |

¹ Median of three per-window P95 command durations. ² One resolve color texture plus depth and any multisample color texture, before compression, pooling and extra drawables. ³ Task physical footprint on the simulator. Attachment storage and process footprint measure different things.

Build-time compilation is the default. The slowest observed runtime source-library call took 387.9 ms, versus 1.0 ms for the slowest compiled-library load at four samples. Warm runtime loads were much faster. Pipeline construction remains a separate cost. The first single-sample pipeline trial took 163.5 ms. Xcode's normal Metal source phase builds both simulator and device libraries. A text resource alias supplies the same shader to the optional runtime comparator. A missing shader entry point and an unsupported 2× sample request both reached the Core Graphics fallback.

Single sampling lowered nominal election attachment storage by 33.73 MiB. The 2× request fell back on all three simulator launches, so there is no 2× GPU timing or image-quality measurement on this target. Keep four samples as the full-quality reference pending device evidence.

## Thematic submission outliers

The archived 0.3 soak contains 5,977 thematic update-to-submission samples. Six exceeded one second, including four above two seconds. Their durations were 3,207.4, 3,028.0, 1,848.3, 2,429.4, 2,608.9 and 1,934.5 ms. The style upload preceding the longest sample recorded 10.0 ms for preparation and 0.3 ms for copying. The old trace lacks a before-draw timing boundary, so the cause of those six stalls remains unresolved. [Archived raw trace](evidence/albers-metal-0.3/lifetime/stress-30m.jsonl.gz)

The corrected 30-minute run recorded 5,854 thematic submissions. Median, P95, P99 and maximum update-to-submission delays were 20.2, 25.3, 27.5 and 550.7 ms. Of the longest interval, 550.4 ms elapsed before the draw entered CPU submission. Submission took 0.3 ms, including 0.2 ms for drawable acquisition. The preceding style upload took 10.3 ms to prepare and 0.3 ms to copy. This localizes the largest observed delay to the period before drawing. The trace cannot distinguish OS scheduling, UI invalidation/coalescing and host contention within that period. No final-run thematic sample exceeded one second. [Final raw trace](evidence/albers-metal-0.3q/lifetime/stress.jsonl.gz) · [Interaction and memory distributions](METAL_0.3Q_REGRESSION_REPORT.md#final-30-minute-lifecycle-qualification)

## Dense national comparison with the soak stopped

Three additional fresh processes per configuration ran after the second simulator finished its stress route. This cohort used the baseline device, text size, dataset and motion. Cells are median callback rates across runs that had at least two callbacks. Individual windows with a single callback remain in the logs. [Dense control traces](evidence/albers-metal-0.3q/dense)

| Configuration | National | 2× | 4× | 8× | 16× | 8× repeat |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Core Graphics thematic | undefined | 4.7 | 33.2 | 56.0 | 56.0 | 56.3 |
| Metal research | 56.7 | 56.3 | 56.5 | 56.4 | 56.5 | 56.4 |
| Metal thematic | 56.5 | 56.6 | 56.7 | 56.6 | 56.4 | 56.6 |
| MapLibre | 59.7 | 60.0 | 59.7 | 60.0 | 60.0 | 60.0 |

| Thematic Metal national window | Trial 1 | Trial 2 | Trial 3 |
| --- | ---: | ---: | ---: |
| Callback gap ms | 18.0 / 19.4 / 20.6 / 27.0 | 17.8 / 19.3 / 20.4 / 29.0 | 18.0 / 19.7 / 24.9 / 33.5 |
| CPU submission ms | 0.1 / 0.2 / 1.4 / 1.6 | 0.2 / 0.3 / 1.3 / 1.4 | 0.2 / 0.3 / 5.6 / 7.6 |
| GPU execution ms | 0.2 / 0.3 / 0.7 / 9.2 | 0.2 / 0.3 / 0.5 / 6.5 | 0.2 / 0.3 / 0.9 / 19.9 |
| Gaps over 25 ms | 1.0 | 1.0 | 1.0 |

Gap and command timings are median / P95 / P99 / maximum within each national window. Gaps over 25 ms are scheduling observations. The simulator provides no physical deadline-miss count.

Core Graphics recorded one national callback in each trial. Its three windows lasted 3.694, 3.672 and 3.668 seconds. An inter-callback rate requires two callbacks. [Machine-readable distributions](evidence/albers-metal-0.3q/dense/summary.json.gz)
