# Albers 0.2 performance qualification

Environment on November 25, 2026 was an Apple M4 Mac16,1 with 24 GiB RAM, macOS 15.6, Xcode 26.3 (17C529), and one iPhone 17 Pro simulator running iOS 26.3. All app comparisons used ARM64 Release builds with MapLibre Native 6.31.0. Reported memory is Mach resident size and physical footprint. Sampled peaks can miss short bursts. The process clock is initialized inside the app, so first-draw times exclude launch work before that point.

## Controls and interpretation

1. The original research app at `1625fef` ran three fresh processes per renderer with an installed 2024 county package. This establishes a baseline outside the 0.2 code.
2. In one 0.2 Release binary, `--albers-baseline` selects the linear scan, duplicate decode and uncached preparation path. The default Albers path shares decoding, indexes geometry and caches scenes and presentations. MapLibre uses the same validated county derivative and candidate style. The measured navigation source comprises 3,153 visible-policy features in Albers. The later builds extract the palette adapter and add release and appearance instrumentation. The two projections have different regional presentation and culling.
3. Navigation windows are normalized to each backend's national scale about 39° N, 97° W, with 1×, 2×, 4× and 8× runs. Albers adds a 16× exploration. Albers uses an approximately three-second elliptical route with a nominal 16 ms camera update, and MapLibre uses four offsets and a return path over roughly six seconds. Ratios compare the same viewport intent. The route duration and motion profiles differ.
4. The initial three-renderer comparison includes a known host interruption near its first repetitions. The remaining cases ran after stopping the second simulator pilot. The raw files preserve all measurements including outliers. A cold process with an installed package is a different condition from a first installation.

Albers records completion of `UIView.draw`, including the application-side geographic draw time. MapLibre emits native render callbacks with first useful and idle markers. Neither callback is a display presentation signal. The simulator rejected the Instruments Animation Hitches template. Physical presented fps, missed frames and GPU timing remain open.

## Startup and preparation

| County case | Median first callback | Range | First callback endpoint |
| --- | ---: | ---: | --- |
| Original Albers | 1,202 ms | 1,188–1,514 ms | First election paint |
| Original MapLibre | 952 ms | 925–971 ms | First fully rendered election callback |
| Linear Albers, with 0.2 instrumentation | 1,652 ms | 1,401–3,680 ms | First geographic draw |
| Indexed Albers, with 0.2 instrumentation | 1,313 ms | 1,181–2,174 ms | First geographic draw |
| MapLibre, with 0.2 instrumentation | 1,416 ms | 1,056–1,505 ms | First useful frame |

The three-way median in the common binary was 339 ms lower for indexed Albers than for linear Albers. The new path removed one read and JSON decode. A typical county sample spent 88 ms decoding, 311 ms validating, 321 ms projecting and 1.65 ms building feature bounds, insets and the index. On the original baseline the repeated read and projection measured 418–423 ms together. Validation and actual projection dominate index construction. There is no tessellation or app-managed GPU upload in this strategy.

| Historical county snapshot | Archive index | Extract | Geometry ready after decode and projection | First draw after app clock start |
| --- | ---: | ---: | ---: | ---: |
| 1788-01-01 | 1,340 ms | 2.3 ms | 22 ms | 1,555 ms |
| 1860-01-01 | 1,272 ms | 5.0 ms | 91 ms | 1,537 ms |
| 1900-01-01 | 1,402 ms | 5.5 ms | 120 ms | 1,685 ms |
| 1980-01-01 | 1,455 ms | 7.5 ms | 131 ms | 1,750 ms |
| 2000-01-01 | 1,541 ms | 12.3 ms | 194 ms | 1,982 ms |

Historical archive validation dominates cold starts. Each normal view draws active records only. The full 17,727-record archive is indexed for temporal selection.

## Navigation and selection

| Zoom | Linear Albers callbacks/s | Indexed Albers callbacks/s | MapLibre callbacks/s | Indexed Albers p95 draw | Indexed Albers p95 callback gap | Linear → indexed candidates per callback |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1× | 0.8 | 0.8 | 60.0 | 7.8 ms | 1,219 ms | 3,886 → 3,625 |
| 2× | 6.9 | 7.0 | 60.0 | 5.1 ms | 151 ms | 3,877 → 2,515 |
| 4× | 22.3 | 22.0 | 59.8 | 2.6 ms | 57 ms | 3,271 → 1,087 |
| 8× | 58.5 | 58.2 | 60.0 | 0.9 ms | 18 ms | 3,271 → 338 |
| 16× | 59.0 | 58.7 | — | 0.3 ms | 17 ms | 3,271 → 97 |

The 1× results have three Albers samples each. At 8× and 16× the index cut bounds candidates by about 90% and 97%. Draw callback cadence was similar with both Albers paths. At lower zoom the app camera driver also produced fewer updates. At 8× the indexed app's p50/p95/p99 callback gaps were 16.7/17.7/28.8 ms, with 175 draws in the window. The raw files give all distributions and repeated 8× results.

In a separate diagnostic Time Profiler run of the indexed 2024 county route, 18,687 of 19,599 app main-thread samples included `CA::Transaction::commit()`. Core Graphics display-list replay appeared in 17,538 and `ripc_DrawPath` in 17,233. The `aa_render` leaf accounted for 7,230 of the main-thread samples. These stack counts overlap. The CPU stack places the high cost in deferred backing-store rasterization during Core Animation commit, downstream of the callback timer. The [app-only tally](evidence/albers-rendering-0.2/comparison/cpu-profile-summary.json) keeps system and other process traces local.

A separate M4 bitmap experiment isolated outline cost using the actual Census county paths in a 402 × 550 point viewport. At 3× backing scale and national zoom, individual fill-and-stroke took a median 419 ms, fill-only 42 ms, aliased fill-and-stroke 172 ms, and a stroke batched into one path per panel 5,810 ms. The batch combines many adjacent segments inside one raster operation. At 8× zoom, individual fill-and-stroke took 10 ms and batched stroke took 15 ms. A separate 0.85-pixel stroke probe showed highly variable results, including a 372 ms national median at 3× backing. Each median uses three measured calls after one warm-up, and the host had concurrent lab work. These are local Core Graphics raster costs, with different conditions from simulator presentation. They weigh against simply batching the county outlines or lowering fidelity as a general solution. [Raster experiment](Tests/AlbersRasterStudy.swift).

The ARM64 geographic harness compared the original scan and indexed exact hit for 5,000 probes per dataset. County totals were 55.2 → 4.9 ms for the Census layer and 154.6 → 58.5 ms for the election derivative. These are batch costs measured outside UI. Using the index was slower in one low-count state batch (68.8 → 83.3 ms), and still below the tap budget. The 45-second indexed simulator switch route saw a maximum 0.3 ms from selection entry to update of the inspector callback, measured separately from gesture recognition and screen presentation.

## Election style and scheduling

The alternating-year lab pilot exercised actual 2020/2024 county presentations, rapid cancellations, inset transitions and style changes. Linear Albers completed 7 year requests in a 47.4-second window. Indexed Albers completed 63 in 51.7 seconds with zero sampled year or mode mismatches. Median request-to-dataset-ready fell from 1,531 to 5.3 ms. The indexed route still had a maximum request-to-dataset-ready of 5,015 ms and a p95 of 946 ms from request to the study's next ready observation. The latter includes observation scheduling, and the timer often woke late. The short trials have unequal counts and characterize timing mechanisms and outliers.

In the linear pilot a request took 4,985 ms to enter its MainActor task. A query interval in the same dataset also took 6,831 ms in one sample, compared with a 175 ms median. In the indexed pilot the median query/cache section was 0.5 ms, and the longest query section still took 5,000 ms. A new CPU map preparation was absent on same-source years. These observations isolate delays outside geographic drawing. The query interval includes SwiftData awaits and scheduler delay, so these data cannot attribute the outliers to SQLite or the render server individually.

The indexed election-paint callback reached a median of 7.7 ms and a maximum of 29.3 ms in the short trial. Independent theme, border and highlight counters label updates that reach a draw. The short run cannot promise that all warm switches complete within 250 ms or that final pixels always appear within 50 ms.

A final simulator regression held the same 3,153-feature county scene through five appearance operations. Its one geometry-ready event occurred before the first change.

| Separate user action | Update to end of application draw |
| --- | ---: |
| Candidate colors to reported margin | 52.7 ms |
| Highlight Cuyahoga County | 77.1 ms |
| Clear selection | 7.3 ms |
| Reported margin to alternate palette | 40.2 ms |
| Emphasize borders | 9.5 ms |

These are single application-side samples. The selection and highlight operations used the focused county viewport. [Raw appearance events](evidence/albers-rendering-0.2/verification/independent-style-operations.jsonl).

## Memory

The indexed renderer sampled a peak of 107.4 MiB physical footprint on the county navigation route. Linear Albers reached 103.0 MiB. Indexed first-draw checkpoints in the three-way startup trial ranged from 95.1 to 101.4 MiB. These samples fit inside the provisional simulator county envelope of about 200 MiB. The index and bounded reuse trade some retained memory for less repeated work. Core Graphics and UIKit own the platform rendering resources in this study. Device GPU allocation remains open.

## Thirty-minute stress and resource lifetime

The independent route lasted 1,800.4 seconds. It attempted 100 alternating county-election year requests, exercised 10 additional superseded requests, changed sources 50 times and recorded 663 successful picks. It repeatedly moved through national, state, feature and inset framing and changed styles. The full log contained 100 election commits, including the return to county results after source churn.

One early 2020 request exceeded the harness's 15-second ready deadline. The request waited 9.7 seconds before entering the MainActor task, and the store-ready event arrived at 20.0 seconds. The previous 2024 map remained active until the next request superseded the timed-out one. Independently replaying the trace found zero commits inconsistent with the most recent request and zero paints inconsistent with their most recent commit.

| Long-run sample | Median | p95 | Maximum |
| --- | ---: | ---: | ---: |
| Election request to commit | 3.3 ms | 8.7 ms | 6,527 ms |
| Election request to ready observation, including the timeout | 27.4 ms | 30.3 ms | 19,976 ms |
| Election paint callback | 12.1 ms | 53.8 ms | 363.7 ms |
| Selection entry to inspection callback | 0.1 ms | 1.2 ms | 118.5 ms |
| Camera transition elapsed | 968 ms | 9,175 ms | 19,188 ms |

The study's frame and request queues still miss their targets in the tails. Camera timings use the simulator's display-link runtime. After four minutes and 50 source changes, 89 physical-footprint checkpoints ranged from 133.5 to 136.5 MiB with a 136.0 MiB median. The sampled peak over the route was 138.9 MiB during historical extraction. Dismissing the map and dropping its geometry and style caches reduced footprint from 136.3 to 97.6 MiB at the release sample. These samples demonstrate bounded footprint during this simulator workload. [Stress summary](evidence/albers-rendering-0.2/stress-summary.json) · [Compressed raw trace](evidence/albers-rendering-0.2/stress.jsonl.gz).

## Evidence and follow-up

The [raw logs](evidence/albers-rendering-0.2/) include the six original launch files, three-way simulator route, geometry checks and study runs. The [physical-device protocol](ALBERS_QUALIFICATION_REPORT.md#physical-device-protocol) distinguishes scheduler delays, application drawing, render-server work and actual display presentations.
