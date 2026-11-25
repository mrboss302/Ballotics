# BalloticsGeoKit 0.4A benchmarks

Release arm64 ran on the iPhone 17 Pro simulator, iOS 26.3.1, on the same M4 host used for 0.3Q. The table compares the preserved branch at `3ed7b61b` to the extracted package. Each run exercised the existing six-window camera route. Electorally styled counties have three launches per renderer and version. Districts and electoral states have one each. The rates are simulator callback observations. Physical presentation timestamps remain unavailable. The archived baseline ran in an earlier host session. The current controls overlapped some other host work, and local free space was tight. An interrupted preliminary cohort is retained separately and excluded from this table. Host load and caching limit causal attribution.

| Geography | Renderer | 1× callbacks/s, old → new | 8× callbacks/s, old → new | 1× P95 gap, ms | 1× sampled peak, MiB |
| --- | --- | ---: | ---: | ---: | ---: |
| election-counties | thematic | 56.5 → 59.0 | 56.6 → 58.9 | 19.4 → 17.3 | 103.0 → 102.7 |
| election-counties | research | 56.7 → 58.7 | 56.4 → 58.7 | 19.4 → 17.4 | 101.3 → 106.3 |
| election-counties | cg | — → — | 56.0 → 58.7 | — → — | 96.8 → 98.1 |
| election-counties | maplibre | 59.7 → 60.0 | 60.0 → 60.0 | 17.1 → 16.9 | 98.3 → 121.4 |
| districts | thematic | 57.0 → 58.2 | 56.9 → 58.8 | 19.4 → 18.3 | 62.5 → 62.8 |
| districts | research | 56.7 → 59.0 | 56.8 → 58.8 | 19.3 → 17.3 | 62.4 → 62.7 |
| districts | cg | 2.8 → 2.1 | 56.2 → 58.9 | 386.5 → 500.3 | 73.8 → 80.4 |
| districts | maplibre | 60.0 → 60.0 | 60.0 → 60.0 | 16.9 → 16.8 | 81.6 → 80.1 |
| electoral-states | thematic | 56.5 → 57.8 | 56.5 → 57.8 | 19.4 → 17.9 | 43.0 → 43.0 |
| electoral-states | research | 56.4 → 58.4 | 57.0 → 58.1 | 19.6 → 17.7 | 43.0 → 43.3 |
| electoral-states | cg | 48.1 → 46.1 | 56.4 → 58.3 | 24.3 → 26.4 | 54.7 → 56.3 |
| electoral-states | maplibre | 60.0 → 60.0 | 59.7 → 60.0 | 16.8 → 16.9 | 33.7 → 32.9 |

The national dense Core Graphics window produced a single frame, too few to calculate a rate. Its 8× viewport culls much of the geography. The Metal national dense navigation target held in these trials. Cross-run differences in footprint and preparation time remain visible, especially in the MapLibre control. Single-trial controls establish directional evidence only. Raw events include CPU encode/drawable timings, camera update counts, gap distributions, GPU samples and all six zoom windows. [Current raw trials](evidence/geokit-0.4a/benchmark) · [0.3Q raw trials](evidence/albers-metal-0.3q)

## Geographic preparation

The table shows median simulator milliseconds for the dense thematic route. Stages overlap or nest and should not be summed.

| Operation | 0.3Q | 0.4A |
| --- | ---: | ---: |
| GeoJSON decode | 103.0 | 103.7 |
| Validation | 309.2 | 310.8 |
| Projection | 318.4 | 323.4 |
| CPU mesh preparation | 173.4 | 179.9 |
| Tessellation within mesh preparation | 126.7 | 129.7 |
| GPU buffer upload | 2.5 | 4.0 |
| Geometry request to GPU completion | 967.7 | 990.3 |

The standalone host run measured 1.38 ms to build the index/layout for 3,153 electoral counties, against 1.37 ms in 0.3Q. Projection in that host run increased from 408 to 532 ms, and 5,000 indexed picks from 23.2 to 24.7 ms. All picks agreed with the linear path. Host timings are single observations with independent process and cache conditions. [Host trace](evidence/geokit-0.4a/AlbersQualification.txt) · [0.3Q host trace](evidence/albers-metal-0.3q/geography.txt)

## Native label costs

The camera test used 2024 and the focus test used 1976. UIKit layout timing includes candidate selection, rectangle collisions, external decisions, text/frame assignments and tile positioning on the main actor. It excludes time waiting for the actor, display presentation and the JSON log write. Content timing includes native measurement-cache lookups, color/accessibility changes and overlay configuration. Anchor readiness includes off-main preparation and delivery to the UI. Labels on these routes used one scene per launch.

| Route | Layout calls | Median / P95 / max layout, ms | Calls >4 ms | Median / P95 / max content, ms | Anchor deliveries | Mesh publications |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| focus | 396 | 0.20 / 0.30 / 0.70 | 0 | 5.20 / 5.20 / 5.20 | 1 | 1 |
| labels | 1017 | 0.30 / 0.50 / 4.80 | 1 | 6.00 / 6.00 / 6.00 | 1 | 1 |

In the navigation route, median / P95 / maximum layout with the key visible was 0.40 / 0.60 / 0.90 ms. The same figures without the key were 0.30 / 0.50 / 4.80 ms. These figures include all layout work. They do not isolate the cost of collisions or the key alone. Anchor readiness took 22.00 / 22.00 / 22.00 ms in the navigation run. The host's 240 per-viewport measurements of text registration took 1.15 / 4.32 / 19.16 ms, with resolver times of 0.06 / 0.25 / 0.45 ms. Recorded app timings establish main-actor work, while attribution of long queue gaps and physical missed presentations needs device profiling. [Overlay summaries](evidence/geokit-0.4a/benchmark/overlay/summary.json) · [Historical label measurements](evidence/geokit-0.4a/labels.jsonl)

## Interaction and resource lifetime

In the 0.3Q lifetime run, median selection was 0.10 ms, warm year commit was 3.75 ms and main-actor queue delay was 0.40 ms. Both versions exercised 100 year changes, while the longer 0.3Q run sampled many more selections. [0.3Q interaction trace](evidence/albers-metal-0.3q/lifetime/summary.json)

The 120-second stress route issued 111 election requests and committed 101. It reported 0 superseded commits and 0 stale paints. The county year phase published 1 geographic mesh including initial load and 198 style uploads. Across the entire route, deliberate source changes and reopen produced 53 mesh publications. Warm election commit median / P95 / maximum was 3.60 / 6.20 / 153.90 ms. Selection took 0.10 / 0.20 / 1.90 ms across 443 attempts with 233 hits. Main-actor queue delay was 0.40 / 2.00 / 3.30 ms. The peak sampled process footprint was 135.9 MiB. The short run cannot characterize a steady-state memory slope. The archived 30-minute run used the 0.3Q binary.

| Dense focus route | Alaska / Hawaii / mainland transition, ms |
| --- | ---: |
| regions-cg | 4942 / 290 / 8121 |
| regions-research | 303 / 287 / 287 |
| regions-thematic | 304 / 303 / 304 |

These transitions include the configured camera animation. The mainland entry focuses that region. Returning to national restores all three regions and is exercised separately in the UI suite.

GPU style copies took 0.30 / 0.50 / 1.10 ms (median / P95 / maximum). From an appearance update to its first Metal submission, timings in ms were clear_selection 2.80 / 11.60 / 16.90, highlight 4.90 / 13.30 / 17.40, thematic_style 15.55 / 20.40 / 24.40. Readiness includes asynchronous scheduling. [Interaction and resource summary](evidence/geokit-0.4a/benchmark/lifetime/summary.json)

## Electoral content and anchor reuse

A separate iPhone 16e UI test switched from 2024 to 2020 and tapped the Massachusetts tile. The historical boundary revision stayed at November 7, 2000. The second registration of 51 labels took 1.2 ms after the initial 5.8 ms, with cached strings increasing from 89 to 102. The next layout took 0.5 ms. The log contains one initial mesh and one initial anchor delivery across both years. This directly checks the effect of changing electoral content. [Content switch trace](evidence/geokit-0.4a/benchmark/content/switch.jsonl.gz)
