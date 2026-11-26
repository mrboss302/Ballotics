# BalloticsGeoKit 0.4C Release benchmarks

The 0.4B and 0.4C dense trials alternated on the same iPhone 16e simulator using iOS 26.3.1, Release arm64, the thematic composition and 4× MSAA. Each version used three fresh processes. Both 8× route windows are included. Other qualification tasks ran on the host during the cohort. The native traces keep outliers and distinguish CPU submission, GPU completion and simulator callbacks.

| Dense county measure | 0.4B | 0.4C |
| --- | ---: | ---: |
| 1× callbacks/s | 58.85 | 59.06 |
| 8× callbacks/s | 58.72 | 58.75 |
| 1× P95 callback gap (ms) | 17.30 | 17.30 |
| 1× P95 CPU submit (ms) | 0.20 | 0.20 |
| 1× P95 GPU command (ms) | 0.10 | 0.20 |
| County projection (ms) | 443.19 / 773.87 / 773.87 / 773.87 | 434.16 / 436.66 / 436.66 / 436.66 |
| CPU mesh preparation (ms) | 369.70 / 387.70 / 387.70 / 387.70 | 213.20 / 268.80 / 268.80 / 268.80 |
| Request to GPU completion (ms) | 1,547.40 / 2,014.30 / 2,014.30 / 2,014.30 | 1,238.90 / 1,552.60 / 1,552.60 / 1,552.60 |
| 1× navigation peak process footprint (MiB) | 107.61 | 103.39 |

Preparation rows show median / P95 / P99 / maximum. Navigation rows show medians of the per-window summary.

A separate confirmation run produced 59.17 callbacks/s at 1× and 58.99 callbacks/s across the two 8× windows. Its P95 callback gaps were 17.20 and 17.30 ms, respectively.

## Native label preparation and placement

| Release observation | Layout median / P95 / P99 / max (ms) | Layout calls | Anchor preparation (ms) | Native content setup and measurement (ms) |
| --- | ---: | ---: | ---: | ---: |
| 0.4B national | 0.30 / 0.50 / 0.50 / 1.00 | 1019 | 23.50 / 23.50 / 23.50 / 23.50 | 5.30 / 5.30 / 5.30 / 5.30 |
| 0.4C editorial | 0.30 / 0.40 / 0.50 / 0.70 | 1017 | 31.60 / 31.60 / 31.60 / 31.60 | 5.60 / 5.60 / 5.60 / 5.60 |
| 0.4C final editorial | 0.30 / 0.40 / 0.50 / 0.90 | 986 | 35.20 / 35.20 / 35.20 / 35.20 | 5.70 / 5.70 / 5.70 / 5.70 |
| 0.4C final regional focus | 0.20 / 0.30 / 0.60 / 1.00 | 389 | 40.90 / 40.90 / 40.90 / 40.90 | 6.10 / 6.10 / 6.10 / 6.10 |

The host study separately records native text measurement, cached anchor preparation and resolver placements for all 240 scenes. The application content timing also includes overlay setup. The diagnostic overlay was disabled during the timed routes.

## Policy and context

| Workload | Eligible features | Submitted triangle indices, first draw | Context preparation (ms) | Context draw (ms) |
| --- | ---: | ---: | ---: | ---: |
| Alabama-2004 | 67 | 12,753 | 422.90 | 0.30 |
| Alabama-2024 | 67 | 12,753 | 244.20 | 0.30 |
| Texas | 254 | 33,903 | 188.30 | 0.20 |
| Florida | 67 | 16,098 | 416.70 | 0.20 |
| Michigan | 83 | 13,473 | 192.70 | 0.50 |
| Virginia | 133 | 24,612 | 214.80 | 0.40 |
| Connecticut | 8 | 8,931 | 247.90 | 0.10 |
| Alaska | 40 | 373,788 | 192.20 | — |
| Hawaii | 5 | 2,571 | 185.70 | — |

Policy resolution and submission-range selection took 1.50 / 8.70 / 9.20 / 9.20 ms (median / P95 / P99 / maximum). The recorded local camera animation took 285.80 / 289.30 / 289.30 / 289.30 ms. Initial state framing starts at the fitted frame when a source opens. The test timelines retain each transition and show one mesh publication per focused inspection and return.

The table uses nine complete focus captures. A subsequent Alabama recapture recorded context preparation at 7.77 seconds while the host was heavily loaded, and it produced no draw sample before the capture timed out. The compact-phone Alabama UI test passed separately. [Alabama preparation outlier](evidence/geokit-0.4c/visual-observations/iPhone-17-Pro/Alabama-2004-context-outlier.jsonl.gz)

## Reuse and lifetime

The 120.17-second opt-in stress route completed 100 year changes, 50 source changes and 214 successful picks. It recorded zero mismatches, timeouts, superseded commits or stale paints. 53 publications matched 53 releases. The same-source year phase published one mesh.

| Interaction | Median / P95 / P99 / max (ms) |
| --- | ---: |
| Geographic selection | 0.10 / 0.20 / 0.30 / 0.40 |
| Style preparation | 6.20 / 9.70 / 10.10 / 10.70 |
| Immutable style upload | 0.30 / 0.50 / 0.60 / 1.00 |
| Source request to observed readiness | 421.65 / 973.20 / 2,624.20 / 2,624.20 |

Peak sampled process footprint was 142.35 MiB. Dismissal dropped process footprint from 129.69 to 86.61 MiB. The context layer has its own projected scene and no GPU allocation. Signed-device presentation, GPU residency and memory pressure remain open.

[Interleaved raw benchmarks](evidence/geokit-0.4c/benchmark) · [Policy transitions](evidence/geokit-0.4c/policy-tests) · [Lifetime trace](evidence/geokit-0.4c/stress)
