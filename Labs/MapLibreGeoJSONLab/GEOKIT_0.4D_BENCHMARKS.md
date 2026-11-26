# BalloticsGeoKit 0.4D Release benchmarks

The 0.4C and 0.4D cohorts used the same iPhone 16e simulator, iOS 26.3, Release arm64 and 4× MSAA. These are paired observations, with one fresh process per route in each cohort. Both 8× windows contribute. Other qualification activity on the host varied across runs. Simulator callbacks and GPU completion are separate from actual display presentation.

## Dense county navigation

| Measure | 0.4C | 0.4D |
| --- | ---: | ---: |
| 1× callbacks/s | 59.24 | 58.91 |
| 8× callbacks/s | 58.80 | 58.73 |
| 1× P95 callback gap (ms) | 17.20 | 17.40 |
| 8× P95 callback gap (ms) | 17.30 | 17.40 |
| 1× P95 CPU submission (ms) | 0.20 | 0.20 |
| 1× P95 GPU command (ms) | 0.10 | 0.30 |
| 1× sampled footprint (MiB) | 104.91 | 98.61 |

## Ordinary electoral navigation

This route disables per-layout logging and developer overlays.

| Measure | 0.4C | 0.4D |
| --- | ---: | ---: |
| 1× callbacks/s | 57.77 | 57.39 |
| 8× callbacks/s | 57.39 | 57.50 |
| 1× P95 callback gap (ms) | 17.80 | 18.50 |
| 8× P95 callback gap (ms) | 18.15 | 18.85 |
| 1× P95 CPU submission (ms) | 0.20 | 0.20 |
| 1× P95 GPU command (ms) | 0.20 | 0.10 |
| 1× sampled footprint (MiB) | 44.10 | 51.69 |

## Instrumented native labels and allocations

Time distributions are median / P95 / P99 / maximum in milliseconds. The final candidate retains separate annotation timing in the same log record as text placement.

| Route | Samples | Total label layout | Annotation and key layout | Content and measurement | Native artwork | Anchor preparation |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 0.4C national | 1029 | 0.30 / 0.40 / 0.60 / 1.90 | — | 8.80 | — | 78.60 |
| 0.4C regional focus | 400 | 0.20 / 0.30 / 0.60 / 1.00 | — | 11.60 | — | 45.10 |
| 0.4D initial national | 953 | 1.10 / 1.80 / 8.30 / 26.50 | 0.10 / 0.20 / 0.30 / 6.90 | 13.30 | 6.40 | 46.60 |
| 0.4D initial regional focus | 397 | 0.20 / 1.10 / 1.80 / 2.80 | 0.20 / 0.40 / 0.80 / 0.80 | 17.50 | 8.80 | 73.60 |
| 0.4D final national | 1001 | 0.40 / 0.60 / 0.80 / 2.40 | 0.10 / 0.20 / 0.30 / 0.80 | 14.40 | 7.20 | 49.70 |
| 0.4D final regional focus | 399 | 0.20 / 0.30 / 0.90 / 3.10 | 0.00 / 0.00 / 0.30 / 1.40 | 13.30 | 6.40 | 44.20 |

The one-run ordinary electoral route sampled 7.59 MiB more footprint at 1×. The narrowly scoped sample does not establish a device memory limit. The final shadow-free editorial path preserves the 4 ms P95 research target.

The host-only 240-scene model separately reports allocation geometry and keys. Its resolver measured 0.19 / 1.46 / 4.68 / 14.81 ms. The content row includes native text measurement and view setup. The detached anchor event includes its scheduling interval.

## Interaction and lifetime

The 90.14-second iPad route applied 100 year changes and 50 source changes, with 144 successful selections and zero mismatches or timeouts. Process footprint dropped from 143.25 MiB before dismissal to 79.78 MiB after release.

All 52 mesh publications were paired with 52 release events.

The synthetic allocation changed from four to five units within one source revision. Its native trace shows one mesh and one anchor publication. The 2024-to-2020 same-source navigation test also records one mesh and one anchor publication.

| Interaction | Median / P95 / P99 / max (ms) |
| --- | ---: |
| Geographic selection | 0.10 / 0.30 / 0.50 / 0.70 |
| Election-year scheduling | 0.70 / 8.00 / 19.40 / 20.50 |
| Submitted election paint | 16.00 / 26.30 / 48.10 / 604.50 |

In the system Reduce Motion route, Alaska, Hawaii and National changes completed in 24.6, 18.0 and 26.8 ms respectively on the simulator. The trace recorded zero animated camera transitions.

[Raw benchmarks](evidence/geokit-0.4d/benchmark) · [Native interaction traces](evidence/geokit-0.4d/native-tests)
