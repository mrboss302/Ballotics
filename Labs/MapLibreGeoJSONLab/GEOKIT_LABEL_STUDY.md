# Historical cartographic label study

All 60 normalized presidential elections from 1788 through 2024 ran against their resolved geographic snapshot, including the post-2000 fallback. The study grouped state and District of Columbia records, measured medium-weight native text, and ran the same placement engine as the iOS lab. Four drawable sizes produced 240 observations. The desktop measurement adapter uses AppKit. UIKit measurements and the physical display can differ. [Raw per-election results with timings and reasons](evidence/geokit-0.4a/labels.jsonl)

## Anchor quality

Across 2,399 jurisdiction-year requests, the projected legacy-style vertex mean fell inside the chosen part 2,300 times. The area-weighted centroid, the qualified 0.3Q search and the candidate pool returned valid interior points 2,399, 2,399 and 2,399 times respectively. The comparison adapts the legacy method to the same regional projected outlines. Its vertex weighting follows sampling density along coasts, and its largest-part choice can miss other usable islands. A geometric centroid can also fall in a hole or a concavity. An interior point alone gives no warranty about two-line text.

The table counts full measured labels that could fit geographically before collision checks on the 370 × 380 viewport. It excludes the externally delegated key. All methods use the same conservative clearance envelopes and native text.

| Anchor source | Full rectangles that fit |
| --- | ---: |
| Vertex mean on greatest-area part | 368 |
| Area centroid on greatest-area part | 707 |
| 0.3Q interior search on greatest bounding area | 563 |
| Bounded pole candidate on greatest-area part | 760 |
| Typography-aware alternatives across ranked parts | 949 |

Use the bounded pole as the first candidate and retain alternate interior positions for measured text. The pole maximizes point clearance approximately. Actual text fit also depends on horizontal and vertical space, part choice and existing placements. An anchor candidate diagram for 1976 is included in the private review folder. Dark dots show the first accepted candidate and rust dots show alternatives.

## Placement by viewport

| Map area in points | Requested | Valid anchors | Internal full | Compact | External | Suppressed | Overlapping outputs | Outside samples |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 350 × 450 | 2,399 | 2,399 | 752 | 527 | 495 | 625 | 0 | 0 |
| 370 × 380 | 2,399 | 2,399 | 949 | 427 | 495 | 528 | 0 | 0 |
| 850 × 620 | 2,399 | 2,399 | 1,845 | 16 | 495 | 43 | 0 | 0 |
| 680 × 135 | 2,399 | 2,399 | 9 | 192 | 495 | 1,703 | 0 | 0 |

The verifier independently sampled nine points of each internal rectangle against original even–odd paths and checked all pairs of emitted rectangles for intersection. The resolver separately checks its complete conservative envelope. A suppressed collision means an alternative was unavailable. The resolver drew no overlapping labels. Individual reason counts remain in each row of the raw data. The shallow landscape map sacrifices considerable internal coverage. External keys, search, inspection and zoom retain access to suppressed content.

## Selected historical results

The following counts use the 370 × 380 viewport. Split elector allocations and unresolved states retain their normalized research status. The absence of verified votes leaves an abbreviation-only label or an explicit unresolved tile.

| Election | Boundary date | Requested / valid | Full | Compact | External | Suppressed | Overlaps / outside | Manual overrides |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1788 | 1789-02-04 | 13 / 13 | 6 | 0 | 7 | 0 | 0 / 0 | 0 |
| 1800 | 1800-12-03 | 17 / 17 | 8 | 0 | 8 | 1 | 0 / 0 | 0 |
| 1824 | 1824-12-01 | 25 / 25 | 2 | 7 | 8 | 8 | 0 / 0 | 0 |
| 1860 | 1860-11-06 | 34 / 34 | 11 | 7 | 8 | 8 | 0 / 0 | 0 |
| 1864 | 1864-11-08 | 37 / 37 | 14 | 5 | 8 | 10 | 0 / 0 | 0 |
| 1872 | 1872-11-05 | 38 / 38 | 13 | 7 | 8 | 10 | 0 / 0 | 0 |
| 1912 | 1912-11-05 | 49 / 49 | 23 | 8 | 8 | 10 | 0 / 0 | 0 |
| 1960 | 1960-11-08 | 51 / 51 | 23 | 9 | 8 | 11 | 0 / 0 | 0 |
| 1976 | 1976-11-02 | 51 / 51 | 24 | 8 | 9 | 10 | 0 / 0 | 0 |
| 2000 | 2000-11-07 | 51 / 51 | 23 | 9 | 9 | 10 | 0 / 0 | 0 |
| 2016 | 2000-11-07 | 51 / 51 | 24 | 8 | 9 | 10 | 0 / 0 | 0 |
| 2024 | 2000-11-07 | 51 / 51 | 28 | 4 | 9 | 10 | 0 / 0 | 0 |

Across the 240 host samples median resolver time was 0.055 ms, with a maximum of 0.450 ms. Cold anchor preparation reached 32.24 ms. Elections reusing the 2000 boundaries generated zero additional groups. No coordinate overrides were needed to meet the measured containment and collision checks. Zero overrides does not establish owner approval or full national label coverage.

The private visual review includes the archived 2.0 screen, the 3.0 WebP design reference, the preserved 0.3Q map and native 0.4A captures. The reference establishes editorial scale, typography and small-state ordering. The research app supplies its own verified counts. Regional focus removes adjacent mainland fragments. Owner review remains open for national inset legibility, shallow viewports, split-fill contrast and the compact external key.

The local review folder contains seven named captures for each of the iPhone 16e, iPhone 17 Pro and iPad A16 simulators. They cover 1788, 1860, 1976, 2024, Alaska, Hawaii and landscape. The dedicated landscape tests verified that the drawable was wider than tall. The iPad showed more internal labels and retained the national key. The compact phone's landscape map occupied a shallow strip below the research controls, consistent with the suppression counts above. The private four-way comparison identifies the election and source of each image. Public district-only images accompany the independent consumer.
