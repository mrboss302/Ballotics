# Geographic and lifecycle regressions

## Geographic checks

The baseline and candidate independently reproduced the election mesh with **334,385 geographic vertices and 326,658 triangles**. Both processed 28,767 parts across 14 real geographic inputs. Each used 24 deterministic interior/exterior probes per part for **690,408 path-versus-triangle comparisons**, with zero disagreements and zero failed parts. The largest measured discrepancy between Double mesh area and the reference was 5.43 × 10⁻⁹ relative. Across the uploaded Float32 layout the maximum position error was 0.135 m. The largest relative part-area difference after Float32 conversion was 1.65%, identical in both runs. The final host run also built each of the 14 scenes through both profiles and compared all vertex, part and segment bytes, triangle and boundary indices, origins and selection ranges. All matched. Small source features are sensitive to this relative metric. Comparing the baseline with the candidate showed one numerical difference in these extrema. The congressional district maximum absolute Float32 area discrepancy changed from 7,526.908 to 7,526.918 m². The candidate constructs its local research frame directly from projected bounds and padding. Counts, global maximum and containment agreed. [Baseline mesh](evidence/albers-metal-0.3q/baseline-mesh.jsonl) · [Candidate mesh](evidence/albers-metal-0.3q/candidate-mesh.jsonl)

| Inputs | Checks |
| --- | --- |
| Census states, districts, counties and election-specific county derivative | Source validation, part counts, mesh/index contracts and even–odd coverage |
| Counties on January 1 in 1788, 1860, 1900, 1980 and 2000 | Historical filtering, geographic identity and tessellation |
| Presidential state boundaries on 1789-02-04, 1860-11-06, 1900-11-06, 1980-11-04 and 2000-11-07 | Source-defined region presence, validated projection and selection |
| Concavity, opposite-winding holes, duplicate/nearly collinear vertices, narrow corridor, 2 cm island, bowtie and collapsed input | Area, analytic triangles versus original path, identified whole-scene fallback |
| Additional island nested inside a hole | Even–odd coverage and expected area of 84 m², including 320 independent containment probes |

Fourteen PROJ 9.3 coordinates differed by at most 1.01 × 10⁻⁸ m. Indexed and linear selection agreed on 5,000 probes for each of the 14 inputs. Additional checks exercised actual reference locations, a multipart feature, a hole, stacking order, and points at both +179.95° and −179.95° on a synthetic Aleutian polygon. Both profiles returned the same identities. The thematic state test checked the same path objects survived the layout change, independently inverted uniform transforms and checked membership against the research scene. The camera regression also projected and picked both inset identities at seven positions along the eased focus transition in portrait and landscape, for 28 round trips. [Projection and indexing](evidence/albers-metal-0.3q/geography.txt) · [Reference selections](evidence/albers-metal-0.3q/projection-selection.txt)

The archive test verified the source hash, records, inclusive endpoints, reproducible snapshot output and invalid-source cases. The election calendar checked all 60 snapshots and the shared November 7, 2000 fallback. The private review captures include 1788 with neither modern inset and 2024 with both. Historic zero/nonparticipating allocation records cannot create geometry. Independent inspection of the bundled data found zero for both jurisdictions in 1956, 3/3 in 1960 and 3/4 in 2024. Labels read joined, verified-positive source allocations. [Historical checks](evidence/albers-metal-0.3q/historical-archive.txt) · [Calendar](evidence/albers-metal-0.3q/election-calendar.txt) · [GeoJSON validation](evidence/albers-metal-0.3q/source-checks.txt)

## Supersession, styling and GPU ownership

* The deliberate 1788 → 1980 → 1860 → 2000 request sequence published 332 and then 3,139 features. Two preparations were canceled. Both published dates matched the latest requested date at publication. [Trace and assertion](evidence/albers-metal-0.3q/cancellation/cancellation-summary.json)
* Starting a county import and immediately requesting states ended with the states dataset and no obsolete imported file. The probe waited for a current successful Metal completion and allowed two more seconds for an old import to attempt publication. [Import trace](evidence/albers-metal-0.3q/ownership/import-supersession.jsonl.gz)
* The renderer checks the source revision before installing prepared buffers. Styles use feature-order-indexed immutable buffers. Geographic commands retain their resources, and each completion checks mesh generation before changing state. Presentation tickets distinguish successful completions of the latest layout and style. CPU hit tests own selection.

## Boundary and pixel coverage

On the 17 Pro's 1,110 × 1,593 drawable, the full-quality equal-scale county land mask matched all 354,808 pixels in the archived 0.3 equal-scale Metal mask. For thematic Metal versus Core Graphics the largest directional land mismatch farther than three backing pixels was 0.00096% on the mainland, 0.04866% in Alaska and zero in Hawaii. The existing 0.5% per-direction acceptance threshold passed in all regions. The inset rectangles in this analysis identify geographic areas for measurement. The thematic images contain no inset frames. [Pixel measurements](evidence/albers-metal-0.3q/aa/coverage.json) · [Research control](evidence/albers-metal-0.3q/aa/research-counties.png) · [Thematic control](evidence/albers-metal-0.3q/aa/counties-cg.png)

Changing from four samples to one changed 88,333 viewport pixels by more than eight levels in at least one color channel. Fine county boundaries and the island coastlines contribute to the difference. Equivalent visual quality at one sample remains unverified. Land masks compare coverage at the tested resolution. Subpixel holes and very small islands still require the source-space checks. [Single sample](evidence/albers-metal-0.3q/aa/counties-metal-1x.png) · [Four samples](evidence/albers-metal-0.3q/aa/counties-metal-4x.png)

The focused Cuyahoga County selection masks had 99.91% intersection over union across 423,355 Core Graphics and 423,686 Metal selected pixels. At least 99.62% of viewport pixels differed by no more than eight channel levels. The selected coastline and interior boundaries remained visible. [Core Graphics detail](evidence/albers-metal-0.3q/aa/detail-cg.png) · [Metal detail](evidence/albers-metal-0.3q/aa/detail-metal.png)

## Final 30-minute lifecycle qualification

The corrected Release executable ran for 1,800.23 seconds on the iPhone 16e simulator. XCTest rotated the device, sent the app to the background and activated it again. The in-app driver dismissed and reopened the map after the midpoint. The source and appearance receipts were checked again after reopening. [Raw session](evidence/albers-metal-0.3q/lifetime/stress.jsonl.gz) · [Assertions and distributions](evidence/albers-metal-0.3q/lifetime/summary.json)

| Observation | Result |
| --- | ---: |
| Election requests / committed year observations | 111 / 100 |
| Geographic source changes | 50 |
| Successful geographic hit tests / attempts | 6,225 / 6,225 |
| Camera steps / presentation profile changes | 6,225 / 890 |
| Request mismatches / readiness timeouts | 0 / 0 |
| Superseded election commits / obsolete style observations | 0 / 0 |
| Mesh publications / releases | 53 / 53 |
| Retirements with GPU work outstanding | 5 |
| Observed maximum outstanding at a layout or release sample | 3 |
| Published vertex / index buffers | 147 / 147 |
| Allocated style/palette buffers across all recolors | 12,554 |
| Projected cache maximum | 2 |
| Renderer fallbacks | 0 |

All 100 observed year changes reused one published county mesh. Every logged profile change kept the currently published byte count and used the layout-only update path. Mesh retirement matched publication over the complete run. Immutable style allocations are cumulative across recolors. Their simultaneous residency depends on the in-flight commands.

| Metric | Median | P95 | P99 | Maximum |
| --- | ---: | ---: | ---: | ---: |
| Footprint after minute four, MiB | 144.27 | 150.28 | 150.35 | 150.49 |
| Selection processing, ms | 0.10 | 0.30 | 0.40 | 3.10 |
| Election year observed ready, ms | 27.30 | 79.60 | 249.60 | 1,533.10 |
| Main-actor election scheduling, ms | 0.40 | 2.90 | 4.00 | 4.00 |
| Style table preparation, ms | 10.10 | 11.30 | 11.80 | 23.00 |
| Style buffer copies, ms | 0.40 | 0.70 | 0.90 | 3.60 |
| Thematic update to submission, ms | 20.20 | 25.30 | 27.50 | 550.70 |
| Source request to observed ready, ms | 317.20 | 527.30 | 1,448.10 | 1,448.10 |
| Completed camera transition, ms | 282.20 | 289.80 | 295.50 | 297.60 |

The highest sampled mesh-preparation footprint was 150.58 MiB. Final dismissal reduced the sampled process footprint from 150.36 to 92.66 MiB after three seconds. The 0.3 steady median/P95 was 119.47/128.00 MiB on a different simulator model and route. This run establishes the observed range for this build and workload. The physical-device memory ceiling remains open.

## Simulator interaction, accessibility and owner review

Fourteen distinct qualification XCTests had passing executions, including the final opt-in soak. On the 17 Pro, both renderer controls returned Alaska and Hawaii on direct national taps. Search, regional focus, the Alaska reporting-unit inspector, the Hawaii electoral inspector, and retaining a selection across a live profile switch passed. The 1808 overlap probe first framed an identified feature, tapped a point at least 16 km inside both original polygons, verified both source IDs, and selected Concordia. It passed on both device sizes. Missing shader and collapsed-geometry tests retained selectable Core Graphics geography. [Native test results, including preliminary failures and retries](evidence/albers-metal-0.3q/accessibility/native-regressions.json)

The regular-size 17 Pro and 16e captures cover the research and thematic state maps, 2024 electoral geography, the nationwide county map, the focused islands, 1788 and landscape. At Accessibility Medium on the 17 Pro, both inset search results opened an inspector with the textual electoral allocation in its accessibility hierarchy. The 1808 overlap chooser also passed at this size. The owner-review package includes the eight original resolution map captures for each simulator and an overview sheet for each. Native VoiceOver speech, focus order and touch exploration remain device checks.

The final motion test enabled the actual iOS simulator preference through Settings, launched the state map, then restored the original disabled value. Alaska, Hawaii and national return each took one sampled frame, with elapsed times of 18.1, 17.3 and 18.1 ms. The app recorded zero timed camera animations. The same checker rejected the default-motion regional control, which recorded three animated transitions. Earlier harness retries included a failed switch restoration and an installation attempt interrupted by low simulator storage. The final XCTest and independent trace assertions both passed. [Motion trace](evidence/albers-metal-0.3q/accessibility/system-motion.jsonl.gz) · [Motion assertions](evidence/albers-metal-0.3q/accessibility/system-motion-summary.json)
