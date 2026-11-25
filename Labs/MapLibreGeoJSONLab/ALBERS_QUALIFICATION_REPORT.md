# Albers Rendering Lab 0.2 qualification

## Decision D. Production architecture remains unqualified

Continue with the optimized Core Graphics Albers prototype and keep MapLibre as the control and the coverage fallback. The projection and selection results support a reusable thematic geometry layer. Evidence is insufficient to replace the shipped rendering architecture. At low zoom, both Albers paths showed substantial simulator frame-callback gaps while MapLibre was close to 60 callbacks/s. The optimized path also had warm-switch outliers above 250 ms. Choosing a Metal backend requires physical-device presentation and compositor evidence.

| Requirement | Result |
| --- | --- |
| Projection, insets and stable identity | PROJ and geodesic area checks passed. All 70,000 indexed and linear selection probes agreed. Synthetic and real historical overlaps preserve all identities. |
| Historical and county geography | All requested historical dates verified. The Connecticut and Alaska substitutions passed local source-coordinate checks. Existing omitted outlying territories are a coverage gap. |
| Theme changes and selection | UIKit UI tests cover result styling, inspection, candidate-to-margin recoloring, source/date changes and searchable feature selection. Long switches exercised independent appearance updates. |
| Same-source recoloring and warm switches | Short-run Core Graphics paint reached a 7.7 ms median and 29.3 ms maximum. Median dataset-ready times improved to 5.3 ms with cache reuse. The end-to-end run still showed rare long delays. |
| Ordinary navigation at 60 presented fps | Open. Low zoom had slow simulator callbacks and substantial CPU antialiasing work after the timed draw. |
| Target footprint and release | The 30-minute simulator route peaked at 138.9 MiB sampled and fell from 136.3 to 97.6 MiB after map dismissal. Physical-device pressure and GPU timing remain open. |
| Offline and publishing | Locally bundled geography and an installed election package support local use in the research lab. |
| Physical-device build and run | Release builds for simulator and generic iOS succeeded. The paired iPhone and iPad were unavailable. Physical performance remains open. |
| Android | Feasible to port the numeric projection, index and contracts to Kotlin and Compose. A production graphics decision requires device evidence on Android too. |

The full interactive simulator suite had nine passes and one Files picker skip. The three final visual and style regressions passed, including a direct Clear selection action and style-only palette and border changes. Independent source validation covered malformed inputs. The projection, historical archive, 60-election date filter, package-version invalidation, rollback and offline store checks passed. [Verification logs](evidence/albers-rendering-0.2/verification/).

## Accessibility

The renderer exposes a labeled map with a **Map view → Find geography** action. The search lists stable feature names and identifiers, and the selected feature opens the same accessible inspector used by point selection. If historical jurisdictions overlap, a separate list names the alternatives. Candidate names and results, no-data, ties, unresolved results, and split allocations have textual equivalents. The zoom-to-feature animation takes the immediate path when iOS Reduce Motion is enabled.

Simulator UI automation exercised search, the selected-feature inspector, state and county framing, rotation and restoration after backgrounding. The focused path also passed at the system's Accessibility Medium text size. VoiceOver speech and focus order and actual system Reduce Motion behavior remain device qualification items.

## Physical-device protocol

1. Install signed Release builds of the original research app and this branch on comparable phone and tablet hardware. Record device model, iOS version, refresh rate, build hash, thermal state and free memory. For steady-state runs seed the same immutable lab packages. Use clean installs for a separate cold-install cohort. Disable network access for the offline run and omit a remote package URL.
2. Run three fresh processes per renderer, interleaving approaches. Use the same viewport, source vintages, fills and camera route. Start with modern counties and congressional districts. Repeat historical counties for 1788 and 2000, followed by repeated county-election switches and Alaska/Hawaii detail. Keep full device and build identifiers in local metadata.
3. Record Instruments App Launch and Swift Concurrency or equivalent scheduling instrumentation. Use Animation Hitches or a system presentation trace to record actual presented frames, missed deadlines and p50/p95/p99 inter-frame times. Compare against `draw`, camera update and MapLibre callback signposts. Capture process and render-server CPU, GPU timing, allocation stacks and measured high-water footprint. Profile 1×, 2×, 4× and 8×, with 16× as exploration for generalized data.
4. Run the 30-minute route with 100 year changes, 50 source changes and at least 100 successful picks. Repeat on a smaller-memory device. Inspect release after leaving the view, transitions during cancellation, offline relaunch, background and foreground, rotation and low-memory warnings. Compare timestamps for request entry, result store and query, source readiness, projection, first display and final style. A callback is useful evidence about app work even when no frame is presented.
5. Turn on VoiceOver and reach Find geography, read the selected row and inspection, and identify a split allocation and an unresolved result without using color. Repeat on accessibility-size Dynamic Type, then enable Reduce Motion and confirm immediate state/county and inset framing. Capture target-size and contrast problems separately from draw performance.

## Data boundaries and publication

Presidential Election Data Foundation 0.1 remains open. File-specific rights and approved hosting for historical and derived Connecticut geography and county-return compilation remain unresolved, as do historical result reviews and physical-device qualification. Versioned result installation is an opt-in research workflow. Review the [0.1 source audit](../../ElectionData/PRESIDENTIAL_DATA_AUDIT.md) before publishing any full input, staged package or derivative.

The repository evidence contains renderer telemetry, code, generated geographic reference numbers and eligible Census-only screenshots. Historical and election-result screenshots are preserved in the ignored `ElectionData/research-staging/validation/albers-0.2` directory. Simulator copies, app bundles, SwiftData stores and system-wide diagnostic traces stay local. The Census visual evidence uses the publicly distributed generalized geographic sources. [Census copyright policy](https://www.census.gov/about/policies/copyright.html).

## Next bounded milestone

Use one physical phone to record two matching Release county routes, one at 1× and one at 8×, and an alternating-2020/2024 warm comparison. The decision hinge is where work accumulates between the camera event and display presentation. If a traced low-zoom bottleneck persists after scheduling and viewport correction, prototype one isolated Metal scene for the county workload with an established licensed tessellator. Keep selection, projection and styles constant. Revisit the decision only after proving frame cadence, holes and resource limits on device.

For that prototype, libtess2 exposes even–odd winding and vertex/index output under SGI Free Software License B. Earcut is a smaller ISC-licensed alternative for simple polygons with holes. Its published robustness caveat for problematic inputs makes the complete historical fixtures important. Start with synthetic and real failing rings, preserve polygon order and stable IDs, and require area and containment equivalence before shipping a dependency. [libtess2 API](https://raw.githubusercontent.com/memononen/libtess2/master/Include/tesselator.h) · [libtess2 license](https://github.com/memononen/libtess2/blob/master/LICENSE.txt) · [Earcut algorithm](https://github.com/mapbox/earcut.hpp#the-algorithm) · [Earcut license](https://github.com/mapbox/earcut.hpp/blob/master/LICENSE).
