# BalloticsGeoKit 0.4B regression checks

## Geography and labels

| Check | Result |
| --- | --- |
| PROJ 9.3 reference | 14 coordinates passed. Maximum coordinate error 1.0022 × 10⁻⁸ m. |
| Indexed versus linear selection | 70,000 probes across 14 inputs matched, including overlap order and hole and multipart membership. |
| Path versus triangles | 690,408 probes across 28,767 parts matched. Concave, narrow, self-crossing and collapsed fixtures retained their behavior. |
| Regional payloads | All 14 inputs matched the 0.4A buffer hashes, origins, feature association and selection ranges. The election county mesh retained 334,385 positions and 326,658 triangles. |
| Historical snapshots | County date transitions, historical overlap and all 60 presidential snapshot references passed. Canonical source hashes and the generated derivative match the control. |
| Cartographic host study | All 240 placements matched the archived 0.4A results after excluding timing. There were zero observed overlapping or out-of-jurisdiction samples and zero geographic coordinate overrides. |
| 370 × 380 reference | 2024 returned 28 full, 4 compact, 9 external and 10 suppressed labels. 1976 returned 24 full, 8 compact, 9 external and 10 suppressed. |

[Host probes](evidence/geokit-0.4b/AlbersQualification.txt) · [Archive validation](evidence/geokit-0.4b/HistoricalArchiveChecks.txt) · [Presidential validation](evidence/geokit-0.4b/PresidentialElectionChecks.txt) · [Label decisions](evidence/geokit-0.4b/labels.jsonl) · [Mesh and source equality](GEOKIT_0.4B_MESH_PARITY.md)

## Rendering and lifecycle

The independent Release package passed ten macOS tests. Six reused the same public interfaces through the standalone `GeoKitSmoke` iOS Simulator test target. They covered mesh identity and cancellation, Core Graphics regional drawing, independent annotation space, wrong or missing shader resources and entry points, unsupported sampling, valid 1× and 4× initialization, atomic source replacement, palette validation, profile changes, selection, suspension and release.

The iPhone 17 Pro Release lab passed ten focused UI tests. They covered Census navigation and resizing, dense county recoloring and same-source reuse, historical state vintages and reuse of the 2000 geography, overlapping historical hits, profile changes with retained selection, inset picking across renderers, election content changes, external tile inspection, missing vertex shader fallback and collapsed-part fallback. The additional matched visual tests passed on both branches. A final compact landscape regression kept the map above 100 points, with the key in its separate region.

The targeted historical request probe attempted four dates and published the initial and final snapshots. Two obsolete preparations were rejected. The final date was January 1, 2000, and the fresh Metal receipt matched. The independent import source-supersession route passed. [Historical cancellation](evidence/geokit-0.4b/benchmark/0.4B/cancellation/cancellation-summary.json) · [Direct source-replacement trace](evidence/geokit-0.4b/benchmark/0.4B/ownership)

The macOS DistrictAtlas product validated 441 records, projected 436, framed TX-18 by stable GEOID and drew the same synthetic categories with either renderer. A copy of the executable with the entire Metal bundle omitted reported Core Graphics fallback. The ten-cycle renderer check used package-compiled shaders and retained the same geographic payload. [DistrictAtlas lifecycle](evidence/geokit-0.4b/district-lifecycle.log) · [Missing bundle probe](evidence/geokit-0.4b/district-missing-shader.log)

The default iOS Release lab and unsigned device app contain the Apple adapter's compiled Metal library and the mesh target's original license notice. The physical device production gate remains open. [Shader and platform boundary](GEOKIT_APPLE_RENDERING_ARCHITECTURE.md)

The final iPhone 17 Pro build completed rendering with the compiled package library at 1× and 4×. The 2× request produced an explicit unsupported-configuration fallback on that simulator. The opt-in runtime-source comparison used the same shader source and completed successfully. A combined landscape/fallback UI run passed the fallback-status check and encountered a SwiftUI pop-up automation miss during search. The standalone fallback test passed on retry, including Hawaii selection. [Final renderer probes](evidence/geokit-0.4b/renderer-probes) · [Release verification](evidence/geokit-0.4b/release-verification.json)

Switching the final UIKit view from 2024 to 2020 delivered two electoral content updates, one anchor computation and one geographic mesh publication. The Massachusetts tile remained selectable after the change. [Final content-switch trace](evidence/geokit-0.4b/renderer-probes/label-content-switch.jsonl.gz)

The final lab also passed its bundled navigation test with the MapLibre control and its system Reduce Motion test. Alaska, Hawaii and mainland each moved in one frame, with zero timed camera transitions. The test restored the prior simulator setting. [Reduced Motion trace summary](evidence/geokit-0.4b/renderer-probes/reduced-motion-summary.json)
