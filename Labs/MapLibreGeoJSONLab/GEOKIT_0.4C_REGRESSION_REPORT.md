# GeoKit 0.4C regressions

| Suite | Result |
| --- | --- |
| PROJ 9.3 reference | All 14 coordinates passed. |
| Geographic selection | 70,000 indexed-versus-linear probes passed across 14 geographic inputs, including regional picks, holes, multipart shapes, dated overlaps and substitutions. |
| Tessellation | All 690,408 path-versus-triangle probes passed, with the collapsed fallback fixture and pinned libtess2. |
| 0.4B payload parity | All 14 inputs matched the archived per-region payload hashes, origins, feature associations and selection ranges. The dense election-county input retains 334,385 vertices and 326,658 triangles. |
| Historical geography | Passed the county-date transitions and all 60 presidential snapshots, with the 2000 boundary fallback and historical regional policy preserved. |
| Cartographic labels | The editorial study checked 240 scenes with zero observed overlaps and sampled containment failures. The exploratory decisions matched 0.4B across all 240 scenes. |
| Source integrity | The tracked geographic and election inputs match the 0.4B bytes. |

The native suites exercise focus framing and masked picks, editorial inline variants, aspect-fitted annotation layout, selected-county inspection, local Alaska State House district inspection, shader initialization and fallback, and recovery from source replacement. The Alabama UI test returns to national while retaining the selected county. DistrictAtlas independently traverses all four policies and retains one mesh identity through each current GPU completion.

The Release matrix passed 12 macOS package tests and 8 standalone iOS simulator rendering tests. The lab passed the two focused inspection tests and the electoral-content switch, historical-overlap and collapsed-polygon fallback UI tests. The Release simulator, unsigned iOS device and DistrictAtlas builds succeeded. The policy traces record a single mesh publication across each focused inspection, including the Alabama return to national.

The landscape UI test passed on the iPhone 16e and 17 Pro simulators. The iPad A16 passed portrait visual inspection. Its windowed configuration retained an 820 × 1,180 point application window after XCTest and Simulator device rotation, so that run failed the wide viewport assertion. The DistrictAtlas window resized to 800 × 550 and 1,050 × 570 points with the Texas focus intact.

[Release test manifest](evidence/geokit-0.4c/release-verification.json) · [Host geographic traces](evidence/geokit-0.4c) · [Label study](GEOKIT_EDITORIAL_LABEL_STUDY.md) · [State focus results](GEOKIT_STATE_FOCUS_STUDY.md)
