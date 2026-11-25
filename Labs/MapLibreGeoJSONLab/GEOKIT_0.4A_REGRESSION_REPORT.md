# Geographic core and label regressions

The extracted library builds in Release as a Swift package and as a local Xcode package dependency of the standalone iOS research target. Fourteen PROJ 9.3 reference coordinates agreed within 1.01 × 10⁻⁸ m. Indexed and linear picking agreed across 70,000 probes, with historical stacking order, holes, multipart features, antimeridian and both national compositions exercised. The raw host results include decode, projection, index construction and selection timings for each input. [Host qualification](evidence/geokit-0.4a/AlbersQualification.txt)

All 14 inputs retained the same vertex, triangle and geographic part counts and tested extrema as 0.3Q. The election county mesh contains 334,385 vertices and 326,658 triangles. Across 28,767 parts, all 690,408 path-versus-triangle comparisons agreed. The per-scene byte checks compare mesh payloads derived from thematic and research profiles. Seven synthetic contour cases, the nested island/hole check, floating-point layout checks and the identified collapsed-polygon fallback remain available. The canonical tracked state, congressional, county and historical-state source bytes matched their 0.3Q Git blobs. The external historical county archive passed its own hash and snapshot tests. [Fresh mesh trace](evidence/geokit-0.4a/mesh.jsonl) · [Source and mesh parity](evidence/geokit-0.4a/source-and-mesh-parity.json) · [County temporal archive](evidence/geokit-0.4a/HistoricalArchiveChecks.txt)

The presidential calendar independently checked all 60 elections, including the shared November 7, 2000 fallback and historical AK/HI territory/state transitions. The label study then evaluated each election at four viewport sizes using the existing normalized research allocations. [Calendar and geographic checks](evidence/geokit-0.4a/PresidentialElectionChecks.txt) · [Label study](GEOKIT_LABEL_STUDY.md)

## Package fixtures

The four Release XCTest methods cover the synthetic geometry and placement cases below. A portable adjacent/overlap GeoJSON file accompanies the suite. [Package tests](../../Packages/BalloticsGeoKit/Tests/BalloticsGeoKitTests/LabelTests.swift)

| Case | Assertions |
| --- | --- |
| One feature with mainland and Hawaiian parts plus an overlapping record | Ordered feature IDs, regional filtering, duplicate source key rejection, profile-shared cache and invalidation on a new scene |
| Concavity, two holes, narrow polygon, oversized two-line text | Valid interior anchor can still reject the rectangle. Compact fallback, suppression and original-path membership of sample positions |
| Adjacent jurisdictions and overlapping records | Collision, stable result after reversing input order, selected-group priority, small pan continuity, viewport exit, callout association and typed ring decoding |
| Historical rule and external presentation | Exact revision and inclusive date, normalized override accepted only inside a group member, priority/zoom, missing text, national tile and screen/world round trip |

The app’s fallback and geometry remain separate from label preparation. The canonical cache has no dependency on label text, colors, GPU command buffers or election values. During the shared 2000-boundary series, the study observed zero newly prepared groups after 2000, including the 2024 election. The short lifecycle benchmark records actual mesh and style ownership under year, selection, palette and layout changes. The 0.3Q 30-minute soak applies to its archived executable. [Earlier long-run evidence](evidence/albers-metal-0.3q/lifetime/summary.json)

## Native integration

The compact iPhone simulator passed the research tests for historical overlapping jurisdictions, live composition switching with retained selection, county styling and election reuse, the collapsed-polygon and missing-shader fallbacks, and inset picking across renderers. The owner-review route exercised the external Massachusetts tile, the shared inspector and isolated Alaska and Hawaii. A dedicated landscape test waited for a genuinely wide map frame before capturing the rendered screen. An initial orientation-only screenshot remained portrait and was replaced.

An additional live electoral-label test switched 2024 to 2020 on the same 2000 boundary revision and selected Massachusetts through the key. The two national paint operations produced two content registrations (5.8 and 1.2 ms), one geographic anchor delivery and one mesh publication, both during initial load. The second label layout took 0.5 ms. The county lifetime route included 100 year changes and 50 source changes. It reported zero timeouts, stale paints or mismatched selections/sources. [Electoral switch trace](evidence/geokit-0.4a/benchmark/content/switch.jsonl.gz) · [Lifetime results](evidence/geokit-0.4a/benchmark/lifetime/summary.json)

The final package suite also checks that decoding a typed coordinate rejects an out-of-range longitude and round-trips a valid geographic limit. The formal geometry and timing cohorts use the unchanged polygon loader. The final typed-coordinate decoding change is outside those execution paths.

The Release simulator and unsigned iOS device builds succeeded. Both compiled Metal library hashes match the corresponding 0.3Q platform builds. The final visual suites passed on iPhone 16e, iPhone 17 Pro and iPad A16 simulators. The first iPad run also missed its tile tap during severe host disk pressure. The isolated rerun on the same binary passed the external selection, both regional focuses and National restoration. The iPad landscape rerun used simulated device rotation because its windowing mode rejected an app-requested rotation. [Build hashes and test inventory](evidence/geokit-0.4a/release-verification.json)
