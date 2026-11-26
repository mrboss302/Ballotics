# BalloticsGeoKit 0.4B qualification

**Decision B — qualified with documented restrictions.** The package supplies the reusable CPU mesh, Metal resources and renderer, Core Graphics drawing and Apple presentation contracts. The research lab and an election-independent macOS consumer use the same adapters, and an independent iOS Simulator target exercises the public API. Qualified scope is Apple research use on the tested data and platforms.

| Area | Evidence and disposition |
| --- | --- |
| Geographic core | Qualified for the tested geography. 14 PROJ fixtures, 70,000 indexed picks, all 60 presidential state snapshots and the county temporal checks passed. Canonical hashes match 0.4A. |
| Mesh | Qualified on the 14 compared inputs. All regional payload hashes matched and all 690,408 containment probes passed. The pinned libtess2 implementation and license moved intact. |
| Metal | Qualified for the tested Release Xcode integrations. Resource bundle lookup, full borders, 4× reference sampling, lifecycle ticketing, replacement and recoverable initialization were exercised. |
| Core Graphics | Available as a reference and with visible fallback status. Whole-scene path rendering retained regional semantics and picking. Dense national rendering remains substantially slower. |
| DistrictAtlas | Qualified as an independent native consumer. It loads 441 Census records, presents 436, supports both renderers and profiles, and finds TX-18. Its categorical colors are synthetic. |
| Lab | The existing election models, SwiftData, search, inspectors, cartographic content and MapLibre remain in the lab. The extracted Apple renderer is shared with DistrictAtlas. |
| Labels and annotations | All 240 host placement scenes matched 0.4A. The final compact landscape view displays geography above the ordered VT–DC key. Editorial density, typography and tile design remain owner-review items. |
| Visual review | Matched state, county, district, election-year, profile, inset and fallback images are held in the private owner-review folder. Final visual acceptance remains with the owner. |
| Venue and physical device | A macOS Apple M4 host, iOS 26.3.1 simulators and an unsigned physical-device build were available. The paired iPhone and iPad remained unavailable for signed-device testing. |
| Packaging | Xcode builds the default Metal resource on macOS and iOS. A SwiftPM CLI consumer supplies a compiled library or uses Core Graphics with an explicit diagnostic. |

[Dependency audit](GEOKIT_0.4B_DEPENDENCY_AUDIT.md) · [Architecture](GEOKIT_APPLE_RENDERING_ARCHITECTURE.md) · [Public API](GEOKIT_PUBLIC_API.md) · [Regressions](GEOKIT_0.4B_REGRESSION_REPORT.md) · [Parity](GEOKIT_0.4B_MESH_PARITY.md) · [Benchmarks](GEOKIT_0.4B_BENCHMARKS.md) · [Resource lifetime](GEOKIT_0.4B_RESOURCE_LIFETIME.md)

Dense county navigation measured about 59 simulator callbacks per second at 1× and 8×. Native label layout was 0.30 ms median and 0.50 ms P95. Preparation observations had host-load and startup variation, and simulator timings leave the physical presentation target open.

Keep physical-device frame pacing, thermal behavior, sustained memory, VoiceOver and system Reduce Motion as production gates. Outlying territories, historical election reconciliation, historic Alaska legislative vintages, source redistribution rights, Ballotics-Data publication and Android rendering remain open.

For 0.4C, limit label work to owner-approved high-value historical and compact cases. Use the private landscape references and keep the established geographic and rendering packages as controls. Run the signed hardware qualification alongside that work when representative devices are available.

## Long-session evidence

- iPad A16 · extraction build completed 1,800.27 seconds, 100 year changes, 50 source changes and 2,969 successful picks. The verifier recorded zero mismatches or timeouts. [Trace and summary](evidence/geokit-0.4b/lifecycle/iPad-A16)
