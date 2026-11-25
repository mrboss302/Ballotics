# Ballotics 4.0 Albers Metal 0.3 qualification

The Metal backend is an isolated research option in the standalone MapLibre GeoJSON lab. It builds on Albers Rendering Lab 0.2 commit `499ae8ee0befbe45c80a3754e67696abd92b0b3b`. The Map view menu retains Core Graphics Albers and MapLibre Native for comparison.

## Outcome B — Metal is promising with conditions

Keep the shared Albers geography, camera, selection and election-presentation contract. Continue the Metal backend for high-volume thematic maps and keep Core Graphics as the reference and fallback. Retain MapLibre for its current coverage and comparison role. The simulator supports the GPU renderer's feasibility with full tested boundaries. Physical deadlines, presentation and memory behavior still constrain a production decision.

## Implementation results

| Stage | Result |
| --- | --- |
| A. Mesh preparation | Pinned libtess2. Both candidates succeeded on 28,767 actual parts. libtess2 also agreed with all probes on the synthetic self-crossing part, where Earcut returned the same area and different coverage. |
| B. Triangle fills | The Census county view rendered 3,144 features, 220,592 geographic vertices and 213,746 geographic triangles. The election view rendered 3,153 features, 334,385 vertices and 326,658 triangles. |
| C. Boundaries and antialiasing | Original ring segments supply geographic outlines. Metal shades batched screen-space capsules with a separate selection pass. The default draws with 4× MSAA. |
| D. Election styling | 2020 and 2024 county results reuse the mesh. Per-feature style changes reuse the same camera, selection index and lookup identities. The existing candidate, reported-margin, alternate-palette, geographic vintage, and inspector behavior was exercised on the simulator. |
| E. Controls | The comparison uses the shared Albers camera motion and a matched geographic route in the MapLibre control. The graphics evidence is reported separately from query, preparation, CPU submission and presentation. |

Host validation passed 14 PROJ fixtures, 70,000 indexed-versus-linear selection probes, and 690,408 path-versus-triangle probes. All 60 presidential boundary dates retained the existing schedule, including February 4, 1789 for the 1788 election, and the shared November 7, 2000 map for later elections. Float32 regional vertex conversion displaced the tested positions by at most 0.135 m.

## Performance verdict

Across 15 fresh-process runs, national full-quality Metal produced a median 59.0 callbacks/s versus 1.9 for the Core Graphics control and 60.0 for MapLibre Native. At 8×, Metal and Core Graphics both produced roughly 58–59 callbacks/s. Metal's national CPU submission P95 was 0.2 ms across the median run. Its median GPU execution P95 was 0.2 ms and the worst observed national GPU command was 19.4 ms. The GPU command tail and the estimated 33.73 MiB attachment increment are the main measured costs of strokes and 4× multisampling. Physical presented frames and per-pass GPU cost stay open.

## Lifecycle and stability

The 30-minute simulator route completed 111 election requests, 50 source changes and 6,081 successful geographic selections. It reported zero request mismatches or readiness timeouts. The median steady footprint was 119.5 MiB, with 128.0 MiB at P95 and a sampled preparation peak of 134.4 MiB. Final dismissal dropped footprint by 41.4 MiB. The short follow-up exercised the corrected map-reopen observation. A separate supersession test found and then verified the fix for an obsolete-preparation publication race.

## Build and native tests

Release simulator and unsigned Release iOS builds passed. The four Metal UI flows and the two selected control flows passed across the two simulators. Those flows covered search, CPU geographic picking, rotation, background and foreground transitions, fallbacks, regional framing, historical boundaries, and election themes. A compact-menu recheck moved palette actions into their own submenu and confirmed both Albers renderers. [Native test evidence](evidence/albers-metal-0.3/native-checks.txt).

## Remaining production gates

- Actual presentation, thermal and memory-pressure behavior, value of region sub-batching and frame deadlines on a physical iPhone and iPad. The simulator command timestamps provide evidence for comparison under the tested conditions.
- Physical VoiceOver and system Reduce Motion, including search, overlapping jurisdictions, large Dynamic Type and the non-color outcome inspector.
- Presentation coverage for American Samoa, Guam, the Northern Mariana Islands, Puerto Rico and the U.S. Virgin Islands. The Albers prototype has the same regional policy as 0.2.
- The outstanding Presidential Election Data Foundation 0.1 qualification requirements, including data and redistribution gates. The resolved county derivative and historical source data were used locally. The publication includes aggregate metrics, original synthetic input, and Census-only screenshots.

## Next milestone

Run a signed Release build on a physical iPhone at 1× and 8× with the same full-quality county map, then repeat 2020–2024 recoloring and map dismissal. Capture actual drawable intervals, deadline misses, GPU passes, physical footprint, memory warnings and thermal state with the same Core Graphics and MapLibre controls. Use those measurements to decide whether finer geographic batching or lower-cost antialiasing improves the 16.7 ms frame budget while preserving the documented coverage tolerances.

## Supporting evidence

[Tessellation study](METAL_TESSELLATION_STUDY.md) · [Renderer architecture](METAL_RENDERER_ARCHITECTURE.md) · [Performance](METAL_PERFORMANCE_BENCHMARKS.md) · [Cartography](METAL_CARTOGRAPHIC_VALIDATION.md) · [Resource lifetime](METAL_RESOURCE_LIFETIME.md) · [Android study](METAL_CROSS_PLATFORM_STUDY.md) · [Reproduction](README.md#albers-metal-rendering-lab-03).
