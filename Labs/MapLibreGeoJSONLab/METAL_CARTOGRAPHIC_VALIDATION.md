# Cartographic validation

Both Albers backends read the same immutable geographic scene. Canonical coordinates remain WGS 84 longitude and latitude. Display positions follow the ellipsoidal Albers equations, the existing antimeridian wrapping and densification, and a common scale across the three panels. Regional offsets are applied only to the display. Metal subtracts the panel's Double-precision origin before storing Float32 local meters.

| Region | Central meridian | Latitude of origin | Standard parallels |
| --- | ---: | ---: | --- |
| Contiguous U.S. | −96° | 23° | 29.5°, 45.5° |
| Alaska | −154° | 50° | 55°, 65° |
| Hawaii | −157° | 3° | 8°, 18° |

## Numeric checks

- The existing PROJ 9.3 reference passed 14 coordinates. Maximum observed projected error was 1.01×10⁻⁸ m. The same suite checked relative geodesic area error at 7.21×10⁻⁹ and validated the Aleutian antimeridian, the inset scale, historical extent and regional layout.
- The projected index and linear reference agreed on 70,000 hit probes. Both returned the Concordia county and parish records with IDs 6860 and 12250 on the historical overlap fixture. The checked election geometry includes the eight Connecticut traditional counties and all 40 substituted Alaska State House districts.
- libtess2 agreed with the independent area and containment references across 28,767 parts. The tests exercise holes, concavity, tiny islands, narrow corridors, duplicate points, multipart geography and a self-crossing outline. The exact Double-space maximum relative area error was 5.43×10⁻⁹.
- The packed Float32 vertices incurred a maximum measured displacement of 0.135 m across the tested inputs, within the 0.5 m guard. Per-part area after packing varied by at most 0.043% for Census counties and 0.064% for election counties. The largest fraction in the historical set was about 1.65% on a very small part. These are relative area changes from coordinate rounding. They do not indicate a multi-meter position change. The complete-part collapse guard passed the 14 actual inputs.
- The original fill authority and CPU picking retain their geometry even when a mesh fails. The synthetic collapsed feature reports its source ID, and a separate valid feature in that scene remains selectable through the Core Graphics fallback.

[Projection and picking output](evidence/albers-metal-0.3/geography/geographic.txt) · [Mesh and packing output](evidence/albers-metal-0.3/geography/tessellators.jsonl) · [Election date tests](evidence/albers-metal-0.3/geography/presidential.txt).

## Geographic coverage

| Geographic input | Supported features | Parts | Qualification |
| --- | ---: | ---: | --- |
| Census counties | 3,144 | 3,446 | Host checks and simulator |
| Election county derivative | 3,153 | 3,886 | Host checks and simulator |
| Modern states | 51 | 316 | Host checks and simulator |
| 119th congressional districts | 436 | 728 | Host checks and simulator |
| Historical counties 1788, 1860, 1900, 1980, 2000 | 332, 2,181, 2,845, 3,143, 3,139 | 640, 2,954, 4,158, 4,217, 4,216 | Inclusive date snapshots, one geography active at a time |
| Historical states and territories 1788, 1860, 1900, 1980, 2000 elections | 16, 41, 52, 51, 51 | 294, 755, 1,053, 1,052, 1,052 | Scheduled election date or federal Election Day |

The election date regression checked all 60 electoral snapshots through 2024. The first election uses February 4, 1789. Elections after 2000 reuse the November 7, 2000 state geometry. County result vintages retain the 2024 source disclosures and Connecticut and Alaska substitute caveats from the existing election data package.

The Albers regional policy excludes American Samoa, Guam, the Northern Mariana Islands, Puerto Rico and the U.S. Virgin Islands. This policy explains the lower bundled Census feature counts relative to MapLibre. Additional outlying-territory presentation stays open. Generalization in the input geography determines the available coastline detail, and zooming magnifies that existing detail.

## Matched detail screenshots

The simulator selected Cuyahoga County through geographic search, framed the same feature through the shared camera and captured the map in both Albers backends. The 1,074×1,732 images had 96.93% exact RGB agreement and 98.36% agreement within eight levels per channel. The amber selection masks had 99.26% intersection over union. Exterior and adjacent county borders remain visible around the lake and in the highlighted area. The visual comparison covers stylized width and antialias differences as well as geography. Source geometry comes from the U.S. Census Bureau's 2024 cartographic boundary files. [Census county boundaries](https://www.census.gov/geographies/mapping-files/2024/geo/carto-boundary-file.html) · [Census public-use citation](https://www.census.gov/about/policies/citation.html).

[Core Graphics detail](evidence/albers-metal-0.3/visual/cuyahoga-cg.png) · [Metal 4× detail](evidence/albers-metal-0.3/visual/cuyahoga-metal-4x.png) · [Pixel comparison](evidence/albers-metal-0.3/visual/detail-coverage.json).

## National coverage and inset alignment

The same simulator and viewport rendered Census counties with Core Graphics, Metal at one sample, and Metal with 4× MSAA. The land screen check uses the blue fill palette, with a blue-to-green separation of at least 24 levels to exclude the darker Alaska and Hawaii labels. A pixel counts as a distant mismatch when the other mask has no fill within three physical pixels. The threshold is at most 0.5% in each direction for each region.

| Region | Blue fill mask intersection over union | Larger directional mismatch beyond three pixels |
| --- | ---: | ---: |
| All panels | 93.79% | 0.00113% |
| Contiguous U.S. | 92.78% | 0.00038% |
| Alaska | 98.20% | 0.00455% |
| Hawaii | 96.62% | 0.00000% |

Thin white borders and coverage smoothing change the exact mask at individual pixels. The measured distant mismatches remain below 0.005% in each region. Inspection showed the same geographic extent and visible Alaska peninsula, Aleutian chain and Hawaiian islands. Corner shapes, edge opacity and stroke joins differ slightly. The 4× pipeline adds intermediate coverage colors around boundaries compared with the 1× configuration. Source generalization and the pixel threshold limit evaluation of the smallest islands.

[Core Graphics nationwide](evidence/albers-metal-0.3/visual/counties-cg.png) · [Metal 1× nationwide](evidence/albers-metal-0.3/visual/counties-metal-1x.png) · [Metal 4× nationwide](evidence/albers-metal-0.3/visual/counties-metal.png) · [Numeric comparison](evidence/albers-metal-0.3/visual/coverage.json).
