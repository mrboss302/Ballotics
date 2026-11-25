# Albers cartographic validation

## Coordinate and area reference

| Region | Central meridian | Latitude of origin | Standard parallels |
| --- | ---: | ---: | --- |
| Contiguous U.S. | −96° | 23° | 29.5°, 45.5° |
| Alaska | −154° | 50° | 55°, 65° |
| Hawaii | −157° | 3° | 8°, 18° |

The ellipsoid is WGS 84, with a semi-major axis of 6,378,137 m and inverse flattening of 298.257223563. `albers-reference.py` uses independently installed pyproj and PROJ 9.3.0. The fixture covers 14 coordinates, including an Aleutian seam pair, and three geodesic area patches. Maximum vector error was approximately (1.01 \times 10^{-8}\) m against a 0.01 m tolerance. Maximum relative patch-area error was approximately (7.21 \times 10^{-9}\) against a (10^{-5}\) tolerance. These are numerical tolerances for the projection, distinct from the resolution of the input boundary files. [PROJ Albers reference](https://proj.org/en/stable/operations/projections/aea.html) · [U.S. Census cartographic boundary files](https://www.census.gov/geographies/mapping-files/time-series/geo/cartographic-boundary.html).

Inset placement is a translation after projection in regional meters. Equal viewport scale preserves comparable areas between panels. A reversed inset layout retained identical paths and Anchorage selected the same Alaska record. Alaska longitude normalization handles 179.95° and −179.95° on opposite signed sides of the same synthetic island. Tests cover holes, both sides of adjacency, a small separate island and two legitimately overlapping features returned in paint order. An interior point in the 1808 Concordia county and parish overlap returned both historical IDs 6860 and 12250. The exact coincident-border predicate follows Core Graphics containment. A boundary tolerance policy would require an explicit specification and more fixtures.

## Source and picking reference

- Modern Census state, congressional district and county scenes contained 51, 436 and 3,144 supported features. The complete MapLibre sources retain the additional five state-equivalent outlying territories. The existing Albers regional policy omits American Samoa, Guam, Northern Mariana Islands, Puerto Rico and the U.S. Virgin Islands. Coverage of these areas is a production gate.
- The 2024 county-results derivative contained 3,153 Albers features. Original-coordinate ray casting agreed with indexed picks in traditional Connecticut county geometry and the Anchorage 2024 State House district. Earlier election results keep the existing boundary-vintage disclosure. Feature matching still follows the election geography resolver.
- The five historical county snapshots (1788, 1860, 1900, 1980 and 2000) contained 332, 2,181, 2,845, 3,143 and 3,139 active features respectively. The matching historical state/election snapshots contain 16, 41, 52, 51 and 51 active features. The first presidential election uses the February 1789 scheduled Electoral College vote. Later national snapshots retain territorial versions and show inset polygons only when the snapshot includes them.
- The index and an independent linear oracle agreed on selection and ordered visible parts using 5,000 probes per scene for 14 scenes (70,000 total). They also agreed on each part's `CGPath`, inset frame and translation. Original geographic-coordinate ray casting provided an additional oracle for interior picks in state and county data, including coastal Hawaii, independent Charlottesville and adjacent Albemarle, and city and district examples. Synthetic tests verify failure to pick through a hole.

The [qualification runner](Tests/AlbersQualification.swift), [PROJ fixture](Tests/Fixtures/albers-proj-wgs84.json) and [benchmark reproduction](README.md#albers-rendering-lab-02) allow these checks locally. The Census screenshots show the inset layout and a selected county at detail scale. A visual crack, display-scale antialiasing problem or clipping difference can still require frame-specific inspection. At 16× the original generalized boundaries remain the accuracy limit. [National state view](evidence/albers-rendering-0.2/visual/census-states-national.png) · [Cuyahoga County focus](evidence/albers-rendering-0.2/visual/census-county-focused.png).
