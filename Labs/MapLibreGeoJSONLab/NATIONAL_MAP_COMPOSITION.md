# National map composition

Thematic National is the default presentation for both Albers renderers in the standalone lab. Alaska and Hawaii share the mainland palette, geographic strokes, selection, and inspector. The Map view menu exposes Equal-Scale Research for comparison. The 0.3 branch retains the original implementation.

## Projection and placement

The input remains WGS 84 longitude and latitude. Each region uses its existing ellipsoidal Albers projection. Central meridian, latitude of origin, and standard parallels in degrees are respectively −96, 23, 29.5/45.5 for the mainland, −154, 50, 55/65 for Alaska, and −157, 3, 8/18 for Hawaii. Longitudes wrap around each region's meridian. Paths use projected easting and negative northing so that increasing y points down.

For each projected point **p**, the shared panel computes **w = s p + t**. The viewport draws **v = viewportCenter + k (w − cameraCenter)**. Picking inverts the viewport first and then computes **p = (w − t) / s** before running the original even–odd path tests. Core Graphics applies both transformations to its context. Metal converts the camera center to regional meters and multiplies the point scale by s in each draw uniform. Original ring segments determine outlines, with widths specified in screen points.

| Setting | Thematic National | Equal-Scale Research |
| --- | --- | --- |
| Uniform regional scales, mainland / AK / HI | 1 / 0.32 / 1.05 | 1 / 1 / 1 |
| Horizontal placement | AK begins at 2.5% of mainland width. HI follows the actual AK extent with a 1.2% gap. | Insets follow the original research order and spacing. |
| Vertical placement | AK starts at 78% of mainland height and HI at 89%. Nearby mainland edges can move either inset down. | Both frames begin 145 km below the mainland. |
| Decoration | Continuous map background and geographic outlines | Original inset headers, padding and frames |

The thematic placement uses the actual lower coastline within each inset's horizontal band. A bounding-box-only trial incorrectly counted the southern tip of Texas when checking its western edge, placing Hawaii too far south. The owner reference available during qualification was the local WebP edition of the named election screen. Its mainland dominance, western AK position and adjacent, slightly lower HI informed the proportions. Election colors, geographic features and allocations come from the selected dataset.

Each regional projection remains equal-area. The thematic composition changes relative area across regions by the square of each region's presentation scale. Whole-country area comparisons belong to the equal-scale profile. Both profiles fit the union of available regions into the viewport with 12 points of margin on each side.

## Geography, camera and labels

* The cached scene owns projected paths, feature order, spatial indexes and the original research layout. A profile change derives regional transforms and feature/state framing bounds from that scene. Both profiles use the same mesh origin and vertex, index and stroke data. The shader suppresses research decoration in the thematic profile.
* Regional framing fits the actual transformed extent. The resulting product of viewport and regional scales cancels the inset reduction when fitting original projected geography. Pinch and pan continue from that camera state. National resets the fit and center. Zoom interpolation uses the same camera for drawing and picking on each step. System Reduce Motion requests an immediate move. Neighboring mainland land may remain visible near the edge of a focused viewport.
* Only panels present in the selected source contribute bounds. Territories retain the existing neutral or unjoined treatment. The election resolver continues to disclose November 7, 2000 boundaries for later elections. American Samoa, Guam, the Northern Mariana Islands, Puerto Rico and the U.S. Virgin Islands remain outside the three-region policy.
* Electoral labels use the historical feature's abbreviation and joined positive allocations. Unresolved counts are omitted. Only states and the District of Columbia qualify. The number represents source-reported electoral votes for that election, including its historical voting rules. A projected point on the largest candidate part must pass the original path's even–odd containment test, including holes. A deterministic outward search handles concave shapes. The overlay uses white bold text with a dark shadow. Insets receive collision priority. Small states can appear as the camera magnifies and collisions clear.
* County and district modes omit the electoral labels at all zooms. Search and the result inspector supply the accessible detail. Fixed-size cartographic text and the inspector serve different reading scales. A small-state tile row remains outside this study.

The captured 16e and 17 Pro maps are available in the local owner-review folder. Census-only public evidence is linked in the qualification report. Owner visual acceptance remains open.
