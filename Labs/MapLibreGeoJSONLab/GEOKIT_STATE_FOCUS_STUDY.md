# GeoKit state focus study

A focused view resolves county or district membership from the active source. It fits the union of those original projected parts and derives the camera scale from the viewport. It keeps the primary source, mesh, style arrays and stable hit IDs. The presentation excludes other counties from submission and selection. Mainland states receive thin context outlines from the separate 2024 Census state scene. Alaska and Hawaii are isolated, and use their real projected parts with no mainland adjacency.

| State | Eligible reporting-geography features | Focus check |
| --- | ---: | --- |
| Alabama | 67 | 2004 and 2024. Statewide and Autauga County inspection. The 2004 result uses the disclosed 2024 boundary fallback. |
| Texas | 254 | Large, wide bounds with only eligible county geometry and visible neighboring outlines. |
| Florida | 67 | Long peninsula, Gulf and coastal geography. |
| Michigan | 83 | Both peninsulas, Great Lakes and multipart features. |
| Virginia | 133 | Counties and independent cities. |
| Connecticut | 8 | Traditional county substitutions. Context uses the 2024 Census state outline, and the county file's unresolved vintage is disclosed. |
| Alaska | 40 | State House reporting districts, antimeridian projection, isolated context. State House District 1 was inspected. Earlier years use the same disclosed district-boundary fallback. |
| Hawaii | 5 | Disconnected island extents, regional isolation and the existing no-data treatment where applicable. |

The inspector retains stable feature identity when returning to national. Decorative paths use no county style or hit index. Statewide inspection identifies local records, joined records and the sum across reported local records. The individual inspection retains the result and provenance. The paired views and diagnostics remain in the private review package.

DistrictAtlas repeats national editorial, national exploratory, Texas state focus, TX-18 inspection and return to National on one published Metal mesh. It projects 436 districts, limits Texas to 38, and uses synthetic categories. The Texas focus was also captured in actual 800 × 550 and 1,050 × 570 point macOS windows. [Independent GPU acknowledgements](evidence/geokit-0.4c/district-policy.log)
