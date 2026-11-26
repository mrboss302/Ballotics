# Ballotics 2.x and 3.0 county map audit

The local preservation checkout and GitHub both identify the reviewed 2.3.1 source as `4d9f44c355b9b0047d23eb7fa33c82d5a9747c3d`. The 3.0 working copy and its forensic inventory preserve a subset of the application. The two owner images establish presentation intent. Their totals, county vintages and swatches are excluded from calculations.

| Evidence | Recovered behavior |
| --- | --- |
| 2.3.1 `ElectionCountyResultsMap.swift` | Uses dated county results from SwiftData and a state-fit Canvas transform. It renders selected-state counties, contextual state paths, selected-county emphasis and path-based tap targets. Alaska and Hawaii are handled separately. |
| 2.3.1 `ElectionCountyDetailedResults.swift` and `PresElectionCountyDataProcessor.swift` | County and statewide choices, state-filtered records and aggregate result charts. The old data layer parsed and loaded locally stored county returns. |
| 2.3.1 `Thematic/ElectionCountyResultsDotMap.swift` and `County/TEST/ElectionCountyDotResultsMap.swift` | Two distinct experiments survive. One presents thematic dots; the county test varies one circle's size with votes and marks selection. These files do not establish which algorithm, if any, the 3.0 controls used. |
| 3.0 `ElectionCountyDetailedResults.swift` | The year and state flow into the county results map call. A selected county changes the result chart. The view includes a searchable county picker and a statewide entry. The called map implementation and supporting model/import layers are incomplete in the recovered copy. |
| 3.0 owner screenshot and website assets | The Alabama reference shows the map, three visualization controls, a Shift button and margin intensity. A companion legacy asset shows a population presentation. The action bindings, exact map-mode ordering, Shift comparison year and calculation, dot parameters and legend scale are unverified. |

Reusable design decisions include fitting the state's own shape, making surrounding geography quiet, keeping search and hit testing tied to real polygons, and using a separate results inspector. 0.4C derives signed margin color from the qualified normalized package. Vintages, statewide totals and the denominator are disclosed. Shift, population and dot-density modes remain later research. [Preserved 2.3.1 source](https://github.com/mrboss302/balloticsIOS/tree/4d9f44c355b9b0047d23eb7fa33c82d5a9747c3d)
