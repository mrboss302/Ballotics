# GeoKit thematic style contract

GeoKit accepts feature-indexed `CGColor` arrays. A single color is categorical or scaled fill. Multiple colors retain the existing symbolic stripe semantics. The consumer supplies colors for missing, unresolved, tied and nonparticipating records. Visibility and camera focus are policy inputs. Color changes, contextual stroke treatment and selection affect appearance. The mesh retains geographic parts, vertices, indices and hit testing.

`DivergingThematicScale` interpolates caller-owned signed color ramps on −1…+1. It provides the same color function for the map and its legend. A sequential scale can use one half of the domain. DistrictAtlas uses synthetic categorical colors only.

The lab computes signed county margin from the existing normalized `marginVotes / totalVotes`, where margin is the reported leader minus runner-up and the denominator is reported total votes. It assigns blue to Democratic leading candidates and red to Republican leading candidates. The full ramp is linear in signed margin from zero to 100 percentage points on either side. Third-party winners keep their categorical color. Missing, unresolved and tied records retain their existing statuses. The inspector discloses the available normalized values, and statewide inspection presents sums explicitly as reported local records.

The neighboring state layer uses neutral outlines from 2024 Census geography. It carries a distinct provenance and supplies no results or labels. The 2004 county result map retains the qualified 2024 boundary fallback, including the documented special reporting geography. No statistics or boundary coordinates were inferred from the design reference.
