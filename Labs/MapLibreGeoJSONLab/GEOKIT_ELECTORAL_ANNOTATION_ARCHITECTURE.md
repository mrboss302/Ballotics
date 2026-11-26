# Electoral annotation architecture

The election adapter joins normalized allocation rows to the active geographic record IDs. The originating row identity, quantity, counting basis, source line, candidate, affiliation and status survive the join. The native ElectoralAnnotationView consumes that content. Its action selects the same feature index the map uses. Results and the inspector use the same typed annotation.

| Layer | Responsibility |
| --- | --- |
| Election presentation | Ballot basis, candidacy and affiliations, allocation unit types, explicit assigned counts, reconciliation and display styles. |
| Reusable geographic package | AnnotationGrid lays out ordered external interaction frames. AnnotationCallouts checks the geography revision, viewport, available rectangle and leader corridor. LabelResolver and its cached group anchors supply contained internal placements. These APIs accept generic feature indices, anchors, sizes and obstacles. |
| Editorial composition | Accept a measured contained allocation graphic first. Then try a short callout in open space. Use the separate annotation region if neither is safe. A bounded supplemental row gives way to the Results key. |
| Apple view | Uses the existing appearance and Metal presentation revisions, paints native tiles and leaders, exposes one accessibility element per allocation and updates selection locally. |

Candidate identity and affiliation are separate. An explicit quantity of two for an at-large allocation gives two squares under a compatible counting basis. Source rows are retained once even when drawn as multiple squares. A source-reported count and an assigned count remain distinct. Early two-vote ballot totals are labeled as reported entries. Unresolved and incompatible totals suppress recipient artwork.

The revision checks and cached interior anchors allow appearance updates to reuse geometry, tessellation and mesh data. The Metal and Core Graphics drawing contracts remain the same. The county policy and DistrictAtlas data remain independent of electoral content.

The opt-in synthetic fixture uses Maine geography as a stable target. All displayed election quantities in the fixture are synthetic. Changing its allocation updates the colors, annotation, inspector and totals through the existing appearance revision using the same scene.
