# BalloticsGeoKit 0.4B mesh parity

The Release host harness linked the extracted mesh target and separately compiled the preserved 0.4A source. It verified 14 datasets with the same inputs and order. Every feature count, geographic part count, logical vertex and triangle count, regional origin, original boundary count and deduplicated count matched. Each regional hash matched for vertex, triangle index, part association, boundary segment, deduplicated index and sorted selection range payloads. The hashes include the existing inset decoration buffers. The geographic vertex and triangle columns exclude those decorations.

| Geographic input | Features | Parts | Vertices | Triangles | Indices including decorations | Original segments | Deduplicated segments |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Census counties | 3,144 | 3,446 | 220,592 | 213,746 | 641,250 | 220,600 | 126,298 |
| Election county geometry | 3,153 | 3,886 | 334,385 | 326,658 | 979,986 | 334,394 | 233,230 |
| Census states | 51 | 316 | 90,473 | 89,841 | 269,535 | 90,482 | 61,239 |
| Congressional districts | 436 | 728 | 173,341 | 171,901 | 515,715 | 173,350 | 102,673 |
| County history 1788-01-01 | 332 | 640 | 10,274 | 8,996 | 26,988 | 10,274 | 6,070 |
| County history 1860-01-01 | 2,181 | 2,954 | 43,310 | 37,433 | 112,299 | 43,315 | 23,649 |
| County history 1900-01-01 | 2,845 | 4,158 | 58,479 | 50,182 | 150,558 | 58,494 | 31,544 |
| County history 1980-01-01 | 3,143 | 4,217 | 59,179 | 50,807 | 152,433 | 59,193 | 33,549 |
| County history 2000-01-01 | 3,139 | 4,216 | 59,018 | 50,648 | 151,956 | 59,032 | 33,472 |
| State history 1789-02-04 | 16 | 294 | 4,176 | 3,588 | 10,764 | 4,176 | 3,391 |
| State history 1860-11-06 | 41 | 755 | 9,670 | 8,160 | 24,480 | 9,670 | 7,149 |
| State history 1900-11-06 | 52 | 1,053 | 13,166 | 11,060 | 33,192 | 13,174 | 10,523 |
| State history 1980-11-04 | 51 | 1,052 | 13,106 | 11,002 | 33,018 | 13,114 | 10,493 |
| State history 2000-11-07 | 51 | 1,052 | 13,106 | 11,002 | 33,018 | 13,114 | 10,493 |

Each profile produced the same payload within each version. The package also verified that the regional parts carry source polygon ordinals and correct feature IDs. All 690,408 path-versus-triangle comparisons agreed across 28,767 geographic parts. The synthetic containment cases and collapsed-part fallback passed. The source manifest records SHA-256 for the unchanged canonical inputs and the locally generated derivative.

[0.4A regional hashes](evidence/geokit-0.4b/mesh-04a.jsonl) · [0.4B regional hashes](evidence/geokit-0.4b/mesh-04b.jsonl) · [Source hashes](evidence/geokit-0.4b/source-hashes.json)
