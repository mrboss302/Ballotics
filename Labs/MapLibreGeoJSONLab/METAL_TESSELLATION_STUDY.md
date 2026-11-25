# Polygon tessellation for Albers Metal 0.3

libtess2 supplies the prototype's even–odd tessellation. Its C API accepts separate contours, preserves an input index for surviving source vertices and identifies vertices it creates where edges intersect. The adapter uses those indices to recover the original Double-precision projected positions. This reduces loss of accuracy from the float coordinates required by the tessellator. Both libraries have portable implementations suitable for investigation on Android. [libtess2 source and API](https://github.com/memononen/libtess2/tree/8dbd6483e920311a58c9af10a10beb278efebc36) · [Earcut source and documented limitations](https://github.com/mapbox/earcut.hpp/tree/2fbafada15dbe5350c606426e17111b91a964e55).

| Candidate | Revision | License and integration |
| --- | --- | --- |
| libtess2 | `8dbd6483e920311a58c9af10a10beb278efebc36` | SGI Free Software License B 2.0. The source and header notices are retained verbatim. The license is included in the repository and in the app bundle. A local Clang module imports the C API into Swift. |
| Earcut | `2fbafada15dbe5350c606426e17111b91a964e55` | ISC. The optional host comparison takes a pinned upstream checkout and uses a small C++ bridge. The lab app ships only libtess2. Distribution of Earcut would require retaining its copyright and permission notice. |

## Correctness

The comparisons used the exact densified projected Core Graphics rings. The reference uses Double-precision shoelace area for a simple exterior-minus-holes polygon and integrates independent even–odd scanline strips for the crossing fixture. Containment compares analytic triangle tests with `CGPath.contains` using the even–odd rule. Samples are deterministic positions within each part's bounding box. Cases exercise concavity, holes with opposite windings, duplicate and near-collinear points, a narrow corridor, a 2 cm square, an intersecting bowtie and a collapsed ring. Real county, state and district files exercise multipart shapes and holes.

libtess2 passed all six area and containment fixtures. Earcut passed five. Both matched the bowtie's total area of 8 m². Earcut disagreed with the original path on 162 of 320 bowtie probes, while libtess2 agreed on all 320. This distinction matters when an archival part must produce the same visible shape and pick result.

The 14 real inputs included both county layers, modern states and districts, five county snapshots and five state and territory snapshots. Each candidate successfully tessellated all 28,767 parts with zero mismatches across 690,408 geometric probes. An x-sorted segment audit found zero proper interior segment crossings among the sampled projected parts. Endpoint contacts and collinear overlaps are outside that audit. The largest libtess2 Double-space relative area error was 5.43×10⁻⁹. The cartographic report covers the GPU buffers and the screen.

| Input | Algorithm | Parts | Vertices | Triangles | UInt32 index bytes | Tessellation time | Additional sampled footprint |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Census counties | libtess2 | 3,446 | 220,592 | 213,746 | 2,564,952 | 86.3 ms | 0.03 MiB |
| Census counties | Earcut | 3,446 | 220,592 | 213,746 | 2,564,952 | 29.2 ms | <0.01 MiB |
| Election counties | libtess2 | 3,886 | 334,385 | 326,658 | 3,919,896 | 149.8 ms | 3.84 MiB |
| Election counties | Earcut | 3,886 | 334,386 | 326,658 | 3,919,896 | 45.7 ms | 0.64 MiB |

CPU timings sum per-part calls in an optimized macOS harness on an Apple M4. The calls include adapter conversion and output copies. The footprint sample is the process increase from the candidate's starting point, sampled every 16 parts. Arena reuse and sampling can hide temporary peaks. Device uploads add inset decorations to the numbers above. [Raw comparisons](evidence/albers-metal-0.3/geography/tessellators.jsonl).

## Failure handling

The runtime preserves original paths and feature identities. It normalizes adjacent points only if they coincide within 0.1 μm in projected space. An invalid contour, out-of-range coordinates, invalid indices, a material area discrepancy or a part that collapses in the GPU vertex layout causes the complete request to fall back to Core Graphics. The error identifies the source feature and its projected part. The synthetic import contains an ordinary visible polygon and an independent collapsed part. The host test verifies the rejection and confirms that the full original scene still contains the visible polygon by its stable ID. The UI test imports the same bundled fixture through the lab’s ordinary importer.
