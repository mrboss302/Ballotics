# Presidential data audit

The seven local inputs match the Git blob IDs at Ballotics `master` commit `4a3abeb38a6bb30fb1aa216da46d90fea1f5cf2d`. Input SHA-256 digests are recorded in the local manifest. Each source remains byte-for-byte intact. [Pinned Ballotics source tree](https://github.com/mrboss302/Ballotics/tree/4a3abeb38a6bb30fb1aa216da46d90fea1f5cf2d).

| Input | Audit result |
| --- | --- |
| `PresElectionData.json` | 60 election keys from 1788 through 2024, 173 result rows and one source-declared president per year. Thirty-two source popular-vote zeroes have ambiguous meaning. The normalized count is null and the original zero remains available. |
| `PresElectionStateData.csv` | 3,155 rows, including 679 source `Vacant State` placeholders. Historical suffixes are split parts. Explicit Maine and Nebraska labels are district allocations, and the paired state rows are at-large allocations. |
| `pres_election_county_data.json` | Seven years and 22,073 reporting records. The source includes county equivalents, Alaska districts, named non-geographic rows and Missouri's Kansas City reporting unit. |
| Candidate and party identity | 112 person IDs and 24 party IDs in the research catalog. Named aliases are reviewed and stored explicitly. Aggregate rows have no person ID. The 1864 National Union ticket and the three Progressive campaigns have separate identities with original labels retained. |

All 60 National Archives pages were captured locally. The comparison script extracts the reported total and majority, counts the presidential vote columns, and compares source counts by state. For 1788–1800 it compares the elector-appointment quantity by state because the original ballot had two undifferentiated votes. The source sometimes groups candidates or shows both appointed and cast counts, so its table header can differ from the sum of the columns. The title row for 1824 names the electoral plurality recipient. The notes and the House tally identify the president chosen. [National Archives election results](https://www.archives.gov/electoral-college/results) · [1824 tally and decision](https://www.archives.gov/education/lessons/electoral-tally).

Across the source totals, 32 elections have unequal reported, national-row or state-row sums, and 44 elections have a difference once candidate-level comparisons are included. The independent Archives comparison found 47 state count differences across 29 elections, including 13 in 2024. The raw reports preserve the basis for each comparison and the archive page digest. The comparison does not certify minor-candidate vote attribution, popular-vote totals or completeness in every election. [Discrepancy report](ELECTION_DATA_DISCREPANCIES.md).

## Reproducing the research release

Run from the Ballotics repository root.

```sh
python3 ElectionData/tools/normalize.py --geometry \
  --evidence ElectionData/research-staging/audit.json
python3 ElectionData/tools/audit_archives.py \
  ElectionData/research-staging/0.1.0/national.json \
  ElectionData/research-staging/reference/national-archives-html \
  ElectionData/research-staging/archives-audit.json
python3 ElectionData/tools/check_publication.py
```

The first two commands use the Python 3 standard library. Install `jsonschema` for the verification command. The normalizer rejects duplicate JSON keys, unknown candidate names, broken joins and duplicate publication identities. Re-running both builds yields identical package, geometry and audit bytes. The lab uses the same generator at build time.

## Shared repository

[Ballotics-Data](https://github.com/mrboss302/Ballotics-Data/tree/3b37ce5febc061f8432ff19abcfe202f7369a5d8) was inspected on `main` at `3b37ce5febc061f8432ff19abcfe202f7369a5d8`. Its provisional v1 contract and fixtures cover Senate aggregates and synthetic validation failures. The presidential schema, fixtures and tools here are a proposed additive namespace. Raw sources, complete generated outputs and source snapshots remain in local research staging pending file-specific rights and hosting review. Provenance checks are still needed for the historical boundary derivative, Connecticut geometry and the underlying county-return compilation.
