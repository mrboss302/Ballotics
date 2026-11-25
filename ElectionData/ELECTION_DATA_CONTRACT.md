# Presidential election data contract 0.1

The wire contract uses UTF-8 JSON, schema version 1 and publication version `0.1.0`. The [Draft 2020-12 schema](contracts/presidential-v1.schema.json) distinguishes the manifest, national payload and county payload. [Synthetic fixtures](fixtures/) exercise a real tie and rejection of numeric geographic identifiers. Swift Codable and Kotlin serialization can read these contracts. Use 64-bit integer fields for counts.

| Entity | Identity and semantics |
| --- | --- |
| Presidential election | `presidential:<year>`. A year denotes the electoral cycle. The winner ID and decision method are independent of the electoral and popular vote quantities. |
| Candidate / party | The explicit registry assigns a stable person ID. The applicable party ID belongs to a result or nomination. Source spelling, party and style are preserved separately. Aggregate rows carry null person and party IDs. |
| National election result | An election-scoped candidate or aggregate entry with electoral votes, optional popular votes, source index and winner flag. A source zero that cannot be verified remains visible as `sourcePopularVotes: 0` alongside `popularVotes: null`. |
| Electoral allocation | One original CSV row with cycle, source line, state jurisdiction, optional district/split part, recipient, votes, join key and review status. `state:11` denotes D.C. Historical suffixes carry source part labels. They do not imply modern congressional boundaries. |
| Reporting jurisdiction | Stable county-equivalent code where verified, with an explicit reviewed crosswalk for Shannon/Oglala Lakota. Traditional Connecticut counties use their county FIPS. Alaska House district IDs carry the election year as a conservative reporting-plan scope. District 99 uses a distinct non-geographic reporting identity. |
| Local result | One cycle and reporting row. The original reporting name/code, listed and reported votes, candidate counts, rank, source share and source margin are available. Normalized shares require a reconciled positive denominator. A margin requires two named candidates at the top and a validated denominator. A positive aggregate leading or tying for the lead yields unresolved status. |
| Geographic source / election geography | The manifest records source ID, file, vintage and original SHA-256. State references use recorded feature IDs with civil-day bounds. County references use source-scoped keys. Join status discloses fallback, missing, changed, overlapping or non-geographic records. Geography files carry polygon coordinates. |
| Dataset manifest | Immutable dataset version, schema version, source digests and payload names, year, byte count, SHA-256 and record count. One national payload, seven independent county payloads. |

```text
0.1.0/
  manifest.json
  national.json
  county-2000.json
  county-2004.json
  county-2008.json
  county-2012.json
  county-2016.json
  county-2020.json
  county-2024.json
```

The app’s separate geographic resource is `county-geometry-2024.geojson`. It is a deterministic renderer input made from the three source files and remains outside the result payloads. The original dated historical state archive remains its own resource. Dependent same-version county files and the national catalog must share one manifest identity. Deploy a changed payload under a new dataset version.

## Install and activation

The lab’s study bundle carries the national and seven county payloads for local evaluation. It imports the national payload at first election entry and installs county years on demand. A configured HTTPS version URL and trusted manifest SHA-256 can supply packages independently. The CLI launch arguments are `--election-package <version-directory-url> <manifest-sha256>`. This milestone accepts remote payloads whose geographic source descriptors match the local release. No research package endpoint was published.

The SwiftData model actor validates the manifest, payload bytes and decoded relationships, then saves edition metadata and rows in one transaction. Each transaction installs one national or county package. Prior packages remain available for reactivation, and county reads use the active national version. A failed or cancelled transaction rolls back before replacing the active edition. The lab opens the previously installed edition offline. The store experiment indexes by logical package/year/kind via query predicates and keeps richer record data in typed Codable rows. Polygon files remain on disk. A future Room implementation can use the same scalar identities and activate editions using a database transaction.
