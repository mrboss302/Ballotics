# Election join coverage

The published county results contain 22,073 reporting records. Of these, 22,027 (99.79%) join to a display polygon, including explicitly labeled fallbacks. The 46 unmapped records contain 2,889,469 source-reported votes across seven cycles. The figure describes reporting rows and may include jurisdictions that overlap other reporting footprints.

| Year | Reporting rows | Polygon joins | Unmapped rows | CT county joins | AK district joins | Alaska HD99 source votes |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 2000 | 3,151 | 4,140 | 12 | 8 | 40 fallback | 0 |
| 2004 | 3,153 | 3,146 | 7 | 8 | 40 fallback | 0 |
| 2008 | 3,153 | 3,146 | 7 | 8 | 40 fallback | 0 |
| 2012 | 3,153 | 3,146 | 7 | 8 | 40 fallback | 0 |
| 2016 | 3,153 | 3,147 | 6 | 8 | 40 fallback | 342 |
| 2020 | 3,155 | 3,151 | 4 | 8 | 40 fallback | 433 |
| 2024 | 3,154 | 3,151 | 3 | 8 | 40 | 401 |

No duplicate polygon join was observed within a county year. Each Alaska dataset includes District 99 in addition to Districts 1–40. Official 2016, 2020 and 2024 returns confirm federal overseas absentee classification. The official 2020 count is 438, including five write-ins omitted by Ballotics. The earlier four zero-valued District 99 rows retain their unresolved classification. Connecticut’s extra statewide write-in rows are present in 2000–2016, with 918 votes in 2012 and 2,616 in 2016. [2016 Alaska return](https://www.elections.alaska.gov/results/16GENR/data/sovc/hd99.pdf) · [2020 Alaska return](https://www.elections.alaska.gov/results/20GENR/data/sovc/HD99p.pdf) · [2024 Alaska return](https://www.elections.alaska.gov/results/24GENR/HD99.pdf).

For the Electoral College, 2,494 of 3,155 CSV rows match a dated state or D.C. feature. Of 2,437 positive allocations, 2,434 match. The three positive records without a state feature are Indiana 1816, Missouri 1820 and Michigan 1836. Their existence is disclosed in the result panel. The remaining rows without matches are zero-valued historical placeholders or rows with unclear status. The archive can include other historical areas active the same day. Their geometry is visible with neutral treatment in the state view.

The full local evidence is `research-staging/audit.json`. It includes all Connecticut and Alaska substitutions by year and code, every unmatched result, and geometry keys without result rows. See the [resolver](ELECTION_GEOGRAPHY_RESOLVER.md) for the matching rules.
