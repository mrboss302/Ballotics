# Metal resource lifetime

Albers camera updates change uniforms on the visible view. They reuse the prepared buffers. The background preparation task retains the projected scene while it builds arrays, then allocates shared Metal buffers and drops the staging arrays after upload. The app's projected-scene cache remains bounded by the limits established in 0.2. During a source switch, an active scene and a preparing or cached scene can overlap. Cancellation lets obsolete work finish its current part and stops publication when the request no longer matches.

## Resource accounting

The election derivative contains 3,153 supported features in 3,886 polygon parts. These are the logical GPU buffer lengths in the national election mesh. Decorative panels account for the additional vertices and indices above the tessellation output.

| Resource | Bytes | MiB |
| --- | ---: | ---: |
| Position and part-ID vertices | 5,350,288 | 5.10 |
| Triangle indices | 3,919,944 | 3.74 |
| Part style and stripe metadata | 31,112 | 0.03 |
| Ring segments and unique-boundary indirection | 8,958,376 | 8.54 |
| Total geographic buffers | 18,259,720 | 17.41 |
| One ordinary county style table and palette | 75,672 | 0.07 |

Shared-buffer length accounts for the visible resource allocation contract. It does not measure physical GPU residency, heap padding, driver objects or compositor backing. CPU arrays contain at least the same payload while they are being copied, plus temporary tessellation and boundary deduplication structures. The measuring build samples task physical footprint and RSS every 10 ms during mesh preparation. These are process measurements and include platform memory attribution independent of our buffer lengths.

The portrait election viewport is smaller than the geometry-only viewport because its result controls occupy screen space. The capture logs the actual drawable dimensions. For a 1,110×1,593 geometry-only drawable, a 32-bit resolve color attachment is 6.75 MiB. Single-sample depth adds another 6.75 MiB. At 4×, multisample color and depth are nominally 26.98 MiB each, or 60.71 MiB including one resolved color attachment. These are uncompressed single-frame texture estimates, with actual residency, pooling and extra drawables left to platform instrumentation.

## Lifecycle

The view tracks its own geographic generation and retires old arrays when the source changes or the view leaves the hierarchy. Existing commands retain their referenced buffers through completion. Their callbacks can release the in-flight permit without modifying an unrelated geographic generation. The view owns one active style palette at a time. A geometry revision or fallback releases that palette and drawable attachments. The three-command gate bounds submitted work. A newer source request can make an old background preparation obsolete before it reaches the view.

An idle map requests frames on changes to camera, layout, theme, selection and activity. Gestures and animations supply invalidations through the same path. The view does not submit new commands while the scene is inactive. Map dismissal releases the drawable and camera presentation resources. A warm reload reuses authorized projected data only if it survives the bounded cache. The map is offline when supplied with bundled or local data. The optional election package download remains a separate, explicit lab option.

## Long route and cancellation

The iPhone 17 Pro simulator completed 1,800.06 seconds, 100 consecutive year observations, 10 additional superseding election requests, 50 source changes, 6,085 navigation steps and 6,081 successful geographic selections. It logged zero state mismatches, readiness timeouts or renderer fallbacks. There were 53 retired mesh generations and 12 retirements while a GPU command remained in flight. Command completions safely released those references. The route included real background and foreground transitions and separate map dismissal and reopening.

| Physical footprint measurement | MiB |
| --- | ---: |
| Median after four minutes | 119.47 |
| P95 after four minutes | 128.00 |
| P99 after four minutes | 128.38 |
| Maximum checkpoint after four minutes | 128.50 |
| Highest sampled preparation footprint | 134.44 |
| Before final dismissal | 117.32 |
| Three seconds after final dismissal | 75.96 |

The highest sampled peak occurred during the midpoint map reopen. The initial harness reported readiness from the old status before the new drawable was submitted. The associated release, 3,153-feature mesh publication and first GPU completion confirm the rebuild. The corrected 120-second follow-up resets status before reopening and confirmed readiness after the new submission. It recorded the deliberate two-second wait plus 1.08 seconds until observing the reopened map ready. The footprint dropped by 41.36 MiB on final release in the long route. Simulator runtime, build, profiling and secondary UI activity affected this long route's host. The matched performance cohort ran separately.

The explicit 1788 → 1980 → 1860 → 2000 supersession probe originally caught two obsolete mesh publications. The corrected backend uses the live request state before publishing and cancels active preparation when a newer source request is pending. It preserved the initial 1788 mesh as the previous complete view, discarded the two superseded preparations and published the 2000 snapshot with 3,139 features. This protects source changes at the boundary between a pending request and a committed dataset. The 30-minute run preceded the targeted fix and exercised sequential geography changes and same-source election supersession. The before-and-after cancellation traces cover the race separately.

[30-minute aggregate](evidence/albers-metal-0.3/lifetime/stress-30m-summary.json) · [Raw 30-minute route](evidence/albers-metal-0.3/lifetime/stress-30m.jsonl.gz) · [Cancellation comparison](evidence/albers-metal-0.3/lifetime/historical-cancel-fixed-summary.json) · [Reopen recheck](evidence/albers-metal-0.3/lifetime/reopen-120s.jsonl.gz).

A static Census map submitted one frame before the first background transition. Its submission counter remained at one through 7.62 seconds in the background. Foreground invalidation submitted one new frame, and the counter then remained at two through the next idle period and 9.65-second background interval. This isolates the on-demand lifecycle from the constant invalidation used by the long route. [Idle and background counters](evidence/albers-metal-0.3/lifetime/idle-background-foreground.jsonl).
