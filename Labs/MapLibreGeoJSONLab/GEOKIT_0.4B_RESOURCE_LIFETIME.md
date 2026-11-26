# BalloticsGeoKit 0.4B resource lifetime

| Resource | Owner and lifetime |
| --- | --- |
| Validated source and projected scene | The caller owns import lifetime and revision. The lab retains its bounded projected cache. Focus, camera, composition, labels and recoloring reuse the canonical projected scene. |
| CPU mesh | A worker owns Swift arrays and libtess2 scratch state part by part. Cancellation is checked at region and part boundaries and before and after GPU allocation. The convenience preparation path drops CPU arrays after allocation and returns the prepared GPU artifact. |
| Preparing GPU geometry | Immutable buffers are private to the preparation result. Both consumers cancel superseded work and recheck their current source or renderer before main-actor publication. |
| Active GPU geometry | One published generation per surface. Replacement verifies the mesh identity and device, and creates the complete style table before publication. |
| Retired GPU geometry | Metal's retained-reference command buffers hold the geometry and style buffers used by an encoded command. Clearing or replacing drops the surface's references immediately and releases drawables. Completion drops the command's references and retires its generation count. Three in-flight commands per surface bound that retention. |
| Appearance | Recoloring publishes a new immutable style-buffer pair. Composition, border, focus, resize and selection advance the appearance ticket and reuse geometry. |
| Frame stages | Upload, command submission, GPU completion and drawable presentation have distinct signals. Current generation and appearance are required for acknowledgements, observable readiness and GPU failure callbacks. The lab additionally guards the consuming request while a replacement is pending. |

`cpu_mesh_bytes` counts contiguous CPU payloads. The individual Metal byte fields count requested buffer lengths. `part_bytes` counts packed GPU feature/stripe associations, and `selection_range_bytes` counts logical CPU ranges inside the prepared object. Dictionaries, command buffers, libtess2 transient allocations and retained Metal allocations add overhead. The lab's preparation footprint sampler includes host allocator and process effects. The drawable resolve, multisample color and depth fields are attachment-size estimates. Physical GPU residency needs a hardware instrumentation trace.

The ten-cycle independent DistrictAtlas route used 4× sampling and showed a GPU completion for all ten creations. Each release dropped 9,413,384 bytes of logical geography; each observed clear reported zero active generations and zero commands in flight. The desktop run measures the create/clear path and the package's compiled library lookup.

## Simulator lifecycle observations

The opt-in Release route exercises rotation, Home and foreground, year recoloring, direct source changes, geographic selection, camera and profile changes, dismissal and reopening. Each accepted session passed every assertion in the stress verifier.

| Measurement | iPad A16 · extraction build | iPhone 16e · final build |
| --- | ---: | ---: |
| Duration (s) | 1,800.27 | 1,800.03 |
| Year changes / source changes / successful selections | 100 / 50 / 2,969 | 100 / 50 / 2,962 |
| Rapid requests / navigation steps | 10 / 6,193 | 10 / 6,178 |
| Mismatches / timeouts / fallbacks | 0 / 0 / 0 | 0 / 0 / 0 |
| Superseded commits / stale paints | 0 / 0 | 0 / 0 |
| Geographic mesh publications in same-source year phase | 1 | 1 |
| Layout changes / mesh publications / releases | 885 / 53 / 53 | 883 / 53 / 53 |
| Maximum projected cache entries | 2 | 2 |
| Maximum observed in-flight commands at layout/release | 1 | 1 |
| Release callbacks with commands still in flight | 3 | 2 |
| Peak sampled process footprint (MiB) | 167.60 | 157.42 |
| Steady process footprint median / P95 / P99 / max (MiB) | 153.14 / 163.86 / 163.91 / 163.92 | 143.69 / 149.89 / 150.02 / 150.05 |
| Process footprint after dismissal (MiB) | 105.00 | 100.36 |
| Observed footprint drop on dismissal (MiB) | 48.14 | 43.14 |

Projected cache size and mesh retirement met the verifier over the full routes. The footprint numbers come from process sampling. Device memory pressure and physical residency require the hardware protocol.

[iPad A16 · extraction build summary and raw trace](evidence/geokit-0.4b/lifecycle/iPad-A16) · [iPhone 16e · final build summary and raw trace](evidence/geokit-0.4b/lifecycle/iPhone-16e-final)
