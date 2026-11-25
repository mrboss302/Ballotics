# Android feasibility

The geographic behavior can be reproduced natively with Kotlin and Jetpack Compose. Start by porting the WGS 84 transform and the date and identity contracts. Build paths, regional bounds and the immutable index on a worker dispatcher. Use the same 64-bit projection and subtract a regional or camera origin before converting drawing positions to a platform float representation. Run the [PROJ and selection fixtures](ALBERS_CARTOGRAPHIC_VALIDATION.md) against both implementations.

| Responsibility | Android approach to evaluate |
| --- | --- |
| Geographic source | Stream or decode offline GeoJSON, validate as on iOS, and keep original WGS 84 plus stable feature and revision IDs. A Room or indexed file storage implementation may differ internally while preserving calendar and validity behavior. |
| Geometry and visibility | Build regional Android `Path` objects with `EVEN_ODD` fill and preserve multipart grouping. Use a packed median BVH for candidates. Keep geometry revisions out of mutable composable state that changes during gestures. |
| Camera and display | Use a Compose Canvas with `DrawScope` transforms and a viewport state object. Cache path resources with a lifetime independent of gesture state. Drive animation with a display clock and retain a stable coordinate system across national and regional framing. |
| Styling and selection | Feed stable-ID style tables into the draw layer. Render clipped symbolic stripe fills and width in display space. Use a portable point-in-ring operation after candidate lookup. Compare edge and overlap semantics with the iOS fixtures. |
| Accessibility | Provide a Compose LazyColumn search, result and overlap dialogs with semantics nodes. Expose the selected geographic identity and textual outcome without creating thousands of independent map children. Validate TalkBack at large text sizes. |
| Qualification | Use Macrobenchmark navigation routes and Perfetto FrameTimeline histories on physical Android devices. Count presentation separately from callback or draw timing. Capture jank, physical memory and resource release after source churn. |

An Android Canvas port is an experiment. Profile whether the installed GPU backend handles paths and clipping efficiently. If the iOS qualification establishes a need for tessellation, evaluate native Vulkan or OpenGL ES only after testing the same requirements on Android. Share numeric fixtures, stable IDs, Mercator-vs-Albers camera normalization, styling rules and output checks between native projects. No Android renderer is included here. [Compose drawing](https://developer.android.com/develop/ui/compose/graphics/draw/overview) · [Android FrameTimeline](https://perfetto.dev/docs/data-sources/frametimeline).
