# Native Android feasibility

The portable contract is a validated geographic revision, an ordered regional scene and a derived mesh. Keep stable source IDs, inclusive validity intervals and applied boundary vintage independent of the rendering API. Persist canonical WGS 84 where rights permit it. Use Double for projection and panel layout on both platforms. Tests can compare the regional origin, feature and part metadata, original segments, area, fill coverage and z order. Two equivalent triangulations can differ in their exact triangle order.

## Components to carry forward

| Contract | Android implementation to evaluate |
| --- | --- |
| WGS 84 and historical date behavior | Port the regional projection, ring densification, date semantics and ID contract to Kotlin or a small shared native library. Run all PROJ and historical selection fixtures in both applications. |
| Regional mesh | libtess2 is portable C. Compile the same pin using the NDK and call it off the UI thread. Preserve Float32 local positions, UInt32 triangle and boundary indices, panel origins, part ordering and feature/style indices. Keep source paths for picking and fallback. |
| Styling | Exchange feature-indexed style ranges and palette entries. Preserve screen-space widths, outcome descriptions and symbolic stripe semantics. Normalize the density conversion between iOS points and Android dp. |
| Interaction | Port the camera/framing definitions, indexed containment, stacked overlap lists and search. Apply Android density and lifecycle on the platform side. Validate TalkBack and font scaling against the native result inspector. |

The checked-in mesh types describe an in-memory contract. A serialized artifact would need an explicit byte order, version, projection and source hashes, signedness and range checks. Any redistribution also depends on the underlying geographic rights. The host fixtures and source hashes are portable before any such format is needed.

## Android drawing paths

| Path | What to measure |
| --- | --- |
| Hardware-accelerated Android Canvas | Start with a cached even–odd Path renderer. Compare culling, antialiasing, frame deadlines and retained path memory. Hardware support for individual draw operations varies with API level. [Android hardware acceleration](https://developer.android.com/develop/ui/views/graphics/hardware-accel). |
| OpenGL ES | Check a batched indexed mesh with screen-space instanced strokes, depth ordering and multisample resolve. Measure command overhead, actual driver behavior and how context recreation affects resource lifetime. [Android OpenGL ES guide](https://developer.android.com/develop/ui/views/graphics/opengl/about-opengl). |
| Vulkan | The same mesh, style and stroke data can feed Vulkan pipelines. Measure the benefit of explicit submission and memory control against pipeline, synchronization and driver-compatibility work. [Android Vulkan guide](https://developer.android.com/codelabs/beginning-vulkan-on-android). |

Use native Android route benchmarks and Perfetto FrameTimeline to compare actual and expected app and display frames. Inspect memory and lifecycle on more than one mobile GPU family. Set the backend decision from equivalent visual evidence and captured presentation data on Android. [Perfetto FrameTimeline](https://perfetto.dev/docs/data-sources/frametimeline).

This milestone produces an iOS Metal research backend and portable qualification fixtures. It leaves Android renderer implementation and backend selection open.
