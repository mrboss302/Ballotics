# BalloticsGeoKit Apple rendering architecture

| Target | Responsibility | Dependencies |
| --- | --- | --- |
| BalloticsGeoKit | Validated polygon source, regional Albers projection, composition, ordered picking, camera, anchor and placement contracts | Foundation, Core Foundation and Core Graphics |
| LibTess2 | Vendored even–odd tessellation at `8dbd6483e920311a58c9af10a10beb278efebc36` | C |
| BalloticsGeoKitMesh | Immutable CPU mesh, regional origins, mesh identity, feature and polygon metadata and original selection edges | BalloticsGeoKit and LibTess2 |
| BalloticsGeoKitApple | Build-time Metal shaders, GPU allocation and retirement, frame readiness, Core Graphics drawing, UIKit gesture host and external annotation viewport layout | BalloticsGeoKit, BalloticsGeoKitMesh, MetalKit and native frameworks |
| DistrictAtlas | AppKit window, Census keys, renderer and profile selection, district search, camera controls, inspection and native labels | BalloticsGeoKit and BalloticsGeoKitApple |

The lab's SwiftUI screen hosts the package's UIKit gesture view through `UIViewRepresentable`. The lab retains source loading, projection-cache policy, camera transition timing, accessibility, the election inspector, the political style mapping and the benchmark route. DistrictAtlas hosts the `MTKView` in AppKit and uses the same camera, style, picking, Metal and Core Graphics interfaces. Both consumers supply colors in projected feature order and retain control of their surrounding screen. The same projected scene drives drawing and hit testing, including regional isolation and historical overlap order.

## Identity and data flow

1. The consumer validates and projects a source with `PolygonSource` and `AlbersScene`. A consumer can attach stable record IDs with `GeographicRecords`.
2. `AlbersMesh.prepare` produces regional Float32 local positions relative to Double-precision origins, UInt32 triangle and boundary indices, part-to-feature indices, geographic part IDs and selection ranges. The original part order and even–odd semantics carry through. The in-memory artifact is backend independent.
3. `GeographicMeshIdentity` combines an explicit caller revision with the token of the canonical projection. The token changes when a scene is projected again, including when routing or geometry-affecting layout inputs change. Composition and other presentation changes preserve that token. Identity is scoped to the process and the live scene.
4. `AlbersMetalPreparedScene` can take the CPU artifact or prepare and allocate off the main actor. `AlbersMetalSurface.publish` verifies the device and the projected scene identity, stages the style table and installs the new generation. Core Graphics draws the original paths from the same scene when chosen or when Metal cannot initialize or prepare.
5. Native views own the text measurement, rendering and accessibility. `MapLabel`, `LabelMeasurements` and `LabelPlacement` stay independent of the GPU. In shallow electoral views the lab assigns a 24-point annotation region through `GeographicMapLayout`. Its established tile order stays in the lab. On compact-height phones the lab aligns the title and source date, and combines the winner and status rows to keep the map viewport usable.

The thematic profile keeps mainland-dominant placement, Alaska at 0.32 and Hawaii at 1.05 of mainland scale. Relative area changes across regions. Equal-Scale Research preserves the separate research profile. Focus filters both drawn parts and picking at the camera boundary, so focusing an inset and returning to national use the same mesh and GPU buffers.

## Shaders and fallback

The Apple target processes `Shaders/AlbersShaders.metal`. Xcode compiles the original shader into the target's resource bundle. The adapter uses fallible bundle lookup so even omission of the entire resource bundle yields an initialization error. It loads the bundle's default library, checks entry points and creates fill and stroke pipelines with the requested sample count. Four samples are the reference. Unsupported counts raise an error. The CLI build system copies the Metal source with the tested toolchain. Use the Xcode product for default Metal discovery or pass a build-time `metallib` URL to the CLI product. The lab retains its explicit runtime compilation probe on the same shader source. [Apple package resources](https://developer.apple.com/documentation/xcode/bundling-resources-with-a-swift-package) · [Apple shader libraries](https://developer.apple.com/documentation/metal/shader-libraries)

Preparation and initial publication report errors through Swift. Later command errors go through `onFailure` for the current generation and appearance. Both consumers fall back to Core Graphics with visible status and retain camera, selection and the original projected paths. Dense national Core Graphics remains a correctness and availability control with lower throughput.

## Portable and Apple-specific boundaries

Identity, projection mathematics, region policy, index, camera and label rules, tessellation semantics, and feature-indexed appearance have portable contracts. The current path representation and containment, Metal and MetalKit, UIKit and AppKit, SwiftUI hosting, and native text and resource lifecycle are Apple implementations. The portable fixtures are checked by the existing host suites. The pinned C tessellator can be built with the Android NDK when a future adapter is designed.
