# Public Apple rendering API

Link `BalloticsGeoKit` and `BalloticsGeoKitApple`. Link `BalloticsGeoKitMesh` directly for CPU-only mesh preparation, validation or backend integration. Use Xcode to build a consumer with the package's default compiled Metal resources.

```swift
import BalloticsGeoKit
import BalloticsGeoKitApple
import CoreGraphics

// Load off the main actor for large sources. The input's identity policy is consumer-owned.
let revision = GeographicRevision(source: "districts", version: "2024-CD119-5m")
let scene = try AlbersScene.load(fileURL).composed(.thematic)
let records = try GeographicRecords(scene: scene, revision: revision) { $0["GEOID"] }
let palette = records.ids.map { _ in [CGColor(red: 0.29, green: 0.49, blue: 0.68, alpha: 1)] }
```

On the main actor, create the renderer and embed it in your UIKit or AppKit view hierarchy. Set its frame and configure the frame and failure callbacks. A SwiftUI consumer can wrap the native view in `UIViewRepresentable` or `NSViewRepresentable`.

```swift
let surface = try AlbersMetalSurface(configuration: .init(samples: 4), diagnostics: {
    diagnostic in receive(diagnostic.kind, diagnostic.fields)
})
guard let device = surface.device else {
    throw GeographicRenderError(message: "No Metal device")
}
let prepared = try await AlbersMetalPreparedScene.prepareAsync(
    scene, revision: revision, records: records, device: device
)
try Task.checkCancellation()
try surface.publish(prepared, scene: scene, palette: palette)

surface.frame = container.bounds
container.addSubview(surface)
let viewport = surface.bounds
let camera = GeographicCamera(
    viewport: viewport,
    center: CGPoint(x: scene.bounds.midX, y: scene.bounds.midY),
    scale: GeographicCamera.fit(scene.bounds, in: viewport)
)
surface.move(camera: camera)
```

A large source replacement should run in a cancellable task tied to the consumer's requested source. Cancel the old task and check the current request after the await. The renderer enforces scene and device compatibility at publication, and guards GPU callbacks by its own generation and appearance tickets. A caller can independently run `AlbersMesh.prepare` and upload it with `AlbersMetalPreparedScene(mesh, device:)`.

```swift
// Hit positions are local points in the geographic viewport.
let selected = records.hits(in: scene, at: camera.world(tap), region: camera.region).first
surface.move(camera: camera, boundary: .init(width: 1.2),
             selected: selected.flatMap(records.index(of:)))
surface.move(camera: camera, selected: nil) // Clear selection.

try surface.updatePalette(newPalette)
try surface.updateLayout(scene.composed(.research))
// Update camera center and scale for the new profile or regional frame.
surface.move(camera: camera)
```

The two adapters accept one color array per feature when a palette is supplied. An empty feature color array uses the base color, one color is a solid fill, and multiple colors give symbolic stripes. Core Graphics draws in a top-left coordinate context. Use `AlbersCoreGraphics.draw(scene, camera: camera, palette: palette, selected: index, in: context)` for the explicit Core Graphics choice or fallback. Catch initialization, preparation and publication errors, and attach `onFailure` for current GPU failures. A consumer should show its active renderer and fallback reason.

Inspect `surface.readiness` for upload, submission, completion and supported physical presentation. `expectPresentation` is the preserved GPU-completion acknowledgement callback. On a hidden or inactive map set `renderAllowed = false` to pause submissions and release drawables. Set it to `true` to resume. Cancel pending work and call `clear()` before releasing the view. `resources` reports logical buffer sizes, estimated attachment bytes and generation counts.

`GeographicInteractionView` translates UIKit tap, double tap, pan and pinch events to overridable local-coordinate hooks. Supply a smaller `interactionViewport` when reserving space with `GeographicMapLayout`. Resolve native labels with `LabelResolver` and put external annotation views in the consuming layout.
