import BalloticsGeoKit
import BalloticsGeoKitApple
import BalloticsGeoKitMesh
import Metal
import XCTest

final class RenderingTests: XCTestCase {
    private let revision = GeographicRevision(source: "fixture", version: "1")

    private func scene(longitude: Double = -106) throws -> AlbersScene {
        let ring = [[longitude, 37], [longitude + 1, 37], [longitude + 1, 38], [longitude, 38], [longitude, 37]]
        return try AlbersScene.project([
            ["type": "Feature", "properties": ["GEOID": "fixture"],
             "geometry": ["type": "Polygon", "coordinates": [ring]]]
        ])
    }

    func testMeshIdentityAndFeatureAssociation() throws {
        let scene = try scene()
        let records = try GeographicRecords(scene: scene, revision: revision) { $0["GEOID"] }
        let mesh = try AlbersMesh.prepare(scene, revision: revision, records: records)
        let composed = try AlbersMesh.prepare(scene.composed(.thematic), revision: revision, records: records)
        XCTAssertEqual(mesh.identity, composed.identity)
        XCTAssertEqual(mesh.featureIDs, records.ids)
        XCTAssertEqual(mesh.panels.first?.geographicParts, [.init(feature: records.ids[0], polygon: 0)])
        XCTAssertGreaterThan(mesh.payloadBytes, 0)
        XCTAssertNotEqual(mesh.identity, GeographicMeshIdentity(revision: revision, scene: try self.scene(longitude: -105)))
        XCTAssertThrowsError(try AlbersMesh.prepare(self.scene(), revision: revision, records: records))
    }

    func testCancelledPreparationCannotProduceAMesh() async throws {
        let scene = try scene()
        let revision = self.revision
        let task = Task {
            withUnsafeCurrentTask { $0?.cancel() }
            return try AlbersMesh.prepare(scene, revision: revision)
        }
        do {
            _ = try await task.value
            XCTFail("Cancelled preparation returned a mesh.")
        } catch is CancellationError {
            return
        }
    }

    func testExternalAnnotationsUseSeparateSpaceInShallowBounds() {
        let bounds = CGRect(x: 0, y: 0, width: 680, height: 135)
        let layout = GeographicMapLayout(bounds: bounds, externalHeight: 24)
        let external = layout.externalAnnotations!
        XCTAssertEqual(layout.viewport.height, 111)
        XCTAssertEqual(external.height, 24)
        XCTAssertEqual(layout.viewport.maxY, external.minY)
        XCTAssertTrue(layout.viewport.intersection(external).isEmpty)
        XCTAssertEqual(layout.viewport.union(external), bounds)
    }

    func testCoreGraphicsReferenceDrawsTheSameRegionalScene() throws {
        let scene = try scene()
        let viewport = CGRect(x: 0, y: 0, width: 64, height: 64)
        var camera = GeographicCamera(viewport: viewport,
            center: CGPoint(x: scene.bounds.midX, y: scene.bounds.midY),
            scale: GeographicCamera.fit(scene.bounds, in: viewport), region: .mainland)
        let colors = [[CGColor(red: 1, green: 0, blue: 0, alpha: 1)]]
        let context = try XCTUnwrap(CGContext(data: nil, width: 64, height: 64, bitsPerComponent: 8,
            bytesPerRow: 256, space: CGColorSpaceCreateDeviceRGB(),
            bitmapInfo: CGImageAlphaInfo.premultipliedLast.rawValue))
        context.clear(viewport)
        let centerOffset = 32 * context.bytesPerRow + 32 * 4
        let pixels = try XCTUnwrap(context.data).assumingMemoryBound(to: UInt8.self)

        camera.region = .hawaii
        XCTAssertEqual(AlbersCoreGraphics.draw(scene, camera: camera, palette: colors, in: context), 0)
        XCTAssertEqual(pixels[centerOffset + 3], 0)
        camera.region = .mainland
        XCTAssertEqual(AlbersCoreGraphics.draw(scene, camera: camera, palette: colors, in: context), 1)
        XCTAssertEqual(pixels[centerOffset], 255)
        XCTAssertEqual(pixels[centerOffset + 3], 255)
    }

    @MainActor
    func testMetalInitializationFailures() async throws {
        guard MTLCreateSystemDefaultDevice() != nil else { throw XCTSkip("No Metal device.") }
        XCTAssertThrowsError(try AlbersMetalSurface(configuration: .init(samples: 0)))
        let missing = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString + ".metallib")
        XCTAssertThrowsError(try AlbersMetalSurface(configuration: .init(shaders: .compiledFile(missing))))
        let configuration = try metalConfiguration()
        XCTAssertThrowsError(try AlbersMetalSurface(configuration: .init(shaders: configuration.shaders,
                                                                          polygonVertexFunction: "missing_probe"))) {
            XCTAssertTrue($0.localizedDescription.contains("missing the polygon vertex function"))
        }
        XCTAssertThrowsError(try AlbersMetalSurface(configuration: .init(shaders: configuration.shaders,
                                                                          polygonVertexFunction: "albers_fill_fragment")))
        XCTAssertNoThrow(try AlbersMetalSurface(configuration: .init(samples: 1, shaders: configuration.shaders)))
    }

    @MainActor
    func testReplacementPaletteLayoutAndRelease() async throws {
        let configuration = try metalConfiguration()
        let surface = try AlbersMetalSurface(configuration: configuration)
        let device = try XCTUnwrap(surface.device)
        let first = try scene()
        let second = try scene(longitude: -104)
        let firstBuffers = try await AlbersMetalPreparedScene.prepareAsync(first, revision: revision, device: device)
        let revised = GeographicRevision(source: revision.source, version: "2")
        let secondBuffers = try await AlbersMetalPreparedScene.prepareAsync(second, revision: revised, device: device)
        try surface.publish(firstBuffers, scene: first, palette: nil)
        XCTAssertEqual(surface.meshIdentity, firstBuffers.identity)
        let initial = try XCTUnwrap(surface.readiness)
        XCTAssertEqual(initial.phase, .uploaded)
        XCTAssertEqual(surface.resources.activeMeshGenerations, 1)
        XCTAssertGreaterThan(surface.resources.geographicBufferBytes, 0)
        let composition = first.composed(.thematic)
        try surface.updateLayout(composition)
        try surface.updatePalette([[CGColor(gray: 0.5, alpha: 1)]])
        surface.move(camera: .init(viewport: CGRect(x: 0, y: 0, width: 600, height: 300),
                                   center: .zero, scale: 0.001, region: .mainland), selected: 0)
        XCTAssertEqual(surface.meshIdentity, initial.mesh)
        XCTAssertNotEqual(surface.readiness?.appearance, initial.appearance)
        XCTAssertThrowsError(try surface.updatePalette([]))
        XCTAssertThrowsError(try surface.publish(secondBuffers, scene: first, palette: nil))
        XCTAssertEqual(surface.meshIdentity, initial.mesh)
        try surface.publish(secondBuffers, scene: second, palette: nil)
        XCTAssertEqual(surface.meshIdentity, secondBuffers.identity)
        XCTAssertThrowsError(try surface.updateLayout(first))
        surface.renderAllowed = false
        surface.renderAllowed = true
        surface.clear()
        XCTAssertNil(surface.readiness)
        XCTAssertEqual(surface.resources.activeMeshGenerations, 0)
        XCTAssertEqual(surface.resources.retiredMeshGenerations, 0)
        XCTAssertEqual(surface.resources.geographicBufferBytes, 0)
    }

    @MainActor
    private func metalConfiguration() throws -> AlbersMetalConfiguration {
        guard MTLCreateSystemDefaultDevice() != nil else { throw XCTSkip("No Metal device.") }
        if let path = ProcessInfo.processInfo.environment["GEOKIT_METALLIB"] {
            return .init(shaders: .compiledFile(URL(fileURLWithPath: path)))
        }
        #if os(macOS)
        // The CLI build system copies Metal sources. Xcode compiles the package resource.
        let bundle = Bundle(for: RenderingTests.self)
        let products = bundle.bundleURL.deletingLastPathComponent()
        let relative = "BalloticsGeoKit_BalloticsGeoKitApple.bundle/Contents/Resources/default.metallib"
        guard FileManager.default.fileExists(atPath: products.appendingPathComponent(relative).path) else {
            throw XCTSkip("Use the Xcode-built Metal resource or set GEOKIT_METALLIB for SwiftPM CLI.")
        }
        #endif
        return .init()
    }
}
