import BalloticsGeoKit
import CoreGraphics
import Foundation

/// A separately projected, decorative source using the same regional Albers coordinates.
public struct GeographicContextLayer {
    public let scene: AlbersScene
    public let provenance: String

    public init(scene: AlbersScene, provenance: String) {
        self.scene = scene
        self.provenance = provenance
    }

    /// Uses the primary scene's presentation transform so national inset placement never implies adjacency.
    @discardableResult
    public func draw(in context: CGContext, camera: GeographicCamera, alignedTo primary: AlbersScene,
                     excluding group: String?, groupKey: String,
                     ink: CGColor = CGColor(gray: 0.44, alpha: 0.50), width: CGFloat = 0.65) -> Int {
        guard let region = camera.region, region == .mainland,
              let panel = primary.panels.first(where: { $0.region == region }),
              let outlines = scene.panels.first(where: { $0.region == region }) else { return 0 }
        let local = panel.local(camera.worldBounds)
        let indices = outlines.visible(in: local)
        context.saveGState()
        defer { context.restoreGState() }
        context.clip(to: camera.viewport)
        let origin = camera.screen(panel.world(CGPoint.zero))
        let scale = camera.scale * panel.scale
        context.translateBy(x: origin.x, y: origin.y)
        context.scaleBy(x: scale, y: scale)
        context.setStrokeColor(ink)
        context.setLineWidth(width / scale)
        var count = 0
        for index in indices {
            let part = outlines.parts[index]
            if let group, scene.properties[part.feature][groupKey] == group { continue }
            context.addPath(part.path)
            count += 1
        }
        context.strokePath()
        return count
    }
}
