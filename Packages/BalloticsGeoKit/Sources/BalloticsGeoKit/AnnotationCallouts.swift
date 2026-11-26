import CoreGraphics
import Foundation

/// Conservative callouts for a geographic scene. An empty rectangle of water is preferred
/// to a placement whose association depends on a line crossing another feature.
public struct AnnotationCallouts {
    public struct Placement {
        public let frame: CGRect
        public let origin: CGPoint
        public let end: CGPoint
    }

    private struct PartBounds {
        let feature: Int
        let bounds: CGRect
    }

    private let parts: [PartBounds]
    private let geometry: UUID

    public init(scene: AlbersScene) {
        geometry = scene.geometryID
        parts = scene.panels.flatMap { panel in
            panel.parts.map { PartBounds(feature: $0.feature, bounds: panel.world($0.bounds)) }
        }
    }

    public func place(features: Set<Int>, anchors: GroupAnchors, size: CGSize, scene: AlbersScene,
                      camera: GeographicCamera, occupied: [CGRect]) -> Placement? {
        guard size.width > 0, size.height > 0, geometry == scene.geometryID else { return nil }
        let panels = Dictionary(uniqueKeysWithValues: scene.panels.map { ($0.region, $0) })
        let directions = [CGPoint(x: 1, y: 0), CGPoint(x: 0, y: -1), CGPoint(x: 0, y: 1),
                          CGPoint(x: -1, y: 0), CGPoint(x: 1, y: -1), CGPoint(x: 1, y: 1),
                          CGPoint(x: -1, y: -1), CGPoint(x: -1, y: 1)]
        for anchor in anchors.candidates.prefix(3) where features.contains(anchor.feature) {
            guard let panel = panels[anchor.region], camera.region == nil || camera.region == anchor.region else { continue }
            let origin = camera.screen(panel.world(anchor.point))
            guard camera.viewport.contains(origin) else { continue }
            for gap: CGFloat in [12, 24, 36] {
                for direction in directions {
                    let center = CGPoint(x: origin.x + direction.x * (size.width / 2 + gap),
                                         y: origin.y + direction.y * (size.height / 2 + gap))
                    let frame = CGRect(x: center.x - size.width / 2, y: center.y - size.height / 2,
                                       width: size.width, height: size.height)
                    let padded = frame.insetBy(dx: -2, dy: -2)
                    guard camera.viewport.contains(padded), !occupied.contains(where: { $0.intersects(padded) }) else { continue }
                    let world = CGRect(origin: camera.world(padded.origin),
                                       size: CGSize(width: padded.width / camera.scale, height: padded.height / camera.scale))
                    guard !parts.contains(where: { $0.bounds.intersects(world) }) else { continue }
                    let end = CGPoint(x: min(max(frame.minX, origin.x), frame.maxX),
                                      y: min(max(frame.minY, origin.y), frame.maxY))
                    let corridor = CGRect(x: min(origin.x, end.x), y: min(origin.y, end.y),
                                          width: abs(origin.x - end.x), height: abs(origin.y - end.y)).insetBy(dx: -1, dy: -1)
                    let lineBounds = CGRect(origin: camera.world(corridor.origin),
                                            size: CGSize(width: corridor.width / camera.scale, height: corridor.height / camera.scale))
                    guard !occupied.contains(where: { $0.intersects(corridor) }),
                          !parts.contains(where: { !features.contains($0.feature) && $0.bounds.intersects(lineBounds) }) else { continue }
                    return Placement(frame: frame, origin: origin, end: end)
                }
            }
        }
        return nil
    }
}
