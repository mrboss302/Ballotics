import CoreGraphics
import Foundation

/// Presentation intent within a caller-owned geographic revision.
public enum CartographicPolicy: Equatable {
    case nationalEditorial
    case nationalExploratory
    case stateFocus(group: String)
    case localInspection(feature: Int, within: String?)
}

public struct CartographicPresentation {
    private let geometryID: UUID
    public let policy: CartographicPolicy
    /// Nil means all features in the camera's region are eligible.
    public let visibleFeatures: Set<Int>?
    public let region: AlbersRegion?
    public let frame: CGRect
    public let group: String?

    public init?(policy: CartographicPolicy, scene: AlbersScene, groupKey: String) {
        geometryID = scene.geometryID
        self.policy = policy
        let feature: Int?
        switch policy {
        case .nationalEditorial, .nationalExploratory:
            visibleFeatures = nil
            frame = scene.bounds
            group = nil
            region = nil
            return
        case .stateFocus(let group):
            self.group = group
            feature = nil
        case .localInspection(let selected, let group):
            guard scene.properties.indices.contains(selected) else { return nil }
            self.group = group
            feature = selected
        }
        let members = group.map { group in
            Set(scene.properties.indices.filter { scene.properties[$0][groupKey] == group })
        }
        guard members?.isEmpty != true, feature.map({ members?.contains($0) != false }) != false else { return nil }
        let framing = feature.map { Set([$0]) } ?? members ?? []
        let panels = scene.panels.filter { panel in panel.parts.contains { framing.contains($0.feature) } }
        guard let panel = panels.first, panels.count == 1 else { return nil }
        region = panel.region
        visibleFeatures = members
        let bounds = panel.parts.reduce(CGRect.null) { bounds, part in
            framing.contains(part.feature) ? bounds.union(panel.world(part.bounds)) : bounds
        }
        guard !bounds.isEmpty else { return nil }
        let padding: CGFloat = feature == nil ? 0.045 : 0.15
        frame = bounds.insetBy(dx: -bounds.width * padding, dy: -bounds.height * padding)
    }

    public func hits(in scene: AlbersScene, at point: CGPoint) -> [Int] {
        guard scene.geometryID == geometryID else { return [] }
        let hits = scene.hits(point, region: region)
        return visibleFeatures.map { features in hits.filter(features.contains) } ?? hits
    }

    public func camera(in viewport: CGRect) -> GeographicCamera {
        GeographicCamera(viewport: viewport, center: CGPoint(x: frame.midX, y: frame.midY),
                         scale: GeographicCamera.fit(frame, in: viewport), region: region)
    }
}
