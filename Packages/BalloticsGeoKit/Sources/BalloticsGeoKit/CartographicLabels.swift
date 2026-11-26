import CoreGraphics
import Foundation

public struct MapLabel {
    public let id: String
    public let features: [Int]
    public var primary, secondary, accessibilityText: String
    public var style = "map"
    public var minimumZoom = 0.0, maximumZoom = Double.infinity
    public var preferred: LabelMode?
    public var typeScale = 1.0
    public var priority = 0
    public var external = false, allowCallout = false
    public init(id: String, features: [Int], primary: String, secondary: String = "", accessibilityText: String = "") {
        self.id = id
        self.features = features
        self.primary = primary
        self.secondary = secondary
        self.accessibilityText = accessibilityText
    }
}

public enum LabelArrangement: String, Codable { case stacked, inline }

public extension MapLabel {
    func text(for arrangement: LabelArrangement, compact: Bool = false) -> String {
        guard !compact, !secondary.isEmpty else { return primary }
        return primary + (arrangement == .inline ? " " : "\n") + secondary
    }
}

public struct LabelMeasurements {
    public let full, compact: CGSize
    public let inline: CGSize?
    public init(full: CGSize, compact: CGSize, inline: CGSize? = nil) {
        self.full = full
        self.compact = compact
        self.inline = inline
    }
}

public enum LabelMode: String, Codable { case full, compact, callout, tile, suppressed }

public struct LabelPlacement {
    public let id: String
    public let mode: LabelMode
    public let frame: CGRect
    public let candidate: Int?
    public let leader: CGPoint?
    public let reason: String
    public let arrangement: LabelArrangement
}

public enum LabelResolver {
    public static func resolve(_ labels: [MapLabel], anchors: [String:GroupAnchors],
                               sizes: [String:LabelMeasurements], scene: AlbersScene, camera: GeographicCamera,
                               zoom: CGFloat, selected: Int? = nil, obstacles: [CGRect] = [],
                               previous: [String:Int] = [:], policy: CartographicPolicy = .nationalExploratory) -> [LabelPlacement] {
        let panels = Dictionary(uniqueKeysWithValues: scene.panels.map { ($0.region,$0) })
        func isSelected(_ spec: MapLabel) -> Bool { selected.map(spec.features.contains) ?? false }
        var occupied = obstacles
        return labels.sorted { a,b in
            if isSelected(a) != isSelected(b) { return isSelected(a) }
            return a.priority == b.priority ? a.id < b.id : a.priority > b.priority
        }.map { spec in
            func result(_ mode: LabelMode, _ reason: String, rect: CGRect = .null, index: Int? = nil,
                        leader: CGPoint? = nil, arrangement: LabelArrangement = .stacked) -> LabelPlacement {
                LabelPlacement(id: spec.id, mode: mode, frame: rect, candidate: index, leader: leader,
                               reason: reason, arrangement: arrangement)
            }
            if case .stateFocus = policy, !isSelected(spec) { return result(.suppressed, "state overview policy") }
            guard !spec.primary.isEmpty else { return result(.suppressed,"missing content") }
            guard (spec.minimumZoom...max(spec.minimumZoom,spec.maximumZoom)).contains(zoom) else { return result(.suppressed,"zoom") }
            guard let points = anchors[spec.id]?.candidates, !points.isEmpty else { return result(.suppressed,"no interior anchor") }
            let visible = points.indices.filter { camera.region == nil || points[$0].region == camera.region }
            guard !visible.isEmpty else { return result(.suppressed,"regional focus") }
            if spec.preferred == .suppressed { return result(.suppressed,"editorial suppression") }
            if spec.external && camera.region == nil && (policy == .nationalEditorial || zoom < 2) { return result(.tile,"external key") }
            guard let size = sizes[spec.id] else { return result(.suppressed,"missing measurement") }
            var order = visible
            if let last = previous[spec.id], let i = order.firstIndex(of:last) { order.remove(at:i); order.insert(last,at:0) }
            var reason = "interior too small"
            let modes: [LabelMode] = spec.preferred == .compact ? [.compact,.full,.callout] :
                spec.preferred == .callout ? [.callout,.full,.compact] : [.full,.compact,.callout]
            for mode in modes {
                if mode == .compact && spec.secondary.isEmpty || mode == .callout && !spec.allowCallout { continue }
                var variants: [(LabelArrangement, CGSize)] = [(.stacked, mode == .compact ? size.compact : size.full)]
                if policy == .nationalEditorial, mode == .full, let inline = size.inline, !spec.secondary.isEmpty {
                    variants.append((.inline, inline))
                }
                for (arrangement, text) in variants where text.width > 0 && text.height > 0 {
                    for i in order {
                        let anchor = points[i]
                        guard let panel = panels[anchor.region] else { continue }
                        let scale = camera.scale * panel.scale
                        let center = camera.screen(panel.world(anchor.point))
                        if mode != .callout && !anchor.fits(width:(text.width+1)/scale,height:(text.height+1)/scale) { continue }
                        let rect = CGRect(x:center.x + (mode == .callout ? 8 : -text.width/2),
                                          y:center.y-text.height/2, width:text.width,height:text.height)
                        guard camera.viewport.contains(rect), camera.viewport.contains(center) else { reason = "viewport"; continue }
                        guard !occupied.contains(where: { $0.intersects(rect) }) else { reason = "collision"; continue }
                        occupied.append(rect.insetBy(dx:-1,dy:-1))
                        return result(mode,"placed",rect:rect,index:i,leader:mode == .callout ? center : nil, arrangement: arrangement)
                    }
                }
            }
            return result(.suppressed,reason)
        }
    }
}

/// ISO civil dates compare lexically. Projected anchors are in signed regional meters (down-positive y).
public struct EditorialRules: Decodable {
    public struct Entry: Decodable {
        public let id: String
        public let revision, from, through: String?
        public let region: AlbersRegion
        public let projected, normalized: [CGPoint]?
        public let priority: Int?
        public let typeScale: Double?
        public let preferred: LabelMode?
    }
    public let version: Int
    public let entries: [Entry]
    public func matching(_ id: String, revision: GeographicRevision, day: String) -> Entry? {
        guard version == 1 else { return nil }
        return entries.first { $0.id == id && ($0.revision == nil || $0.revision == revision.version)
            && ($0.from ?? "") <= day && day <= ($0.through ?? "9999-12-31") }
    }
}

extension GroupAnchors {
    public func applying(_ rule: EditorialRules.Entry, scene: AlbersScene, features: [Int]) -> GroupAnchors {
        guard let panel = scene.panels.first(where: { $0.region == rule.region }) else { return self }
        let points = (rule.projected ?? []) + (rule.normalized ?? []).filter {
            (0...1).contains($0.x) && (0...1).contains($0.y)
        }.map { CGPoint(x:panel.geographicBounds.minX+$0.x*panel.geographicBounds.width,
                       y:panel.geographicBounds.minY+$0.y*panel.geographicBounds.height) }
        let valid = points.compactMap { point in
            panel.parts.first { features.contains($0.feature) && $0.bounds.contains(point) && $0.path.contains(point,using:.evenOdd) }
                .flatMap { LabelPolygon($0).anchor(point,region:rule.region) }
        }
        return GroupAnchors(candidates:valid+candidates, averageInside:averageInside,
                            centroidInside:centroidInside, baselineInside:baselineInside)
    }
}

public extension MapLabel {
    /// Apply before measuring. The consumer measures its typography token at the returned scale.
    func applying(_ entry: EditorialRules.Entry) -> Self {
        guard entry.id == id else { return self }
        var label = self
        label.priority = entry.priority ?? priority
        label.preferred = entry.preferred
        if let factor = entry.typeScale, factor.isFinite, factor > 0 { label.typeScale = factor }
        if entry.preferred == .tile { label.external = true }
        if entry.preferred == .callout { label.allowCallout = true }
        return label
    }
}
