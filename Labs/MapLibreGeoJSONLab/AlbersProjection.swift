import CoreGraphics
import Foundation

struct AlbersProjection {
    private static let radians = Double.pi / 180
    private static let semimajor = 6_378_137.0
    private static let flattening = 1 / 298.257223563
    private static let eccentricitySquared = flattening * (2 - flattening)
    private static let eccentricity = sqrt(eccentricitySquared)

    private let meridian: Double
    private let n: Double
    private let c: Double
    private let originRadius: Double

    init(meridian: Double, origin: Double, parallels: (Double, Double)) {
        self.meridian = meridian
        let first = parallels.0 * Self.radians
        let second = parallels.1 * Self.radians
        let q1 = Self.authalic(first)
        let q2 = Self.authalic(second)
        let m1 = Self.meridianScaleSquared(first)
        let m2 = Self.meridianScaleSquared(second)
        let cone = (m1 - m2) / (q2 - q1)
        n = cone
        c = m1 + cone * q1
        originRadius = Self.semimajor * sqrt(c - cone * Self.authalic(origin * Self.radians)) / cone
    }

    /// Easting and northing in WGS 84 projected meters.
    func project(longitude: Double, latitude: Double) -> CGPoint {
        let radius = Self.semimajor * sqrt(max(0, c - n * Self.authalic(latitude * Self.radians))) / n
        let angle = n * Self.longitudeDelta(longitude - meridian) * Self.radians
        return CGPoint(x: radius * sin(angle), y: originRadius - radius * cos(angle))
    }

    static func longitudeDelta(_ degrees: Double) -> Double {
        let wrapped = degrees.truncatingRemainder(dividingBy: 360)
        if wrapped > 180 { return wrapped - 360 }
        if wrapped < -180 { return wrapped + 360 }
        return wrapped
    }

    private static func authalic(_ latitude: Double) -> Double {
        let sine = sin(latitude)
        return (1 - eccentricitySquared) *
            (sine / (1 - eccentricitySquared * sine * sine) + atanh(eccentricity * sine) / eccentricity)
    }

    private static func meridianScaleSquared(_ latitude: Double) -> Double {
        let sine = sin(latitude)
        let cosine = cos(latitude)
        return cosine * cosine / (1 - eccentricitySquared * sine * sine)
    }
}

enum AlbersRegion: CaseIterable {
    case mainland, alaska, hawaii

    var projection: AlbersProjection {
        switch self {
        case .mainland: return .init(meridian: -96, origin: 23, parallels: (29.5, 45.5))
        case .alaska: return .init(meridian: -154, origin: 50, parallels: (55, 65))
        case .hawaii: return .init(meridian: -157, origin: 3, parallels: (8, 18))
        }
    }

    var insetLabel: String? {
        switch self {
        case .mainland: return nil
        case .alaska: return "Alaska"
        case .hawaii: return "Hawaii"
        }
    }

    static func locate(properties: [String: Any], outer: [[Double]]) -> Self? {
        let jurisdiction = (properties["STATE_TERR"] as? String ?? properties["NAME"] as? String ?? "").lowercased()
        switch properties["STATEFP"] as? String {
        case "02": return .alaska
        case "15": return .hawaii
        case "60", "66", "69", "72", "78": return nil
        case .some: return .mainland
        case nil: break
        }
        if jurisdiction.contains("alaska") { return .alaska }
        if jurisdiction.contains("hawai") { return .hawaii }
        if properties["TERR_TYPE"] != nil { return .mainland }
        // Preserve geographic placement for historical territorial records
        // whose jurisdiction strings predate statehood.
        guard let point = outer.first else { return .mainland }
        if point[1] > 50 && (point[0] < -130 || point[0] > 165) { return .alaska }
        if (18...29).contains(point[1]) && (-180 ... -154).contains(point[0]) { return .hawaii }
        return .mainland
    }
}

enum NationalMapProfile: String, CaseIterable {
    case thematic = "Thematic National"
    case research = "Equal-Scale Research"
}

struct NationalInsetComposition {
    var alaskaScale: CGFloat = 0.32
    var hawaiiScale: CGFloat = 1.05
    var westernMargin: CGFloat = 0.025
    var separation: CGFloat = 0.012
}

struct AlbersScene {
    struct Preparation {
        let decodedMilliseconds: Double
        let projectedMilliseconds: Double
        let indexedMilliseconds: Double
    }

    struct InsetLayout {
        var gap: CGFloat = 145_000
        var padding: CGFloat = 100_000
        var labelRoom: CGFloat = 285_000
        var alaskaFirst = true
    }

    struct Part {
        let feature: Int
        let path: CGPath
        let bounds: CGRect

        // Clip each projected edge to the horizontal strip before finding its southernmost point.
        // A distant corner of a state's bounding box can otherwise push an island out of the composition.
        func lowerEdge(in band: ClosedRange<CGFloat>) -> CGFloat? {
            var previous = CGPoint.zero
            var first = CGPoint.zero
            var bottom: CGFloat?
            func edge(to next: CGPoint) {
                defer { previous = next }
                guard max(previous.x, next.x) >= band.lowerBound, min(previous.x, next.x) <= band.upperBound else { return }
                let dx = next.x - previous.x
                if dx == 0 { bottom = max(bottom ?? -.infinity, previous.y, next.y); return }
                let a = min(1, max(0, (band.lowerBound - previous.x) / dx))
                let b = min(1, max(0, (band.upperBound - previous.x) / dx))
                bottom = max(bottom ?? -.infinity, previous.y + (next.y - previous.y) * a,
                             previous.y + (next.y - previous.y) * b)
            }
            path.applyWithBlock { pointer in
                let item = pointer.pointee
                switch item.type {
                case .moveToPoint: first = item.points[0]; previous = first
                case .addLineToPoint: edge(to: item.points[0])
                case .closeSubpath: edge(to: first)
                default: break
                }
            }
            return bottom
        }

        // Search from the box center outward and verify the original even–odd path, including holes.
        var interiorAnchor: CGPoint? {
            for radius in 0...8 {
                for y in -radius...radius {
                    for x in -radius...radius where max(abs(x), abs(y)) == radius {
                        let point = CGPoint(x: bounds.midX + CGFloat(x) * bounds.width / 17,
                                            y: bounds.midY + CGFloat(y) * bounds.height / 17)
                        if path.contains(point, using: .evenOdd) { return point }
                    }
                }
            }
            return nil
        }
    }

    struct Panel {
        let region: AlbersRegion
        let parts: [Part]
        var translation: CGPoint
        var frame: CGRect
        let geographicBounds: CGRect
        let researchLocalFrame: CGRect
        var scale: CGFloat = 1
        var showsFrame: Bool
        fileprivate let index: AlbersSpatialIndex?

        func local(_ point: CGPoint) -> CGPoint {
            CGPoint(x: (point.x - translation.x) / scale, y: (point.y - translation.y) / scale)
        }

        func local(_ rect: CGRect) -> CGRect {
            CGRect(origin: local(rect.origin), size: CGSize(width: rect.width / scale, height: rect.height / scale))
        }

        func world(_ point: CGPoint) -> CGPoint {
            CGPoint(x: point.x * scale + translation.x, y: point.y * scale + translation.y)
        }

        func world(_ rect: CGRect) -> CGRect {
            CGRect(origin: world(rect.origin), size: CGSize(width: rect.width * scale, height: rect.height * scale))
        }

        // The draw order is also the selection stacking order.
        func visible(in window: CGRect) -> [Int] {
            if let index { return index.overlapping(window).sorted() }
            return parts.indices.filter { parts[$0].bounds.intersects(window) }
        }
    }

    let panels: [Panel]
    let properties: [[String: String]]
    let bounds: CGRect
    let preparation: Preparation
    private let featureFrames: [CGRect]
    private let stateFrames: [String: CGRect]

    static func load(_ url: URL, indexed: Bool = true, layout: InsetLayout = InsetLayout()) throws -> Self {
        let started = ProcessInfo.processInfo.systemUptime
        let features = try GeoJSON.readFeatures(url)
        return try project(features, indexed: indexed, layout: layout,
                           decodeMilliseconds: (ProcessInfo.processInfo.systemUptime - started) * 1_000)
    }

    static func project(_ features: [GeoJSON.Object], indexed: Bool = true,
                        layout: InsetLayout = InsetLayout(), decodeMilliseconds: Double = 0) throws -> Self {
        let started = ProcessInfo.processInfo.systemUptime
        let projections = Dictionary(uniqueKeysWithValues: AlbersRegion.allCases.map { ($0, $0.projection) })
        var grouped: [AlbersRegion: [Part]] = [:]
        var attributes: [[String: String]] = []
        for feature in features {
            try Task.checkCancellation()
            let original = feature["properties"] as? [String: Any] ?? [:]
            let polygons = try GeoJSON.rings(for: feature)
            var included = false
            for rings in polygons {
                guard rings.allSatisfy({ ring in ring.allSatisfy { $0.count >= 2 } }) else {
                    throw GeoJSON.Invalid(message: "A projected polygon has an incomplete position.")
                }
                guard let outer = rings.first, let region = AlbersRegion.locate(properties: original, outer: outer),
                      let projection = projections[region] else { continue }
                let path = makePath(rings, projection: projection)
                grouped[region, default: []].append(Part(feature: attributes.count, path: path,
                                                         bounds: path.boundingBoxOfPath))
                included = true
            }
            if included {
                attributes.append(original.compactMapValues {
                    if let value = $0 as? String { return value }
                    return ($0 as? NSNumber)?.stringValue
                })
            }
        }
        guard !attributes.isEmpty else {
            throw GeoJSON.Invalid(message: "This dataset has no polygons in the Albers overview.")
        }
        let projected = ProcessInfo.processInfo.systemUptime
        let panels = Self.layout(grouped, using: layout, indexed: indexed)
        return assembled(panels, attributes, Preparation(decodedMilliseconds: decodeMilliseconds,
            projectedMilliseconds: (projected - started) * 1_000,
            indexedMilliseconds: (ProcessInfo.processInfo.systemUptime - projected) * 1_000))
    }

    // Compose from the cached research scene. Paths and spatial indexes use copy-on-write storage.
    func composed(_ profile: NationalMapProfile, using config: NationalInsetComposition = .init()) -> Self {
        guard profile == .thematic else { return self }
        let mainland = panels.first { $0.region == .mainland }
        let width = mainland?.frame.width ?? panels.map { $0.geographicBounds.width }.max() ?? 1
        let height = mainland?.frame.height ?? 0
        var x = mainland == nil ? 0 : width * config.westernMargin
        let gap = width * config.separation
        var result = panels
        for i in result.indices where result[i].region != .mainland {
            let alaska = result[i].region == .alaska
            let factor = alaska ? config.alaskaScale : config.hawaiiScale
            let box = result[i].geographicBounds
            var top = height * (alaska ? 0.78 : 0.89)
            // Clear even unusual historical coastlines. Only parts in the inset's horizontal band matter.
            if let mainland {
                let band = CGRect(x: x, y: mainland.frame.minY, width: box.width * factor, height: height)
                let localBand = mainland.local(band)
                for j in mainland.visible(in: localBand) {
                    let part = mainland.parts[j]
                    guard mainland.world(part.bounds).maxY + gap > top,
                          let bottom = part.lowerEdge(in: localBand.minX...localBand.maxX) else { continue }
                    top = max(top, mainland.world(CGPoint(x: 0, y: bottom)).y + gap)
                }
            }
            result[i].scale = factor
            result[i].translation = CGPoint(x: x - box.minX * factor, y: top - box.minY * factor)
            result[i].frame = result[i].world(box)
            result[i].showsFrame = false
            x = result[i].frame.maxX + gap
        }
        return Self.assembled(result, properties, preparation)
    }

    private static func assembled(_ panels: [Panel], _ attributes: [[String: String]], _ preparation: Preparation) -> Self {
        var frames = Array(repeating: CGRect.null, count: attributes.count)
        for panel in panels {
            for part in panel.parts {
                frames[part.feature] = frames[part.feature].union(panel.world(part.bounds))
            }
        }
        var states: [String: CGRect] = [:]
        for (index, fields) in attributes.enumerated() {
            if let code = fields["STATEFP"] {
                states[code] = states[code, default: .null].union(frames[index])
            }
        }
        return Self(panels: panels, properties: attributes,
                    bounds: panels.reduce(.null) { $0.union($1.frame) },
                    preparation: preparation,
                    featureFrames: frames, stateFrames: states)
    }

    func hit(_ point: CGPoint) -> Int? {
        for panel in panels where panel.frame.contains(point) {
            let local = panel.local(point)
            if let part = panel.parts.reversed().first(where: {
                $0.bounds.contains(local) && $0.path.contains(local, using: .evenOdd)
            }) { return part.feature }
        }
        return nil
    }

    func hits(_ point: CGPoint) -> [Int] {
        var seen = Set<Int>()
        var matches: [Int] = []
        for panel in panels where panel.frame.contains(point) {
            let local = panel.local(point)
            let candidates = panel.index?.containing(local).sorted(by: >) ?? Array(panel.parts.indices.reversed())
            for candidate in candidates {
                let part = panel.parts[candidate]
                if part.bounds.contains(local), part.path.contains(local, using: .evenOdd),
                   seen.insert(part.feature).inserted {
                    matches.append(part.feature)
                }
            }
        }
        return matches
    }

    func frame(for feature: Int) -> CGRect { featureFrames[feature] }
    func frame(forState code: String) -> CGRect? { stateFrames[code] }

    private static func makePath(_ polygons: [[[Double]]], projection: AlbersProjection) -> CGPath {
        let path = CGMutablePath()
        func screenPoint(_ longitude: Double, _ latitude: Double) -> CGPoint {
            let point = projection.project(longitude: longitude, latitude: latitude)
            return CGPoint(x: point.x, y: -point.y)
        }
        for ring in polygons {
            guard let first = ring.first else { continue }
            path.move(to: screenPoint(first[0], first[1]))
            for (start, end) in zip(ring, ring.dropFirst()) {
                let dx = AlbersProjection.longitudeDelta(end[0] - start[0])
                let dy = end[1] - start[1]
                // Sample long GeoJSON segments so projection curves survive at detail zoom.
                let pieces = max(1, Int(ceil(max(abs(dx), abs(dy)))))
                for step in 1...pieces {
                    let t = Double(step) / Double(pieces)
                    path.addLine(to: screenPoint(start[0] + dx * t, start[1] + dy * t))
                }
            }
            path.closeSubpath()
        }
        return path.copy() ?? path
    }

    private static func layout(_ parts: [AlbersRegion: [Part]], using layout: InsetLayout, indexed: Bool) -> [Panel] {
        func panel(_ region: AlbersRegion, x: CGFloat, y: CGFloat) -> Panel? {
            guard let shapes = parts[region], !shapes.isEmpty else { return nil }
            let box = shapes.reduce(CGRect.null) { $0.union($1.bounds) }
            let padding = region == .mainland ? 0 : layout.padding
            let header = region == .mainland ? 0 : layout.labelRoom
            let frame = CGRect(x: x, y: y, width: box.width + 2 * padding, height: box.height + padding + header)
            return Panel(region: region, parts: shapes,
                         translation: CGPoint(x: x + padding - box.minX, y: y + header - box.minY), frame: frame,
                         geographicBounds: box, researchLocalFrame: CGRect(x: box.minX - padding, y: box.minY - header,
                            width: frame.width, height: frame.height), showsFrame: region != .mainland, index: indexed ? AlbersSpatialIndex(shapes.map(\.bounds)) : nil)
        }

        let mainland = panel(.mainland, x: 0, y: 0)
        let insetY = mainland.map { $0.frame.maxY + layout.gap } ?? 0
        let first = panel(layout.alaskaFirst ? .alaska : .hawaii, x: 0, y: insetY)
        let second = panel(layout.alaskaFirst ? .hawaii : .alaska,
                           x: first.map { $0.frame.maxX + layout.gap } ?? 0, y: insetY)
        return [mainland, first, second].compactMap { $0 }
    }
}

/// A packed, immutable bounding-volume hierarchy. Leaves refer to the original draw-order indices.
private struct AlbersSpatialIndex {
    struct Node {
        let bounds: CGRect
        let entries: Range<Int>
        let left: Int
        let right: Int
    }

    private var nodes: [Node] = []
    private var order: [Int]
    private let boxes: [CGRect]

    init(_ boxes: [CGRect]) {
        self.boxes = boxes
        order = Array(boxes.indices)
        build(order.indices)
    }

    @discardableResult
    private mutating func build(_ range: Range<Int>) -> Int {
        let bound = order[range].reduce(CGRect.null) { $0.union(boxes[$1]) }
        let index = nodes.count
        nodes.append(Node(bounds: bound, entries: range, left: -1, right: -1))
        if range.count > 12 {
            let horizontal = bound.width >= bound.height
            let sorted = order[range].sorted {
                horizontal ? boxes[$0].midX < boxes[$1].midX : boxes[$0].midY < boxes[$1].midY
            }
            order.replaceSubrange(range, with: sorted)
            let middle = range.lowerBound + range.count / 2
            let left = build(range.lowerBound..<middle)
            let right = build(middle..<range.upperBound)
            nodes[index] = Node(bounds: bound, entries: range, left: left, right: right)
        }
        return index
    }

    func overlapping(_ window: CGRect) -> [Int] {
        if window.contains(nodes[0].bounds) { return Array(boxes.indices) }
        return matching { $0.intersects(window) }
    }

    func containing(_ point: CGPoint) -> [Int] {
        matching { $0.contains(point) }
    }

    private func matching(_ includes: (CGRect) -> Bool) -> [Int] {
        var matches: [Int] = []
        func visit(_ index: Int) {
            let node = nodes[index]
            guard includes(node.bounds) else { return }
            if node.left < 0 {
                for item in order[node.entries] where includes(boxes[item]) { matches.append(item) }
            } else {
                visit(node.left)
                visit(node.right)
            }
        }
        visit(0)
        return matches
    }
}
