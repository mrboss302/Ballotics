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

struct AlbersScene {
    struct Part {
        let feature: Int
        let path: CGPath
        let bounds: CGRect
    }

    struct Panel {
        let region: AlbersRegion
        let parts: [Part]
        let translation: CGPoint
        let frame: CGRect

        func local(_ point: CGPoint) -> CGPoint {
            CGPoint(x: point.x - translation.x, y: point.y - translation.y)
        }

        func world(_ rect: CGRect) -> CGRect {
            rect.offsetBy(dx: translation.x, dy: translation.y)
        }
    }

    let panels: [Panel]
    let properties: [[String: String]]
    let bounds: CGRect

    static func load(_ url: URL) throws -> Self {
        let features = try GeoJSON.readFeatures(url)
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
        return Self(panels: layout(grouped), properties: attributes)
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

    func frame(for feature: Int) -> CGRect {
        panels.reduce(.null) { result, panel in
            panel.parts.lazy.filter { $0.feature == feature }.reduce(result) {
                $0.union(panel.world($1.bounds))
            }
        }
    }

    private init(panels: [Panel], properties: [[String: String]]) {
        self.panels = panels
        self.properties = properties
        bounds = panels.reduce(.null) { $0.union($1.frame) }
    }

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

    private static func layout(_ parts: [AlbersRegion: [Part]]) -> [Panel] {
        let gap: CGFloat = 145_000
        let insetPadding: CGFloat = 100_000
        let labelRoom: CGFloat = 285_000

        func panel(_ region: AlbersRegion, x: CGFloat, y: CGFloat) -> Panel? {
            guard let shapes = parts[region], !shapes.isEmpty else { return nil }
            let box = shapes.reduce(CGRect.null) { $0.union($1.bounds) }
            let padding = region == .mainland ? 0 : insetPadding
            let header = region == .mainland ? 0 : labelRoom
            let frame = CGRect(x: x, y: y, width: box.width + 2 * padding, height: box.height + padding + header)
            return Panel(region: region, parts: shapes,
                         translation: CGPoint(x: x + padding - box.minX, y: y + header - box.minY), frame: frame)
        }

        let mainland = panel(.mainland, x: 0, y: 0)
        let insetY = mainland.map { $0.frame.maxY + gap } ?? 0
        let alaska = panel(.alaska, x: 0, y: insetY)
        let hawaii = panel(.hawaii, x: alaska.map { $0.frame.maxX + gap } ?? 0, y: insetY)
        return [mainland, alaska, hawaii].compactMap { $0 }
    }
}
