#if canImport(BalloticsGeoKit)
import BalloticsGeoKit
#endif
import CoreFoundation
import Foundation

enum Geography: String, CaseIterable, Identifiable {
    case districts = "Districts"
    case counties = "Counties"
    case states = "States"
    case imported = "Imported"
    case historical = "Historical Counties"
    case presidential = "Presidential Elections"

    var id: String { rawValue }

    var resource: String? {
        switch self {
        case .districts: return "cb_2024_us_cd119_5m"
        case .counties: return "cb_2024_us_county_5m"
        case .states: return "cb_2024_us_state_5m"
        case .imported, .historical, .presidential: return nil
        }
    }

    var censusSchema: (count: Int, idWidth: Int, suffix: String?)? {
        switch self {
        case .districts: return (441, 4, "CD119FP")
        case .counties: return (3235, 5, "COUNTYFP")
        case .states: return (56, 2, nil)
        case .imported, .historical, .presidential: return nil
        }
    }
}

struct GeoBounds {
    var west = Double.infinity
    var south = Double.infinity
    var east = -Double.infinity
    var north = -Double.infinity

    mutating func include(_ x: Double, _ y: Double) {
        west = min(west, x)
        south = min(south, y)
        east = max(east, x)
        north = max(north, y)
    }
}

struct GeoJSON {
    typealias Invalid = PolygonSource.Invalid

    struct Summary {
        var features = 0
        var multipart = 0
        var parts = 0
        var rings = 0
        var positions = 0
        var bounds = GeoBounds()
        var stateBounds: [String: GeoBounds] = [:]
        var selectionKey: String?

        mutating func include(_ other: Self) {
            features += other.features
            multipart += other.multipart
            parts += other.parts
            rings += other.rings
            positions += other.positions
            bounds.include(other.bounds.west, other.bounds.south)
            bounds.include(other.bounds.east, other.bounds.north)
        }
    }

    typealias Object = [String: Any]
    private typealias Rings = [[[Double]]]

    static func validate(_ url: URL, geography: Geography) throws -> Summary {
        try validate(readFeatures(url), geography: geography)
    }

    static func validate(_ features: [Object], geography: Geography) throws -> Summary {
        var summary = Summary()
        var candidateIDs: [String: Set<AnyHashable>] = ["GEOID": [], "ID_NUM": [], "$id": []]
        let schema = geography.censusSchema
        guard schema == nil || schema?.count == features.count else {
            throw Invalid(message: "The bundled file has an unexpected feature count.")
        }
        for (index, feature) in features.enumerated() {
            try Task.checkCancellation()
            do {
                let properties = feature["properties"] as? Object ?? [:]
                if let schema {
                    try checkCensusID(properties, width: schema.idWidth, suffix: schema.suffix)
                }

                for key in ["GEOID", "ID_NUM", "$id"] where candidateIDs[key] != nil {
                    let value = key == "$id" ? feature["id"] : properties[key]
                    guard let id = identifier(value), candidateIDs[key]?.insert(id).inserted == true else {
                        candidateIDs.removeValue(forKey: key)
                        continue
                    }
                }

                let details = try summarizeFeature(feature)
                summary.include(details)
                if geography == .states, let id = properties["GEOID"] as? String {
                    summary.stateBounds[id] = details.bounds
                }
            } catch {
                throw Invalid(message: "Feature \(index + 1). \(error.localizedDescription)")
            }
        }
        if schema != nil, candidateIDs["GEOID"] == nil {
            throw Invalid(message: "The bundled file contains a missing or duplicate GEOID.")
        }
        summary.selectionKey = ["GEOID", "ID_NUM", "$id"].first { candidateIDs[$0] != nil }
        return summary
    }

    static func summarizeFeature(_ feature: [String: Any]) throws -> Summary {
        let (geometry, polygons) = try polygonGeometry(feature)
        var summary = Summary()
        summary.features = 1
        summary.multipart = geometry["type"] as? String == "MultiPolygon" ? 1 : 0
        summary.parts = polygons.count
        for rings in polygons {
            summary.rings += rings.count
            for ring in rings {
                guard ring.count >= 4, ring.first == ring.last else {
                    throw Invalid(message: "Polygon rings must be closed with at least four positions.")
                }
                summary.positions += ring.count
                for position in ring {
                    guard (2...3).contains(position.count), position.allSatisfy(\.isFinite),
                          (-180...180).contains(position[0]), (-90...90).contains(position[1]) else {
                        throw Invalid(message: "A position is outside the WGS 84 longitude/latitude range.")
                    }
                    summary.bounds.include(position[0], position[1])
                }
            }
        }
        return summary
    }

    // Only used when an import lacks stable identifiers. A renderer hit is required
    // first. Resolve the full original polygon, which can extend across many tiles.
    static func originalPolygon(in url: URL, longitude: Double, latitude: Double) throws -> Data? {
        for feature in try readFeatures(url).reversed() {
            try Task.checkCancellation()
            let (geometry, polygons) = try polygonGeometry(feature)
            if containsPolygon(polygons, longitude: longitude, latitude: latitude) {
                return try JSONSerialization.data(withJSONObject: geometry)
            }
        }
        return nil
    }

    static func containsFeature(_ feature: [String: Any], longitude: Double, latitude: Double) throws -> Bool {
        let (_, polygons) = try polygonGeometry(feature)
        return containsPolygon(polygons, longitude: longitude, latitude: latitude)
    }

    static func matchingHistoricalIDs(in url: URL, candidates: Set<Int>,
                                      longitude: Double, latitude: Double) throws -> Set<Int> {
        var matches = Set<Int>()
        for feature in try readFeatures(url) {
            try Task.checkCancellation()
            if let properties = feature["properties"] as? Object, let id = properties["ID_NUM"] as? Int,
               candidates.contains(id), try containsFeature(feature, longitude: longitude, latitude: latitude) {
                matches.insert(id)
            }
        }
        return matches
    }

    private static func containsPolygon(_ polygons: [Rings], longitude: Double, latitude: Double) -> Bool {
        polygons.contains { rings in
            contains(rings[0], longitude, latitude)
                && !rings.dropFirst().contains { contains($0, longitude, latitude) }
        }
    }

    private static func contains(_ ring: [[Double]], _ x: Double, _ y: Double) -> Bool {
        var inside = false
        for (a, b) in zip(ring, ring.dropFirst()) where (a[1] > y) != (b[1] > y) {
            let edge = a[0] + (b[0] - a[0]) * (y - a[1]) / (b[1] - a[1])
            if x < edge { inside.toggle() }
        }
        return inside
    }

    static func readFeatures(_ url: URL) throws -> [Object] { try PolygonSource.readFeatures(url) }
    static func rings(for feature: Object) throws -> [[[[Double]]]] { try PolygonSource.rings(for: feature) }
    private static func polygonGeometry(_ feature: Object) throws -> (Object, [Rings]) {
        try PolygonSource.polygonGeometry(feature)
    }

    private static func identifier(_ value: Any?) -> AnyHashable? {
        if let string = value as? String, !string.isEmpty { return string }
        if let number = value as? NSNumber, CFGetTypeID(number) != CFBooleanGetTypeID() {
            return number
        }
        return nil
    }

    private static func checkCensusID(_ fields: Object, width: Int, suffix: String?) throws {
        guard let id = fields["GEOID"] as? String,
              id.utf8.count == width, id.utf8.allSatisfy({ (48...57).contains($0) }),
              let state = fields["STATEFP"] as? String, state.count == 2,
              let qualified = fields["GEOIDFQ"] as? String, qualified.hasSuffix("US" + id),
              id == state + (suffix.flatMap { fields[$0] as? String } ?? "") else {
            throw Invalid(message: "GEOID, STATEFP, GEOIDFQ, or geographic component codes disagree.")
        }
    }
}

final class ImportedFile {
    let displayName: String
    let url: URL
    let summary: GeoJSON.Summary
    let preparationSeconds: Double

    static func prepare(_ file: URL) throws -> ImportedFile {
        let start = ProcessInfo.processInfo.systemUptime
        let access = file.startAccessingSecurityScopedResource()
        defer { if access { file.stopAccessingSecurityScopedResource() } }
        let destination = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString).appendingPathExtension("geojson")
        do {
            try FileManager.default.copyItem(at: file, to: destination)
            let summary = try GeoJSON.validate(destination, geography: .imported)
            return ImportedFile(file.lastPathComponent, destination, summary,
                                ProcessInfo.processInfo.systemUptime - start)
        } catch {
            try? FileManager.default.removeItem(at: destination)
            throw error
        }
    }

    private init(_ displayName: String, _ url: URL, _ summary: GeoJSON.Summary, _ seconds: Double) {
        self.displayName = displayName
        self.url = url
        self.summary = summary
        self.preparationSeconds = seconds
    }

    deinit { try? FileManager.default.removeItem(at: url) }
}
