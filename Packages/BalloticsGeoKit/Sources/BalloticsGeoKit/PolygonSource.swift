import Foundation
import CoreFoundation

public enum PolygonSource {
    public typealias Object = [String: Any]
    public typealias Rings = [[[Double]]]
    public struct Invalid: LocalizedError {
        public let message: String
        public init(message: String) { self.message = message }
        public var errorDescription: String? { message }
    }

    public static func readFeatures(_ url: URL) throws -> [Object] {
        let data = try Data(contentsOf: url, options: .mappedIfSafe)
        guard let root = try JSONSerialization.jsonObject(with: data) as? Object else {
            throw Invalid(message: "Expected a GeoJSON object.")
        }
        switch root["type"] as? String {
        case "FeatureCollection":
            guard let features = root["features"] as? [Object], !features.isEmpty else {
                throw Invalid(message: "The FeatureCollection must contain polygon features.")
            }
            return features
        case "Feature":
            return [root]
        case "Polygon", "MultiPolygon":
            return [["type": "Feature", "geometry": root, "properties": NSNull()]]
        default:
            throw Invalid(message: "Choose a Polygon, MultiPolygon, or a collection of polygon features.")
        }
    }

    public static func rings(for feature: Object) throws -> [[[[Double]]]] {
        let polygons = try polygonGeometry(feature).1
        for rings in polygons {
            for ring in rings {
                guard ring.count >= 4, ring.first == ring.last, ring.allSatisfy({ point in
                    (2...3).contains(point.count) && point.allSatisfy(\.isFinite)
                        && (-180...180).contains(point[0]) && (-90...90).contains(point[1])
                }) else { throw Invalid(message: "Invalid or unclosed geographic ring.") }
            }
        }
        return polygons
    }

    public static func polygonGeometry(_ feature: Object) throws -> (Object, [Rings]) {
        guard feature["type"] as? String == "Feature",
              feature["properties"] is Object || feature["properties"] is NSNull,
              let geometry = feature["geometry"] as? Object else {
            throw Invalid(message: "Expected a feature with properties and polygon geometry.")
        }
        // Check numeric types before bridging to Double, so JSON booleans cannot
        // masquerade as coordinates.
        guard validCoordinateNumbers(geometry["coordinates"]) else {
            throw Invalid(message: "Polygon coordinates must contain only JSON numbers.")
        }
        if geometry["type"] as? String == "Polygon",
           let rings = geometry["coordinates"] as? Rings, !rings.isEmpty {
            return (geometry, [rings])
        }
        if geometry["type"] as? String == "MultiPolygon",
           let polygons = geometry["coordinates"] as? [Rings],
           !polygons.isEmpty, polygons.allSatisfy({ !$0.isEmpty }) {
            return (geometry, polygons)
        }
        throw Invalid(message: "Only nonempty Polygon or MultiPolygon geometry is supported.")
    }

    private static func validCoordinateNumbers(_ value: Any?) -> Bool {
        if let entries = value as? [Any] {
            return entries.allSatisfy { validCoordinateNumbers($0) }
        }
        guard let number = value as? NSNumber else { return false }
        return CFGetTypeID(number) != CFBooleanGetTypeID()
    }

}
