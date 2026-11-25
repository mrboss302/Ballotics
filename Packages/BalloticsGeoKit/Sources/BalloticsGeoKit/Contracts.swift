import Foundation
import CoreGraphics

public struct GeographicRevision: Hashable, Codable {
    public let source: String
    public let version: String
    public init(source: String, version: String) {
        self.source = source
        self.version = version
    }
}

public struct GeographicFeatureID: Hashable, Codable {
    public let revision: GeographicRevision
    public let record: String
    public init(revision: GeographicRevision, record: String) {
        self.revision = revision
        self.record = record
    }
}

public struct GeographicPartID: Hashable, Codable {
    public let feature: GeographicFeatureID
    /// Polygon ordinal within this feature, in source order. Ring zero is the exterior.
    public let polygon: Int
    public init(feature: GeographicFeatureID, polygon: Int) {
        self.feature = feature
        self.polygon = polygon
    }
}

public struct GeographicCamera {
    public var viewport: CGRect
    public var center: CGPoint
    public var scale: CGFloat
    public var region: AlbersRegion?
    public init(viewport: CGRect, center: CGPoint, scale: CGFloat, region: AlbersRegion? = nil) {
        self.viewport = viewport
        self.center = center
        self.scale = max(scale, .leastNormalMagnitude)
        self.region = region
    }
    public static func fit(_ frame: CGRect, in viewport: CGRect) -> CGFloat {
        min(max(1, viewport.width - 24) / max(1, frame.width),
            max(1, viewport.height - 24) / max(1, frame.height))
    }
    public func screen(_ world: CGPoint) -> CGPoint {
        CGPoint(x: viewport.midX + (world.x - center.x) * scale, y: viewport.midY + (world.y - center.y) * scale)
    }
    public func world(_ point: CGPoint) -> CGPoint {
        CGPoint(x: center.x + (point.x - viewport.midX) / scale, y: center.y + (point.y - viewport.midY) / scale)
    }
}

/// Shared native Core Graphics path adapter. Callers own transforms, colors and line widths.
public enum GeographicPathDrawing {
    public static func fillAndStroke(_ part: AlbersScene.Part, in context: CGContext) {
        context.addPath(part.path)
        context.drawPath(using: .eoFillStroke)
    }
}

/// Source adapters choose stable record keys. Dense integer offsets belong only to one scene revision.
public struct GeographicRecords {
    public let ids: [GeographicFeatureID]
    private let offsets: [GeographicFeatureID:Int]
    private let canonical: GeographicAnchorCache
    public init(scene: AlbersScene, revision: GeographicRevision, key: ([String:String])->String?) throws {
        var values: [GeographicFeatureID] = [], index: [GeographicFeatureID:Int] = [:]
        for fields in scene.properties {
            guard let raw = key(fields), !raw.isEmpty else { throw PolygonSource.Invalid(message:"Missing feature identity.") }
            let id = GeographicFeatureID(revision:revision,record:raw)
            guard index[id] == nil else { throw PolygonSource.Invalid(message:"Duplicate feature identity.") }
            index[id] = values.count
            values.append(id)
        }
        canonical = scene.labelCache
        ids = values
        offsets = index
    }
    public func index(of id: GeographicFeatureID) -> Int? { offsets[id] }
    public func hits(in scene: AlbersScene, at world: CGPoint, region: AlbersRegion? = nil) -> [GeographicFeatureID] {
        guard scene.labelCache === canonical else { return [] }
        return scene.hits(world,region:region).map { ids[$0] }
    }
}

/// Coordinates and the ordering of source parts stay in the scene. GPU resources remain renderer-owned.
public protocol RegionalMeshPayload {
    var origin: CGPoint { get }
    var uniqueSegmentIndices: [UInt32] { get }
    var indices: [UInt32] { get }
}


public struct GeographicCoordinate: Hashable, Codable {
    public let longitude: Double
    public let latitude: Double
    public init?(longitude: Double, latitude: Double) {
        guard (-180...180).contains(longitude), (-90...90).contains(latitude) else { return nil }
        self.longitude = longitude
        self.latitude = latitude
    }
}

public struct GeographicPolygon {
    public let id: GeographicPartID
    public let exterior: [GeographicCoordinate]
    public let holes: [[GeographicCoordinate]]
    public static func decode(feature: PolygonSource.Object, identity: GeographicFeatureID) throws -> [Self] {
        try PolygonSource.rings(for:feature).enumerated().map { index, polygon in
            let rings = try polygon.map { ring in try ring.map { value in
                guard let coordinate = GeographicCoordinate(longitude:value[0],latitude:value[1]) else {
                    throw PolygonSource.Invalid(message:"Invalid WGS 84 position.")
                }
                return coordinate
            } }
            return Self(id:.init(feature:identity,polygon:index),exterior:rings[0],holes:Array(rings.dropFirst()))
        }
    }
}

public extension AlbersProjection {
    func project(_ coordinate: GeographicCoordinate) -> CGPoint {
        project(longitude:coordinate.longitude,latitude:coordinate.latitude)
    }
}
