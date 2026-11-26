import BalloticsGeoKit
import Foundation

public struct GeographicMeshIdentity: Hashable, Sendable {
    public let revision: GeographicRevision
    private let projectedGeometry: UUID

    public init(revision: GeographicRevision, scene: AlbersScene) {
        self.revision = revision
        projectedGeometry = scene.geometryID
    }

    package func matches(_ scene: AlbersScene) -> Bool {
        projectedGeometry == scene.geometryID
    }
}

public struct GeographicMeshError: LocalizedError {
    public let message: String
    public var errorDescription: String? { message }

    package init(message: String) { self.message = message }
}
