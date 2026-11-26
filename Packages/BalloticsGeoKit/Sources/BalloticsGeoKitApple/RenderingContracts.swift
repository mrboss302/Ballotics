import BalloticsGeoKitMesh
import CoreGraphics
import Foundation

public struct AlbersMetalConfiguration {
    public enum Shaders {
        case package
        case compiledFile(URL)
        case runtimeSource(URL)
    }

    public let samples: Int
    public let boundaries: Bool
    public let shaders: Shaders
    public let polygonVertexFunction: String

    public init(samples: Int = 4, boundaries: Bool = true, shaders: Shaders = .package,
                polygonVertexFunction: String = "albers_fill_vertex") {
        self.samples = samples
        self.boundaries = boundaries
        self.shaders = shaders
        self.polygonVertexFunction = polygonVertexFunction
    }
}

public struct GeographicBoundaryStyle {
    public let width: CGFloat

    public init(width: CGFloat = 0.48) {
        self.width = width.isFinite ? max(0, width) : 0.48
    }
}

public struct GeographicRenderError: LocalizedError {
    public let message: String
    public var errorDescription: String? { message }

    public init(message: String) { self.message = message }
}

public struct GeographicRenderDiagnostic {
    public enum Kind: String {
        case pipeline = "metal_pipeline"
        case layout = "metal_layout"
        case release = "metal_release"
        case sceneActivity = "metal_scene_activity"
        case styleUpload = "metal_style_upload"
        case firstSubmit = "metal_first_submit"
        case firstComplete = "metal_first_complete"
        case firstPresent = "metal_first_present"
        case appearanceSubmit = "metal_appearance_submit"
        case appearancePresent = "metal_appearance_present"
    }

    public let kind: Kind
    public let fields: [String: Any]
}

public struct GeographicRenderResources {
    public let geographicBufferBytes: Int
    public let selectionRangeBytes: Int
    public let styleBufferBytes: Int
    public let inFlightCommands: Int
    public let activeMeshGenerations: Int
    public let retiredMeshGenerations: Int
    /// Estimated resolve, color and depth attachment bytes for one drawable.
    public let estimatedAttachmentBytes: Int
}

public struct GeographicRenderReadiness: Equatable {
    public enum Phase: Int { case uploaded, submitted, completed, presented }

    public let mesh: GeographicMeshIdentity
    public let appearance: Int
    public let phase: Phase
}

enum RenderTiming {
    static var now: Double { ProcessInfo.processInfo.systemUptime }
    static func milliseconds(_ interval: Double) -> Double { (interval * 10_000).rounded() / 10 }
}
