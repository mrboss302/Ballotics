import CoreGraphics
import Foundation
import LibTess2

/// The contours are the densified regional paths already used for Core Graphics and picking.
struct ProjectedPolygon {
    struct Edge {
        let start: SIMD2<Double>
        let end: SIMD2<Double>
    }

    let rings: [[SIMD2<Double>]]
    var points: [SIMD2<Double>] { rings.flatMap { $0 } }
    var edges: [Edge] {
        rings.flatMap { ring in
            ring.indices.map { Edge(start: ring[$0], end: ring[($0 + 1) % ring.count]) }
        }
    }

    init(_ path: CGPath) throws {
        var contours: [[SIMD2<Double>]] = []
        var invalid = false
        path.applyWithBlock { pointer in
            let element = pointer.pointee
            switch element.type {
            case .moveToPoint:
                let p = element.points[0]
                contours.append([SIMD2(Double(p.x), Double(p.y))])
            case .addLineToPoint:
                let p = element.points[0]
                let point = SIMD2(Double(p.x), Double(p.y))
                guard let last = contours.indices.last else { invalid = true; return }
                if let previous = contours[last].last, Self.same(previous, point) { return }
                contours[last].append(point)
            case .closeSubpath:
                guard let last = contours.indices.last, let first = contours[last].first,
                      let end = contours[last].last else { invalid = true; return }
                if Self.same(first, end) { contours[last].removeLast() }
            default:
                invalid = true
            }
        }
        guard !invalid, !contours.isEmpty, contours.allSatisfy({ ring in
            ring.count >= 3 && ring.allSatisfy { $0.x.isFinite && $0.y.isFinite }
        }) else {
            throw GeoJSON.Invalid(message: "A projected part has an invalid or collapsed contour.")
        }
        rings = contours
    }

    private static func same(_ a: SIMD2<Double>, _ b: SIMD2<Double>) -> Bool {
        abs(a.x - b.x) < 1e-7 && abs(a.y - b.y) < 1e-7
    }

    static func cross(_ a: SIMD2<Double>, _ b: SIMD2<Double>) -> Double { a.x * b.y - a.y * b.x }

    var contourArea: Double {
        let origin = rings[0][0]
        return rings.enumerated().reduce(0) { sum, entry in
            let (index, ring) = entry
            let area = ring.indices.reduce(0.0) { total, i in
                total + Self.cross(ring[i] - origin, ring[(i + 1) % ring.count] - origin)
            }
            return sum + (index == 0 ? 1 : -1) * abs(area) / 2
        }
    }

    /// Include crossing elevations and vertex elevations to integrate even–odd spans.
    /// Within each resulting strip, the interval endpoints vary linearly.
    func evenOddArea(crossings: [Double]) -> Double {
        let segments = edges
        let levels = Set(points.map(\.y) + crossings).sorted()
        var area = 0.0
        for (bottom, top) in zip(levels, levels.dropFirst()) where bottom < top {
            let y = (bottom + top) / 2
            var intersections: [Double] = []
            for edge in segments {
                let a = edge.start
                let b = edge.end
                if (a.y > y) != (b.y > y) {
                    intersections.append(a.x + (b.x - a.x) * (y - a.y) / (b.y - a.y))
                }
            }
            intersections.sort()
            for i in stride(from: 0, to: intersections.count - 1, by: 2) {
                area += (intersections[i + 1] - intersections[i]) * (top - bottom)
            }
        }
        return area
    }

    func crossingElevations() -> [Double] {
        let ordered = edges.map { edge in
            (edge, min(edge.start.x, edge.end.x), max(edge.start.x, edge.end.x))
        }.sorted { $0.1 < $1.1 }
        var elevations: [Double] = []
        for (i, first) in ordered.enumerated() {
            let a = first.0
            for second in ordered.dropFirst(i + 1) {
                if second.1 > first.2 { break }
                let b = second.0
                if max(a.start.y, a.end.y) < min(b.start.y, b.end.y)
                    || max(b.start.y, b.end.y) < min(a.start.y, a.end.y) { continue }
                let u = a.end - a.start
                let v = b.end - b.start
                let determinant = Self.cross(u, v)
                guard determinant != 0 else { continue }
                let separation = b.start - a.start
                let t = Self.cross(separation, v) / determinant
                let s = Self.cross(separation, u) / determinant
                if t > 1e-10, t < 1 - 1e-10, s > 1e-10, s < 1 - 1e-10 {
                    elevations.append(a.start.y + t * u.y)
                }
            }
        }
        return elevations
    }
}

struct ProjectedTriangles {
    let vertices: [SIMD2<Double>]
    let indices: [UInt32]

    var area: Double {
        stride(from: 0, to: indices.count, by: 3).reduce(0) { sum, i in
            let a = vertices[Int(indices[i])]
            let b = vertices[Int(indices[i + 1])]
            let c = vertices[Int(indices[i + 2])]
            return sum + abs(ProjectedPolygon.cross(b - a, c - a)) / 2
        }
    }

    func contains(_ point: SIMD2<Double>) -> Bool {
        for i in stride(from: 0, to: indices.count, by: 3) {
            let a = vertices[Int(indices[i])]
            let b = vertices[Int(indices[i + 1])]
            let c = vertices[Int(indices[i + 2])]
            guard ProjectedPolygon.cross(b - a, c - a) != 0 else { continue }
            let u = ProjectedPolygon.cross(b - a, point - a)
            let v = ProjectedPolygon.cross(c - b, point - b)
            let w = ProjectedPolygon.cross(a - c, point - c)
            if (u >= 0 && v >= 0 && w >= 0) || (u <= 0 && v <= 0 && w <= 0) { return true }
        }
        return false
    }
}

enum AlbersTessellation {
    static func triangles(_ polygon: ProjectedPolygon) throws -> ProjectedTriangles {
        let points = polygon.points
        guard points.count <= Int(Int32.max) else {
            throw GeoJSON.Invalid(message: "Polygon exceeds the libtess2 signed index range.")
        }
        let origin = points[0]
        guard let tess = tessNewTess(nil) else {
            throw GeoJSON.Invalid(message: "libtess2 allocation failed.")
        }
        defer { tessDeleteTess(tess) }
        for ring in polygon.rings {
            let local = ring.map { SIMD2<Float>($0 - origin) }
            guard local.allSatisfy({ max(abs($0.x), abs($0.y)) <= 8_388_608 }) else {
                throw GeoJSON.Invalid(message: "A contour exceeds the libtess2 coordinate range.")
            }
            local.withUnsafeBytes { bytes in
                tessAddContour(tess, 2, bytes.baseAddress, Int32(MemoryLayout<SIMD2<Float>>.stride), Int32(local.count))
            }
        }
        let success = tessTesselate(tess, Int32(TESS_WINDING_ODD.rawValue),
                                    Int32(TESS_POLYGONS.rawValue), 3, 2, nil)
        let vertexCount = Int(tessGetVertexCount(tess))
        let indexCount = Int(tessGetElementCount(tess)) * 3
        guard success != 0, vertexCount >= 3, indexCount >= 3,
              let coordinates = tessGetVertices(tess), let inputIndices = tessGetVertexIndices(tess),
              let triangles = tessGetElements(tess) else {
            throw GeoJSON.Invalid(message: "libtess2 produced no valid triangles (status \(tessGetStatus(tess).rawValue)).")
        }
        let vertices = (0..<vertexCount).map { i -> SIMD2<Double> in
            let sourceIndex = Int(inputIndices[i])
            if points.indices.contains(sourceIndex) { return points[sourceIndex] }
            return origin + SIMD2(Double(coordinates[i * 2]), Double(coordinates[i * 2 + 1]))
        }
        let rawIndices = UnsafeBufferPointer(start: triangles, count: indexCount)
        guard rawIndices.allSatisfy({ $0 >= 0 && Int($0) < vertexCount }) else {
            throw GeoJSON.Invalid(message: "libtess2 returned an invalid triangle index.")
        }
        return ProjectedTriangles(vertices: vertices, indices: rawIndices.map(UInt32.init))
    }

    static func verifiedArea(of triangles: ProjectedTriangles, in polygon: ProjectedPolygon) throws -> Double {
        let actual = triangles.area
        let simple = polygon.contourArea
        if agrees(actual, simple) { return simple }
        let exact = polygon.evenOddArea(crossings: polygon.crossingElevations())
        guard agrees(actual, exact) else {
            throw GeoJSON.Invalid(message: "Mesh area \(actual) differs from the projected even–odd area \(exact).")
        }
        return exact
    }

    private static func agrees(_ actual: Double, _ expected: Double) -> Bool {
        actual.isFinite && actual > 0 && expected > 0 && abs(actual - expected) <= max(1e-4, expected * 0.0001)
    }
}

/// A backend-neutral, interleavable buffer layout. Positions are regional meters relative to origin.
struct AlbersMesh {
    struct Vertex {
        let position: SIMD2<Float>
        let part: UInt32
        let reserved: UInt32 = 0
    }

    struct Segment {
        let start: SIMD2<Float>
        let end: SIMD2<Float>
        let part: UInt32
        let reserved: UInt32 = 0
    }

    struct Part {
        let feature: UInt32
        let stripeOriginX: Float
    }

    struct Panel: RegionalMeshPayload {
        let origin: CGPoint
        let vertices: [Vertex]
        let indices: [UInt32]
        let parts: [Part]
        let segments: [Segment]
        let uniqueSegmentIndices: [UInt32]
        let featureSegments: [Int: [Range<Int>]]
    }

    struct Statistics {
        var parts = 0
        var vertices = 0
        var triangles = 0
        var segments = 0
        var uniqueSegments = 0
        var maxFloatError = 0.0
        var tessellationSeconds = 0.0
    }

    let panels: [Panel]
    let statistics: Statistics

    private struct SegmentKey: Hashable {
        let first: SIMD2<Float>
        let second: SIMD2<Float>

        init(_ a: SIMD2<Float>, _ b: SIMD2<Float>) {
            let inOrder = a.x < b.x || (a.x == b.x && a.y <= b.y)
            first = inOrder ? a : b
            second = inOrder ? b : a
        }
    }

    static func prepare(_ scene: AlbersScene) throws -> Self {
        var totals = Statistics()
        var panels: [Panel] = []
        for panel in scene.panels {
            let localFrame = panel.researchLocalFrame
            let origin = CGPoint(x: localFrame.midX, y: localFrame.midY)
            let offset = SIMD2(Double(origin.x), Double(origin.y))
            var vertices: [Vertex] = []
            var indices: [UInt32] = []
            var parts: [Part] = [Part(feature: .max, stripeOriginX: 0)]
            var segments: [Segment] = []
            var segmentIndices: [UInt32] = []
            var unique: [SegmentKey: Int] = [:]
            var selection: [Int: [Range<Int>]] = [:]

            func appendEdge(_ a: SIMD2<Float>, _ b: SIMD2<Float>, part: UInt32) {
                guard a != b else { return }
                let index = UInt32(segments.count)
                segments.append(Segment(start: a, end: b, part: part))
                let key = SegmentKey(a, b)
                if let existing = unique[key] {
                    // Give an exact shared edge the uppermost incident part's depth.
                    segmentIndices[existing] = index
                } else {
                    unique[key] = segmentIndices.count
                    segmentIndices.append(index)
                }
            }

            if panel.region.insetLabel != nil {
                let x = Float(localFrame.width / 2)
                let y = Float(localFrame.height / 2)
                let corners: [SIMD2<Float>] = [SIMD2(-x, -y), SIMD2(x, -y), SIMD2(x, y), SIMD2(-x, y)]
                vertices.append(contentsOf: corners.map { Vertex(position: $0, part: 0) })
                indices.append(contentsOf: [0, 1, 2, 0, 2, 3])
                for i in corners.indices { appendEdge(corners[i], corners[(i + 1) % corners.count], part: 0) }
            }

            for part in panel.parts {
                try Task.checkCancellation()
                do {
                    let polygon = try ProjectedPolygon(part.path)
                    let start = ProcessInfo.processInfo.systemUptime
                    let triangles = try AlbersTessellation.triangles(polygon)
                    totals.tessellationSeconds += ProcessInfo.processInfo.systemUptime - start
                    _ = try AlbersTessellation.verifiedArea(of: triangles, in: polygon)
                    guard triangles.vertices.count <= Int(UInt32.max) - vertices.count,
                          polygon.points.count <= Int(UInt32.max) - segments.count,
                          parts.count < Int(UInt32.max), part.feature < Int(UInt32.max) else {
                        throw GeoJSON.Invalid(message: "Regional mesh exceeds the UInt32 index range.")
                    }
                    let base = UInt32(vertices.count)
                    let index = UInt32(parts.count)
                    parts.append(Part(feature: UInt32(part.feature),
                                      stripeOriginX: Float(Double(part.bounds.minX) - offset.x)))
                    for point in triangles.vertices {
                        let local = point - offset
                        let value = SIMD2<Float>(local)
                        let error = SIMD2<Double>(value) - local
                        totals.maxFloatError = max(totals.maxFloatError, hypot(error.x, error.y))
                        vertices.append(Vertex(position: value, part: index))
                    }
                    let renderable = stride(from: 0, to: triangles.indices.count, by: 3).contains { i in
                        let a = SIMD2<Double>(vertices[Int(base + triangles.indices[i])].position)
                        let b = SIMD2<Double>(vertices[Int(base + triangles.indices[i + 1])].position)
                        let c = SIMD2<Double>(vertices[Int(base + triangles.indices[i + 2])].position)
                        return ProjectedPolygon.cross(b - a, c - a) != 0
                    }
                    guard renderable else {
                        throw GeoJSON.Invalid(message: "The projected part collapsed at GPU precision.")
                    }
                    indices.append(contentsOf: triangles.indices.map { base + $0 })
                    let startOfPart = segments.count
                    for edge in polygon.edges {
                        appendEdge(SIMD2<Float>(edge.start - offset), SIMD2<Float>(edge.end - offset), part: index)
                    }
                    selection[part.feature, default: []].append(startOfPart..<segments.count)
                    totals.parts += 1
                    totals.vertices += triangles.vertices.count
                    totals.triangles += triangles.indices.count / 3
                } catch {
                    let attributes = scene.properties[part.feature]
                    let identifier = attributes["GEOID"] ?? attributes["ID_NUM"] ?? attributes["NAME"] ?? String(part.feature)
                    throw GeoJSON.Invalid(message: "Feature \(identifier), part \(parts.count). \(error.localizedDescription)")
                }
            }
            totals.segments += segments.count
            totals.uniqueSegments += segmentIndices.count
            panels.append(Panel(origin: origin, vertices: vertices, indices: indices, parts: parts,
                                segments: segments, uniqueSegmentIndices: segmentIndices, featureSegments: selection))
        }
        guard totals.maxFloatError < 0.5 else {
            throw GeoJSON.Invalid(message: "Regional floating point conversion exceeded the half-meter precision guard.")
        }
        return Self(panels: panels, statistics: totals)
    }
}
