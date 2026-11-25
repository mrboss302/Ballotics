import CoreGraphics
import Foundation
#if COMPARE_EARCUT
import EarcutComparison
#endif

@main
struct MetalMeshChecks {
    static func main() throws {
        var candidates: [(String, (ProjectedPolygon) throws -> ProjectedTriangles)] = [
            ("libtess2", AlbersTessellation.triangles)
        ]
        #if COMPARE_EARCUT
        candidates.append(("earcut", earcut))
        #endif
        let fixtures: [(String, [[[Double]]], Double)] = [
            ("concave", [[[0,0], [6,0], [6,1], [2,1], [2,5], [0,5]]], 14),
            ("opposite-winding-holes", [[[0,0], [10,0], [10,10], [0,10]],
                                        [[1,1], [1,3], [3,3], [3,1]],
                                        [[6,6], [9,6], [9,9], [6,9]]], 87),
            ("duplicate-nearly-collinear", [[[0,0], [0,0], [1,0.00000001], [5,0], [5,4], [0,4]]], 19.999999975),
            ("narrow-corridor", [[[0,0], [2,0], [2,0.99], [3,0.99], [3,0], [5,0], [5,2],
                                  [3,2], [3,1.01], [2,1.01], [2,2], [0,2]]], 8.02),
            ("small-island", [[[0,0], [0.02,0], [0.02,0.02], [0,0.02]]], 0.0004),
            ("island-inside-hole", [[[0,0],[12,0],[12,12],[0,12]],
                [[2,2],[10,2],[10,10],[2,10]], [[5,5],[7,5],[7,7],[5,7]]], 84),
            ("self-crossing", [[[0,0], [4,4], [0,4], [4,0]]], 8),
        ]

        for (name, makeTriangles) in candidates {
            for (label, rings, referenceArea) in fixtures {
                let path = makePath(rings)
                let polygon = try ProjectedPolygon(path)
                do {
                    let mesh = try makeTriangles(polygon)
                    let bounds = path.boundingBoxOfPath
                    let current = agreement(path: path, mesh: mesh, bounds: bounds, sampleCount: 320)
                    let good = abs(mesh.area - referenceArea) < 0.00001 && current == 0
                    emit(["test": label, "algorithm": name, "area_m2": mesh.area,
                          "expected_m2": referenceArea, "probe_mismatches": current, "pass": good])
                    if name == "libtess2" { precondition(good, label) }
                } catch {
                    emit(["test": label, "algorithm": name, "error": error.localizedDescription])
                    if name == "libtess2" { throw error }
                }
            }
        }
        do {
            let line = try ProjectedPolygon(makePath([[[0,0], [1,1], [2,2]]]))
            _ = try AlbersTessellation.triangles(line)
            preconditionFailure("Collapsed input unexpectedly produced a mesh.")
        } catch {
            emit(["test": "collapsed-ring", "pass": true, "action": "whole-scene fallback"])
        }

        if let fixture = ProcessInfo.processInfo.environment["METAL_FALLBACK_FIXTURE"] {
            let url = URL(fileURLWithPath: fixture)
            precondition(try! GeoJSON.validate(url, geography: .imported).features == 2)
            let scene = try AlbersScene.load(url)
            precondition(scene.properties.count == 2)
            do {
                _ = try AlbersMesh.prepare(scene)
                preconditionFailure("The collapsed fixture should require the original-path fallback.")
            } catch {
                precondition(error.localizedDescription.contains("fixture-collapsed"))
                let panel = scene.panels[0]
                let validPart = panel.parts.first { scene.properties[$0.feature]["GEOID"] == "fixture-visible" }!
                let point = CGPoint(x: validPart.bounds.midX + panel.translation.x,
                                    y: validPart.bounds.midY + panel.translation.y)
                precondition(scene.hits(point).contains(validPart.feature))
                emit(["test": "whole-scene-fallback", "pass": true, "source": "synthetic",
                      "visible_feature": "fixture-visible", "failed_feature": "fixture-collapsed"])
            }
        }

        for argument in CommandLine.arguments.dropFirst() {
            let url = URL(fileURLWithPath: argument)
            let scene = try AlbersScene.load(url)
            var parts: [(CGPath, CGRect, ProjectedPolygon)] = []
            var crossings = 0
            for panel in scene.panels {
                for part in panel.parts {
                    let polygon = try ProjectedPolygon(part.path)
                    if !polygon.crossingElevations().isEmpty { crossings += 1 }
                    parts.append((part.path, part.bounds, polygon))
                }
            }
            try checkBufferContract(scene: scene, dataset: url.lastPathComponent)
            for (name, makeTriangles) in candidates {
                var failures: [String] = []
                var vertices = 0
                var triangles = 0
                var mismatches = 0
                var maximumRelativeError = 0.0
                var preparationSeconds = 0.0
                let before = MemorySample.read()?.footprint ?? 0
                var peak = before
                for (i, entry) in parts.enumerated() {
                    let (path, bounds, polygon) = entry
                    do {
                        let start = ProcessInfo.processInfo.systemUptime
                        let mesh = try makeTriangles(polygon)
                        preparationSeconds += ProcessInfo.processInfo.systemUptime - start
                        let expected = try AlbersTessellation.verifiedArea(of: mesh, in: polygon)
                        vertices += mesh.vertices.count
                        triangles += mesh.indices.count / 3
                        maximumRelativeError = max(maximumRelativeError, abs(expected - mesh.area) / expected)
                        mismatches += agreement(path: path, mesh: mesh, bounds: bounds, sampleCount: 24)
                    } catch {
                        failures.append("\(i): \(error.localizedDescription)")
                    }
                    if i.isMultiple(of: 16) { peak = max(peak, MemorySample.read()?.footprint ?? 0) }
                }
                emit(["dataset": url.lastPathComponent, "algorithm": name, "features": scene.properties.count,
                      "parts": parts.count, "crossing_parts": crossings, "failed_parts": failures.count,
                      "sample_failures": Array(failures.prefix(6)), "vertices": vertices, "triangles": triangles,
                      "index_bytes": triangles * 3 * MemoryLayout<UInt32>.stride,
                      "maximum_relative_area_error": maximumRelativeError, "probe_mismatches": mismatches,
                      "measurements_per_part": 24, "tessellation_ms": preparationSeconds * 1_000,
                      "sampled_extra_mib": Double(peak - before) / 1_048_576])
                if name == "libtess2" { precondition(failures.isEmpty && mismatches == 0, url.lastPathComponent) }
            }
        }
    }

    private static func checkBufferContract(scene: AlbersScene, dataset: String) throws {
        precondition(MemoryLayout<AlbersMesh.Vertex>.stride == 16)
        precondition(MemoryLayout<AlbersMesh.Part>.stride == 8)
        precondition(MemoryLayout<AlbersMesh.Segment>.stride == 24)
        let mesh = try AlbersMesh.prepare(scene)
        var maxRelative = 0.0
        var maxAbsolute = 0.0
        for (panel, projected) in zip(mesh.panels, scene.panels) {
            var start = projected.region.insetLabel == nil ? 0 : 6
            let indices = panel.indices
            let origin = SIMD2(Double(panel.origin.x), Double(panel.origin.y))
            precondition(panel.parts.count == projected.parts.count + 1)
            precondition(panel.uniqueSegmentIndices.allSatisfy { panel.segments.indices.contains(Int($0)) })
            for (i, sourcePart) in projected.parts.enumerated() {
                let partIndex = UInt32(i + 1)
                precondition(panel.parts[Int(partIndex)].feature == UInt32(sourcePart.feature))
                var twiceArea = 0.0
                while start < indices.count, panel.vertices[Int(indices[start])].part == partIndex {
                    let a = panel.vertices[Int(indices[start])]
                    let b = panel.vertices[Int(indices[start + 1])]
                    let c = panel.vertices[Int(indices[start + 2])]
                    precondition(b.part == partIndex && c.part == partIndex)
                    let da = SIMD2<Double>(a.position) + origin
                    let db = SIMD2<Double>(b.position) + origin
                    let dc = SIMD2<Double>(c.position) + origin
                    twiceArea += abs(ProjectedPolygon.cross(db - da, dc - da))
                    start += 3
                }
                let polygon = try ProjectedPolygon(sourcePart.path)
                let expected = polygon.contourArea
                if expected > 0, twiceArea.isFinite {
                    let error = abs(twiceArea / 2 - expected)
                    maxAbsolute = max(maxAbsolute, error)
                    maxRelative = max(maxRelative, error / expected)
                }
            }
            precondition(start == indices.count)
            precondition(Set(panel.featureSegments.keys) == Set(projected.parts.map(\.feature)))
        }
        emit(["test": "buffer-layout", "dataset": dataset, "vertices": mesh.statistics.vertices,
              "triangles": mesh.statistics.triangles, "max_float_vertex_error_m": mesh.statistics.maxFloatError,
              "max_float_area_error_m2": maxAbsolute, "max_float_area_relative_error": maxRelative,
              "original_segments": mesh.statistics.segments, "unique_segments": mesh.statistics.uniqueSegments,
              "region_count": scene.panels.count, "pass": true])
    }

    private static func agreement(path: CGPath, mesh: ProjectedTriangles, bounds: CGRect, sampleCount: Int) -> Int {
        var disagreements = 0
        for sample in 0..<sampleCount {
            let x = Double((sample * 73 + 31) % 997) / 997
            let y = Double((sample * 37 + 19) % 991) / 991
            let point = CGPoint(x: bounds.minX + bounds.width * x, y: bounds.minY + bounds.height * y)
            if path.contains(point, using: .evenOdd) != mesh.contains(SIMD2(Double(point.x), Double(point.y))) {
                disagreements += 1
            }
        }
        return disagreements
    }

    private static func makePath(_ rings: [[[Double]]]) -> CGPath {
        let path = CGMutablePath()
        for ring in rings {
            path.move(to: CGPoint(x: ring[0][0], y: ring[0][1]))
            for point in ring.dropFirst() { path.addLine(to: CGPoint(x: point[0], y: point[1])) }
            path.closeSubpath()
        }
        return path
    }

    private static func emit(_ record: [String: Any]) {
        let bytes = try! JSONSerialization.data(withJSONObject: record, options: .sortedKeys)
        print(String(decoding: bytes, as: UTF8.self))
        fflush(stdout)
    }

    #if COMPARE_EARCUT
    private static func earcut(_ polygon: ProjectedPolygon) throws -> ProjectedTriangles {
        let points = polygon.points
        let coordinates = points.flatMap { [$0.x, $0.y] }
        let lengths = polygon.rings.map { Int32($0.count) }
        var output: UnsafeMutablePointer<UInt32>?
        let count = comparison_earcut(coordinates, lengths, Int32(lengths.count), &output)
        guard count >= 3, let output else { throw GeoJSON.Invalid(message: "Earcut produced no triangles.") }
        defer { free(output) }
        let indices = Array(UnsafeBufferPointer(start: output, count: Int(count)))
        guard indices.allSatisfy({ Int($0) < points.count }) else {
            throw GeoJSON.Invalid(message: "Earcut returned an invalid triangle index.")
        }
        return ProjectedTriangles(vertices: points, indices: indices)
    }
    #endif
}
