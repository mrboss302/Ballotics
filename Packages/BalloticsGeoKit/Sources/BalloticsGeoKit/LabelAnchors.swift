import Foundation
import CoreGraphics

/// An envelope is an axis-aligned rectangle centered on an interior candidate. Dimensions are in regional meters.
public struct LabelAnchor {
    public let point: CGPoint
    public let region: AlbersRegion
    public let feature: Int
    public let clearance: CGFloat
    let envelopes: [CGSize]

    public func fits(width: CGFloat, height: CGFloat) -> Bool {
        envelopes.contains { width < $0.width && height < $0.height }
    }
}

struct LabelPolygon {
    var rings: [[CGPoint]] = []
    let part: AlbersScene.Part
    let area: CGFloat
    let centroid: CGPoint
    let average: CGPoint

    init(_ part: AlbersScene.Part) {
        self.part = part
        var rings: [[CGPoint]] = []
        part.path.applyWithBlock { ptr in
            let e = ptr.pointee
            if e.type == .moveToPoint { rings.append([e.points[0]]) }
            if e.type == .addLineToPoint { rings[rings.count - 1].append(e.points[0]) }
        }
        self.rings = rings
        let outer = rings.first ?? []
        let n = CGFloat(max(1, outer.count))
        average = outer.reduce(.zero) { CGPoint(x: $0.x + $1.x/n, y: $0.y + $1.y/n) }
        // Use local coordinates to preserve small polygons far from the regional origin.
        let origin = part.bounds.origin
        var total: CGFloat = 0, moment = CGPoint.zero
        for (index, ring) in rings.enumerated() where ring.count >= 3 {
            var twiceArea: CGFloat = 0, cx: CGFloat = 0, cy: CGFloat = 0
            var previous = ring.last!
            for current in ring {
                let x = previous.x-origin.x, y = previous.y-origin.y
                let u = current.x-origin.x, v = current.y-origin.y
                let cross = x*v-u*y
                twiceArea += cross
                cx += (x+u)*cross
                cy += (y+v)*cross
                previous = current
            }
            guard twiceArea != 0 else { continue }
            let weight = abs(twiceArea) * (index == 0 ? 1 : -1)
            total += weight
            moment.x += weight*cx/(3*twiceArea)
            moment.y += weight*cy/(3*twiceArea)
        }
        area = total/2
        centroid = total > 0 ? CGPoint(x: origin.x+moment.x/total, y: origin.y+moment.y/total) : average
    }

    /// Distance to the entire ring boundary, including holes. With an aspect, return the largest
    /// safe half-height in the scaled L-infinity metric. Its minimum on a segment occurs at an
    /// endpoint or at an intersection with one of the two diagonals.
    func distance(_ point: CGPoint, aspect: CGFloat? = nil) -> CGFloat {
        var minimum = CGFloat.infinity
        for ring in rings {
            guard var previous = ring.last else { continue }
            for current in ring {
                let x = (previous.x-point.x)/(aspect ?? 1), y = previous.y-point.y
                let dx = (current.x-previous.x)/(aspect ?? 1), dy = current.y-previous.y
                previous = current
                if aspect != nil {
                    var value = min(max(abs(x),abs(y)), max(abs(x+dx),abs(y+dy)))
                    for sign: CGFloat in [-1, 1] {
                        let divisor = dx-sign*dy
                        if divisor == 0 { continue }
                        let t = (sign*y-x)/divisor
                        if t > 0 && t < 1 { value = min(value,abs(y+t*dy)) }
                    }
                    minimum = min(minimum,value)
                } else {
                    let len = dx*dx+dy*dy
                    let t = len > 0 ? min(1,max(0,-(x*dx+y*dy)/len)) : 0
                    minimum = min(minimum,(x+t*dx)*(x+t*dx)+(y+t*dy)*(y+t*dy))
                }
            }
        }
        return aspect == nil ? sqrt(minimum) : minimum
    }

    func contains(_ point: CGPoint) -> Bool { part.path.contains(point, using: .evenOdd) }

    func anchor(_ point: CGPoint, region: AlbersRegion) -> LabelAnchor? {
        guard contains(point) else { return nil }
        let envelopes = [0.65, 1.1, 1.8, 3.0, 5.0].map { aspect in
            let height = 2 * distance(point, aspect: aspect)
            return CGSize(width: height*aspect, height: height)
        }
        return LabelAnchor(point: point, region: region, feature: part.feature,
                           clearance: distance(point), envelopes: envelopes)
    }

    /// Bounded best-first pole search. Highly detailed coastlines share the same evaluation budget.
    func candidates() -> [CGPoint] {
        struct Cell { let point: CGPoint; let radius, distance, upper: CGFloat }
        let box = part.bounds
        func cell(_ point: CGPoint, _ radius: CGFloat) -> Cell {
            let d = distance(point) * (contains(point) ? 1 : -1)
            return Cell(point: point, radius: radius, distance: d, upper: d+radius*sqrt(2))
        }
        let radius = max(box.width,box.height)/2
        guard radius > 0 else { return [] }
        var queue = [cell(CGPoint(x: box.midX,y: box.midY),radius)]
        var best = queue[0], alternatives: [Cell] = []
        for _ in 0..<64 {
            guard let i = queue.indices.max(by: { queue[$0].upper < queue[$1].upper }) else { break }
            let c = queue.remove(at:i)
            if c.distance > 0 { alternatives.append(c) }
            if c.distance > best.distance { best = c }
            if c.upper-best.distance <= radius/80 { continue }
            let half = c.radius/2
            for x in [-half, half] { for y in [-half,half] {
                queue.append(cell(CGPoint(x:c.point.x+x,y:c.point.y+y),half))
            }}
        }
        var points = [best.point, centroid, average]
        if let base = part.interiorAnchor { points.append(base) }
        for c in alternatives.sorted(by: { $0.distance > $1.distance }) {
            if points.count >= 7 { break }
            if points.allSatisfy({ hypot($0.x-c.point.x,$0.y-c.point.y) > radius/6 }) { points.append(c.point) }
        }
        return points
    }
}

public struct GroupAnchors {
    public let candidates: [LabelAnchor]
    public let averageInside, centroidInside, baselineInside: Bool
    init(candidates: [LabelAnchor], averageInside: Bool, centroidInside: Bool, baselineInside: Bool) {
        self.candidates = candidates
        self.averageInside = averageInside
        self.centroidInside = centroidInside
        self.baselineInside = baselineInside
    }
    public init(scene: AlbersScene, features: [Int]) {
        let members = Set(features)
        // Bound work on archipelagos. Rank by actual area among the greatest bounding areas.
        let parts = scene.panels.flatMap { panel in
            panel.parts.filter { members.contains($0.feature) }.map { (panel.region,$0) }
        }.sorted { $0.1.bounds.width*$0.1.bounds.height > $1.1.bounds.width*$1.1.bounds.height }
        let ranked = parts.prefix(8).map { ($0.0,LabelPolygon($0.1)) }.sorted { $0.1.area > $1.1.area }
        averageInside = ranked.first.map { $0.1.contains($0.1.average) } ?? false
        centroidInside = ranked.first.map { $0.1.contains($0.1.centroid) } ?? false
        baselineInside = parts.first?.1.interiorAnchor != nil
        candidates = ranked.prefix(3).flatMap { region, shape in
            var seen: [CGPoint] = []
            return shape.candidates().compactMap { point -> LabelAnchor? in
                guard !seen.contains(point) else { return nil }
                seen.append(point)
                return shape.anchor(point, region: region)
            }
        }
    }
}

/// The canonical projected scene and its presentation profiles share this cache. It stores no styles or views.
public final class GeographicAnchorCache {
    package let geometryID = UUID()
    private let lock = NSLock()
    private var groups: [[Int]: GroupAnchors] = [:]
    public init() {}
    public func anchors(in scene: AlbersScene, features: [Int]) -> GroupAnchors {
        let key = features.sorted()
        lock.lock()
        defer { lock.unlock() }
        if let cached = groups[key] { return cached }
        let prepared = GroupAnchors(scene: scene, features: key)
        groups[key] = prepared
        return prepared
    }
    public var count: Int {
        lock.lock()
        defer { lock.unlock() }
        return groups.count
    }
}
