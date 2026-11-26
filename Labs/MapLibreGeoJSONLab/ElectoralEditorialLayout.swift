import CoreGraphics
#if canImport(BalloticsGeoKit)
import BalloticsGeoKit
#endif

struct ElectoralEditorialLayout {
    struct Item {
        let id: String
        let frame: CGRect
        var leader: AnnotationCallouts.Placement? = nil
    }

    let artwork: [Item]
    let tiles: [Item]
    let separator: CGRect?
    static let artworkSize = CGSize(width: 27, height: 39)

    static func content(scene: AlbersScene, specifications: [MapLabel],
                        records: [String: ElectoralAnnotation]) -> ([MapLabel], [String: ElectoralAnnotation]) {
        var content: [String: ElectoralAnnotation] = [:]
        let specifications = specifications.map { input in
            var spec = input
            let keys = Set(spec.features.compactMap { scene.properties[$0]["ID_NUM"] })
            if keys.count == 1, let key = keys.first, let allocation = records[key] {
                content[spec.id] = allocation
                spec.external = ElectoralCartography.keyOrder.contains(spec.id)
                    && (allocation.status == .reported || allocation.status == .unresolved)
            }
            return spec
        }
        return (specifications, content)
    }

    static func keyGrid(count: Int, width: CGFloat) -> AnnotationGrid {
        let automatic = AnnotationGrid(count: count, width: width)
        guard automatic.height > automatic.target else { return automatic }
        let rows = Int(automatic.height / automatic.target)
        return AnnotationGrid(count: count, width: width, maximumColumns: (count + rows - 1) / rows)
    }

    static func reservedHeight(count: Int, width: CGFloat, height: CGFloat) -> CGFloat {
        guard width >= 44 else { return 0 }
        let key = keyGrid(count: count, width: width).height
        let supplemental: CGFloat = height - key >= 176 ? 46 : 0
        return key + supplemental
    }

    static func resolve(_ decisions: [LabelPlacement], specs: [String: MapLabel], artworkSpecs: [MapLabel],
                        allocations: [String: ElectoralAnnotation], anchors: [String: GroupAnchors],
                        callouts: AnnotationCallouts?, scene: AlbersScene, camera: GeographicCamera,
                        region: CGRect, zoom: CGFloat) -> Self {
        var artwork: [Item] = []
        var external: [Item] = []
        var separator: CGRect?
        let internalLabels = decisions.filter { $0.mode == .full || $0.mode == .compact }
        let labelFrames = Dictionary(uniqueKeysWithValues: internalLabels.map { ($0.id, $0.frame) })
        let artworkIDs = Set(artworkSpecs.map(\.id))
        if !artworkSpecs.isEmpty {
            let sizes = Dictionary(uniqueKeysWithValues: artworkSpecs.map {
                ($0.id, LabelMeasurements(full: artworkSize, compact: .zero))
            })
            let placements = LabelResolver.resolve(artworkSpecs, anchors: anchors, sizes: sizes, scene: scene, camera: camera,
                zoom: zoom, obstacles: internalLabels.filter { !artworkIDs.contains($0.id) }.map(\.frame), policy: .nationalEditorial)
            for placement in placements where placement.mode == .full {
                // A failed artwork attempt leaves its ordinary abbreviation or count in place.
                guard !labelFrames.contains(where: { $0.key != placement.id && $0.value.intersects(placement.frame) }) else { continue }
                artwork.append(Item(id: placement.id, frame: placement.frame))
            }
        }
        var drawn = Set(artwork.map(\.id))
        if let callouts {
            var occupied = internalLabels.filter { !drawn.contains($0.id) }.map(\.frame) + artwork.map(\.frame)
            for spec in artworkSpecs where !drawn.contains(spec.id) {
                guard let candidate = anchors[spec.id],
                      let placement = callouts.place(features: Set(spec.features), anchors: candidate, size: artworkSize,
                          scene: scene, camera: camera, occupied: occupied.filter { $0 != labelFrames[spec.id] }) else { continue }
                drawn.insert(spec.id)
                artwork.append(Item(id: spec.id, frame: placement.frame, leader: placement))
                occupied.append(placement.frame)
                occupied.append(CGRect(x: min(placement.origin.x, placement.end.x), y: min(placement.origin.y, placement.end.y),
                    width: abs(placement.origin.x - placement.end.x),
                    height: abs(placement.origin.y - placement.end.y)).insetBy(dx: -1, dy: -1))
            }
        }
        let active = Set(decisions.filter { $0.mode == .tile }.map(\.id))
        let key = ElectoralCartography.keyOrder.filter(active.contains)
        let grid = keyGrid(count: key.count, width: region.width)
        for (id, frame) in zip(key, grid.frames(in: region)) {
            external.append(Item(id: id, frame: frame))
        }
        let remaining = CGRect(x: region.minX, y: region.minY + grid.height + 2, width: region.width,
                               height: max(0, region.height - grid.height - 2))
        let capacity = remaining.height >= 44 ? Int(remaining.width / 44) : 0
        if capacity > 0 {
            func priority(_ placement: LabelPlacement) -> Int {
                let hasCount = specs[placement.id]?.secondary.isEmpty == false
                if placement.mode == .suppressed && hasCount { return 0 }
                if allocations[placement.id]?.allocationArtwork == true { return 1 }
                if placement.mode == .compact && hasCount { return 2 }
                return 4
            }
            let overflow = decisions.filter { !active.contains($0.id) && !drawn.contains($0.id) && priority($0) < 4 }
                .sorted { a, b in priority(a) == priority(b) ? a.id < b.id : priority(a) < priority(b) }
                .prefix(capacity)
            let extraGrid = AnnotationGrid(count: overflow.count, width: remaining.width)
            if !overflow.isEmpty && !key.isEmpty {
                let width = min(CGFloat(overflow.count) * 44, 264)
                separator = CGRect(x: remaining.midX - width / 2, y: remaining.minY - 2, width: width, height: 0.5)
            }
            for (decision, frame) in zip(overflow, extraGrid.frames(in: remaining)) {
                external.append(Item(id: decision.id, frame: frame))
            }
        }
        return Self(artwork: artwork, tiles: external, separator: separator)
    }
}
