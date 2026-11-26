import CoreGraphics

/// Ordered, nonoverlapping interaction frames for artwork kept outside a geographic viewport.
public struct AnnotationGrid {
    public let count: Int
    public let columns: Int
    public let target: CGFloat

    public init(count: Int, width: CGFloat, target: CGFloat = 44, maximumColumns: Int? = nil) {
        self.count = max(0, count)
        self.target = target.isFinite ? max(1, target) : 44
        let limit = max(1, min(count, maximumColumns ?? count))
        let available = width.isFinite ? max(0, width) / self.target : 0
        columns = available >= CGFloat(limit) ? limit : max(1, Int(available))
    }

    public var height: CGFloat {
        guard count > 0 else { return 0 }
        return CGFloat((count - 1) / columns + 1) * target
    }

    public func frames(in region: CGRect) -> [CGRect] {
        guard count > 0, region.width >= target, region.height >= height else { return [] }
        return (0..<count).map { index in
            let row = index / columns
            let items = min(columns, count - row * columns)
            let x = region.midX - CGFloat(items) * target / 2
            return CGRect(x: x + CGFloat(index % columns) * target, y: region.minY + CGFloat(row) * target,
                          width: target, height: target)
        }
    }
}
