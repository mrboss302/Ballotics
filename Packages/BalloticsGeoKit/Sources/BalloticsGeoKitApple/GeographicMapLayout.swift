import CoreGraphics

/// Local view coordinates. A consumer supplies the height needed by its own annotation content.
public struct GeographicMapLayout {
    public let viewport: CGRect
    public let externalAnnotations: CGRect?

    public init(bounds: CGRect, externalHeight: CGFloat = 0) {
        let reserved = externalHeight.isFinite ? min(max(0, externalHeight), max(0, bounds.height - 1)) : 0
        viewport = CGRect(x: bounds.minX, y: bounds.minY, width: bounds.width, height: bounds.height - reserved)
        externalAnnotations = reserved > 0
            ? CGRect(x: bounds.minX, y: viewport.maxY, width: bounds.width, height: reserved)
            : nil
    }
}
