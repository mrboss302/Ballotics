import CoreGraphics

/// Local view coordinates. A consumer supplies the height needed by its own annotation content.
public struct GeographicMapLayout {
    public let viewport: CGRect
    public let externalAnnotations: CGRect?

    public init(bounds: CGRect, externalHeight: CGFloat = 0, idealMapAspect: CGFloat? = nil) {
        let reserved = externalHeight.isFinite ? min(max(0, externalHeight), max(0, bounds.height - 1)) : 0
        let available = max(0, bounds.height - reserved)
        let height: CGFloat
        if let aspect = idealMapAspect, aspect.isFinite, aspect > 0 {
            let fitMargin: CGFloat = 24
            height = min(available, max(1, bounds.width - fitMargin) / aspect + fitMargin)
        } else {
            height = available
        }
        viewport = CGRect(x: bounds.minX, y: bounds.minY + (available - height) / 2,
                          width: bounds.width, height: height)
        externalAnnotations = reserved > 0
            ? CGRect(x: bounds.minX, y: viewport.maxY, width: bounds.width, height: reserved)
            : nil
    }
}
