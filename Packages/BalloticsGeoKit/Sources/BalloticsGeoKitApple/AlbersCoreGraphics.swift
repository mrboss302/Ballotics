import BalloticsGeoKit
import CoreGraphics

public enum AlbersCoreGraphics {
    /// Draw into a context with the same top-left coordinate convention as the camera.
    /// Palette entries are indexed by the projected scene's feature order.
    @discardableResult
    public static func draw(_ scene: AlbersScene, camera: GeographicCamera, palette: [[CGColor]]? = nil,
                            boundary: GeographicBoundaryStyle = .init(), selected: Int? = nil,
                            alternate: Bool = false, indexed: Bool = true, visibleFeatures: Set<Int>? = nil, in context: CGContext) -> Int {
        let visible = camera.worldBounds
        let scale = camera.scale
        let fill = alternate ? CGColor(red: 0.23, green: 0.51, blue: 0.40, alpha: 1)
                             : CGColor(red: 0.29, green: 0.49, blue: 0.68, alpha: 1)
        let styles = palette?.count == scene.properties.count ? palette : nil
        var candidateCount = 0

        context.saveGState()
        defer { context.restoreGState() }
        context.translateBy(x: camera.viewport.midX, y: camera.viewport.midY)
        context.scaleBy(x: scale, y: scale)
        context.translateBy(x: -camera.center.x, y: -camera.center.y)
        for panel in scene.panels where (camera.region == nil || camera.region == panel.region) && panel.frame.intersects(visible) {
            if panel.showsFrame {
                let frame = CGPath(roundedRect: panel.frame, cornerWidth: 6 / scale, cornerHeight: 6 / scale, transform: nil)
                context.addPath(frame)
                context.setFillColor(CGColor(red: 0.89, green: 0.93, blue: 0.95, alpha: 1))
                context.setStrokeColor(CGColor(red: 0.72, green: 0.79, blue: 0.83, alpha: 1))
                context.setLineWidth(1 / scale)
                context.drawPath(using: .fillStroke)
            }
            context.saveGState()
            context.translateBy(x: panel.translation.x, y: panel.translation.y)
            context.scaleBy(x: panel.scale, y: panel.scale)
            let scale = scale * panel.scale
            let window = panel.local(visible)
            context.setStrokeColor(CGColor(gray: 0.96, alpha: 0.85))
            context.setLineWidth(boundary.width / scale)
            func render(_ part: AlbersScene.Part) {
                guard visibleFeatures?.contains(part.feature) != false else { return }
                let colors = styles?[part.feature] ?? []
                context.setFillColor(colors.first ?? fill)
                if colors.count > 1 {
                    drawSplit(part.path, bounds: part.bounds, window: window, colors: colors, scale: scale, in: context)
                    if boundary.width > 0 {
                        context.addPath(part.path)
                        context.strokePath()
                    }
                } else if boundary.width > 0 {
                    GeographicPathDrawing.fillAndStroke(part, in: context)
                } else {
                    context.addPath(part.path)
                    context.drawPath(using: .eoFill)
                }
            }
            if indexed {
                let indices = panel.visible(in: window)
                candidateCount += indices.count
                for index in indices { render(panel.parts[index]) }
            } else {
                candidateCount += panel.parts.count
                for part in panel.parts where part.bounds.intersects(window) { render(part) }
            }
            if let selected, visibleFeatures?.contains(selected) != false {
                context.setFillColor(CGColor(red: 1, green: 0.76, blue: 0.29, alpha: 1))
                context.setStrokeColor(CGColor(red: 0.36, green: 0.21, blue: 0.06, alpha: 1))
                context.setLineWidth(1.5 / scale)
                for part in panel.parts where part.feature == selected && part.bounds.intersects(window) {
                    context.addPath(part.path)
                    context.drawPath(using: styles == nil ? .eoFillStroke : .stroke)
                }
            }
            context.restoreGState()
        }
        return candidateCount
    }

    private static func drawSplit(_ path: CGPath, bounds: CGRect, window: CGRect, colors: [CGColor],
                                  scale: CGFloat, in context: CGContext) {
        context.saveGState()
        defer { context.restoreGState() }
        context.addPath(path)
        context.clip(using: .evenOdd)
        let width = 5 / scale
        let tile = width * CGFloat(colors.count)
        context.setFillColor(colors[0])
        context.fill(bounds)
        for (index, color) in colors.enumerated().dropFirst() {
            context.setFillColor(color)
            let origin = bounds.minX + CGFloat(index) * width
            let start = origin + max(0, floor((window.minX - origin) / tile)) * tile
            let end = min(bounds.maxX, window.maxX)
            guard start < end else { continue }
            for x in stride(from: start, to: end, by: tile) {
                context.fill(CGRect(x: x, y: bounds.minY, width: width, height: bounds.height))
            }
        }
    }
}
