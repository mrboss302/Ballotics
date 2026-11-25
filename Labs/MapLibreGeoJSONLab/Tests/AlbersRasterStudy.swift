import CoreGraphics
import Foundation

@main
struct AlbersRasterStudy {
    enum DrawMode: String, CaseIterable {
        case fillAndStroke, fillOnly, aliasedFillAndStroke, batchedStroke, subpixelStroke
    }

    static func main() throws {
        precondition((2...3).contains(CommandLine.arguments.count),
                     "Specify the Census county GeoJSON and optionally one draw mode.")
        let scene = try AlbersScene.load(URL(fileURLWithPath: CommandLine.arguments[1]))
        let zoomLevels = [1, 2, 4, 8, 16]
        let modes: [DrawMode]
        if CommandLine.arguments.count == 3 {
            guard let mode = DrawMode(rawValue: CommandLine.arguments[2]) else {
                throw GeoJSON.Invalid(message: "Unknown raster study mode.")
            }
            modes = [mode]
        } else {
            modes = DrawMode.allCases
        }
        // Same 402 pt-wide iPhone study viewport proxy with three backing scales.
        for backingScale in [1, 2, 3] {
            for zoom in zoomLevels {
                for mode in modes {
                    try measure(scene, backingScale: backingScale, zoom: zoom, mode: mode)
                }
            }
        }
    }

    private static func measure(_ scene: AlbersScene, backingScale: Int, zoom: Int, mode: DrawMode) throws {
        let width = 402 * backingScale
        let height = 550 * backingScale
        let colorSpace = CGColorSpaceCreateDeviceRGB()
        guard let context = CGContext(data: nil, width: width, height: height, bitsPerComponent: 8,
                                      bytesPerRow: 0, space: colorSpace,
                                      bitmapInfo: CGImageAlphaInfo.premultipliedLast.rawValue) else {
            throw GeoJSON.Invalid(message: "Could not create the benchmark bitmap.")
        }
        let main = scene.panels.first { $0.region == .mainland }!
        let anchor = main.region.projection.project(longitude: -97, latitude: 39)
        let center = zoom == 1
            ? CGPoint(x: scene.bounds.midX, y: scene.bounds.midY)
            : CGPoint(x: anchor.x + main.translation.x, y: -anchor.y + main.translation.y)
        let fitX = CGFloat(width - 24 * backingScale) / scene.bounds.width
        let fitY = CGFloat(height - 24 * backingScale) / scene.bounds.height
        let scale = min(fitX, fitY) * CGFloat(zoom)
        let visible = CGRect(x: center.x - CGFloat(width) / (2 * scale),
                             y: center.y - CGFloat(height) / (2 * scale),
                             width: CGFloat(width) / scale, height: CGFloat(height) / scale)
        let candidates = scene.panels.map { panel in
            panel.visible(in: visible.offsetBy(dx: -panel.translation.x, dy: -panel.translation.y))
        }
        context.setShouldAntialias(mode != .aliasedFillAndStroke)
        var samples: [Double] = []
        for trial in 0..<4 {
            let start = ProcessInfo.processInfo.systemUptime
            context.setFillColor(red: 0.93, green: 0.95, blue: 0.96, alpha: 1)
            context.fill(CGRect(x: 0, y: 0, width: width, height: height))
            context.saveGState()
            context.translateBy(x: CGFloat(width) / 2, y: CGFloat(height) / 2)
            context.scaleBy(x: scale, y: -scale)
            context.translateBy(x: -center.x, y: -center.y)
            context.setFillColor(red: 0.29, green: 0.49, blue: 0.68, alpha: 1)
            context.setStrokeColor(gray: 0.96, alpha: 1)
            let strokePixels = mode == .subpixelStroke ? 0.85 : 0.48 * CGFloat(backingScale)
            context.setLineWidth(strokePixels / scale)
            for (panel, indices) in zip(scene.panels, candidates) {
                context.saveGState()
                context.translateBy(x: panel.translation.x, y: panel.translation.y)
                let batch = mode == .batchedStroke ? CGMutablePath() : nil
                for index in indices {
                    let path = panel.parts[index].path
                    batch?.addPath(path)
                    context.addPath(path)
                    context.drawPath(using: mode == .fillOnly || batch != nil ? .eoFill : .eoFillStroke)
                }
                if let batch {
                    context.addPath(batch)
                    context.strokePath()
                }
                context.restoreGState()
            }
            context.restoreGState()
            if trial > 0 { samples.append((ProcessInfo.processInfo.systemUptime - start) * 1_000) }
        }
        let report: [String: Any] = ["zoom": zoom, "backing_scale": backingScale, "mode": mode.rawValue,
                                     "parts": candidates.reduce(0) { $0 + $1.count }, "sample_ms": samples,
                                     "median_ms": samples.sorted()[1]]
        let data = try JSONSerialization.data(withJSONObject: report, options: .sortedKeys)
        print(String(decoding: data, as: UTF8.self))
    }
}
