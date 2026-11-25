import SwiftUI
import UIKit

struct AlbersMapView: View {
    @ObservedObject var state: LabState
    @State private var scene: AlbersScene?
    @State private var selected: Int?

    var body: some View {
        AlbersCanvasView(scene: scene, selected: selected, alternateColor: state.alternateColor,
                         cameraRequest: state.cameraRequest) { index in
            guard !state.historicalPending, state.dataset?.geography == state.geography else { return }
            selected = index
            state.inspection = index.flatMap { scene?.properties[$0] }.map { Inspection(attributes: $0) }
        }
        .task(id: state.dataset?.requestedAt) {
            scene = nil
            selected = nil
            guard let dataset = state.dataset else { return }
            state.status = "Projecting local geometry…"
            // Capture the dataset as well as its URL so a temporary snapshot stays alive.
            let work = Task.detached(priority: .userInitiated) {
                try withExtendedLifetime(dataset) { try AlbersScene.load(dataset.url) }
            }
            do {
                let projected = try await withTaskCancellationHandler {
                    try await work.value
                } onCancel: {
                    work.cancel()
                }
                try Task.checkCancellation()
                guard state.dataset?.requestedAt == dataset.requestedAt else { return }
                scene = projected
                state.status = "\(projected.properties.count.formatted()) features · Albers equal area"
            } catch {
                guard !Task.isCancelled else { return }
                state.status = "Albers view unavailable"
                state.failure = error.localizedDescription
            }
        }
    }
}

private struct AlbersCanvasView: UIViewRepresentable {
    let scene: AlbersScene?
    let selected: Int?
    let alternateColor: Bool
    let cameraRequest: (Int, Viewpoint)
    let onSelect: (Int?) -> Void

    func makeUIView(context: Context) -> AlbersCanvas { AlbersCanvas() }

    func updateUIView(_ canvas: AlbersCanvas, context: Context) {
        canvas.update(scene: scene, selected: selected, alternateColor: alternateColor,
                      cameraRequest: cameraRequest, onSelect: onSelect)
    }
}

private final class AlbersCanvas: UIView {
    private var scene: AlbersScene?
    private var selected: Int?
    private var alternateColor = false
    private var onSelect: ((Int?) -> Void)?
    private var lastCameraRequest: Int?
    private var centerPoint = CGPoint.zero
    private var zoom: CGFloat = 1

    init() {
        super.init(frame: .zero)
        backgroundColor = UIColor(red: 0.93, green: 0.95, blue: 0.96, alpha: 1)
        contentMode = .redraw
        isAccessibilityElement = true
        accessibilityIdentifier = "albers-map"
        accessibilityLabel = "Albers equal-area map"
        accessibilityHint = "Use Map view to focus Alaska or Hawaii."
        let single = UITapGestureRecognizer(target: self, action: #selector(selectPolygon(_:)))
        let double = UITapGestureRecognizer(target: self, action: #selector(doubleTap(_:)))
        double.numberOfTapsRequired = 2
        single.require(toFail: double)
        let pan = UIPanGestureRecognizer(target: self, action: #selector(pan(_:)))
        pan.maximumNumberOfTouches = 1
        [single, double, pan, UIPinchGestureRecognizer(target: self, action: #selector(pinch(_:)))].forEach(addGestureRecognizer)
    }

    required init?(coder: NSCoder) { nil }

    func update(scene: AlbersScene?, selected: Int?, alternateColor: Bool,
                cameraRequest: (Int, Viewpoint), onSelect: @escaping (Int?) -> Void) {
        // The parent supplies a new generation by clearing the scene while projecting.
        if self.scene == nil, let scene {
            centerPoint = CGPoint(x: scene.bounds.midX, y: scene.bounds.midY)
            zoom = 1
        }
        self.scene = scene
        self.selected = selected
        self.alternateColor = alternateColor
        self.onSelect = onSelect
        if let lastCameraRequest, lastCameraRequest != cameraRequest.0 { move(cameraRequest.1) }
        lastCameraRequest = cameraRequest.0
        setNeedsDisplay()
    }

    override func draw(_ rect: CGRect) {
        guard let scene, let context = UIGraphicsGetCurrentContext() else { return }
        let scale = mapScale
        let visible = CGRect(x: centerPoint.x - bounds.width / (2 * scale),
                             y: centerPoint.y - bounds.height / (2 * scale),
                             width: bounds.width / scale, height: bounds.height / scale)
        context.saveGState()
        context.translateBy(x: bounds.midX, y: bounds.midY)
        context.scaleBy(x: scale, y: scale)
        context.translateBy(x: -centerPoint.x, y: -centerPoint.y)
        let fill = alternateColor ? UIColor(red: 0.23, green: 0.51, blue: 0.40, alpha: 1)
                                  : UIColor(red: 0.29, green: 0.49, blue: 0.68, alpha: 1)
        for panel in scene.panels where panel.frame.intersects(visible) {
            if panel.region.insetLabel != nil {
                let frame = CGPath(roundedRect: panel.frame, cornerWidth: 6 / scale, cornerHeight: 6 / scale, transform: nil)
                context.addPath(frame)
                context.setFillColor(UIColor(red: 0.89, green: 0.93, blue: 0.95, alpha: 1).cgColor)
                context.setStrokeColor(UIColor(red: 0.72, green: 0.79, blue: 0.83, alpha: 1).cgColor)
                context.setLineWidth(1 / scale)
                context.drawPath(using: .fillStroke)
            }
            context.saveGState()
            context.translateBy(x: panel.translation.x, y: panel.translation.y)
            let window = visible.offsetBy(dx: -panel.translation.x, dy: -panel.translation.y)
            context.setFillColor(fill.cgColor)
            context.setStrokeColor(UIColor(white: 0.96, alpha: 0.85).cgColor)
            context.setLineWidth(0.48 / scale)
            for part in panel.parts where part.bounds.intersects(window) {
                context.addPath(part.path)
                context.drawPath(using: .eoFillStroke)
            }
            if let selected {
                context.setFillColor(UIColor(red: 1, green: 0.76, blue: 0.29, alpha: 1).cgColor)
                context.setStrokeColor(UIColor(red: 0.36, green: 0.21, blue: 0.06, alpha: 1).cgColor)
                context.setLineWidth(1.5 / scale)
                for part in panel.parts where part.feature == selected && part.bounds.intersects(window) {
                    context.addPath(part.path)
                    context.drawPath(using: .eoFillStroke)
                }
            }
            context.restoreGState()
        }
        context.restoreGState()

        for panel in scene.panels where panel.frame.intersects(visible) {
            guard let label = panel.region.insetLabel else { continue }
            let point = CGPoint(x: bounds.midX + (panel.frame.minX - centerPoint.x) * scale + 6,
                                y: bounds.midY + (panel.frame.minY - centerPoint.y) * scale + 4)
            (label as NSString).draw(at: point, withAttributes: [
                .font: UIFont.systemFont(ofSize: 10, weight: .semibold),
                .foregroundColor: UIColor(red: 0.18, green: 0.29, blue: 0.36, alpha: 1)
            ])
        }
    }

    private var mapScale: CGFloat {
        guard let scene else { return 1 }
        return scale(toFit: scene.bounds) * zoom
    }

    private func scale(toFit frame: CGRect) -> CGFloat {
        min(max(1, bounds.width - 24) / max(1, frame.width),
            max(1, bounds.height - 24) / max(1, frame.height))
    }

    private func world(_ point: CGPoint) -> CGPoint {
        CGPoint(x: centerPoint.x + (point.x - bounds.midX) / mapScale,
                y: centerPoint.y + (point.y - bounds.midY) / mapScale)
    }

    private func move(_ viewpoint: Viewpoint) {
        guard let scene else { return }
        let target: CGRect?
        switch viewpoint {
        case .national: target = scene.bounds
        case .alaska: target = scene.panels.first { $0.region == .alaska }?.frame
        case .hawaii: target = scene.panels.first { $0.region == .hawaii }?.frame
        case .selectedState: target = selected.map(scene.frame(for:))
        }
        guard let target, !target.isNull else { return }
        centerPoint = CGPoint(x: target.midX, y: target.midY)
        zoom = min(16, max(1, scale(toFit: target.insetBy(dx: -target.width * 0.045,
                                                         dy: -target.height * 0.045)) / scale(toFit: scene.bounds)))
        setNeedsDisplay()
    }

    private func magnify(by factor: CGFloat, anchor: CGPoint) {
        guard scene != nil else { return }
        let before = world(anchor)
        zoom = min(16, max(1, zoom * factor))
        let after = world(anchor)
        centerPoint.x += before.x - after.x
        centerPoint.y += before.y - after.y
        constrainCenter()
        setNeedsDisplay()
    }

    private func constrainCenter() {
        guard let box = scene?.bounds else { return }
        centerPoint.x = min(box.maxX, max(box.minX, centerPoint.x))
        centerPoint.y = min(box.maxY, max(box.minY, centerPoint.y))
    }

    @objc private func selectPolygon(_ gesture: UITapGestureRecognizer) {
        onSelect?(scene?.hit(world(gesture.location(in: self))))
    }

    @objc private func doubleTap(_ gesture: UITapGestureRecognizer) {
        magnify(by: 2, anchor: gesture.location(in: self))
    }

    @objc private func pinch(_ gesture: UIPinchGestureRecognizer) {
        guard gesture.state == .changed else { return }
        magnify(by: gesture.scale, anchor: gesture.location(in: self))
        gesture.scale = 1
    }

    @objc private func pan(_ gesture: UIPanGestureRecognizer) {
        guard scene != nil, gesture.state == .changed else { return }
        let movement = gesture.translation(in: self)
        centerPoint.x -= movement.x / mapScale
        centerPoint.y -= movement.y / mapScale
        constrainCenter()
        gesture.setTranslation(.zero, in: self)
        setNeedsDisplay()
    }
}
