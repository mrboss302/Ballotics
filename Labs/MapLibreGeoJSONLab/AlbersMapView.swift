import SwiftUI
import UIKit

enum AlbersTheme: String, CaseIterable {
    case candidates, alternate, margin

    private static let statusColors: Set<String> = [
        ElectionMapPresentation.noData, ElectionMapPresentation.nonparticipating,
        ElectionMapPresentation.unresolved, ElectionMapPresentation.tied
    ]

    func color(_ hex: String) -> UIColor {
        let color = UIColor(electionHex: hex)
        guard self == .alternate, !Self.statusColors.contains(hex) else { return color }
        var hue: CGFloat = 0
        var saturation: CGFloat = 0
        var brightness: CGFloat = 0
        color.getHue(&hue, saturation: &saturation, brightness: &brightness, alpha: nil)
        return UIColor(hue: (hue + 0.27).truncatingRemainder(dividingBy: 1),
                       saturation: saturation, brightness: brightness, alpha: 1)
    }

    static func marginColor(_ fraction: Double) -> UIColor {
        let strength = min(1, max(0, fraction))
        return UIColor(red: 0.85 - 0.72 * strength, green: 0.91 - 0.61 * strength,
                       blue: 0.98 - 0.37 * strength, alpha: 1)
    }

    func missingMarginColor(_ hex: String) -> CGColor {
        UIColor(electionHex: Self.statusColors.contains(hex) ? hex : ElectionMapPresentation.noData).cgColor
    }
}

private struct AlbersPaintConfiguration {
    struct Revision: Equatable {
        let data: Int
        let style: String
    }

    let revision: Revision
    let prepare: (AlbersScene) -> [[CGColor]]?
    let votes: [String: Int]?
    let statuses: [String:String]
    let didDraw: ((Double) -> Void)?
}

private extension ElectionMapPresentation {
    func albersColors(in scene: AlbersScene, theme: AlbersTheme) -> [[CGColor]] {
        let field = mode == .countyResults ? "GEOID" : "ID_NUM"
        let colors = Dictionary(uniqueKeysWithValues: Set(fills.values.flatMap { $0 }).map {
            ($0, theme.color($0).cgColor)
        })
        let noData = UIColor(electionHex: Self.noData).cgColor
        let nonparticipating = UIColor(electionHex: Self.nonparticipating).cgColor
        return scene.properties.map { props in
            if let key = props[field], let fill = fills[key] {
                if theme == .margin, mode == .countyResults {
                    if let margin = margins[key] { return [AlbersTheme.marginColor(margin).cgColor] }
                    return fill.map(theme.missingMarginColor)
                }
                return fill.compactMap { colors[$0] }
            }
            let nonparticipant = mode == .electoralCollege && props["TERR_TYPE"] != "State"
                && props["TERR_TYPE"] != "District of Columbia"
            return [nonparticipant ? nonparticipating : noData]
        }
    }
}

struct AlbersMapView: View {
    @ObservedObject var state: LabState
    @Binding var showGeographySearch: Bool
    @Environment(\.scenePhase) private var scenePhase
    @State private var scene: AlbersScene?
    @State private var sceneRequest: Double?
    @State private var selected: Int?
    @State private var overlapping: [Int] = []

    var body: some View {
        let currentRequest = sceneRequest
        let currentScene = currentRequest == state.dataset?.requestedAt ? scene : nil
        let receipt = state.expectedMetalReceipt
        AlbersCanvasView(scene: currentScene, profile: state.nationalProfile, onPresented: {
            guard state.expectedMetalReceipt == receipt, state.dataset?.requestedAt == currentRequest,
                  state.renderer == .metal, !state.dismissedStudyMap, !state.historicalPending, !state.importing,
                  state.dataset?.geography == state.geography else { return }
            state.metalReceipt = receipt
        }, selected: selected, alternateColor: state.alternateColor,
                         cameraRequest: state.cameraRequest, paint: paintConfiguration,
                         thickBorders: state.thickAlbersBorders, baseline: state.baselineAlbers,
                         studyProbe: state.albersStudyProbe, metal: state.renderer == .metal,
                         active: scenePhase == .active, request: currentRequest,
                         geometryPending: state.historicalPending || state.importing || state.dataset?.geography != state.geography,
                         canPublish: {
            state.renderer == .metal && !state.dismissedStudyMap && !state.historicalPending
                && !state.importing && state.dataset?.geography == state.geography && state.dataset?.requestedAt == currentRequest
        }, onStatus: { message in
            guard state.dataset?.geography == state.geography, state.dataset?.requestedAt == currentRequest else { return }
            state.status = message
        }) { hits in
            guard !state.historicalPending, let currentRequest,
                  state.dataset?.requestedAt == currentRequest, state.dataset?.geography == state.geography else { return }
            if hits.count > 1 {
                overlapping = hits
                showGeographySearch = true
            } else {
                select(hits.first, in: currentScene)
            }
        }
        .sheet(isPresented: $showGeographySearch, onDismiss: { overlapping = [] }) {
            if let currentScene {
                AlbersFeaturePicker(properties: currentScene.properties, overlapping: overlapping) { index in
                    guard state.dataset?.requestedAt == currentRequest, !state.historicalPending else { return }
                    select(index, in: currentScene)
                    state.moveCamera(.selectedFeature)
                }
            }
        }
        .onChange(of: state.dataset?.requestedAt) { _, _ in
            overlapping = []
            showGeographySearch = false
        }
        .onChange(of: state.electionRevision) { _, _ in selected = nil }
        .onChange(of: state.inspection == nil) { _, isClear in
            if isClear { selected = nil }
        }
        .task(id: state.dataset?.requestedAt) {
            scene = nil
            selected = nil
            guard let dataset = state.dataset else { return }
            state.status = "Projecting local geometry…"
            let start = LabLog.now
            let baseline = state.baselineAlbers
            let cached = baseline ? nil : state.albersCache.take(dataset.albersKey)
            // Capture the dataset as well as its URL so a temporary snapshot stays alive.
            let work = Task.detached(priority: .userInitiated) {
                try withExtendedLifetime(dataset) {
                    try cached ?? AlbersScene.load(dataset.url, indexed: !baseline)
                }
            }
            do {
                let projected = try await withTaskCancellationHandler {
                    try await work.value
                } onCancel: {
                    work.cancel()
                }
                try Task.checkCancellation()
                guard state.dataset?.requestedAt == dataset.requestedAt else { return }
                if !baseline {
                    state.albersCache.put(projected, at: dataset.albersKey, positions: dataset.summary.positions)
                }
                scene = projected
                sceneRequest = dataset.requestedAt
                let prep = projected.preparation
                LabLog.record("albers_geometry_ready", ["dataset": dataset.geography.rawValue,
                    "positions": dataset.summary.positions, "features": projected.properties.count,
                    "cache_hit": cached != nil, "decode_ms": prep.decodedMilliseconds,
                    "projection_ms": prep.projectedMilliseconds, "index_layout_ms": prep.indexedMilliseconds,
                    "elapsed_ms": LabLog.milliseconds(LabLog.now - start)])
                if dataset.geography == .presidential {
                    LabLog.record("election_albers_projection", ["year": state.election?.year ?? 0,
                       "mode": state.electionMode.rawValue, "elapsed_ms": LabLog.milliseconds(LabLog.now - start)])
                }
                state.status = state.renderer == .metal ? "Preparing geographic mesh…"
                    : "\(projected.properties.count.formatted()) features · Albers equal area"
            } catch {
                guard !Task.isCancelled else { return }
                state.status = "Albers view unavailable"
                state.failure = error.localizedDescription
            }
        }
    }

    private var paintConfiguration: AlbersPaintConfiguration {
        let result = state.geography == .presidential ? state.electionPresentation : nil
        let theme = state.albersTheme
        return AlbersPaintConfiguration(
            revision: .init(data: state.electionRevision, style: theme.rawValue),
            prepare: { scene in result?.albersColors(in: scene, theme: theme) },
            votes: result?.mode == .electoralCollege ? result?.electoralVotesByGeometry : nil,
            statuses: result?.details.compactMapValues { $0.first?.1 } ?? [:],
            didDraw: result.map { presentation in
                { elapsed in
                    LabLog.record("election_albers_paint", ["year": presentation.year,
                        "mode": presentation.mode.rawValue, "elapsed_ms": elapsed])
                }
            })
    }

    private func select(_ index: Int?, in scene: AlbersScene?) {
        selected = index
        if let index, let scene, scene.properties.indices.contains(index) { state.inspect(scene.properties[index]) }
        else { state.inspection = nil }
    }
}

private struct AlbersFeaturePicker: View {
    let properties: [[String: String]]
    let overlapping: [Int]
    let onSelect: (Int) -> Void
    @Environment(\.dismiss) private var dismiss
    @State private var query = ""

    private func title(_ index: Int) -> String {
        let fields = properties[index]
        return fields["NAMELSAD"] ?? fields["FULL_NAME"] ?? fields["NAME"] ?? "Geography"
    }

    private func detail(_ index: Int) -> String {
        let fields = properties[index]
        return [fields["STATE_TERR"] ?? fields["STUSPS"] ?? fields["STATEFP"],
                fields["TERR_TYPE"], fields["GEOID"] ?? fields["ID_NUM"]].compactMap { $0 }.joined(separator: " · ")
    }

    var body: some View {
        let candidates = overlapping.isEmpty ? Array(properties.indices) : overlapping
        let matches = candidates.filter {
            query.isEmpty || title($0).localizedStandardContains(query) || detail($0).localizedStandardContains(query)
        }
        NavigationStack {
            List(matches, id: \.self) { index in
                Button {
                    onSelect(index)
                    dismiss()
                } label: {
                    VStack(alignment: .leading) {
                        Text(title(index))
                        Text(detail(index)).font(.caption).foregroundStyle(.secondary)
                    }
                    .accessibilityElement(children: .combine)
                }
                .accessibilityIdentifier("albers-feature-\(index)")
            }
            .searchable(text: $query, placement: .navigationBarDrawer(displayMode: .always), prompt: "Search geography")
            .navigationTitle(overlapping.isEmpty ? "Find geography" : "Overlapping boundaries")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar { Button("Done") { dismiss() } }
        }
    }
}

private struct AlbersCanvasView: UIViewRepresentable {
    let scene: AlbersScene?
    let profile: NationalMapProfile
    let onPresented: () -> Void
    let selected: Int?
    let alternateColor: Bool
    let cameraRequest: (Int, Viewpoint)
    let paint: AlbersPaintConfiguration
    let thickBorders: Bool
    let baseline: Bool
    let studyProbe: Int
    let metal: Bool
    let active: Bool
    let request: Double?
    let geometryPending: Bool
    let canPublish: () -> Bool
    let onStatus: (String) -> Void
    let onSelect: ([Int]) -> Void

    func makeUIView(context: Context) -> AlbersCanvas { AlbersCanvas(baseline: baseline) }

    func updateUIView(_ canvas: AlbersCanvas, context: Context) {
        canvas.update(source: scene, profile: profile, onPresented: onPresented, selected: selected, alternateColor: alternateColor,
                      cameraRequest: cameraRequest, paint: paint, thickBorders: thickBorders,
                      studyProbe: studyProbe, metal: metal, active: active,
                      request: request, geometryPending: geometryPending, canPublish: canPublish,
                      onStatus: onStatus, onSelect: onSelect)
    }
}

private final class AlbersCanvas: UIView, UIGestureRecognizerDelegate {
    func gestureRecognizer(_ gestureRecognizer: UIGestureRecognizer, shouldReceive touch: UITouch) -> Bool {
        !(touch.view is UIControl || touch.view?.superview is UIControl)
    }
    private final class Clock: NSObject {
        let tick: () -> Void
        init(tick: @escaping () -> Void) { self.tick = tick }
        @objc func frame() { tick() }
    }

    private struct CameraTransition {
        let start: Double
        let center: CGPoint
        let zoom: CGFloat
        let goalCenter: CGPoint
        let goalZoom: CGFloat
    }

    private var scene: AlbersScene?
    private var selected: Int?
    private var featurePaint: [[CGColor]]?
    private var paintedRevision: AlbersPaintConfiguration.Revision?
    private var thickBorders = false
    private var pendingPaintMetric: (Double, (Double) -> Void)?
    private var pendingAppearanceMetric: (Double, String)?
    private var alternateColor = false
    private var onSelect: (([Int]) -> Void)?
    private let baseline: Bool
    private var lastCameraRequest: Int?
    private var centerPoint = CGPoint.zero
    private var zoom: CGFloat = 1
    private var animation: CameraTransition?
    private var displayLink: CADisplayLink?
    private var routeTask: Task<Void, Never>?
    private var routeStarted = false
    private var routeResults: FrameSamples?
    private var drawTimes: [Double] = []
    private var routeCandidates = 0
    private var lastStudyProbe = 0
    private var awaitingFirstDraw = false
    private var metal: AlbersMetalSurface?
    private var meshTask: Task<Void, Never>?
    private var metalRequested = false
    private var metalFailed = false
    private var metalReady = false
    private var metalRevision: Double?
    private var active = true
    private var canPublishMetal: (() -> Bool)?
    private var onStatus: ((String) -> Void)?
    private var profile: NationalMapProfile?
    private var onPresented: (() -> Void)?
    private let labelOverlay = ElectoralLabelOverlay()
    private var focusedRegion: AlbersRegion?
    private var insetLabels: [UILabel] = []

    private func requestReviewOrientation() {
        guard ProcessInfo.processInfo.arguments.contains("--review-landscape"),
              let scene = window?.windowScene else { return }
        // Reproduce a landscape viewport even when the simulator ignores virtual accelerometer events.
        DispatchQueue.main.async {
            scene.requestGeometryUpdate(.iOS(interfaceOrientations: .landscape)) { error in
                LabLog.record("geokit_review_orientation_error", ["reason": error.localizedDescription])
            }
        }
    }

    init(baseline: Bool) {
        self.baseline = baseline
        super.init(frame: .zero)
        backgroundColor = UIColor(red: 0.93, green: 0.95, blue: 0.96, alpha: 1)
        clipsToBounds = true
        contentMode = .redraw
        isAccessibilityElement = true
        accessibilityIdentifier = "albers-map"
        accessibilityLabel = "Albers equal-area map"
        accessibilityHint = "Use Find geography in Map view to select a shape by name."
        let single = UITapGestureRecognizer(target: self, action: #selector(selectPolygon(_:)))
        let double = UITapGestureRecognizer(target: self, action: #selector(doubleTap(_:)))
        double.numberOfTapsRequired = 2
        single.require(toFail: double)
        single.delegate = self
        double.delegate = self
        let pan = UIPanGestureRecognizer(target: self, action: #selector(pan(_:)))
        pan.maximumNumberOfTouches = 1
        [single, double, pan, UIPinchGestureRecognizer(target: self, action: #selector(pinch(_:)))].forEach(addGestureRecognizer)
    }

    required init?(coder: NSCoder) { nil }

    override func didMoveToWindow() {
        super.didMoveToWindow()
        requestReviewOrientation()
        if window == nil {
            animation = nil
            displayLink?.invalidate()
            displayLink = nil
            routeTask?.cancel()
            stopMetal(removeSurface: true)
        } else if metalRequested, !metalReady, meshTask == nil, !metalFailed, canPublishMetal?() == true,
                  let scene, let revision = metalRevision {
            prepareMetal(scene, revision: revision)
        }
    }

    private var lastLayoutSize = CGSize.zero

    override func layoutSubviews() {
        super.layoutSubviews()
        guard lastLayoutSize != bounds.size else { return }
        lastLayoutSize = bounds.size
        metal?.frame = bounds
        invalidateMap()
    }

    func update(source: AlbersScene?, profile: NationalMapProfile, onPresented: @escaping () -> Void, selected incomingSelection: Int?, alternateColor: Bool,
                cameraRequest: (Int, Viewpoint), paint: AlbersPaintConfiguration,
                thickBorders: Bool, studyProbe: Int, metal requested: Bool, active: Bool,
                request: Double?, geometryPending: Bool, canPublish: @escaping () -> Bool,
                onStatus: @escaping (String) -> Void, onSelect: @escaping ([Int]) -> Void) {
        let isNewScene = source != nil && (self.scene == nil || metalRevision != request)
        let layoutChanged = self.profile != profile
        self.profile = profile
        self.onPresented = onPresented
        let scene = source == nil ? nil : (isNewScene || layoutChanged ? source?.composed(profile) : self.scene)
        let selected = incomingSelection.flatMap { scene?.properties.indices.contains($0) == true && !isNewScene ? $0 : nil }
        let cameraChanged = lastCameraRequest != cameraRequest.0
        let styleChanged = paintedRevision != paint.revision
        let backendChanged = metalRequested != requested
        let activityChanged = self.active != active
        self.active = active
        metal?.renderAllowed = active
        self.onStatus = onStatus
        canPublishMetal = canPublish
        if geometryPending { cancelMeshPreparation() }
        if LabLog.enabled, scene != nil, !isNewScene {
            if styleChanged {
                pendingAppearanceMetric = (LabLog.now, "thematic_style")
            } else if self.thickBorders != thickBorders {
                pendingAppearanceMetric = (LabLog.now, "border_width")
            } else if self.selected != selected {
                pendingAppearanceMetric = (LabLog.now, selected == nil ? "clear_selection" : "highlight")
            } else if self.alternateColor != alternateColor {
                pendingAppearanceMetric = (LabLog.now, "palette")
            }
        }
        let changed = layoutChanged || isNewScene || (self.scene != nil && scene == nil) || self.selected != selected
            || self.alternateColor != alternateColor || self.thickBorders != thickBorders
            || styleChanged || cameraChanged
        if isNewScene || layoutChanged, let scene {
            focusedRegion = nil
            centerPoint = CGPoint(x: scene.bounds.midX, y: scene.bounds.midY)
            zoom = 1
            awaitingFirstDraw = true
            animation = nil
        }
        if isNewScene || styleChanged || (self.scene != nil && scene == nil) {
            let start = LabLog.now
            featurePaint = scene.flatMap(paint.prepare)
            pendingPaintMetric = featurePaint == nil ? nil : paint.didDraw.map { (start, $0) }
            paintedRevision = paint.revision
        }
        if layoutChanged, metalReady, let scene { metal?.updateLayout(scene.panels) }
        self.scene = scene
        if isNewScene || layoutChanged || styleChanged || scene == nil {
            labelOverlay.update(parent: self, scene: scene, votes: paint.votes, statuses: paint.statuses,
                                colors: featurePaint, select: { [weak self] feature in self?.onSelect?([feature]) },
                                ready: { [weak self] in self?.positionLabels() })
        }
        self.selected = selected
        self.alternateColor = alternateColor
        self.thickBorders = thickBorders
        self.onSelect = onSelect
        if backendChanged || metalRevision != request || (isNewScene && requested) || scene == nil && metalReady {
            stopMetal(removeSurface: !requested)
            metalRequested = requested
            metalRevision = request
            if backendChanged, !requested, let scene {
                sendStatus("\(scene.properties.count.formatted()) features · Albers equal area")
            }
        }
        if requested, !geometryPending, !metalReady, !metalFailed, meshTask == nil, let scene, let request {
            prepareMetal(scene, revision: request)
        }
        if styleChanged, metalReady {
            do { try metal?.updatePalette(featurePaint) }
            catch { failMetal(error.localizedDescription) }
        }
        if metalReady, styleChanged || layoutChanged { metal?.expectPresentation(onPresented) }
        if let pending = pendingAppearanceMetric, metalReady {
            metal?.markAppearance(pending.1, at: pending.0)
            pendingAppearanceMetric = nil
        }
        if lastCameraRequest != nil, cameraChanged { move(cameraRequest.1, animated: !isNewScene) }
        lastCameraRequest = cameraRequest.0
        if baseline || changed || backendChanged || (requested && activityChanged) { invalidateMap() }
        if studyProbe != lastStudyProbe, studyProbe > 0 {
            lastStudyProbe = studyProbe
            // Defer the callback until after UIViewRepresentable's state update.
            Task { [weak self] in self?.sampleSelection() }
        }
    }

    override func draw(_ rect: CGRect) {
        guard !metalRequested || metalFailed else { return }
        guard let scene, let context = UIGraphicsGetCurrentContext() else { return }
        let measure = LabLog.enabled
        let drawStart = measure ? LabLog.now : 0
        var candidateCount = 0
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
        for panel in scene.panels where (focusedRegion == nil || focusedRegion == panel.region) && panel.frame.intersects(visible) {
            if panel.showsFrame {
                let frame = CGPath(roundedRect: panel.frame, cornerWidth: 6 / scale, cornerHeight: 6 / scale, transform: nil)
                context.addPath(frame)
                context.setFillColor(UIColor(red: 0.89, green: 0.93, blue: 0.95, alpha: 1).cgColor)
                context.setStrokeColor(UIColor(red: 0.72, green: 0.79, blue: 0.83, alpha: 1).cgColor)
                context.setLineWidth(1 / scale)
                context.drawPath(using: .fillStroke)
            }
            context.saveGState()
            context.translateBy(x: panel.translation.x, y: panel.translation.y)
            context.scaleBy(x: panel.scale, y: panel.scale)
            let scale = scale * panel.scale
            let window = panel.local(visible)
            context.setFillColor(fill.cgColor)
            context.setStrokeColor(UIColor(white: 0.96, alpha: 0.85).cgColor)
            context.setLineWidth((thickBorders ? 1.2 : 0.48) / scale)
            func render(_ part: AlbersScene.Part) {
                if let paint = featurePaint?[part.feature], let base = paint.first {
                    context.setFillColor(base)
                    if paint.count > 1 {
                        drawSplit(part.path, bounds: part.bounds, window: window, colors: paint, scale: scale, in: context)
                    }
                }
                if (featurePaint?[part.feature].count ?? 0) > 1 {
                    context.addPath(part.path)
                    context.strokePath()
                } else {
                    GeographicPathDrawing.fillAndStroke(part, in: context)
                }
            }
            if baseline {
                candidateCount += panel.parts.count
                for part in panel.parts where part.bounds.intersects(window) { render(part) }
            } else {
                let visible = panel.visible(in: window)
                candidateCount += visible.count
                for index in visible { render(panel.parts[index]) }
            }
            if let selected {
                context.setFillColor(UIColor(red: 1, green: 0.76, blue: 0.29, alpha: 1).cgColor)
                context.setStrokeColor(UIColor(red: 0.36, green: 0.21, blue: 0.06, alpha: 1).cgColor)
                context.setLineWidth(1.5 / scale)
                for part in panel.parts where part.feature == selected && part.bounds.intersects(window) {
                    context.addPath(part.path)
                    context.drawPath(using: featurePaint == nil ? .eoFillStroke : .stroke)
                }
            }
            context.restoreGState()
        }
        context.restoreGState()

        if let metric = pendingPaintMetric {
            metric.1(LabLog.milliseconds(LabLog.now - metric.0))
            pendingPaintMetric = nil
        }
        for panel in scene.panels where (focusedRegion == nil || focusedRegion == panel.region) && panel.frame.intersects(visible) {
            guard panel.showsFrame, let label = panel.region.insetLabel else { continue }
            let point = CGPoint(x: bounds.midX + (panel.frame.minX - centerPoint.x) * scale + 6,
                                y: bounds.midY + (panel.frame.minY - centerPoint.y) * scale + 4)
            (label as NSString).draw(at: point, withAttributes: [
                .font: UIFont.systemFont(ofSize: 10, weight: .semibold),
                .foregroundColor: UIColor(red: 0.18, green: 0.29, blue: 0.36, alpha: 1)
            ])
        }
        if measure {
            let duration = LabLog.milliseconds(LabLog.now - drawStart)
            if routeResults != nil {
                routeResults?.addFrame()
                drawTimes.append(duration)
                routeCandidates += candidateCount
            }
            if awaitingFirstDraw {
                awaitingFirstDraw = false
                var fields = MemorySample.read()?.fields("first_draw") ?? [:]
                fields["features"] = scene.properties.count
                fields["draw_ms"] = duration
                if let request = metalRevision {
                    fields["geometry_request_ms"] = LabLog.milliseconds(LabLog.now - request)
                }
                LabLog.record("albers_first_draw", fields)
            }
            if let metric = pendingAppearanceMetric {
                pendingAppearanceMetric = nil
                LabLog.record("albers_appearance_draw", ["operation": metric.1, "draw_ms": duration,
                    "update_to_draw_ms": LabLog.milliseconds(LabLog.now - metric.0)])
            }
            startRouteIfNeeded()
        }
    }

    private func sendStatus(_ message: String) {
        let revision = metalRevision
        let surface = metal
        Task { [weak self] in
            guard let self, self.metalRevision == revision, self.metal === surface else { return }
            self.onStatus?(message)
        }
    }

    private func prepareMetal(_ scene: AlbersScene, revision: Double) {
        sendStatus("Preparing geographic mesh…")
        do {
            let surface = try metal ?? AlbersMetalSurface(configuration: .launch)
            if metal == nil {
                metal = surface
                surface.renderAllowed = active
                surface.frame = bounds
                surface.isHidden = true
                insertSubview(surface, at: 0)
            }
            guard let device = surface.device else {
                throw GeoJSON.Invalid(message: "Metal device became unavailable.")
            }
            LabLog.record("metal_mesh_preparing", ["revision": revision - LabLog.start])
            let work = Task.detached(priority: .userInitiated) {
                let sampler = MemorySampler()
                let start = LabLog.now
                let mesh = try AlbersMesh.prepare(scene)
                let prepared = LabLog.now
                try Task.checkCancellation()
                let buffers = try AlbersMetalBuffers(mesh, device: device)
                var fields = sampler.finish(event: "metal_mesh", since: start)
                fields.merge(buffers.byteCounts) { _, new in new }
                fields["prepare_ms"] = LabLog.milliseconds(prepared - start)
                fields["tessellation_ms"] = LabLog.milliseconds(mesh.statistics.tessellationSeconds)
                fields["upload_ms"] = LabLog.milliseconds(LabLog.now - prepared)
                fields["features"] = scene.properties.count
                fields["revision"] = revision - LabLog.start
                fields["vertex_buffers"] = buffers.panels.count
                fields["index_buffers"] = buffers.panels.count
                fields["parts"] = mesh.statistics.parts
                fields["vertices"] = mesh.statistics.vertices
                fields["triangles"] = mesh.statistics.triangles
                fields["segments"] = mesh.statistics.segments
                fields["unique_segments"] = mesh.statistics.uniqueSegments
                fields["max_float_error_m"] = mesh.statistics.maxFloatError
                return (buffers, fields)
            }
            meshTask = Task { [weak self, weak surface] in
                do {
                    let (buffers, fields) = try await withTaskCancellationHandler {
                        try await work.value
                    } onCancel: { work.cancel() }
                    try Task.checkCancellation()
                    guard let self, let surface, self.metalRequested, self.metalRevision == revision else { return }
                    self.meshTask = nil
                    guard self.canPublishMetal?() == true else {
                        LabLog.record("metal_mesh_superseded", ["revision": revision - LabLog.start])
                        return
                    }
                    try surface.updatePalette(self.featurePaint)
                    surface.publish(buffers, layout: self.scene?.panels ?? scene.panels)
                    if let acknowledge = self.onPresented { surface.expectPresentation(acknowledge) }
                    surface.onFirstFrame = { [weak self] in
                        guard let self, self.canPublishMetal?() == true else { return }
                        LabLog.record("metal_geometry_complete", ["features": scene.properties.count,
                            "geometry_request_ms": LabLog.milliseconds(LabLog.now - revision),
                            "revision": revision - LabLog.start])
                        self.sendStatus("\(scene.properties.count.formatted()) features · Albers Metal")
                        self.startRouteIfNeeded()
                    }
                    surface.onFailure = { [weak self] error in self?.failMetal(error) }
                    surface.onFrame = { [weak self] duration in
                        guard let self else { return }
                        if let metric = self.pendingPaintMetric {
                            self.pendingPaintMetric = nil
                            metric.1(LabLog.milliseconds(LabLog.now - metric.0))
                        }
                        guard self.routeResults != nil else { return }
                        self.routeResults?.addFrame()
                        self.drawTimes.append(duration)
                    }
                    self.metalReady = true
                    surface.isHidden = false
                    LabLog.record("metal_mesh_ready", fields)
                    self.invalidateMap()
                } catch {
                    guard !Task.isCancelled, let self, self.metalRevision == revision else { return }
                    self.meshTask = nil
                    if self.canPublishMetal?() == true { self.failMetal(error.localizedDescription) }
                }
            }
        } catch { failMetal(error.localizedDescription) }
    }

    private func cancelMeshPreparation() {
        guard let meshTask else { return }
        meshTask.cancel()
        self.meshTask = nil
        var fields: [String: Any] = [:]
        if let revision = metalRevision { fields["revision"] = revision - LabLog.start }
        LabLog.record("metal_mesh_cancelled", fields)
    }

    private func stopMetal(removeSurface: Bool) {
        cancelMeshPreparation()
        pendingAppearanceMetric = nil
        metalReady = false
        metalFailed = false
        metal?.clear()
        metal?.isHidden = true
        metal?.onFirstFrame = nil
        metal?.onFrame = nil
        insetLabels.forEach { $0.removeFromSuperview() }
        insetLabels.removeAll()
        if removeSurface {
            metal?.removeFromSuperview()
            metal = nil
        }
    }

    private func failMetal(_ reason: String) {
        stopMetal(removeSurface: true)
        metalFailed = true
        LabLog.record("metal_fallback", ["reason": reason])
        sendStatus("Core Graphics fallback · \(reason)")
        setNeedsDisplay()
    }

    private var geographicCamera: GeographicCamera {
        GeographicCamera(viewport: bounds, center: centerPoint, scale: mapScale, region: focusedRegion)
    }

    private func positionLabels() {
        guard let scene else { return }
        labelOverlay.position(in: scene, camera: geographicCamera, zoom: zoom, selected: selected)
    }

    private func invalidateMap() {
        positionLabels()
        if !metalRequested || metalFailed {
            setNeedsDisplay()
            return
        }
        guard let metal, metalReady, active else { return }
        metal.focusedRegion = focusedRegion
        metal.move(center: centerPoint, scale: mapScale, stroke: thickBorders ? 1.2 : 0.48,
                   selected: selected, alternate: alternateColor)
        if let scene {
            let insets = scene.panels.filter { $0.showsFrame && (focusedRegion == nil || focusedRegion == $0.region) }
            if insets.count != insetLabels.count {
                insetLabels.forEach { $0.removeFromSuperview() }
                insetLabels = insets.map { panel in
                    let label = UILabel()
                    label.text = panel.region.insetLabel
                    label.font = .systemFont(ofSize: 10, weight: .semibold)
                    label.textColor = UIColor(red: 0.18, green: 0.29, blue: 0.36, alpha: 1)
                    label.sizeToFit()
                    label.isUserInteractionEnabled = false
                    label.isAccessibilityElement = false
                    addSubview(label)
                    return label
                }
            }
            for (panel, label) in zip(insets, insetLabels) {
                label.frame.origin = CGPoint(x: bounds.midX + (panel.frame.minX - centerPoint.x) * mapScale + 6,
                                             y: bounds.midY + (panel.frame.minY - centerPoint.y) * mapScale + 4)
            }
        }
    }

    private func startRouteIfNeeded() {
        if !routeStarted, LabLog.enabled, ProcessInfo.processInfo.arguments.contains("--albers-region-route") {
            routeStarted = true
            routeTask = Task { [weak self] in await self?.runRegionRoute() }
        } else if !routeStarted, LabLog.enabled, ProcessInfo.processInfo.arguments.contains("--albers-route") {
            routeStarted = true
            routeTask = Task { [weak self] in await self?.runRoute() }
        }
    }

    private func drawSplit(_ path: CGPath, bounds: CGRect, window: CGRect,
                           colors: [CGColor], scale: CGFloat, in context: CGContext) {
        context.saveGState()
        context.addPath(path)
        context.clip(using: .evenOdd)
        // Tile stripes in screen pixels while preserving the projected polygon and its holes.
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
        context.restoreGState()
    }

    private var mapScale: CGFloat {
        guard let scene else { return 1 }
        return scale(toFit: scene.bounds) * zoom
    }

    private func scale(toFit frame: CGRect) -> CGFloat {
        GeographicCamera.fit(frame, in: bounds)
    }

    private func world(_ point: CGPoint) -> CGPoint {
        geographicCamera.world(point)
    }

    private func move(_ viewpoint: Viewpoint, animated: Bool) {
        guard let scene else { return }
        let target: CGRect?
        switch viewpoint {
        case .national: target = scene.bounds
        case .alaska: target = scene.panels.first { $0.region == .alaska }?.frame
        case .hawaii: target = scene.panels.first { $0.region == .hawaii }?.frame
        case .selectedFeature: target = selected.map(scene.frame(for:))
        case .selectedState:
            target = selected.flatMap { scene.properties[$0]["STATEFP"] }.flatMap(scene.frame(forState:))
        }
        guard let target, !target.isNull else { return }
        switch viewpoint {
        case .alaska: focusedRegion = .alaska
        case .hawaii: focusedRegion = .hawaii
        case .national: focusedRegion = nil
        case .selectedState, .selectedFeature:
            focusedRegion = selected.flatMap { feature in scene.panels.first { $0.parts.contains { $0.feature == feature } }?.region }
        }
        let nextCenter = CGPoint(x: target.midX, y: target.midY)
        let nextZoom = viewpoint == .national ? 1 : min(256, max(1, scale(toFit: target.insetBy(dx: -target.width * 0.045,
                                                         dy: -target.height * 0.045)) / scale(toFit: scene.bounds)))
        if animated, !baseline, !UIAccessibility.isReduceMotionEnabled, window != nil, routeResults == nil {
            animation = CameraTransition(start: LabLog.now, center: centerPoint, zoom: zoom,
                                         goalCenter: nextCenter, goalZoom: nextZoom)
            if displayLink == nil {
                let clock = Clock { [weak self] in self?.animateCamera() }
                let link = CADisplayLink(target: clock, selector: #selector(Clock.frame))
                link.add(to: .main, forMode: .common)
                displayLink = link
            }
        } else {
            animation = nil
            centerPoint = nextCenter
            zoom = nextZoom
            invalidateMap()
        }
    }

    private func animateCamera() {
        guard let animation else {
            displayLink?.invalidate()
            displayLink = nil
            return
        }
        let t = min(1, (LabLog.now - animation.start) / 0.28)
        let eased = t * t * (3 - 2 * t)
        centerPoint = CGPoint(x: animation.center.x + (animation.goalCenter.x - animation.center.x) * eased,
                              y: animation.center.y + (animation.goalCenter.y - animation.center.y) * eased)
        zoom = animation.zoom * pow(animation.goalZoom / animation.zoom, eased)
        invalidateMap()
        if t == 1 {
            self.animation = nil
            LabLog.record("albers_camera_transition", ["elapsed_ms": LabLog.milliseconds(LabLog.now - animation.start)])
        }
    }

    private func magnify(by factor: CGFloat, anchor: CGPoint) {
        guard scene != nil else { return }
        animation = nil
        let before = world(anchor)
        zoom = min(256, max(1, zoom * factor))
        let after = world(anchor)
        centerPoint.x += before.x - after.x
        centerPoint.y += before.y - after.y
        constrainCenter()
        invalidateMap()
    }

    private func constrainCenter() {
        guard let box = scene?.bounds else { return }
        centerPoint.x = min(box.maxX, max(box.minX, centerPoint.x))
        centerPoint.y = min(box.maxY, max(box.minY, centerPoint.y))
    }

    @objc private func selectPolygon(_ gesture: UITapGestureRecognizer) {
        select(at: world(gesture.location(in: self)))
    }

    private func select(at point: CGPoint) {
        let start = LabLog.now
        let hits = baseline ? (scene?.hit(point, region: focusedRegion).map { [$0] } ?? []) : (scene?.hits(point, region: focusedRegion) ?? [])
        onSelect?(hits)
        if featurePaint != nil || lastStudyProbe > 0 {
            LabLog.record("election_albers_selection", ["hit": !hits.isEmpty,
                "elapsed_ms": LabLog.milliseconds(LabLog.now - start)])
        }
    }

    private func sampleSelection() {
        guard let scene, !scene.panels.isEmpty else { return }
        let panel = scene.panels[lastStudyProbe % scene.panels.count]
        let location: (Double, Double)
        switch panel.region {
        case .mainland: location = (-78.491, 38.032)
        case .alaska: location = (-149.9003, 61.2181)
        case .hawaii: location = (-157.8583, 21.3069)
        }
        let projected = panel.region.projection.project(longitude: location.0, latitude: location.1)
        select(at: panel.world(CGPoint(x: projected.x, y: -projected.y)))
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
        animation = nil
        let movement = gesture.translation(in: self)
        centerPoint.x -= movement.x / mapScale
        centerPoint.y -= movement.y / mapScale
        constrainCenter()
        gesture.setTranslation(.zero, in: self)
        invalidateMap()
    }

    private func runRegionRoute() async {
        guard let scene else { return }
        for region in [AlbersRegion.alaska, .hawaii, .mainland] {
            guard region == .mainland || scene.panels.contains(where: { $0.region == region }) else { continue }
            let target: Viewpoint = region == .alaska ? .alaska : region == .hawaii ? .hawaii : .national
            metal?.beginWindow()
            let start = LabLog.now
            move(target, animated: true)
            routeResults = FrameSamples()
            repeat {
                do { try await Task.sleep(for: .milliseconds(16)) } catch { return }
            } while animation != nil
            var fields = routeResults?.fields() ?? [:]
            fields["elapsed_ms"] = LabLog.milliseconds(LabLog.now - start)
            fields["region"] = String(describing: region)
            fields["zoom"] = zoom
            if let metal { fields.merge(metal.finishWindow()) { _, new in new } }
            LabLog.record("albers_region_transition", fields)
            routeResults = nil
            guard region != .mainland else { continue }
            let origin = centerPoint
            let baseZoom = zoom
            let started = LabLog.now
            routeResults = FrameSamples()
            metal?.beginWindow()
            while LabLog.now - started < 3 {
                let phase = (LabLog.now - started) * 2 * .pi / 3
                zoom = baseZoom * (1.25 - 0.25 * cos(phase))
                centerPoint.x = origin.x + bounds.width / mapScale * 0.08 * sin(phase)
                invalidateMap()
                do { try await Task.sleep(for: .milliseconds(16)) } catch { return }
            }
            var navigation = routeResults?.fields() ?? [:]
            navigation["region"] = String(describing: region)
            if let metal { navigation.merge(metal.finishWindow()) { _, new in new } }
            LabLog.record("albers_region_navigation", navigation)
            routeResults = nil
        }
        LabLog.record("albers_region_route_done")
    }

    private func runRoute() async {
        guard let scene, let mainland = scene.panels.first(where: { $0.region == .mainland }) else { return }
        let projection = mainland.region.projection
        for level in CountyCameraRoute.levels {
            if Task.isCancelled { return }
            zoom = CGFloat(level)
            routeResults = FrameSamples()
            drawTimes = []
            routeCandidates = 0
            metal?.beginWindow()
            let started = LabLog.now
            let end = started + CountyCameraRoute.duration
            var step = 0
            while LabLog.now < end && !Task.isCancelled {
                let location = CountyCameraRoute.center(elapsed: LabLog.now - started, level: level)
                let point = projection.project(longitude: location.longitude, latitude: location.latitude)
                centerPoint = CGPoint(x: point.x + mainland.translation.x, y: -point.y + mainland.translation.y)
                invalidateMap()
                do { try await Task.sleep(for: .milliseconds(16)) }
                catch { return }
                step += 1
            }
            var fields = routeResults?.fields() ?? [:]
            if metalReady, let metal {
                // Commands already submitted can finish outside the camera window.
                try? await Task.sleep(for: .milliseconds(50))
                fields.merge(metal.finishWindow()) { _, new in new }
                let visible = CGRect(x: centerPoint.x - bounds.width / (2 * mapScale),
                                     y: centerPoint.y - bounds.height / (2 * mapScale),
                                     width: bounds.width / mapScale, height: bounds.height / mapScale)
                var candidates: Set<Int> = []
                var partCount = 0
                for panel in scene.panels where (focusedRegion == nil || focusedRegion == panel.region) && panel.frame.intersects(visible) {
                    let parts = panel.visible(in: panel.local(visible))
                    partCount += parts.count
                    for index in parts { candidates.insert(panel.parts[index].feature) }
                }
                fields["visible_part_candidates_last_camera"] = partCount
                fields["visible_feature_candidates_last_camera"] = candidates.count
            }
            fields["zoom_multiplier"] = level
            fields["baseline"] = baseline
            fields["renderer"] = metalReady ? "metal" : "core_graphics"
            fields["camera_updates"] = step
            fields["cull_candidates"] = routeCandidates
            let timings = drawTimes.sorted()
            if !timings.isEmpty {
                for percentile in [50, 95, 99] {
                    fields["p\(percentile)_draw_ms"] = timings[Int(ceil(Double(timings.count * percentile) / 100)) - 1]
                }
            }
            LabLog.record("albers_navigation", fields)
            routeResults = nil
        }
        move(.national, animated: false)
        LabLog.record("albers_route_done")
    }
}
