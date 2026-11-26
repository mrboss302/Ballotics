import UIKit
import BalloticsGeoKit

@MainActor
final class ElectoralLabelOverlay {
    private let font = UIFont.monospacedDigitSystemFont(ofSize: 8, weight: .medium)
    private var measurements: [String:CGSize] = [:]
    private var specs: [MapLabel] = []
    private var byID: [String:MapLabel] = [:]
    private var labels: [String:UILabel] = [:]
    private var tiles: [String:UIButton] = [:]
    private var editorialTiles: [String: ElectoralAnnotationView] = [:]
    private var artworkViews: [String: ElectoralAnnotationView] = [:]
    private var allocations: [String: ElectoralAnnotation] = [:]
    private var keyIDs: [String] = []
    private var artworkSpecs: [MapLabel] = []
    private var calloutGeometry: AnnotationCallouts?
    private var calloutPanels: [CGRect] = []
    private var leaders: [AnnotationCallouts.Placement] = []
    var hasSeparatorsOrLeaders: Bool { separator != nil || !leaders.isEmpty }
    private var separator: CGRect?
    private var accessibleOrder: [ObjectIdentifier] = []
    private var diagnosticAnnotationFrames: [CGRect] = []
    private var sizes: [String:LabelMeasurements] = [:]
    private var paintedText: [String:String] = [:]
    private var anchors: [String:GroupAnchors] = [:]
    private var previous: [String:Int] = [:]
    private var cache: GeographicAnchorCache?
    private var worker: Task<Void,Never>?
    private var select: ((Int) -> Void)?
    private weak var parent: UIView?
    private var logged = false
    private var editorial = true
    private let measureLayout = ProcessInfo.processInfo.arguments.contains("--label-metrics")
    private let diagnostics = ProcessInfo.processInfo.arguments.contains("--cartography-debug")
    private var diagnosticDecisions: [LabelPlacement] = []

    private func attributes(for typography: UIFont) -> [NSAttributedString.Key: Any] {
        let paragraph = NSMutableParagraphStyle()
        paragraph.alignment = .center
        if editorial {
            let lineHeight = max(9, typography.pointSize * 1.125)
            paragraph.minimumLineHeight = lineHeight
            paragraph.maximumLineHeight = lineHeight
        }
        return [.font: typography, .paragraphStyle: paragraph]
    }

    private func measure(_ text: String, typography: UIFont) -> CGSize {
        let key = "\(editorial) / \(typography.pointSize) / \(text)"
        if let size = measurements[key] { return size }
        let options: NSStringDrawingOptions = editorial ? [.usesLineFragmentOrigin] : [.usesLineFragmentOrigin, .usesFontLeading]
        let rect = (text as NSString).boundingRect(with: CGSize(width: 500, height: 100),
            options: options, attributes: attributes(for: typography), context: nil)
        let size = CGSize(width: ceil(rect.width) + 2, height: ceil(rect.height) + 1)
        measurements[key] = size
        return size
    }

    func externalHeight(width: CGFloat, height: CGFloat) -> CGFloat {
        guard !specs.isEmpty else { return 0 }
        return ElectoralEditorialLayout.reservedHeight(count: keyIDs.count, width: width, height: height)
    }

    private func makeAnnotation(on parent: UIView, id: String, kind: String) -> ElectoralAnnotationView {
        let view = ElectoralAnnotationView(frame: .zero)
        view.isHidden = true
        view.isAccessibilityElement = false
        view.accessibilityElementsHidden = true
        view.accessibilityIdentifier = "geokit-\(kind)-\(id)"
        view.addAction(UIAction { [weak self] _ in
            guard let self, let index = self.byID[id]?.features.first else { return }
            self.select?(index)
        }, for: .touchUpInside)
        parent.addSubview(view)
        return view
    }

    func update(parent: UIView, scene: AlbersScene?, votes: [String:Int]?, statuses: [String:String], annotations: [String: ElectoralAnnotation],
                colors: [[CGColor]]?, unitColor: (String) -> UIColor, editorial: Bool, select: @escaping (Int)->Void, ready: @escaping ()->Void) {
        let start = LabLog.now
        let typographyChanged = self.editorial != editorial
        self.editorial = editorial
        if diagnostics { diagnosticDecisions = [] }
        self.select = select
        self.parent = parent
        let panels = scene?.panels.map(\.frame) ?? []
        if votes != nil && editorial {
            if calloutGeometry == nil || cache !== scene?.labelCache || calloutPanels != panels {
                calloutGeometry = scene.map(AnnotationCallouts.init)
                calloutPanels = panels
            }
        } else {
            calloutGeometry = nil
            calloutPanels = []
            leaders.removeAll()
            separator = nil
        }
        parent.isAccessibilityElement = true
        specs = scene.flatMap { geo in votes.map { ElectoralCartography.specifications(scene:geo,votes:$0,statuses:statuses) } } ?? []
        allocations.removeAll(keepingCapacity: true)
        artworkSpecs.removeAll(keepingCapacity: true)
        if editorial, let scene {
            (specs, allocations) = ElectoralEditorialLayout.content(scene: scene, specifications: specs, records: annotations)
        }
        keyIDs = ElectoralCartography.keyOrder.filter { code in specs.contains { $0.id == code && $0.external } }
        byID = Dictionary(uniqueKeysWithValues: specs.map { ($0.id,$0) })
        let ids = Set(byID.keys)
        for id in labels.keys.filter({ !ids.contains($0) }) { labels.removeValue(forKey:id)?.removeFromSuperview() }
        for id in tiles.keys.filter({ !ids.contains($0) }) { tiles.removeValue(forKey:id)?.removeFromSuperview() }
        for id in editorialTiles.keys.filter({ !ids.contains($0) || !editorial }) {
            editorialTiles.removeValue(forKey: id)?.removeFromSuperview()
        }
        for id in artworkViews.keys.filter({ allocations[$0]?.allocationArtwork != true || !editorial }) {
            artworkViews.removeValue(forKey: id)?.removeFromSuperview()
        }
        if typographyChanged || cache !== scene?.labelCache || specs.isEmpty {
            worker?.cancel()
            worker = nil
            cache = scene?.labelCache
            labels.values.forEach { $0.isHidden = true }
            tiles.values.forEach { $0.isHidden = true }
            editorialTiles.values.forEach { $0.isHidden = true }
            artworkViews.values.forEach { $0.isHidden = true }
            parent.accessibilityElements = nil
            accessibleOrder.removeAll()
            anchors.removeAll()
            previous.removeAll()
        }
        guard let scene, !specs.isEmpty else {
            parent.accessibilityElements = nil
            accessibleOrder.removeAll()
            return
        }
        sizes.removeAll(keepingCapacity:true)
        paintedText.removeAll(keepingCapacity: true)
        var artworkMilliseconds = 0.0
        for spec in specs {
            let view = labels[spec.id] ?? UILabel()
            if view.superview == nil {
                view.numberOfLines = 2
                view.textAlignment = .center
                view.font = font
                view.isHidden = true
                view.isAccessibilityElement = false
                parent.addSubview(view)
                labels[spec.id] = view
            }
            let i = spec.features[0]
            let paints = colors.flatMap { $0.indices.contains(i) ? $0[i] : nil } ?? []
            let fill = paints.first.map(UIColor.init(cgColor:)) ?? .darkGray
            view.textColor = paints.count > 1 ? .black : fill.annotationInk
            view.backgroundColor = paints.count > 1
                ? UIColor(red: 0.98, green: 0.97, blue: 0.93, alpha: 0.94) : .clear
            view.layer.cornerRadius = 2
            view.clipsToBounds = true
            let typography = UIFont.monospacedDigitSystemFont(ofSize: min(13, max(8, 8 * spec.typeScale)), weight: .medium)
            view.font = typography
            sizes[spec.id] = .init(full: measure(spec.text(for: .stacked), typography: typography),
                                   compact: measure(spec.primary, typography: typography),
                                   inline: spec.secondary.isEmpty ? nil : measure(spec.text(for: .inline), typography: typography))
            if editorial {
                let artworkStart = LabLog.enabled ? LabLog.now : 0
                tiles[spec.id]?.isHidden = true
                let allocation = allocations[spec.id]
                let count = allocation?.displayCount ?? (spec.secondary.isEmpty ? nil : spec.secondary)
                if spec.external || allocation?.allocationArtwork == true || !spec.secondary.isEmpty {
                    let button = editorialTiles[spec.id] ?? makeAnnotation(on: parent, id: spec.id, kind: "tile")
                    editorialTiles[spec.id] = button
                    button.configure(code: spec.primary, count: count, fill: fill, ink: fill.annotationInk,
                                     allocation: allocation, artwork: true, compact: false, unitColor: unitColor)
                }
                if allocation?.allocationArtwork == true, allocation?.squares != nil, !spec.external {
                    let artwork = artworkViews[spec.id] ?? makeAnnotation(on: parent, id: spec.id, kind: "allocation")
                    artworkViews[spec.id] = artwork
                    artwork.configure(code: spec.primary, count: count, fill: fill, ink: fill.annotationInk,
                                      allocation: allocation, artwork: true, compact: true, unitColor: unitColor)
                    artworkSpecs.append(spec)
                }
                if LabLog.enabled { artworkMilliseconds += LabLog.milliseconds(LabLog.now - artworkStart) }
            } else if spec.external {
                let button = tiles[spec.id] ?? UIButton(type:.custom)
                if button.superview == nil {
                    button.titleLabel?.font = font
                    button.layer.cornerRadius = 3
                    button.isHidden = true
                    button.accessibilityIdentifier = "geokit-tile-"+spec.id
                    let id = spec.id
                    button.addAction(UIAction { [weak self] _ in
                        guard let self, let index = self.byID[id]?.features.first else { return }
                        self.select?(index)
                    },for:.touchUpInside)
                    parent.addSubview(button)
                    tiles[spec.id] = button
                }
                button.backgroundColor = fill
                button.setTitleColor(fill.annotationInk,for:.normal)
                button.setTitle(spec.primary+" "+(spec.secondary.isEmpty ? "?" : spec.secondary)+(paints.count > 1 ? " ◧" : ""),for:.normal)
                button.accessibilityLabel = spec.accessibilityText
            } else { tiles[spec.id]?.isHidden = true }
        }
        logged = false
        if diagnostics {
            LabLog.record("annotation_content", ["requested": allocations.count,
                "allocation_artwork": artworkSpecs.map(\.id), "geographic_features": scene.properties.count])
        }
        if LabLog.enabled { LabLog.record("geokit_label_content",["labels":specs.count,"cached_texts":measurements.count,"annotation_artwork_ms":artworkMilliseconds,"elapsed_ms":LabLog.milliseconds(LabLog.now-start)]) }
        guard anchors.isEmpty, worker == nil else { return }
        let groups = specs.map { ($0.id,$0.features) }
        let task = Task.detached(priority:.userInitiated) {
            var result: [String:GroupAnchors] = [:]
            for (id, features) in groups {
                if Task.isCancelled { break }
                result[id] = scene.labelCache.anchors(in:scene,features:features,editorial:editorial)
            }
            return result
        }
        worker = Task { [weak self] in
            let result = await withTaskCancellationHandler { await task.value } onCancel: { task.cancel() }
            guard !Task.isCancelled, let self, self.cache === scene.labelCache, self.editorial == editorial else { return }
            self.anchors = result
            self.worker = nil
            LabLog.record("geokit_anchors_ready",["groups":result.count,"elapsed_ms":LabLog.milliseconds(LabLog.now-start)])
            ready()
        }
    }

    func position(in scene: AlbersScene, camera: GeographicCamera, zoom: CGFloat, selected: Int?,
                  externalAnnotations: CGRect?, policy: CartographicPolicy) {
        guard !anchors.isEmpty else { return }
        let start = LabLog.now
        let keyVisible = camera.region == nil && zoom < 2 && specs.contains(where: \.external)
        let keyTop = externalAnnotations.map { $0.minY + 2 } ?? min(camera.viewport.maxY-24, max(camera.viewport.minY,
            camera.screen(CGPoint(x:scene.bounds.midX,y:scene.bounds.maxY)).y+5))
        let obstacles = keyVisible && externalAnnotations == nil
            ? [CGRect(x:camera.viewport.minX,y:keyTop,width:camera.viewport.width,height:21)] : []
        let decisions = LabelResolver.resolve(specs, anchors:anchors,sizes:sizes,scene:scene,camera:camera,zoom:zoom,selected:selected,obstacles:obstacles,previous:previous,policy:policy)
        if diagnostics { diagnosticDecisions = decisions }
        let external = Set(decisions.filter { $0.mode == .tile }.map(\.id))
        if !editorial { parent?.isAccessibilityElement = external.isEmpty }
        let order = ElectoralCartography.keyOrder.filter(external.contains)
        let keyWidth = externalAnnotations?.width ?? camera.viewport.width
        let w = min(33,max(0,keyWidth-8)/CGFloat(max(1,order.count)))
        for d in decisions {
            guard let label = labels[d.id], let spec = byID[d.id] else { continue }
            let tile = tiles[d.id]
            tile?.isHidden = d.mode != .tile
            tile?.accessibilityElementsHidden = d.mode != .tile
            tile?.isAccessibilityElement = d.mode == .tile
            if let index = order.firstIndex(of:d.id) {
                tile?.frame = CGRect(x:camera.viewport.midX-w*CGFloat(order.count)/2+CGFloat(index)*w,y:keyTop,width:w-1,height:20)
                tile?.layer.borderWidth = selected.map(spec.features.contains) == true ? 2 : 0
                tile?.layer.borderColor = UIColor.systemOrange.cgColor
                tile?.accessibilityTraits = selected.map(spec.features.contains) == true ? [.button,.selected] : [.button]
            }
            label.isHidden = d.mode != .full && d.mode != .compact
            if !label.isHidden {
                let text = spec.text(for: d.arrangement, compact: d.mode == .compact)
                label.numberOfLines = d.arrangement == .inline ? 1 : 2
                if paintedText[d.id] != text {
                    if editorial {
                        var attrs = attributes(for: label.font)
                        attrs[.foregroundColor] = label.textColor
                        label.attributedText = NSAttributedString(string: text, attributes: attrs)
                    } else {
                        label.text = text
                    }
                    paintedText[d.id] = text
                }
                label.frame = d.frame
            }
            if let index = d.candidate { previous[d.id] = index }
        }
        let measure = LabLog.enabled && (!logged || measureLayout)
        let artworkStart = measure ? LabLog.now : 0
        if editorial {
            layoutEditorial(decisions, anchors: anchors, scene: scene, camera: camera, zoom: zoom,
                            selection: selected, region: externalAnnotations, policy: policy)
        } else {
            leaders.removeAll()
            parent?.accessibilityElements = nil
            accessibleOrder.removeAll()
        }
        if measure {
            LabLog.record("geokit_label_layout",["labels":decisions.count,"external":external.count,
                "separate_external_region": externalAnnotations != nil,
                "internal":decisions.filter { $0.mode == .full || $0.mode == .compact }.count,
                "annotation_ms": LabLog.milliseconds(LabLog.now-artworkStart),
                "elapsed_ms":LabLog.milliseconds(LabLog.now-start)])
            logged = true
        }
    }
    private func layoutEditorial(_ decisions: [LabelPlacement], anchors: [String: GroupAnchors], scene: AlbersScene,
                                 camera: GeographicCamera, zoom: CGFloat, selection: Int?, region: CGRect?,
                                 policy: CartographicPolicy) {
        var accessible: [ElectoralAnnotationView] = []
        diagnosticAnnotationFrames.removeAll(keepingCapacity: true)
        leaders.removeAll(keepingCapacity: true)
        separator = nil
        let visible = policy == .nationalEditorial && camera.region == nil
        if visible, let region {
            let layout = ElectoralEditorialLayout.resolve(decisions, specs: byID, artworkSpecs: artworkSpecs,
                allocations: allocations, anchors: anchors, callouts: calloutGeometry, scene: scene, camera: camera,
                region: region, zoom: zoom)
            separator = layout.separator
            for item in layout.artwork {
                guard let view = artworkViews[item.id] else { continue }
                update(view, id: item.id, frame: item.frame, selection: selection)
                labels[item.id]?.isHidden = true
                accessible.append(view)
                if let leader = item.leader { leaders.append(leader) }
            }
            for item in layout.tiles {
                placeTile(item.id, frame: item.frame, selection: selection, using: &accessible)
            }
            if diagnostics || (LabLog.enabled && !logged) {
                LabLog.record("annotation_layout", ["artwork": layout.artwork.map(\.id),
                    "tiles": layout.tiles.map(\.id), "leaders": leaders.count,
                    "viewport_width": camera.viewport.width, "viewport_height": camera.viewport.height])
            }
        }
        let order = accessible.map(ObjectIdentifier.init)
        if order != accessibleOrder {
            let shown = Set(order)
            for group in [editorialTiles.values, artworkViews.values] {
                for view in group {
                    let visible = shown.contains(ObjectIdentifier(view))
                    view.isHidden = !visible
                    view.isAccessibilityElement = visible
                    view.accessibilityElementsHidden = !visible
                }
            }
            parent?.accessibilityElements = accessible.isEmpty ? nil : accessible
            accessibleOrder = order
        }
        if parent?.isAccessibilityElement != accessible.isEmpty {
            parent?.isAccessibilityElement = accessible.isEmpty
        }
        if diagnostics, let region {
            diagnosticAnnotationFrames = [region] + accessible.map(\.frame)
        }
    }

    private func placeTile(_ id: String, frame: CGRect, selection: Int?, using views: inout [ElectoralAnnotationView]) {
        guard let tile = editorialTiles[id] else { return }
        update(tile, id: id, frame: frame, selection: selection)
        views.append(tile)
    }

    private func update(_ view: ElectoralAnnotationView, id: String, frame: CGRect, selection: Int?) {
        if view.frame != frame { view.frame = frame }
        let isSelected = selection.map { byID[id]?.features.contains($0) == true } ?? false
        if view.isSelected != isSelected { view.isSelected = isSelected }
    }

    func drawLeaders(in context: CGContext) {
        context.saveGState()
        defer { context.restoreGState() }
        context.setStrokeColor(UIColor(red: 0.31, green: 0.29, blue: 0.24, alpha: 0.64).cgColor)
        context.setLineWidth(0.65)
        if let separator {
            context.setFillColor(UIColor(red: 0.31, green: 0.29, blue: 0.24, alpha: 0.25).cgColor)
            context.fill(separator)
        }
        for leader in leaders {
            context.move(to: leader.origin)
            context.addLine(to: leader.end)
            context.strokePath()
        }
    }

    func drawDiagnostics(in context: CGContext, scene: AlbersScene, camera: GeographicCamera) {
        context.saveGState()
        defer { context.restoreGState() }
        context.setStrokeColor(UIColor.systemBlue.cgColor)
        for frame in diagnosticAnnotationFrames { context.stroke(frame.insetBy(dx: 0.5, dy: 0.5)) }
        context.clip(to: camera.viewport)
        context.setFillColor(UIColor.systemBlue.cgColor)
        for leader in leaders {
            context.fillEllipse(in: CGRect(x: leader.origin.x - 2, y: leader.origin.y - 2, width: 4, height: 4))
        }
        for decision in diagnosticDecisions {
            if let candidates = anchors[decision.id]?.candidates {
                for (index, anchor) in candidates.prefix(7).enumerated() {
                    guard camera.region == nil || camera.region == anchor.region,
                          let panel = scene.panels.first(where: { $0.region == anchor.region }) else { continue }
                    let point = camera.screen(panel.world(anchor.point))
                    let accepted = index == decision.candidate
                    context.setFillColor((accepted ? UIColor.systemGreen : UIColor.systemOrange).cgColor)
                    context.fillEllipse(in: CGRect(x: point.x - 1.5, y: point.y - 1.5, width: 3, height: 3))
                    if accepted, let envelope = anchor.envelopes.dropFirst(2).first {
                        let scale = camera.scale * panel.scale
                        let w = envelope.width * scale, h = envelope.height * scale
                        context.setStrokeColor(UIColor.systemPurple.cgColor)
                        context.stroke(CGRect(x: point.x - w/2, y: point.y - h/2, width: w, height: h))
                    }
                }
            }
            if !decision.frame.isNull {
                context.setStrokeColor(UIColor.systemGreen.cgColor)
                context.stroke(decision.frame)
            }
        }
        let text = diagnosticDecisions.filter { $0.mode == .suppressed }.prefix(12)
            .map { "\($0.id) \($0.reason)" }.joined(separator: "\n")
        let attributes: [NSAttributedString.Key: Any] = [.font: UIFont.monospacedSystemFont(ofSize: 8, weight: .medium),
                                                         .foregroundColor: UIColor.black, .backgroundColor: UIColor.white]
        (text as NSString).draw(at: CGPoint(x: camera.viewport.minX + 4, y: camera.viewport.minY + 4), withAttributes: attributes)
    }

}
