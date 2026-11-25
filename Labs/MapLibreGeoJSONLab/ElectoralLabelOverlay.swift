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
    private var sizes: [String:LabelMeasurements] = [:]
    private var anchors: [String:GroupAnchors] = [:]
    private var previous: [String:Int] = [:]
    private var cache: GeographicAnchorCache?
    private var worker: Task<Void,Never>?
    private var select: ((Int) -> Void)?
    private weak var parent: UIView?
    private var logged = false

    private func measure(_ text: String) -> CGSize {
        if let size = measurements[text] { return size }
        let rect = (text as NSString).boundingRect(with: CGSize(width: 500,height: 100),
            options: [.usesLineFragmentOrigin,.usesFontLeading], attributes: [.font:font], context:nil)
        let size = CGSize(width:ceil(rect.width)+2,height:ceil(rect.height)+1)
        measurements[text] = size
        return size
    }

    private func ink(_ fill: UIColor) -> UIColor {
        var r: CGFloat = 0, g: CGFloat = 0, b: CGFloat = 0, a: CGFloat = 0
        guard fill.getRed(&r,green:&g,blue:&b,alpha:&a) else { return .label }
        func linear(_ v: CGFloat) -> CGFloat { v <= 0.04045 ? v/12.92 : pow((v+0.055)/1.055,2.4) }
        return 0.2126*linear(r)+0.7152*linear(g)+0.0722*linear(b) > 0.179 ? .black : .white
    }

    func update(parent: UIView, scene: AlbersScene?, votes: [String:Int]?, statuses: [String:String],
                colors: [[CGColor]]?, select: @escaping (Int)->Void, ready: @escaping ()->Void) {
        let start = LabLog.now
        self.select = select
        self.parent = parent
        parent.isAccessibilityElement = true
        specs = scene.flatMap { geo in votes.map { ElectoralCartography.specifications(scene:geo,votes:$0,statuses:statuses) } } ?? []
        byID = Dictionary(uniqueKeysWithValues: specs.map { ($0.id,$0) })
        let ids = Set(byID.keys)
        for id in labels.keys.filter({ !ids.contains($0) }) { labels.removeValue(forKey:id)?.removeFromSuperview() }
        for id in tiles.keys.filter({ !ids.contains($0) }) { tiles.removeValue(forKey:id)?.removeFromSuperview() }
        if cache !== scene?.labelCache || specs.isEmpty {
            worker?.cancel()
            worker = nil
            cache = scene?.labelCache
            labels.values.forEach { $0.isHidden = true }
            tiles.values.forEach { $0.isHidden = true }
            anchors.removeAll()
            previous.removeAll()
        }
        guard let scene, !specs.isEmpty else { return }
        sizes.removeAll(keepingCapacity:true)
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
            view.textColor = ink(fill)
            sizes[spec.id] = .init(full:measure([spec.primary,spec.secondary].filter { !$0.isEmpty }.joined(separator:"\n")), compact:measure(spec.primary))
            if spec.external {
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
                button.setTitleColor(ink(fill),for:.normal)
                button.setTitle(spec.primary+" "+(spec.secondary.isEmpty ? "?" : spec.secondary)+(paints.count > 1 ? " ◧" : ""),for:.normal)
                button.accessibilityLabel = spec.accessibilityText
            } else { tiles[spec.id]?.isHidden = true }
        }
        logged = false
        if LabLog.enabled { LabLog.record("geokit_label_content",["labels":specs.count,"cached_texts":measurements.count,"elapsed_ms":LabLog.milliseconds(LabLog.now-start)]) }
        guard anchors.isEmpty, worker == nil else { return }
        let groups = specs.map { ($0.id,$0.features) }
        let task = Task.detached(priority:.userInitiated) {
            var result: [String:GroupAnchors] = [:]
            for (id, features) in groups {
                if Task.isCancelled { break }
                result[id] = scene.labelCache.anchors(in:scene,features:features)
            }
            return result
        }
        worker = Task { [weak self] in
            let result = await withTaskCancellationHandler { await task.value } onCancel: { task.cancel() }
            guard !Task.isCancelled, let self, self.cache === scene.labelCache else { return }
            self.anchors = result
            self.worker = nil
            LabLog.record("geokit_anchors_ready",["groups":result.count,"elapsed_ms":LabLog.milliseconds(LabLog.now-start)])
            ready()
        }
    }

    func position(in scene: AlbersScene, camera: GeographicCamera, zoom: CGFloat, selected: Int?) {
        guard !anchors.isEmpty else { return }
        let start = LabLog.now
        let keyVisible = camera.region == nil && zoom < 2 && specs.contains(where: \.external)
        let obstacles = keyVisible ? [CGRect(x:camera.viewport.minX,y:camera.viewport.maxY-25,width:camera.viewport.width,height:25)] : []
        let decisions = LabelResolver.resolve(specs, anchors:anchors,sizes:sizes,scene:scene,camera:camera,zoom:zoom,selected:selected,obstacles:obstacles,previous:previous)
        let external = Set(decisions.filter { $0.mode == .tile }.map(\.id))
        parent?.isAccessibilityElement = external.isEmpty
        let order = ElectoralCartography.keyOrder.filter(external.contains)
        let w = min(33,(camera.viewport.width-8)/CGFloat(max(1,order.count)))
        for d in decisions {
            guard let label = labels[d.id], let spec = byID[d.id] else { continue }
            let tile = tiles[d.id]
            tile?.isHidden = d.mode != .tile
            tile?.accessibilityElementsHidden = d.mode != .tile
            tile?.isAccessibilityElement = d.mode == .tile
            if let index = order.firstIndex(of:d.id) {
                tile?.frame = CGRect(x:camera.viewport.midX-w*CGFloat(order.count)/2+CGFloat(index)*w,y:camera.viewport.maxY-24,width:w-1,height:20)
                tile?.layer.borderWidth = selected.map(spec.features.contains) == true ? 2 : 0
                tile?.layer.borderColor = UIColor.systemOrange.cgColor
                tile?.accessibilityTraits = selected.map(spec.features.contains) == true ? [.button,.selected] : [.button]
            }
            label.isHidden = d.mode != .full && d.mode != .compact
            if !label.isHidden {
                let text = d.mode == .full && !spec.secondary.isEmpty ? spec.primary+"\n"+spec.secondary : spec.primary
                if label.text != text { label.text = text }
                label.frame = d.frame
            }
            if let index = d.candidate { previous[d.id] = index }
        }
        if LabLog.enabled && (!logged || ProcessInfo.processInfo.arguments.contains("--label-metrics")) {
            LabLog.record("geokit_label_layout",["labels":decisions.count,"external":external.count,
                "internal":decisions.filter { $0.mode == .full || $0.mode == .compact }.count,
                "elapsed_ms":LabLog.milliseconds(LabLog.now-start)])
            logged = true
        }
    }
}
