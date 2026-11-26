import AppKit
import BalloticsGeoKit
import BalloticsGeoKitApple

final class DistrictLabels: NSView {
    var drawing: (() -> Void)?
    override var isFlipped: Bool { true }
    override func hitTest(_ point: NSPoint) -> NSView? { nil }
    override func draw(_ dirtyRect: NSRect) { drawing?() }
}

final class DistrictMap: NSView {
    // Postal abbreviations from the companion 2024 Census state file.
    static let abbreviations = [
        "01":"AL", "02":"AK", "04":"AZ", "05":"AR", "06":"CA", "08":"CO", "09":"CT",
        "10":"DE", "11":"DC", "12":"FL", "13":"GA", "15":"HI", "16":"ID", "17":"IL",
        "18":"IN", "19":"IA", "20":"KS", "21":"KY", "22":"LA", "23":"ME", "24":"MD",
        "25":"MA", "26":"MI", "27":"MN", "28":"MS", "29":"MO", "30":"MT", "31":"NE",
        "32":"NV", "33":"NH", "34":"NJ", "35":"NM", "36":"NY", "37":"NC", "38":"ND",
        "39":"OH", "40":"OK", "41":"OR", "42":"PA", "44":"RI", "45":"SC", "46":"SD",
        "47":"TN", "48":"TX", "49":"UT", "50":"VT", "51":"VA", "53":"WA", "54":"WV",
        "55":"WI", "56":"WY", "60":"AS", "66":"GU", "69":"MP", "72":"PR", "78":"VI",
    ]
    private(set) var scene: AlbersScene
    let records: GeographicRecords
    let revision = GeographicRevision(source:"Census congressional districts",version:"2024-CD119-5m")
    var selected: Int?
    var focus: AlbersRegion?
    var target: CGRect
    var showLabels = false { didSet { labels.needsDisplay = true } }
    var rendererChanged: ((String) -> Void)?
    var firstMetalFrame: (() -> Void)?
    var diagnostics: ((GeographicRenderDiagnostic) -> Void)?
    private let labels = DistrictLabels()
    private let configuration: AlbersMetalConfiguration
    private let palette: [[CGColor]]
    private let contextLayer: GeographicContextLayer?
    private var presentation: CartographicPresentation?
    private var nationalPolicy: CartographicPolicy = .nationalEditorial
    private var requestedMetal = true
    private var metal: AlbersMetalSurface?
    private var preparation: Task<Void, Never>?
    private var metalReady = false
    var selectionChanged: ((String)->Void)?
    override var isFlipped: Bool { true }
    init(url: URL, contextURL: URL?, configuration: AlbersMetalConfiguration = .init()) throws {
        contextLayer = try contextURL.map { url in
            GeographicContextLayer(scene: try AlbersScene.load(url), provenance: "2024 Census 5m state boundaries")
        }
        self.configuration = configuration
        let raw = try PolygonSource.readFeatures(url)
        let ids = raw.compactMap { ($0["properties"] as? [String:Any])?["GEOID"] as? String }
        guard raw.count == 441, Set(ids).count == raw.count,
              raw.allSatisfy({ row in
                  guard let f = row["properties"] as? [String:Any], let id = f["GEOID"] as? String,
                        let state = f["STATEFP"] as? String, let district = f["CD119FP"] as? String else { return false }
                  return id == state+district && id.count == 4 && f["GEOIDFQ"] as? String == "5001900US"+id
              }) else { throw PolygonSource.Invalid(message:"Expected the 441 unique 119th Congress source records.") }
        scene = try AlbersScene.project(raw).composed(.thematic)
        records = try GeographicRecords(scene:scene,revision:revision) { $0["GEOID"] }
        target = scene.bounds
        let hexColors: [Int] = [0x54859B, 0xA7B695, 0x93BCC3, 0xD4B791, 0x839BA6]
        let colors = hexColors.map { color in
            let red = CGFloat((color >> 16) & 255) / 255
            let green = CGFloat((color >> 8) & 255) / 255
            let blue = CGFloat(color & 255) / 255
            return CGColor(red: red, green: green, blue: blue, alpha: 1)
        }
        palette = records.ids.map { [colors[(Int($0.record) ?? 0) % colors.count]] }
        super.init(frame:CGRect(x:0,y:0,width:900,height:560))
        labels.drawing = { [weak self] in self?.drawLabels() }
        addSubview(labels)
        setAccessibilityLabel("Albers congressional district map")
    }
    required init?(coder:NSCoder) { nil }
    var camera: GeographicCamera {
        GeographicCamera(viewport:bounds,center:CGPoint(x:target.midX,y:target.midY),scale:GeographicCamera.fit(target,in:bounds),region:focus)
    }
    private var policy: CartographicPolicy { presentation?.policy ?? nationalPolicy }

    func setNationalPolicy(editorial: Bool) {
        nationalPolicy = editorial ? .nationalEditorial : .nationalExploratory
        apply(nationalPolicy)
    }

    private func apply(_ policy: CartographicPolicy) {
        guard let updated = CartographicPresentation(policy: policy, scene: scene, groupKey: "STATEFP") else { return }
        presentation = updated
        focus = updated.region
        target = updated.frame
        refresh()
    }

    @discardableResult
    func focusState(_ code: String) -> Bool {
        guard scene.properties.contains(where: { $0["STATEFP"] == code }) else { return false }
        if let selected, scene.properties[selected]["STATEFP"] != code {
            select(nil, frame: false)
        }
        apply(.stateFocus(group: code))
        selectionChanged?(selectionDescription)
        return true
    }

    var selectionDescription: String {
        guard let selected else {
            if let group = presentation?.group {
                let name = Self.abbreviations[group] ?? group
                return "\(name) statewide · \(presentation?.visibleFeatures?.count ?? 0) districts"
            }
            return "Select a district"
        }
        return (scene.properties[selected]["NAMELSAD"] ?? "District") + " · GEOID " + records.ids[selected].record
    }

    func select(_ index: Int?, frame: Bool) {
        selected = index
        selectionChanged?(selectionDescription)
        if let index, frame {
            let group = presentation?.group == scene.properties[index]["STATEFP"] ? presentation?.group : nil
            apply(.localInspection(feature: index, within: group))
        }
        refresh()
    }
    func search(_ query:String) -> Bool {
        let q = query.trimmingCharacters(in:.whitespacesAndNewlines).uppercased()
        if let code = Self.abbreviations.first(where: { $0.value == q })?.key { return focusState(code) }
        let index = records.index(of:.init(revision:revision,record:q)) ?? scene.properties.firstIndex {
            guard let code = Self.abbreviations[$0["STATEFP"] ?? ""], let cd = $0["CD119FP"] else { return false }
            return code+"-"+cd == q
        }
        guard let index else { return false }; select(index,frame:true); return true
    }
    func frameRegion(_ region:AlbersRegion?) {
        guard let region else { apply(nationalPolicy); return }
        presentation = nil
        focus = region
        target = scene.panels.first { $0.region == region }?.frame ?? scene.bounds
        refresh()
    }
    override func magnify(with event: NSEvent) { zoom(by: 1 / max(0.1, 1 + event.magnification)) }

    private func zoom(by factor: CGFloat) {
        let next = target.insetBy(dx: target.width * (1 - factor) / 2, dy: target.height * (1 - factor) / 2)
        if next.width > 250 && next.width < scene.bounds.width * 2 { target = next }
        refresh()
    }

    override func mouseUp(with event:NSEvent) {
        let point = camera.world(convert(event.locationInWindow, from: nil))
        let hits = presentation?.hits(in: scene, at: point) ?? scene.hits(point, region: focus)
        select(hits.first, frame: event.clickCount == 2)
    }
    override func scrollWheel(with event:NSEvent) {
        let c = camera
        if event.modifierFlags.contains(.option) {
            let factor = exp(max(-0.3,min(0.3,event.scrollingDeltaY/70)))
            zoom(by: factor)
            return
        } else { target = target.offsetBy(dx:-event.scrollingDeltaX/c.scale,dy:-event.scrollingDeltaY/c.scale) }
        refresh()
    }
    func useMetal(_ requested: Bool) {
        requestedMetal = requested
        if requested && window != nil {
            startMetal()
        } else {
            stopMetal()
            rendererChanged?("Core Graphics")
        }
    }

    override func viewDidMoveToWindow() {
        super.viewDidMoveToWindow()
        if window == nil { stopMetal() }
        else if requestedMetal { startMetal() }
    }

    override func layout() {
        super.layout()
        metal?.frame = bounds
        labels.frame = bounds
        refresh()
    }

    private func startMetal() {
        guard metal == nil else { return }
        do {
            let surface = try AlbersMetalSurface(configuration: configuration, diagnostics: diagnostics)
            guard let device = surface.device else {
                throw GeographicRenderError(message: "Metal device became unavailable.")
            }
            metal = surface
            surface.frame = bounds
            surface.isHidden = true
            surface.onFailure = { [weak self] reason in self?.fallback(reason) }
            let samples = surface.sampleCount
            surface.onFirstFrame = { [weak self] in
                self?.rendererChanged?("Metal · \(samples)× samples")
                self?.firstMetalFrame?()
            }
            addSubview(surface, positioned: .below, relativeTo: labels)
            rendererChanged?("Preparing geographic mesh…")
            let scene = self.scene
            let revision = self.revision
            let records = self.records
            preparation = Task { [weak self, weak surface] in
                do {
                    let prepared = try await AlbersMetalPreparedScene.prepareAsync(scene, revision: revision,
                                                                                  records: records, device: device)
                    try Task.checkCancellation()
                    guard let self, let surface, self.metal === surface else { return }
                    try surface.publish(prepared, scene: self.scene, palette: self.palette,
                                        visibleFeatures: self.presentation?.visibleFeatures)
                    self.preparation = nil
                    self.metalReady = true
                    surface.isHidden = false
                    self.refresh()
                } catch {
                    guard !Task.isCancelled, let self, let surface, self.metal === surface else { return }
                    self.fallback(error.localizedDescription)
                }
            }
        } catch { fallback(error.localizedDescription) }
    }

    private func stopMetal() {
        preparation?.cancel()
        preparation = nil
        metalReady = false
        metal?.onFirstFrame = nil
        metal?.onFailure = nil
        metal?.clear()
        metal?.removeFromSuperview()
        metal = nil
        refresh()
    }

    private func fallback(_ reason: String) {
        stopMetal()
        rendererChanged?("Core Graphics fallback · \(reason)")
    }

    func setProfile(_ profile: NationalMapProfile) {
        scene = scene.composed(profile)
        if metalReady {
            do { try metal?.updateLayout(scene) }
            catch { fallback(error.localizedDescription) }
        }
        if let presentation { apply(presentation.policy) } else { frameRegion(focus) }
    }

    private func refresh() {
        metal?.setVisibleFeatures(presentation?.visibleFeatures)
        metal?.move(camera: camera, selected: selected)
        labels.needsDisplay = true
        needsDisplay = true
    }

    override func hitTest(_ point: NSPoint) -> NSView? {
        super.hitTest(point) == nil ? nil : self
    }

    override func draw(_ dirtyRect: NSRect) {
        guard !metalReady, let ctx = NSGraphicsContext.current?.cgContext else { return }
        ctx.setFillColor(CGColor(red: 0.93, green: 0.95, blue: 0.96, alpha: 1))
        ctx.fill(bounds)
        AlbersCoreGraphics.draw(scene, camera: camera, palette: palette, selected: selected,
                                visibleFeatures: presentation?.visibleFeatures, in: ctx)
    }

    private func drawLabels() {
        let c = camera
        if let group = presentation?.group, let ctx = NSGraphicsContext.current?.cgContext {
            contextLayer?.draw(in: ctx, camera: c, alignedTo: scene, excluding: group, groupKey: "STATEFP")
        }
        for panel in scene.panels where panel.showsFrame && (focus == nil || focus == panel.region) {
            guard let name = panel.region.insetLabel else { continue }
            let point = c.screen(panel.frame.origin)
            (name as NSString).draw(at: CGPoint(x: point.x + 6, y: point.y + 4),
                                     withAttributes: [.font: NSFont.systemFont(ofSize: 10, weight: .semibold),
                                                      .foregroundColor: NSColor.darkGray])
        }
        guard showLabels else { return }
        var visible = Set<Int>()
        for panel in scene.panels where focus == nil || focus == panel.region {
            for index in panel.visible(in: panel.local(c.worldBounds)) {
                let feature = panel.parts[index].feature
                if presentation?.visibleFeatures?.contains(feature) != false { visible.insert(feature) }
            }
        }
        if showLabels {
            let font=NSFont.systemFont(ofSize:10,weight:.medium)
            // Limit preparation to the visible detail workload; national scale offers search.
            let specs=visible.sorted().filter { feature in
                if policy == .nationalEditorial { return feature == selected || visible.count <= 24 }
                return visible.count <= 55 || feature == selected
            }.map { i in
                let f=scene.properties[i]
                return MapLabel(id:records.ids[i].record,features:[i],primary:(Self.abbreviations[f["STATEFP"] ?? ""] ?? "")+"-"+(f["CD119FP"] ?? ""))
            }
            let anchors=Dictionary(uniqueKeysWithValues:specs.map { ($0.id,scene.labelCache.anchors(in:scene,features:$0.features)) })
            let sizes=Dictionary(uniqueKeysWithValues:specs.map {
                let size=($0.primary as NSString).size(withAttributes:[.font:font])
                return ($0.id,LabelMeasurements(full:size,compact:size))
            })
            let placements=LabelResolver.resolve(specs,anchors:anchors,sizes:sizes,scene:scene,camera:c,zoom:c.scale/GeographicCamera.fit(scene.bounds,in:bounds),selected:selected,policy:policy)
            for p in placements where p.mode == .full {
                let text=specs.first { $0.id==p.id }!.primary
                (text as NSString).draw(in:p.frame,withAttributes:[.font:font,.foregroundColor:NSColor.black])
            }
        }
    }
    func verifyPolicyReuse() async throws {
        guard let metal, let identity = metal.meshIdentity else {
            throw GeographicRenderError(message: "DistrictAtlas has no active Metal mesh.")
        }
        let steps: [(String, () -> Bool)] = [
            ("national editorial", { self.setNationalPolicy(editorial: true); return true }),
            ("national exploratory", { self.setNationalPolicy(editorial: false); return true }),
            ("Texas state focus", { self.focusState("48") }),
            ("TX-18 local inspection", { self.search("TX-18") }),
            ("return to national", { self.frameRegion(nil); return true })
        ]
        for (name, apply) in steps {
            guard apply(), metal.meshIdentity == identity else {
                throw GeographicRenderError(message: "\(name) did not retain the active mesh.")
            }
            await withCheckedContinuation { continuation in
                metal.expectPresentation { continuation.resume() }
            }
            print("\(name): current GPU completion, same mesh, \(presentation?.visibleFeatures?.count ?? records.ids.count) eligible districts")
        }
    }

    func export(_ path:String) throws {
        guard let image=bitmapImageRepForCachingDisplay(in:bounds) else { throw PolygonSource.Invalid(message:"Could not create bitmap.") }
        cacheDisplay(in:bounds,to:image)
        try image.representation(using:.png,properties:[:])!.write(to:URL(fileURLWithPath:path))
    }
}

final class Controller: NSObject, NSSearchFieldDelegate {
    let map: DistrictMap
    let status=NSTextField(labelWithString:"Select a district")
    let rendererStatus = NSTextField(labelWithString: "Preparing geographic mesh…")
    let search=NSSearchField()
    init(_ map:DistrictMap) { self.map=map }
    @objc func searchDistrict() {
        if !map.search(search.stringValue) { status.stringValue="No matching district" }
    }
    @objc func region(_ sender:NSButton) {
        map.frameRegion(["Alaska":AlbersRegion.alaska,"Hawaii":.hawaii][sender.title])
    }
    @objc func labels(_ sender:NSButton) { map.showLabels = sender.state == .on }
    @objc func renderer(_ sender: NSPopUpButton) { map.useMetal(sender.indexOfSelectedItem == 0) }
    @objc func policy(_ sender: NSPopUpButton) { map.setNationalPolicy(editorial: sender.indexOfSelectedItem == 0) }
    @objc func profile(_ sender: NSPopUpButton) { map.setProfile(sender.indexOfSelectedItem == 0 ? .thematic : .research) }
    func run() {
        let window=NSWindow(contentRect:NSRect(x:0,y:0,width:900,height:650),styleMask:[.titled,.closable,.resizable],backing:.buffered,defer:false)
        window.title="119th Congress · Census districts"
        let stack=NSStackView();stack.orientation = .vertical;stack.alignment = .leading;stack.spacing=8
        let bar=NSStackView()
        search.placeholderString="TX or TX-18";search.target=self;search.action=#selector(searchDistrict)
        bar.addArrangedSubview(search);search.widthAnchor.constraint(equalToConstant:150).isActive=true
        for s in ["National","Alaska","Hawaii"] { bar.addArrangedSubview(NSButton(title:s,target:self,action:#selector(region(_:)))) }
        let detail = NSButton(checkboxWithTitle: "Labels", target: self, action: #selector(labels(_:)))
        detail.state = map.showLabels ? .on : .off
        bar.addArrangedSubview(detail)
        let backend = NSPopUpButton()
        backend.addItems(withTitles: ["Metal", "Core Graphics"])
        backend.target = self
        backend.action = #selector(renderer(_:))
        bar.addArrangedSubview(backend)
        let composition = NSPopUpButton()
        composition.addItems(withTitles: ["Thematic", "Equal Scale"])
        composition.target = self
        composition.action = #selector(profile(_:))
        bar.addArrangedSubview(composition)
        let labelPolicy = NSPopUpButton()
        labelPolicy.addItems(withTitles: ["Editorial", "Exploratory"])
        labelPolicy.target = self
        labelPolicy.action = #selector(policy(_:))
        bar.addArrangedSubview(labelPolicy)
        for v in [bar,map,status,rendererStatus,NSTextField(labelWithString:"Synthetic categories · Three-region coverage · Option-scroll to zoom")] { stack.addArrangedSubview(v) }
        status.stringValue = map.selectionDescription
        map.selectionChanged = { [weak self] in self?.status.stringValue = $0 }
        map.rendererChanged = { [weak self] message in
            self?.rendererStatus.stringValue = message
            if (CommandLine.arguments.contains("--verify-metal") || CommandLine.arguments.contains("--verify-policy")), message.hasPrefix("Core Graphics fallback") {
                FileHandle.standardError.write(Data((message + "\n").utf8))
                exit(1)
            }
        }
        window.contentView=stack;map.widthAnchor.constraint(equalTo:stack.widthAnchor).isActive=true
        window.center()
        window.makeKeyAndOrderFront(nil)
        NSApp.activate(ignoringOtherApps: true)
        withExtendedLifetime(window) { NSApp.run() }
    }
}

do {
    let args=CommandLine.arguments
    guard args.count>=2 else { throw PolygonSource.Invalid(message:"DistrictAtlas GEOJSON [--render PNG] [--district GEOID] [--shader-library PATH] [--verify-metal]") }
    let app=NSApplication.shared
    let shaders: AlbersMetalConfiguration.Shaders
    if let i = args.firstIndex(of: "--shader-library"), args.indices.contains(i + 1) {
        shaders = .compiledFile(URL(fileURLWithPath: args[i + 1]))
    } else {
        shaders = .package
    }
    let url = URL(fileURLWithPath: args[1])
    let companion = url.deletingLastPathComponent().appendingPathComponent("cb_2024_us_state_5m.json")
    let context = FileManager.default.fileExists(atPath: companion.path) ? companion : nil
    let map = try DistrictMap(url: url, contextURL: context, configuration: .init(shaders: shaders))
    if let i = args.firstIndex(of: "--state"), args.indices.contains(i+1), !map.search(args[i+1]) {
        throw PolygonSource.Invalid(message: "Unknown state.")
    }
    if args.contains("--exploratory") { map.setNationalPolicy(editorial: false) }
    if args.contains("--verify-policy") {
        map.firstMetalFrame = { [weak map] in
            guard let map else { return }
            Task {
                do { try await map.verifyPolicyReuse(); exit(0) }
                catch {
                    FileHandle.standardError.write(Data((error.localizedDescription + "\n").utf8))
                    exit(1)
                }
            }
        }
        DispatchQueue.main.asyncAfter(deadline: .now() + 30) { exit(1) }
    }
    if args.contains("--verify-metal") {
        let cycles = 10
        var completed = 0
        map.diagnostics = { event in
            guard let data = try? JSONSerialization.data(withJSONObject: event.fields, options: [.sortedKeys]) else { return }
            print(event.kind.rawValue, String(decoding: data, as: UTF8.self))
        }
        map.firstMetalFrame = { [weak map] in
            guard let map else { return }
            completed += 1
            print("Metal GPU completion \(completed)/\(cycles) · \(map.records.ids.count) projected districts")
            DispatchQueue.main.async {
                if completed == cycles {
                    map.useMetal(false)
                    exit(0)
                }
                map.useMetal(false)
                map.useMetal(true)
            }
        }
        DispatchQueue.main.asyncAfter(deadline: .now() + 45) {
            FileHandle.standardError.write(Data("DistrictAtlas Metal verification timed out.\n".utf8))
            exit(1)
        }
    }
    if let i=args.firstIndex(of:"--district"), args.indices.contains(i+1) {
        guard map.search(args[i+1]) else { throw PolygonSource.Invalid(message:"Unknown district.") }
        map.showLabels=true
    }
    if let i=args.firstIndex(of:"--render"), args.indices.contains(i+1) {
        try map.export(args[i+1])
        print("441 source records; \(map.records.ids.count) projected; \(map.scene.panels.count) regions; selection \(map.selected.map { map.records.ids[$0].record } ?? "none")")
    } else { app.setActivationPolicy(.regular); Controller(map).run() }
} catch { FileHandle.standardError.write(Data((error.localizedDescription+"\n").utf8)); exit(1) }
