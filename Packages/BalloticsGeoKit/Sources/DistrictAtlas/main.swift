import AppKit
import BalloticsGeoKit

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
    let scene: AlbersScene
    let records: GeographicRecords
    let revision = GeographicRevision(source:"Census congressional districts",version:"2024-CD119-5m")
    var selected: Int?
    var focus: AlbersRegion?
    var target: CGRect
    var showLabels = false
    var selectionChanged: ((String)->Void)?
    override var isFlipped: Bool { true }
    init(url: URL) throws {
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
        super.init(frame:CGRect(x:0,y:0,width:900,height:560))
    }
    required init?(coder:NSCoder) { nil }
    var camera: GeographicCamera {
        GeographicCamera(viewport:bounds,center:CGPoint(x:target.midX,y:target.midY),scale:GeographicCamera.fit(target,in:bounds),region:focus)
    }
    func select(_ index: Int?, frame: Bool) {
        selected = index
        if let index {
            let f = scene.properties[index]
            selectionChanged?((f["NAMELSAD"] ?? "District")+" · GEOID "+records.ids[index].record)
            if frame {
                let box = scene.frame(for:index)
                target = box.insetBy(dx:-box.width*0.15,dy:-box.height*0.15)
                focus = scene.panels.first { $0.parts.contains { $0.feature == index } }?.region
            }
        }
        needsDisplay = true
    }
    func search(_ query:String) -> Bool {
        let q = query.trimmingCharacters(in:.whitespacesAndNewlines).uppercased()
        let index = records.index(of:.init(revision:revision,record:q)) ?? scene.properties.firstIndex {
            guard let code = Self.abbreviations[$0["STATEFP"] ?? ""], let cd = $0["CD119FP"] else { return false }
            return code+"-"+cd == q
        }
        guard let index else { return false }; select(index,frame:true); return true
    }
    func frameRegion(_ region:AlbersRegion?) {
        focus = region
        target = region.flatMap { r in scene.panels.first { $0.region==r }?.frame } ?? scene.bounds
        needsDisplay = true
    }
    override func mouseUp(with event:NSEvent) {
        let ids = records.hits(in:scene,at:camera.world(convert(event.locationInWindow,from:nil)),region:focus)
        select(ids.first.flatMap(records.index(of:)),frame:event.clickCount==2)
    }
    override func scrollWheel(with event:NSEvent) {
        let c = camera
        if event.modifierFlags.contains(.option) {
            let factor = exp(max(-0.3,min(0.3,event.scrollingDeltaY/70)))
            let next = target.insetBy(dx:target.width*(1-factor)/2,dy:target.height*(1-factor)/2)
            if next.width > 250 && next.width < scene.bounds.width*2 { target=next }
        } else { target = target.offsetBy(dx:-event.scrollingDeltaX/c.scale,dy:-event.scrollingDeltaY/c.scale) }
        needsDisplay=true
    }
    override func draw(_ dirtyRect:NSRect) {
        guard let ctx = NSGraphicsContext.current?.cgContext else { return }
        let c = camera
        ctx.setFillColor(CGColor(gray:0.97,alpha:1)); ctx.fill(bounds)
        let window = CGRect(origin:c.world(bounds.origin),size:CGSize(width:bounds.width/c.scale,height:bounds.height/c.scale))
        let palette=[0x54859B,0xA7B695,0x93BCC3,0xD4B791,0x839BA6]
        var visible = Set<Int>()
        for panel in scene.panels where focus == nil || focus == panel.region {
            ctx.saveGState()
            let origin=c.screen(panel.world(.zero as CGPoint))
            ctx.translateBy(x:origin.x,y:origin.y);ctx.scaleBy(x:c.scale*panel.scale,y:c.scale*panel.scale)
            ctx.setLineWidth(0.5/(c.scale*panel.scale));ctx.setStrokeColor(CGColor(gray:1,alpha:1))
            for i in panel.visible(in:panel.local(window)) {
                let part=panel.parts[i], key=records.ids[part.feature].record
                let color=selected==part.feature ? 0xD78135 : palette[(Int(key) ?? 0)%palette.count]
                ctx.setFillColor(CGColor(red:CGFloat(color>>16)/255,green:CGFloat((color>>8)&255)/255,blue:CGFloat(color&255)/255,alpha:1))
                GeographicPathDrawing.fillAndStroke(part,in:ctx); visible.insert(part.feature)
            }
            ctx.restoreGState()
        }
        if showLabels {
            let font=NSFont.systemFont(ofSize:10,weight:.medium)
            // Limit preparation to the visible detail workload; national scale offers search.
            let specs=visible.sorted().filter { visible.count <= 55 || $0==selected }.map { i in
                let f=scene.properties[i]
                return MapLabel(id:records.ids[i].record,features:[i],primary:(Self.abbreviations[f["STATEFP"] ?? ""] ?? "")+"-"+(f["CD119FP"] ?? ""))
            }
            let anchors=Dictionary(uniqueKeysWithValues:specs.map { ($0.id,scene.labelCache.anchors(in:scene,features:$0.features)) })
            let sizes=Dictionary(uniqueKeysWithValues:specs.map {
                let size=($0.primary as NSString).size(withAttributes:[.font:font])
                return ($0.id,LabelMeasurements(full:size,compact:size))
            })
            let placements=LabelResolver.resolve(specs,anchors:anchors,sizes:sizes,scene:scene,camera:c,zoom:c.scale/GeographicCamera.fit(scene.bounds,in:bounds))
            for p in placements where p.mode == .full {
                let text=specs.first { $0.id==p.id }!.primary
                (text as NSString).draw(in:p.frame,withAttributes:[.font:font,.foregroundColor:NSColor.black])
            }
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
    let search=NSSearchField()
    init(_ map:DistrictMap) { self.map=map }
    @objc func searchDistrict() {
        if !map.search(search.stringValue) { status.stringValue="No matching district" }
    }
    @objc func region(_ sender:NSButton) {
        map.frameRegion(["Alaska":AlbersRegion.alaska,"Hawaii":.hawaii][sender.title])
    }
    @objc func labels(_ sender:NSButton) { map.showLabels=sender.state == .on; map.needsDisplay=true }
    func run() {
        let window=NSWindow(contentRect:NSRect(x:0,y:0,width:900,height:650),styleMask:[.titled,.closable,.resizable],backing:.buffered,defer:false)
        window.title="119th Congress · Census districts"
        let stack=NSStackView();stack.orientation = .vertical;stack.alignment = .leading;stack.spacing=8
        let bar=NSStackView()
        search.placeholderString="District GEOID or TX-18";search.target=self;search.action=#selector(searchDistrict)
        bar.addArrangedSubview(search);search.widthAnchor.constraint(equalToConstant:200).isActive=true
        for s in ["National","Alaska","Hawaii"] { bar.addArrangedSubview(NSButton(title:s,target:self,action:#selector(region(_:)))) }
        bar.addArrangedSubview(NSButton(checkboxWithTitle:"Detail labels",target:self,action:#selector(labels(_:))))
        for v in [bar,map,status,NSTextField(labelWithString:"Synthetic categories · Three-region coverage · Option-scroll to zoom")] { stack.addArrangedSubview(v) }
        map.selectionChanged = { [weak self] in self?.status.stringValue=$0 }
        window.contentView=stack;map.widthAnchor.constraint(equalTo:stack.widthAnchor).isActive=true
        window.center();window.makeKeyAndOrderFront(nil);NSApp.activate(ignoringOtherApps:true);NSApp.run()
    }
}

do {
    let args=CommandLine.arguments
    guard args.count>=2 else { throw PolygonSource.Invalid(message:"DistrictAtlas GEOJSON [--render PNG] [--district GEOID]") }
    let app=NSApplication.shared
    let map=try DistrictMap(url:URL(fileURLWithPath:args[1]))
    if let i=args.firstIndex(of:"--district"), args.indices.contains(i+1) {
        guard map.search(args[i+1]) else { throw PolygonSource.Invalid(message:"Unknown district.") }
        map.showLabels=true
    }
    if let i=args.firstIndex(of:"--render"), args.indices.contains(i+1) {
        try map.export(args[i+1])
        print("441 source records; \(map.records.ids.count) projected; \(map.scene.panels.count) regions; selection \(map.selected.map { map.records.ids[$0].record } ?? "none")")
    } else { app.setActivationPolicy(.regular); Controller(map).run() }
} catch { FileHandle.standardError.write(Data((error.localizedDescription+"\n").utf8)); exit(1) }
