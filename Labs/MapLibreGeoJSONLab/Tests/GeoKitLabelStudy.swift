import AppKit
import Foundation

@main
struct GeoKitLabelStudy {
    /// Optional local review image with the first interior candidate and eligible alternatives.
    static func renderAnchors(_ scene: AlbersScene, groups: [String:GroupAnchors], to file: String) throws {
        let w=1050, h=660
        let ctx=CGContext(data:nil,width:w,height:h,bitsPerComponent:8,bytesPerRow:w*4,
                          space:CGColorSpaceCreateDeviceRGB(),bitmapInfo:CGImageAlphaInfo.noneSkipLast.rawValue)!
        ctx.translateBy(x:0,y:CGFloat(h))
        ctx.scaleBy(x:1,y:-1)
        let viewport=CGRect(x:0,y:0,width:w,height:h)
        let cam=GeographicCamera(viewport:viewport,center:CGPoint(x:scene.bounds.midX,y:scene.bounds.midY),scale:GeographicCamera.fit(scene.bounds,in:viewport))
        ctx.setFillColor(CGColor(gray:0.98,alpha:1));ctx.fill(viewport)
        for panel in scene.panels {
            ctx.saveGState()
            let origin=cam.screen(panel.world(CGPoint.zero))
            ctx.translateBy(x:origin.x,y:origin.y);ctx.scaleBy(x:cam.scale*panel.scale,y:cam.scale*panel.scale)
            ctx.setLineWidth(0.65/(cam.scale*panel.scale));ctx.setStrokeColor(CGColor(gray:1,alpha:1))
            ctx.setFillColor(CGColor(red:0.68,green:0.77,blue:0.81,alpha:1))
            for part in panel.parts { GeographicPathDrawing.fillAndStroke(part,in:ctx) }
            ctx.restoreGState()
        }
        for id in groups.keys.sorted() {
            for (i,a) in groups[id]!.candidates.prefix(5).enumerated().reversed() {
                let panel=scene.panels.first { $0.region == a.region }!
                let point=cam.screen(panel.world(a.point)), r:CGFloat = i==0 ? 2.4 : 1.3
                ctx.setFillColor(i==0 ? CGColor(gray:0.11,alpha:1) : CGColor(red:0.75,green:0.32,blue:0.15,alpha:1))
                ctx.fillEllipse(in:CGRect(x:point.x-r,y:point.y-r,width:2*r,height:2*r))
            }
        }
        try NSBitmapImageRep(cgImage:ctx.makeImage()!).representation(using:.png,properties:[:])!.write(to:URL(fileURLWithPath:file))
    }

    static func main() throws {
        let args = CommandLine.arguments
        guard (3...4).contains(args.count) else { throw PolygonSource.Invalid(message:"Supply historical state archive and normalized national JSON.") }
        let archive = try HistoricalArchive(url:URL(fileURLWithPath:args[1]))
        let data = try Data(contentsOf:URL(fileURLWithPath:args[2]))
        let national = try JSONSerialization.jsonObject(with:data) as! [String:Any]
        let elections = national["elections"] as! [[String:Any]]
        var lastDate = "", previousScene: AlbersScene?
        let font = NSFont.monospacedDigitSystemFont(ofSize:8,weight:.medium)
        func measure(_ text: String) -> CGSize {
            let r = (text as NSString).boundingRect(with:CGSize(width:500,height:100),options:[.usesLineFragmentOrigin,.usesFontLeading],attributes:[.font:font])
            return CGSize(width:ceil(r.width)+2,height:ceil(r.height)+1)
        }
        let clock = { ProcessInfo.processInfo.systemUptime }
        for election in elections {
            let year = election["year"] as! Int
            let date = (election["geography"] as! [String:Any])["snapshotDate"] as! String
            if previousScene == nil || date != lastDate {
                let snap = try archive.snapshot(on: HistoricalDay(date))
                let collection = try JSONSerialization.jsonObject(with:snap.data) as! [String:Any]
                previousScene = try AlbersScene.project(collection["features"] as! [[String:Any]]).composed(.thematic)
                lastDate = date
            }
            let scene = previousScene!
            let rows = election["allocations"] as! [[String:Any]]
            let ids = Set((election["results"] as! [[String:Any]]).compactMap { $0["candidateID"] as? String })
            let groups = Dictionary(grouping:rows.filter { $0["geometryKey"] is String }) { $0["geometryKey"] as! String }
            var votes:[String:Int] = [:], statuses:[String:String] = [:]
            for (key, group) in groups {
                let positive = group.filter { ($0["electoralVotes"] as? Int ?? 0) > 0 }
                let unresolved = group.contains { $0["status"] as? String == "unresolved" } || positive.contains { !ids.contains($0["candidateID"] as? String ?? "") }
                statuses[key] = unresolved ? "Unresolved allocation" : positive.isEmpty ? "Nonparticipating" : Set(positive.compactMap { $0["candidateID"] as? String }).count > 1 ? "Split electoral allocation" : "Reported electoral allocation"
                if !unresolved && !positive.isEmpty { votes[key] = positive.reduce(0) { $0+($1["electoralVotes"] as! Int) } }
            }
            let specs = ElectoralCartography.specifications(scene:scene,votes:votes,statuses:statuses)
            let before = scene.labelCache.count
            let t = clock()
            let anchors = Dictionary(uniqueKeysWithValues:specs.map { ($0.id,scene.labelCache.anchors(in:scene,features:$0.features)) })
            let anchorMS = (clock()-t)*1000, m = clock()
            let sizes = Dictionary(uniqueKeysWithValues:specs.map { ($0.id,LabelMeasurements(full:measure([$0.primary,$0.secondary].filter { !$0.isEmpty }.joined(separator:"\n")),compact:measure($0.primary))) })
            let measureMS = (clock()-m)*1000
            if year == 1976 && args.count == 4 { try renderAnchors(scene,groups:anchors,to:args[3]) }
            for (width,height) in [(350,450),(370,380),(850,620),(680,135)] {
                let vp = CGRect(x:0,y:0,width:width,height:height)
                let camera = GeographicCamera(viewport:vp,center:CGPoint(x:scene.bounds.midX,y:scene.bounds.midY),scale:GeographicCamera.fit(scene.bounds,in:vp))
                let keyTop = min(vp.maxY-24,max(vp.minY,camera.screen(CGPoint(x:scene.bounds.midX,y:scene.bounds.maxY)).y+5))
                let obstacles = specs.contains(where: \.external) ? [CGRect(x:0,y:keyTop,width:vp.width,height:21)] : []
                let l = clock()
                let placed = LabelResolver.resolve(specs,anchors:anchors,sizes:sizes,scene:scene,camera:camera,zoom:1,obstacles:obstacles)
                let elapsed = (clock()-l)*1000
                var outside=0, collisions=0
                let inside = placed.filter { $0.mode == .full || $0.mode == .compact }
                for (i, placement) in inside.enumerated() {
                    let candidate = anchors[placement.id]!.candidates[placement.candidate!]
                    let panel = scene.panels.first { $0.region == candidate.region }!
                    let rect = placement.frame
                    let points = [rect.minX,rect.midX,rect.maxX].flatMap { x in [rect.minY,rect.midY,rect.maxY].map { CGPoint(x:x,y:$0) } }
                    if !points.allSatisfy({ point in
                        let local = panel.local(camera.world(point))
                        return panel.parts.contains { $0.feature == candidate.feature && $0.bounds.contains(local) && $0.path.contains(local,using:.evenOdd) }
                    }) { outside += 1 }
                    collisions += inside.dropFirst(i+1).filter { $0.frame.intersects(rect) }.count
                }
                let reasons = Dictionary(grouping:placed.filter { $0.mode == .suppressed },by:\.reason).mapValues(\.count)
                let record:[String:Any] = ["year":year,"revision":date,"viewport":[width,height],"requested":specs.count,"valid":anchors.values.filter { !$0.candidates.isEmpty }.count,
                    "legacy_valid":anchors.values.filter(\.averageInside).count,"centroid_valid":anchors.values.filter(\.centroidInside).count,"baseline_valid":anchors.values.filter(\.baselineInside).count,
                    "full":placed.filter { $0.mode == .full }.count,"compact":placed.filter { $0.mode == .compact }.count,"external":placed.filter { $0.mode == .tile }.count,"suppressed":reasons.values.reduce(0,+),"reasons":reasons,
                    "overlaps":collisions,"outside":outside,"overrides":0,"anchor_ms":anchorMS,"new_groups":scene.labelCache.count-before,"measurement_ms":measureMS,"resolve_ms":elapsed]
                print(String(decoding:try JSONSerialization.data(withJSONObject:record,options:[.sortedKeys]),as:UTF8.self))
                precondition(outside==0 && collisions==0 && placed.count==specs.count)
            }
        }
    }
}
