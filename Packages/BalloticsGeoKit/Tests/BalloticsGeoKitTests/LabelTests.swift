import XCTest
import CoreGraphics
@testable import BalloticsGeoKit

final class LabelTests: XCTestCase {
    let square = [[-101.0,34],[-96,34],[-96,39],[-101,39],[-101,34]]
    func feature(_ name:String, _ polygons:[[[[Double]]]]) -> PolygonSource.Object {
        ["type":"Feature","properties":["code":name],"geometry":["type":"MultiPolygon","coordinates":polygons]]
    }
    func testMembershipRevisionAndIsolation() throws {
        let second = [[-159.0,19],[-156,19],[-156,21],[-159,21],[-159,19]]
        let features = [feature("one",[[square],[second]]),feature("two",[[square]])]
        let canonical = try AlbersScene.project(features)
        let scene = canonical.composed(.thematic)
        XCTAssertTrue(scene.labelCache === canonical.labelCache)
        let rev = GeographicRevision(source:"fixture",version:"old")
        let records = try GeographicRecords(scene:scene,revision:rev) { $0["code"] }
        XCTAssertThrowsError(try GeographicRecords(scene:scene,revision:rev) { _ in "duplicate" })
        let p = scene.panels.first { $0.region == .mainland }!
        let inside = p.world(p.parts[0].interiorAnchor!)
        XCTAssertEqual(records.hits(in:scene,at:inside).map(\.record),["two","one"])
        XCTAssertEqual(scene.hits(inside,region:.hawaii),[])
        XCTAssertEqual(scene.labelCache.anchors(in:scene,features:[0]).candidates.map(\.region).contains(.hawaii),true)
        _ = canonical.labelCache.anchors(in:canonical,features:[0])
        XCTAssertEqual(scene.labelCache.count,1)
        XCTAssertFalse(try AlbersScene.project(features).labelCache === scene.labelCache)
    }

    func testRectangleMustClearHolesAndNarrowParts() throws {
        let hole1 = [[-100.0,35],[-99,35],[-99,37],[-100,37],[-100,35]]
        let hole2 = hole1.map { [$0[0]+2,$0[1]] }
        let narrow = [[-96.0,34],[-95.99,34],[-95.99,39],[-96,39],[-96,34]]
        let concave = [[-110.0,30],[-104,30],[-104,31],[-109,31],[-109,36],[-110,36],[-110,30]]
        let scene = try AlbersScene.project([feature("holes",[[square,hole1,hole2]]),feature("narrow",[[narrow]]),feature("concave",[[concave]])])
        let specs = (0..<3).map { MapLabel(id:String($0),features:[$0],primary:"AB",secondary:"40") }
        let anchors = Dictionary(uniqueKeysWithValues:specs.map { ($0.id,scene.labelCache.anchors(in:scene,features:$0.features)) })
        XCTAssertTrue(anchors.values.allSatisfy { !$0.candidates.isEmpty })
        let camera = GeographicCamera(viewport:CGRect(x:0,y:0,width:600,height:420),center:CGPoint(x:scene.bounds.midX,y:scene.bounds.midY),scale:0.00035)
        let sizes = Dictionary(uniqueKeysWithValues:specs.map { ($0.id,LabelMeasurements(full:CGSize(width:2000,height:300),compact:CGSize(width:9,height:10))) })
        let layout = LabelResolver.resolve(specs,anchors:anchors,sizes:sizes,scene:scene,camera:camera,zoom:1)
        XCTAssertFalse(layout.contains { $0.mode == .full })
        XCTAssertEqual(layout.first { $0.id=="1" }?.mode,.suppressed)
        XCTAssertTrue(layout.contains { $0.mode == .compact })
        for d in layout where d.mode == .compact {
            let a = anchors[d.id]!.candidates[d.candidate!]
            for x in [d.frame.minX,d.frame.midX,d.frame.maxX] {
                for y in [d.frame.minY,d.frame.midY,d.frame.maxY] {
                    XCTAssertTrue(scene.hits(camera.world(CGPoint(x:x,y:y))).contains(a.feature))
                }
            }
        }
    }

    func testPortableFixtureAndStableResolution() throws {
        let url = try XCTUnwrap(Bundle.module.url(forResource:"label-overlap",withExtension:"json",subdirectory:"Fixtures"))
        let scene = try AlbersScene.load(url)
        let camera = GeographicCamera(viewport:CGRect(x:0,y:0,width:320,height:300),
            center:CGPoint(x:scene.bounds.midX,y:scene.bounds.midY),scale:0.0004)
        let labels = scene.properties.indices.map { MapLabel(id:scene.properties[$0]["id"]!,features:[$0],primary:"A") }
        let anchors = Dictionary(uniqueKeysWithValues:labels.map { ($0.id,scene.labelCache.anchors(in:scene,features:$0.features)) })
        let sizes = Dictionary(uniqueKeysWithValues:labels.map { ($0.id,LabelMeasurements(full:.init(width:85,height:70),compact:.zero)) })
        func place(_ specs: [MapLabel], _ cam: GeographicCamera, selected: Int? = nil) -> [LabelPlacement] {
            LabelResolver.resolve(specs,anchors:anchors,sizes:sizes,scene:scene,camera:cam,zoom:1,selected:selected)
        }
        let a = place(labels,camera)
        let b = place(Array(labels.reversed()),camera)
        XCTAssertEqual(a.map(\.id),b.map(\.id))
        XCTAssertEqual(a.map(\.mode),b.map(\.mode))
        XCTAssertTrue(a.contains { $0.reason=="collision" })
        XCTAssertEqual(place(labels,camera,selected:0).first?.id,"west")
        var moved=camera
        moved.center.x += 20
        XCTAssertEqual(a.map(\.mode),place(labels,moved).map(\.mode))
        moved.center.x += 8_000_000
        XCTAssertTrue(place(labels,moved).allSatisfy { $0.mode == .suppressed })
        var callout = labels[0]
        callout.allowCallout=true
        let call = LabelResolver.resolve([callout],anchors:anchors,sizes:[callout.id:.init(full:CGSize(width:150,height:16),compact:.zero)],scene:scene,camera:camera,zoom:1)
        XCTAssertEqual(call[0].mode,.callout)
        XCTAssertNotNil(call[0].leader)
        let rev=GeographicRevision(source:"fixture",version:"1")
        let raw=try PolygonSource.readFeatures(url)
        let shape=try GeographicPolygon.decode(feature:raw[0],identity:.init(revision:rev,record:"west"))
        XCTAssertEqual(shape[0].exterior.count,5)
        XCTAssertTrue(shape[0].holes.isEmpty)
        XCTAssertNil(GeographicCoordinate(longitude:181,latitude:0))
    }

    func testExternalPlacementPrioritiesOverridesAndCamera() throws {
        let scene = try AlbersScene.project([feature("one",[[square]])])
        let p = scene.panels[0]
        var first=MapLabel(id:"a",features:[0],primary:"A",secondary:"12"), second=first
        first.priority=2; first.external=true
        second=MapLabel(id:"b",features:[0],primary:"A long jurisdiction name")
        let group=scene.labelCache.anchors(in:scene,features:[0])
        let groups=["a":group,"b":group], sizes=["a":LabelMeasurements(full:.init(width:18,height:18),compact:.init(width:8,height:9)),"b":.init(full:.init(width:9999,height:99),compact:.zero)]
        let camera=GeographicCamera(viewport:.init(x:0,y:0,width:300,height:300),center:.init(x:scene.bounds.midX,y:scene.bounds.midY),scale:0.0004)
        let resolutions=LabelResolver.resolve([second,first],anchors:groups,sizes:sizes,scene:scene,camera:camera,zoom:1)
        XCTAssertEqual(resolutions.map(\.mode),[.tile,.suppressed])
        let at=scene.frame(for:0).origin
        XCTAssertEqual(camera.world(camera.screen(at)).x,at.x,accuracy:1e-6)
        let config = #"{"version":1,"entries":[{"id":"a","region":"mainland","revision":"one","from":"1790-01-01","through":"1800-01-01","normalized":[[0.5,0.5],[9,9]],"priority":5,"typeScale":0.9,"preferred":"compact"}]}"#
        let rules=try JSONDecoder().decode(EditorialRules.self,from:Data(config.utf8))
        let rev=GeographicRevision(source:"test",version:"one")
        XCTAssertNil(rules.matching("a",revision:rev,day:"1800-01-02"))
        let rule=try XCTUnwrap(rules.matching("a",revision:rev,day:"1795-01-01"))
        let overridden=group.applying(rule,scene:scene,features:[0])
        XCTAssertEqual(overridden.candidates.count,group.candidates.count+1)
        XCTAssertTrue(p.parts[0].path.contains(overridden.candidates[0].point,using:.evenOdd))
        var missing=first;missing.primary=""
        XCTAssertEqual(LabelResolver.resolve([missing],anchors:groups,sizes:sizes,scene:scene,camera:camera,zoom:1)[0].reason,"missing content")
        first.minimumZoom=2
        XCTAssertEqual(LabelResolver.resolve([first],anchors:groups,sizes:sizes,scene:scene,camera:camera,zoom:1)[0].reason,"zoom")
    }
}
