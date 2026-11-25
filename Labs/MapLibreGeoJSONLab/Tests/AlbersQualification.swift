import Foundation

@main
struct AlbersQualification {
    struct Reference: Decodable {
        struct Coordinate: Decodable {
            let region: String
            let lon: Double
            let lat: Double
            let easting: Double
            let northing: Double
        }
        struct Patch: Decodable {
            let region: String
            let ring: [[Double]]
            let area: Double
        }
        let proj: String
        let points: [Coordinate]
        let patches: [Patch]
    }

    static func main() throws {
        precondition(CommandLine.arguments.count == 5,
                     "Specify repository root, historical county archive, projection fixture and bundled county derivative.")
        let root = URL(fileURLWithPath: CommandLine.arguments[1])
        let countyArchive = URL(fileURLWithPath: CommandLine.arguments[2])
        let fixture = URL(fileURLWithPath: CommandLine.arguments[3])
        let derivative = URL(fileURLWithPath: CommandLine.arguments[4])
        let ref = try JSONDecoder().decode(Reference.self, from: Data(contentsOf: fixture))
        try checkProjection(ref)
        try checkHolesAndOverlap()

        let state = root.appendingPathComponent("cb_2024_us_state_5m.json")
        let district = root.appendingPathComponent("cb_2024_us_cd119_5m.json")
        let county = root.appendingPathComponent("cb_2024_us_county_5m.json")
        for (name, url, expected) in [("states", state, 51), ("districts", district, 436), ("counties", county, 3144),
                                      ("county-results", derivative, 3153)] {
            try checkScene(name, url: url, expected: expected)
        }
        try checkReferenceHits(state, probes: [(.mainland, -121.5, 38.5), (.mainland, -77.0365, 38.8977),
                                               (.alaska, -149.9003, 61.2181), (.hawaii, -157.8583, 21.3069)])
        try checkInsetLayout(state)
        try checkReferenceHits(county, probes: [(.mainland, -112.07, 33.45), (.mainland, -73.974, 40.78),
                                                (.mainland, -78.491, 38.032), (.hawaii, -157.8583, 21.3069)])
        try checkReferenceHits(derivative, probes: [(.mainland, -72.674, 41.762),
                                                    (.alaska, -149.9003, 61.2181)])

        let temp = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString + ".geojson")
        defer { try? FileManager.default.removeItem(at: temp) }
        let archive = try HistoricalArchive(url: countyArchive)
        let historicalCountyCounts = [1788: 332, 1860: 2181, 1900: 2845, 1980: 3143, 2000: 3139]
        for year in historicalCountyCounts.keys.sorted() {
            let day = try HistoricalDay("\(year)-01-01")
            let snapshot = try archive.snapshot(on: day)
            try snapshot.data.write(to: temp, options: .atomic)
            try checkScene("historical-county-\(year)", url: temp, expected: historicalCountyCounts[year]!)
        }
        try checkReferenceHits(temp, probes: [(.mainland, -78.491, 38.032), (.mainland, -78.55, 38.043)])
        try checkHistoricalOverlap(archive, snapshotAt: temp)
        let stateArchive = try HistoricalArchive(url: root.appendingPathComponent("US_HistStateTerr_Gen05.json"))
        let elections = try PresidentialElection.available(asOf: HistoricalDay("2024-12-31"))
        for year in [1788, 1860, 1900, 1980, 2000] {
            let election = elections.first { $0.year == year }!
            let snapshot = try stateArchive.snapshot(on: election.boundaries.day)
            try snapshot.data.write(to: temp, options: .atomic)
            try checkScene("historical-state-\(year)", url: temp, expected: snapshot.summary.features)
        }
    }

    private static func checkProjection(_ ref: Reference) throws {
        var maximum = 0.0
        for point in ref.points {
            let region = AlbersRegion.allCases.first { String(describing: $0) == point.region }!
            let actual = region.projection.project(longitude: point.lon, latitude: point.lat)
            maximum = max(maximum, hypot(actual.x - point.easting, actual.y - point.northing))
        }
        precondition(maximum < 0.01)
        var maxAreaError = 0.0
        for patch in ref.patches {
            let projection = AlbersRegion.allCases.first { String(describing: $0) == patch.region }!.projection
            let points = patch.ring.map { projection.project(longitude: $0[0], latitude: $0[1]) }
            var doubleArea: CGFloat = 0
            let next = Array(points.dropFirst()) + [points[0]]
            for (a, b) in zip(points, next) {
                doubleArea += a.x * b.y - b.x * a.y
            }
            maxAreaError = max(maxAreaError, abs(abs(Double(doubleArea) / 2) / patch.area - 1))
        }
        precondition(maxAreaError < 0.00001)
        print("PROJ \(ref.proj) · \(ref.points.count) coordinates · max error \(maximum) m · max area error \(maxAreaError)")
    }

    private static func checkScene(_ tag: String, url: URL, expected: Int) throws {
        let linear = try AlbersScene.load(url, indexed: false)
        let indexed = try AlbersScene.load(url)
        precondition(linear.properties == indexed.properties && indexed.properties.count == expected, tag)
        precondition(indexed.bounds == linear.bounds)
        for (a, b) in zip(linear.panels, indexed.panels) {
            precondition(a.frame == b.frame && a.translation == b.translation)
            precondition(zip(a.parts, b.parts).allSatisfy { $0.path == $1.path })
        }
        var samples = [CGPoint]()
        for sequence in 0..<5_000 {
            let x = CGFloat((sequence * 37) % 997) / 997
            let y = CGFloat((sequence * 73) % 991) / 991
            samples.append(CGPoint(x: linear.bounds.minX + linear.bounds.width * x,
                                   y: linear.bounds.minY + linear.bounds.height * y))
        }
        let start = ProcessInfo.processInfo.systemUptime
        let old = samples.map(linear.hit)
        let middle = ProcessInfo.processInfo.systemUptime
        let new = samples.map { indexed.hits($0).first }
        let finish = ProcessInfo.processInfo.systemUptime
        precondition(old == new)
        for (a, b) in zip(linear.panels, indexed.panels) {
            for zoom in [1, 2, 4, 8, 16] {
                for point in samples.prefix(20) {
                    let local = a.local(point)
                    let frame = CGRect(x: local.x, y: local.y, width: a.frame.width / CGFloat(zoom),
                                       height: a.frame.height / CGFloat(zoom))
                    precondition(a.visible(in: frame) == b.visible(in: frame))
                }
            }
        }
        let report: [String: Any] = ["dataset": tag, "features": expected, "probes": samples.count,
            "hits": old.compactMap { $0 }.count, "linear_ms": (middle - start) * 1_000,
            "indexed_ms": (finish - middle) * 1_000,
            "decode_ms": indexed.preparation.decodedMilliseconds,
            "project_ms": indexed.preparation.projectedMilliseconds,
            "index_layout_ms": indexed.preparation.indexedMilliseconds]
        let bytes = try JSONSerialization.data(withJSONObject: report, options: .sortedKeys)
        print(String(decoding: bytes, as: UTF8.self))
    }

    private static func checkReferenceHits(_ url: URL, probes: [(AlbersRegion, Double, Double)]) throws {
        let original = try GeoJSON.readFeatures(url)
        let scene = try AlbersScene.load(url)
        for (region, lon, lat) in probes {
            let contained = try original.filter { try GeoJSON.containsFeature($0, longitude: lon, latitude: lat) }
            let identifier = contained.first.map { ($0["properties"] as? GeoJSON.Object)?["GEOID"] == nil ? "ID_NUM" : "GEOID" } ?? "GEOID"
            let expected = Set(contained.compactMap { ($0["properties"] as? GeoJSON.Object)?[identifier] }.map { "\($0)" })
            let indices = scene.hits(world(scene, region, lon, lat))
            let actual = Set(indices.compactMap { scene.properties[$0][identifier] })
            precondition(!expected.isEmpty && actual == expected, "\(url.lastPathComponent) \(lon),\(lat): \(expected) vs \(actual)")
        }
    }

    private static func checkInsetLayout(_ url: URL) throws {
        let normal = try AlbersScene.load(url)
        let themed = normal.composed(.thematic)
        let ak = themed.panels.first { $0.region == .alaska }!
        let hi = themed.panels.first { $0.region == .hawaii }!
        precondition(ak.scale < 1 && hi.frame.minX > ak.frame.maxX && hi.frame.minY > ak.frame.minY)
        precondition(themed.panels.allSatisfy { !$0.showsFrame })
        for (a, b) in zip(normal.panels, themed.panels) {
            precondition(zip(a.parts, b.parts).allSatisfy { $0.path === $1.path })
            for part in b.parts {
                guard let local = part.interiorAnchor else { continue }
                let point = b.world(local)
                precondition(hypot(b.local(point).x - local.x, b.local(point).y - local.y) < 1e-7)
                precondition(themed.hits(point) == normal.hits(a.world(local)))
            }
        }
        for (panel, lon, lat) in [(ak, -152.4, 64.7), (hi, -155.5, 19.62)] {
            let meters = panel.region.projection.project(longitude: lon, latitude: lat)
            let local = CGPoint(x: meters.x, y: -meters.y)
            let expected = themed.hits(panel.world(local))
            precondition(!expected.isEmpty)
            for size in [CGSize(width: 358, height: 497), CGSize(width: 718, height: 112)] {
                let fit = min((size.width - 24) / themed.bounds.width, (size.height - 24) / themed.bounds.height)
                let focus = min((size.width - 24) / (panel.frame.width * 1.09),
                                (size.height - 24) / (panel.frame.height * 1.09))
                for t in [0.0, 0.1, 0.25, 0.5, 0.75, 0.9, 1.0] {
                    let eased = t * t * (3 - 2 * t)
                    let scale = fit * pow(focus / fit, eased)
                    let cx = themed.bounds.midX + (panel.frame.midX - themed.bounds.midX) * eased
                    let cy = themed.bounds.midY + (panel.frame.midY - themed.bounds.midY) * eased
                    let transform = CGAffineTransform(translationX: size.width / 2, y: size.height / 2)
                        .scaledBy(x: scale, y: scale).translatedBy(x: -cx, y: -cy)
                        .translatedBy(x: panel.translation.x, y: panel.translation.y)
                        .scaledBy(x: panel.scale, y: panel.scale)
                    let screen = local.applying(transform)
                    let world = CGPoint(x: cx + (screen.x - size.width / 2) / scale,
                                        y: cy + (screen.y - size.height / 2) / scale)
                    precondition(themed.hits(world) == expected)
                }
            }
        }
        print("Inset camera round trips · 2 regions × 2 viewports × 7 transition positions")
        let reversed = try AlbersScene.load(url, layout: .init(gap: 185_000, alaskaFirst: false))
        precondition(reversed.panels.map(\.region) == [.mainland, .hawaii, .alaska])
        precondition(reversed.panels[1].frame.maxX < reversed.panels[2].frame.minX)
        for original in normal.panels {
            let moved = reversed.panels.first { $0.region == original.region }!
            precondition(zip(original.parts, moved.parts).allSatisfy { $0.path == $1.path })
        }
        let alaska = reversed.hits(world(reversed, .alaska, -149.9003, 61.2181))
        precondition(alaska.first.map { reversed.properties[$0]["GEOID"] } == "02")
    }

    private static func checkHistoricalOverlap(_ archive: HistoricalArchive, snapshotAt url: URL) throws {
        let day = try HistoricalDay("1808-01-01")
        let snapshot = try archive.snapshot(on: day)
        try snapshot.data.write(to: url, options: .atomic)
        let scene = try AlbersScene.load(url)
        let canonical = try GeoJSON.readFeatures(url)
        let target = canonical.first { ($0["properties"] as? GeoJSON.Object)?["ID_NUM"] as? Int == 6860 }!
        let bounds = try GeoJSON.summarizeFeature(target).bounds
        for east in 1..<10 {
            for north in 1..<10 {
                let lon = bounds.west + (bounds.east - bounds.west) * Double(east) / 10
                let lat = bounds.south + (bounds.north - bounds.south) * Double(north) / 10
                if try GeoJSON.containsFeature(target, longitude: lon, latitude: lat) {
                    let ids = scene.hits(world(scene, .mainland, lon, lat)).compactMap { scene.properties[$0]["ID_NUM"] }
                    precondition(Set(ids).isSuperset(of: ["6860", "12250"]))
                    try checkReferenceHits(url, probes: [(.mainland, lon, lat)])
                    print("Historical overlap · Concordia county and parish · IDs 6860 and 12250")
                    return
                }
            }
        }
        preconditionFailure("A reference point within the coincident Concordia polygons was not found.")
    }

    private static func world(_ scene: AlbersScene, _ region: AlbersRegion, _ lon: Double, _ lat: Double) -> CGPoint {
        let panel = scene.panels.first { $0.region == region }!
        let point = region.projection.project(longitude: lon, latitude: lat)
        return panel.world(CGPoint(x: point.x, y: -point.y))
    }

    private static func checkHolesAndOverlap() throws {
        func square(_ west: Double, _ south: Double, _ east: Double, _ north: Double) -> [[Double]] {
            [[west, south], [east, south], [east, north], [west, north], [west, south]]
        }
        let first: GeoJSON.Object = ["type": "Feature", "properties": ["GEOID": "first"],
            "geometry": ["type": "MultiPolygon", "coordinates": [
                [square(-101, 38, -98, 41), square(-100.5, 38.5, -99.5, 39.5)],
                [square(-96, 40, -95.98, 40.02)]
            ]]]
        let second: GeoJSON.Object = ["type": "Feature", "properties": ["GEOID": "overlap"],
            "geometry": ["type": "Polygon", "coordinates": [square(-100, 40, -97, 42)]]]
        let adjacent: GeoJSON.Object = ["type": "Feature", "properties": ["GEOID": "adjacent"],
            "geometry": ["type": "Polygon", "coordinates": [square(-98, 38, -96, 39)]]]
        let island: GeoJSON.Object = ["type": "Feature", "properties": ["STATEFP": "02", "GEOID": "aleutian"],
            "geometry": ["type": "Polygon", "coordinates": [
                [[179.8, 51.9], [-179.8, 51.9], [-179.8, 52.1], [179.8, 52.1], [179.8, 51.9]]
            ]]]
        let scene = try AlbersScene.project([first, second, adjacent, island])
        precondition(scene.hits(world(scene, .mainland, -100, 39)).isEmpty)
        precondition(scene.hits(world(scene, .mainland, -99, 40.5)) == [1, 0])
        precondition(scene.hits(world(scene, .mainland, -95.99, 40.01)) == [0])
        precondition(scene.hits(world(scene, .mainland, -98.01, 38.5)) == [0])
        precondition(scene.hits(world(scene, .mainland, -97.99, 38.5)) == [2])
        for profile in NationalMapProfile.allCases {
            let map = scene.composed(profile)
            precondition(map.hits(world(map, .mainland, -100, 39)).isEmpty)
            precondition(map.hits(world(map, .mainland, -99, 40.5)) == [1, 0])
            for longitude in [179.95, -179.95] {
                precondition(map.hits(world(map, .alaska, longitude, 52)) == [3])
            }
        }
    }
}
