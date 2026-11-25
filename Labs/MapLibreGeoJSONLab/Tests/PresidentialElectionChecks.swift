import Foundation

@main
struct PresidentialElectionChecks {
    static func main() throws {
        let source = URL(fileURLWithPath: CommandLine.arguments[1])
        let archive = try HistoricalArchive(url: source)
        let features = try GeoJSON.readFeatures(source)
        let endOf2024 = try HistoricalDay("2024-12-31")
        let elections = PresidentialElection.available(asOf: endOf2024)
        precondition(elections.map(\.year) == Array(stride(from: 2024, through: 1788, by: -4)))
        let dates = Dictionary(uniqueKeysWithValues: elections.map { ($0.year, $0.electionDay.description) })
        for (year, expected) in [1788: "1789-02-04", 1792: "1792-12-05", 1844: "1844-12-04",
                                 1848: "1848-11-07", 2000: "2000-11-07", 2004: "2004-11-02", 2024: "2024-11-05"] {
            precondition(dates[year] == expected)
        }
        let before2028 = try HistoricalDay("2028-11-06")
        let on2028 = try HistoricalDay("2028-11-07")
        precondition(PresidentialElection.available(asOf: before2028).first?.year == 2024)
        precondition(PresidentialElection.available(asOf: on2028).first?.year == 2028)

        let lastArchiveDay = try HistoricalDay("2000-11-07")
        let modern = try archive.snapshot(on: lastArchiveDay)
        precondition(modern.summary.features == 51)
        for election in elections {
            let snapshot = try archive.snapshot(on: election.boundaries.day)
            let decoded = try JSONSerialization.jsonObject(with: snapshot.data) as! GeoJSON.Object
            let actual = decoded["features"] as! [GeoJSON.Object]
            let day = election.boundaries.day.number
            let expected = features.filter { feature in
                let fields = feature["properties"] as! GeoJSON.Object
                return (fields["START_N"] as! Int) <= day && day <= (fields["END_N"] as! Int)
            }
            precondition(!expected.isEmpty && actual.count == expected.count)
            for (a, b) in zip(actual, expected) {
                precondition(NSDictionary(dictionary: a).isEqual(to: b))
            }
            if election.uses2000Boundaries {
                precondition(snapshot.data == modern.data && snapshot.manifest == modern.manifest)
            }
        }

        let temporary = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString + ".geojson")
        defer { try? FileManager.default.removeItem(at: temporary) }
        let insetNames: [(Int, String, String?)] = [
            (1896, "Alaska District", nil),
            (1900, "Alaska District", "Hawaii Territory"),
            (1956, "Alaska Territory", "Hawaii Territory"),
            (1960, "Alaska", "Hawaii")
        ]
        for (year, alaska, hawaii) in insetNames {
            let election = elections.first { $0.year == year }!
            let snapshot = try archive.snapshot(on: election.boundaries.day)
            try snapshot.data.write(to: temporary, options: .atomic)
            let scene = try AlbersScene.load(temporary)
            precondition(scene.properties.count == snapshot.summary.features)
            checkInset(scene, region: .alaska, longitude: -149.9003, latitude: 61.2181, name: alaska)
            checkInset(scene, region: .hawaii, longitude: -157.8583, latitude: 21.3069, name: hawaii)
        }
        print("Verified \(elections.count) presidential snapshots, the shared 2000 map, and historical Alaska and Hawaii insets.")
    }

    private static func checkInset(_ scene: AlbersScene, region: AlbersRegion, longitude: Double,
                                   latitude: Double, name: String?) {
        let panel = scene.panels.first { $0.region == region }
        guard let name else {
            precondition(panel == nil)
            return
        }
        let translation = panel!.translation
        let point = region.projection.project(longitude: longitude, latitude: latitude)
        let index = scene.hit(CGPoint(x: point.x + translation.x, y: -point.y + translation.y))
        precondition(index.map { scene.properties[$0]["NAME"] == name } == true)
    }
}
