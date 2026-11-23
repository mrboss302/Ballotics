import Foundation

@main
struct HistoricalArchiveChecks {
    static func main() throws {
        precondition(CommandLine.arguments.count == 2)
        let input = URL(fileURLWithPath: CommandLine.arguments[1])
        let archive = try HistoricalArchive(url: input)
        precondition(archive.records.count == 17_727)
        precondition(Set(archive.records.map(\.entity)).count == 3_890)
        precondition(archive.coverage.lowerBound.description == "1629-03-04")
        precondition(archive.coverage.upperBound.description == "2000-12-31")
        precondition(archive.sourceSHA256 == "af037e36183c9274d8be905b966ba775b548b6ec5210360c5814a7b952133a7b")

        for (date, count) in zip(HistoricalRequest.presets, [332, 455, 2_181, 2_845, 3_114, 3_143, 3_139]) {
            let snapshot = try archive.snapshot(on: date.day)
            precondition(snapshot.summary.features == count)
            precondition(snapshot.summary.multipart > 0)
            let again = try archive.snapshot(on: date.day)
            precondition(snapshot.data == again.data && snapshot.manifest == again.manifest)
            let json = try JSONSerialization.jsonObject(with: snapshot.data) as! [String: Any]
            let features = json["features"] as! [[String: Any]]
            let identifiers = features.map { ($0["properties"] as! [String: Any])["ID_NUM"] as! Int }
            let matched: [Int] = try archive.active(on: date.day).map(\.id)
            precondition(identifiers == matched)
            if date.day.description == "2000-01-01" {
                precondition(snapshot.summary.rings > snapshot.summary.parts)
                precondition(snapshot.manifest.outputSHA256 == "70d9f28be93fc9750427fdbdd424ab9cc56626a374497b32816f953ff7e40a35")
            }
            print(date.day, count, snapshot.summary.parts, snapshot.summary.positions, snapshot.manifest.bytes,
                  snapshot.manifest.outputSHA256)
        }

        let transitions: [(before: String, effective: String, old: [Int], new: [Int])] = [
            ("1729-06-15", "1729-06-16", [12836, 12842], [12837, 12843, 12848]),
            ("1731-07-09", "1731-07-10", [7238], [7239, 7252]),
            ("1836-02-03", "1836-02-04", [2946], [2938, 2947]),
            ("1853-03-24", "1853-03-25", [1599, 1804], [1556, 1600, 1805]),
            ("1909-06-30", "1909-07-01", [2941], [2942, 2987]),
            ("1952-06-30", "1952-07-01", [16007, 16100], [16101]),
            ("1997-11-12", "1997-11-13", [2944], [2945])
        ]
        for transition in transitions {
            let before = try ids(archive, transition.before)
            let effective = try ids(archive, transition.effective)
            precondition(before.isSuperset(of: transition.old) && before.isDisjoint(with: transition.new))
            precondition(effective.isSuperset(of: transition.new) && effective.isDisjoint(with: transition.old))
            print("Verified transition", transition.effective, transition.old, "→", transition.new)
        }

        let recent = try HistoricalDay("2000-01-01")
        let city = try archive.matchingIDs(on: recent, candidates: [15811, 15946], longitude: -78.491,
                                           latitude: 38.032)
        let county = try archive.matchingIDs(on: recent, candidates: [15811, 15946], longitude: -78.55,
                                             latitude: 38.043)
        precondition(city == [15946] && county == [15811])
        print("Source ring confirmation distinguishes Albemarle from its Charlottesville hole.")
        let concurrent = try ids(archive, "1808-01-01")
        precondition(concurrent.isSuperset(of: [6860, 12250]))
        let colbertLast = try ids(archive, "1867-11-28")
        let colbertGap = try ids(archive, "1867-11-29")
        let colbertReturn = try ids(archive, "1870-01-24")
        precondition(colbertLast.contains(349) && colbertGap.isDisjoint(with: [349, 350]) && colbertReturn.contains(350))
        let lastDay = try ids(archive, "2000-12-31")
        precondition(lastDay.count == 3_141)
        for invalid in ["1629-03-03", "2001-01-01", "2024-01-01", "1700-02-29", "1788-1-01", "0000-01-01"] {
            expectFailure { _ = try archive.active(on: HistoricalDay(invalid)) }
        }
        let leap = try HistoricalDay("1600-02-29")
        precondition(leap.number == 16000229)
        try synthetic()
        print("Historical archive checks passed.")
    }

    private static func ids(_ archive: HistoricalArchive, _ date: String) throws -> Set<Int> {
        Set(try archive.active(on: HistoricalDay(date)).map(\.id))
    }

    private static func expectFailure(_ body: () throws -> Void) {
        do {
            try body()
            preconditionFailure("Invalid input succeeded")
        } catch {}
    }

    private static func synthetic() throws {
        let folder = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: folder, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: folder) }
        let file = folder.appendingPathComponent("fixture.json")
        let polygon = #"{"type":"Polygon","coordinates":[[[0,0],[4,0],[4,4],[0,4],[0,0]],[[1,1],[1,2],[2,2],[2,1],[1,1]]]}"#
        func record(_ id: Int, entity: String = "county", version: Int = 1,
                    start: String = #""1790-01-01T00:00:00.000Z""#,
                    end: String = #""1790-01-01T00:00:00.000Z""#) -> String {
            """
            {"type":"Feature","geometry":\(polygon),"properties":{"ID_NUM":\(id),"ID":"\(entity)","VERSION":\(version),"START_DATE":\(start),"END_DATE":\(end)}}
            """
        }
        func write(_ records: [String]) throws {
            let collection = #"{"meta":{"note":"quoted: \"[}]"},"features":["# + records.joined(separator: ",")
                + #"],"type":"FeatureCollection"}"#
            try Data(collection.utf8).write(to: file)
        }
        // Two distinct entities may overlap completely on a shared day.
        try write([record(1), record(2, entity: "different")])
        let oneDay = try HistoricalArchive(url: file)
        let both = try oneDay.snapshot(on: HistoricalDay("1790-01-01"))
        precondition(both.summary.features == 2 && both.summary.rings == 4)
        precondition(String(decoding: both.data, as: UTF8.self).contains(polygon))
        let derived = folder.appendingPathComponent("published.geojson")
        let manifestURL = folder.appendingPathComponent("manifest.json")
        try both.data.write(to: derived)
        try JSONEncoder().encode(both.manifest).write(to: manifestURL)
        let read = try HistoricalArchive.loadPublished(geoJSON: derived, manifestURL: manifestURL,
                                                       date: HistoricalDay("1790-01-01"))
        precondition(read.0 == both.manifest && read.1.features == 2)
        try Data((String(decoding: both.data, as: UTF8.self) + " ").utf8).write(to: derived)
        expectFailure {
            _ = try HistoricalArchive.loadPublished(geoJSON: derived, manifestURL: manifestURL,
                                                    date: HistoricalDay("1790-01-01"))
        }
        try write([record(1), record(2, entity: "open", end: "null")])
        expectFailure { _ = try HistoricalArchive(url: file) }
        let open = try HistoricalArchive(url: file, coverageThrough: HistoricalDay("1790-12-31"))
        let active = try ids(open, "1790-12-31")
        precondition(active == [2])
        for records in [
            [record(1), record(1, entity: "different")],
            [record(1), record(2)],
            [record(1, end: #""1789-12-31T00:00:00.000Z""#)],
            [record(1, start: "null")],
            [record(1).replacingOccurrences(of: #","END_DATE":"1790-01-01T00:00:00.000Z""#, with: "")],
            [record(1).replacingOccurrences(of: polygon, with: #"{"type":"Point","coordinates":[0,0]}"#)]
        ] {
            try write(records)
            expectFailure { _ = try HistoricalArchive(url: file) }
        }
        try Data(#"{"type":"FeatureCollection","features":[{}],"features":[{}]}"#.utf8).write(to: file)
        expectFailure { _ = try HistoricalArchive(url: file) }
    }
}
