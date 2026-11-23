import Foundation

@main
struct GeoJSONChecks {
    static func main() throws {
        let root = URL(fileURLWithPath: CommandLine.arguments[1], isDirectory: true)
        let datasets: [(Geography, Int, Int, Int)] = [
            (.districts, 441, 58, 175_196),
            (.counties, 3_235, 109, 227_438),
            (.states, 56, 24, 91_908)
        ]
        for (geography, expectedFeatures, expectedMultipart, expectedPositions) in datasets {
            let url = root.appendingPathComponent(geography.resource!).appendingPathExtension("json")
            let summary = try GeoJSON.validate(url, geography: geography)
            precondition(summary.features == expectedFeatures)
            precondition(summary.multipart == expectedMultipart)
            precondition(summary.positions == expectedPositions)
            precondition(summary.selectionKey == "GEOID")
            if geography == .states {
                precondition(summary.stateBounds["02"] != nil && summary.stateBounds["15"] != nil)
            }
            printSummary(geography.rawValue, summary)
        }
        if CommandLine.arguments.count > 2 {
            let historical = URL(fileURLWithPath: CommandLine.arguments[2])
            let summary = try GeoJSON.validate(historical, geography: .imported)
            precondition(summary.features == 17_727 && summary.multipart == 1_937 && summary.positions == 742_895)
            precondition(summary.selectionKey == "ID_NUM")
            printSummary("Historical counties", summary)
        }

        let file = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString).appendingPathExtension("geojson")
        defer { try? FileManager.default.removeItem(at: file) }
        let polygon = #"{"type":"Polygon","coordinates":[[[0,0],[4,0],[4,4],[0,4],[0,0]],[[1,1],[1,2],[2,2],[2,1],[1,1]]]}"#
        try Data(polygon.utf8).write(to: file)
        let summary = try GeoJSON.validate(file, geography: .imported)
        precondition(summary.features == 1 && summary.rings == 2 && summary.selectionKey == nil)
        let interior = try GeoJSON.originalPolygon(in: file, longitude: 3, latitude: 3)
        let hole = try GeoJSON.originalPolygon(in: file, longitude: 1.5, latitude: 1.5)
        precondition(interior != nil && hole == nil)
        let collection = """
        {"type":"FeatureCollection","features":[
          {"type":"Feature","id":1,"geometry":\(polygon),"properties":{}},
          {"type":"Feature","id":1,"geometry":\(polygon),"properties":{}}
        ]}
        """
        try Data(collection.utf8).write(to: file)
        let duplicates = try GeoJSON.validate(file, geography: .imported)
        precondition(duplicates.selectionKey == nil)
        let explicitID = """
        {"type":"Feature","id":"island","geometry":\(polygon),"properties":null}
        """
        try Data(explicitID.utf8).write(to: file)
        let explicit = try GeoJSON.validate(file, geography: .imported)
        precondition(explicit.selectionKey == "$id")
        for invalid in [
            "{",
            #"{"type":"Point","coordinates":[0,0]}"#,
            #"{"type":"Polygon","coordinates":[[[181,0],[0,4],[0,0],[181,0]]]}"#,
            #"{"type":"Polygon","coordinates":[[[false,0],[0,4],[0,0],[false,0]]]}"#,
            #"{"type":"Polygon","coordinates":[[[0,0],[4,0],[4,4],[0,4]]]}"#,
            #"{"type":"FeatureCollection","features":[]}"#
        ] {
            try Data(invalid.utf8).write(to: file)
            do {
                _ = try GeoJSON.validate(file, geography: .imported)
                fatalError("Invalid GeoJSON passed validation: \(invalid)")
            } catch {
                // Invalid files are rejected before MapLibre receives a URL.
            }
        }
        print("GeoJSON checks passed.")
    }

    static func printSummary(_ name: String, _ summary: GeoJSON.Summary) {
        let b = summary.bounds
        print("\(name) features=\(summary.features) multipart=\(summary.multipart) parts=\(summary.parts) "
              + "rings=\(summary.rings) positions=\(summary.positions) "
              + "bounds=[\(b.west),\(b.south),\(b.east),\(b.north)] id=\(summary.selectionKey ?? "none")")
    }
}
