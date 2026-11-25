import Foundation

@main
struct AlbersChecks {
    static func main() throws {
        let root = URL(fileURLWithPath: CommandLine.arguments[1])
        let references: [(AlbersRegion, Double, Double, Double, Double)] = [
            (.mainland, -100, 40, -338390.5875485082, 1894100.1400790422),
            (.alaska, -149.9003, 61.2181, 219349.5792169305, 1255301.539724613),
            (.hawaii, -157.8583, 21.3069, -89672.96910849206, 2024424.4097657378),
            (.alaska, 179.8, 52, -1761531.8629073475, 573036.4512801345)
        ]
        // Independent reference values from PROJ 9.3 with the same WGS 84 parameters.
        for (region, longitude, latitude, easting, northing) in references {
            let point = region.projection.project(longitude: longitude, latitude: latitude)
            precondition(abs(point.x - easting) < 0.01 && abs(point.y - northing) < 0.01)
        }
        let alaska = AlbersRegion.alaska.projection
        let west = alaska.project(longitude: -179.8, latitude: 52)
        let east = alaska.project(longitude: 179.8, latitude: 52)
        precondition(hypot(west.x - east.x, west.y - east.y) < 30_000)

        for (file, count) in [("state", 51), ("county", 3144), ("cd119", 436)] {
            let scene = try AlbersScene.load(root.appendingPathComponent("cb_2024_us_\(file)_5m.json"))
            precondition(scene.properties.count == count && scene.panels.count == 3)
            for (index, panel) in scene.panels.enumerated() {
                precondition(scene.bounds.contains(panel.frame) && panel.frame.width.isFinite)
                precondition(scene.panels[..<index].allSatisfy { !$0.frame.intersects(panel.frame) })
            }
            if file == "state" {
                check(scene, .mainland, -121.5, 38.5, "California")
                check(scene, .alaska, -149.9003, 61.2181, "Alaska")
                check(scene, .hawaii, -157.8583, 21.3069, "Hawaii")
            }
        }
        if CommandLine.arguments.count > 2 {
            let historical = try AlbersScene.load(URL(fileURLWithPath: CommandLine.arguments[2]))
            precondition(historical.properties.count == 3139)
            let city = hit(historical, .mainland, -78.491, 38.032)
            precondition(city?["ID_NUM"] == "15946")
        }
        print("Albers reference coordinates, antimeridian, insets, and polygon hits passed.")
    }

    private static func check(_ scene: AlbersScene, _ region: AlbersRegion,
                              _ longitude: Double, _ latitude: Double, _ expected: String) {
        precondition(hit(scene, region, longitude, latitude)?["NAME"] == expected)
    }

    private static func hit(_ scene: AlbersScene, _ region: AlbersRegion,
                            _ longitude: Double, _ latitude: Double) -> [String: String]? {
        let point = region.projection.project(longitude: longitude, latitude: latitude)
        guard let panel = scene.panels.first(where: { $0.region == region }),
              let feature = scene.hit(CGPoint(x: point.x + panel.translation.x, y: -point.y + panel.translation.y))
        else { return nil }
        return scene.properties[feature]
    }
}
