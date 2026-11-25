import Foundation
#if canImport(BalloticsGeoKit)
import BalloticsGeoKit
#endif

enum ElectoralCartography {
    static let keyOrder = ["VT", "NH", "MA", "RI", "CT", "NJ", "DE", "MD", "DC"]

    static func specifications(scene: AlbersScene, votes: [String:Int], statuses: [String:String]) -> [MapLabel] {
        let indices = scene.properties.indices.filter {
            let fields = scene.properties[$0]
            return ["State", "District of Columbia"].contains(fields["TERR_TYPE"]) && fields["ABBR_NAME"] != nil
        }
        let groups = Dictionary(grouping: indices) { scene.properties[$0]["ABBR_NAME"]! }
        return groups.keys.sorted().map { code in
            let features = groups[code]!
            let keys = Set(features.compactMap { scene.properties[$0]["ID_NUM"] })
            let states = Set(keys.compactMap { statuses[$0] })
            let unresolved = states.contains("Unresolved allocation")
            let counts = keys.compactMap { votes[$0] }
            let count = !unresolved && !counts.isEmpty ? String(counts.reduce(0,+)) : ""
            let status = states.sorted().joined(separator: ", ")
            let name = scene.properties[features[0]]["STATE_TERR"] ?? code
            var label = MapLabel(id: code, features: features, primary: code, secondary: count,
                                 accessibilityText: [name, count.isEmpty ? nil : count + " electoral votes",
                                                     status.isEmpty ? "Allocation unavailable" : status].compactMap { $0 }.joined(separator: ", "))
            label.external = keyOrder.contains(code) && (!count.isEmpty || unresolved)
            label.priority = ["AK","HI"].contains(code) ? 2 : 0
            return label
        }
    }
}
