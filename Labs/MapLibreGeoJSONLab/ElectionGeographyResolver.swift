import Foundation

enum ElectionMapMode: String, CaseIterable, Identifiable {
    case electoralCollege = "Electoral College"
    case countyResults = "County results"

    var id: String { rawValue }
}

enum ElectionGeographyResolver {
    static func description(for election: PresidentialElection, mode: ElectionMapMode) -> String {
        if mode == .electoralCollege { return election.snapshotDescription }
        let fallback = election.year < 2024 ? " (fallback)" : ""
        return "2024 Census counties and AK House districts\(fallback) · CT traditional counties (vintage unverified)"
    }

    static func resolveState(_ election: PresidentialElection, record: ElectionRecord) throws -> HistoricalRequest {
        guard record.year == election.year, record.geography.sourceID == "historical-states",
              record.geography.snapshotDate == election.boundaries.day.description else {
            throw ElectionPackageError.invalid("State boundary date disagrees with the election calendar")
        }
        return election.boundaries
    }

    static func featureKey(_ attributes: [String: Any], mode: ElectionMapMode) -> String? {
        let value = attributes[mode == .countyResults ? "GEOID" : "ID_NUM"]
        return (value as? String) ?? (value as? NSNumber)?.stringValue
    }
}
