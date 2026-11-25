import CryptoKit
import Foundation

struct ElectionManifest: Codable, Sendable {
    struct Payload: Codable, Equatable, Sendable {
        let file: String
        let sha256: String
        let bytes: Int
        let records: Int
        let year: Int?
    }

    struct GeographicSource: Codable, Equatable, Sendable {
        let id: String
        let file: String
        let vintage: String
        let sha256: String
    }

    let schemaVersion: Int
    let datasetVersion: String
    let payloads: [Payload]
    let geographySources: [GeographicSource]
    let inputHashes: [String: String]
    let countySourceMetadata: [String: String]

    func identity() throws -> String {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        return try ElectionPayloadValidation.hash(encoder.encode(self))
    }

    func payload(year: Int?) throws -> Payload {
        guard schemaVersion == 1 else { throw ElectionPackageError.unsupportedSchema(schemaVersion) }
        let expected = Set(["national.json"] + Self.countyYears.map { "county-\($0).json" })
        let sourceIDs = Set(["historical-states", "census-county-2024", "connecticut-traditional", "alaska-house-2024"])
        guard geographySources.count == sourceIDs.count, Set(geographySources.map(\.id)) == sourceIDs,
              geographySources.allSatisfy({ $0.sha256.range(of: #"^[a-f0-9]{64}$"#, options: .regularExpression) != nil }),
              datasetVersion.range(of: #"^[0-9]+\.[0-9]+\.[0-9]+$"#, options: .regularExpression) != nil,
              payloads.count == expected.count, Set(payloads.map(\.file)) == expected,
              payloads.allSatisfy({ entry in
                  entry.file == (entry.year.map { "county-\($0).json" } ?? "national.json")
                      && entry.bytes > 0 && entry.bytes <= 24_000_000 && entry.records > 0
                      && entry.sha256.range(of: #"^[a-f0-9]{64}$"#, options: .regularExpression) != nil
              }),
              let entry = payloads.first(where: { $0.year == year }) else {
            throw ElectionPackageError.invalid("Manifest payload inventory")
        }
        return entry
    }

    static let countyYears = Array(stride(from: 2000, through: 2024, by: 4))
}

enum ElectionPackageError: LocalizedError {
    case unsupportedSchema(Int)
    case invalid(String)
    case checksum
    case missing
    case version

    var errorDescription: String? {
        switch self {
        case .unsupportedSchema(let value): "Unsupported election schema \(value)."
        case .invalid(let detail): "Invalid election package. \(detail)."
        case .checksum: "Election package failed SHA-256 or size validation."
        case .missing: "Election package is unavailable offline."
        case .version: "County results require the matching national dataset version."
        }
    }
}

struct ElectionIdentity: Codable, Sendable {
    let id: String
    let name: String
}

struct NationalElectionPackage: Codable, Sendable {
    let candidates: [ElectionIdentity]
    let parties: [ElectionIdentity]
    let elections: [ElectionRecord]
}

struct ElectionRecord: Codable, Sendable {
    struct Geography: Codable, Sendable {
        let sourceID: String
        let snapshotDate: String
        let status: String
    }
    struct Totals: Codable, Sendable {
        let reported: Int
        let nationalRows: Int
        let allocationRows: Int
    }
    let id: String
    let year: Int
    let winnerID: String
    let decision: String
    let rules: String
    let reportedMajority: Int
    let totals: Totals
    let geography: Geography
    struct ReviewNote: Codable, Sendable {
        let kind: String
        let jurisdiction: String?
        let text: String
        let votes: Int?
        let source: String
    }

    let results: [NationalVoteRecord]
    let reviewNotes: [ReviewNote]
    var allocations: [ElectoralAllocation]
}

struct NationalVoteRecord: Codable, Sendable {
    let candidateID: String?
    let name: String
    let partyID: String?
    let kind: String
    let sourceParty: String
    let sourceStyle: String
    let electoralVotes: Int
    let popularVotes: Int?
    let sourcePopularVotes: Int?
    let winner: Bool
    let sourceIndex: Int
}

struct ElectoralAllocation: Codable, Sendable {
    let id: String
    let jurisdictionID: String
    let stateName: String
    let sourceLabel: String
    let sourceName: String
    let sourceStyle: String
    let candidateID: String?
    let partyID: String?
    let electoralVotes: Int
    let kind: String
    let part: String?
    let status: String
    let geometryKey: String?
    let sourceLine: Int
}

struct CountyElectionPackage: Codable, Sendable {
    let year: Int
    let localResults: [LocalElectionRecord]
}

struct LocalElectionRecord: Codable, Sendable {
    struct Vote: Codable, Sendable {
        let candidateID: String?
        let name: String
        let partyID: String?
        let sourceParty: String
        let votes: Int
        let share: Double?
        let sourceShare: Double
        let sourceRank: Int
        let sourceMargin: Double
    }
    struct ReportingReview: Codable, Sendable {
        let source: String
        let reportedVotes: Int
    }

    let id: String
    let jurisdictionID: String
    let stateCode: String
    let stateName: String
    let name: String
    let sourceCode: String
    let kind: String
    let geometryKey: String?
    let geographySourceID: String?
    let joinStatus: String
    let status: String
    let winnerID: String?
    let totalVotes: Int
    let listedVotes: Int
    let marginVotes: Int?
    let results: [Vote]
    let reportingReview: ReportingReview?
}

enum ElectionPayloadValidation {
    static func hash(_ data: Data) -> String {
        SHA256.hash(data: data).map { String(format: "%02x", $0) }.joined()
    }

    static func check(_ data: Data, against entry: ElectionManifest.Payload) throws {
        guard data.count == entry.bytes, hash(data) == entry.sha256 else { throw ElectionPackageError.checksum }
    }

    static func national(_ data: Data, count: Int) throws -> NationalElectionPackage {
        let package = try JSONDecoder().decode(NationalElectionPackage.self, from: data)
        let candidates = Set(package.candidates.map(\.id))
        let parties = Set(package.parties.map(\.id))
        guard candidates.count == package.candidates.count, parties.count == package.parties.count,
              package.elections.count == count, count == 60,
              Set(package.elections.map(\.year)) == Set(stride(from: 1788, through: 2024, by: 4)) else {
            throw ElectionPackageError.invalid("National coverage or duplicate identity")
        }
        for election in package.elections {
            guard election.id == "presidential:\(election.year)", candidates.contains(election.winnerID),
                  election.results.filter(\.winner).map(\.candidateID) == [election.winnerID],
                  Set(election.allocations.map(\.id)).count == election.allocations.count,
                  election.results.allSatisfy({ (0...2_000).contains($0.electoralVotes) && ($0.popularVotes ?? 0) >= 0
                      && ($0.candidateID.map(candidates.contains) ?? ($0.kind != "candidate"))
                      && ($0.partyID.map(parties.contains) ?? true) }),
                  election.allocations.allSatisfy({ (0...2_000).contains($0.electoralVotes)
                      && ($0.candidateID.map(candidates.contains) ?? ($0.electoralVotes == 0))
                      && ($0.partyID.map(parties.contains) ?? true) }),
                  election.totals.allocationRows == election.allocations.reduce(0, { $0 + $1.electoralVotes }),
                  election.totals.nationalRows == election.results.reduce(0, { $0 + $1.electoralVotes }) else {
                throw ElectionPackageError.invalid("National result or allocation in \(election.year)")
            }
        }
        return package
    }

    static func county(_ data: Data, year: Int, count: Int, candidates: Set<String>, parties: Set<String>) throws
        -> CountyElectionPackage {
        let package = try JSONDecoder().decode(CountyElectionPackage.self, from: data)
        let rows = package.localResults
        let keys = rows.compactMap(\.geometryKey)
        guard package.year == year, rows.count == count, Set(rows.map(\.id)).count == rows.count,
              Set(keys).count == keys.count else {
            throw ElectionPackageError.invalid("County coverage or duplicate identifier")
        }
        for row in rows {
            let ids = row.results.compactMap(\.candidateID)
            guard (0...1_000_000_000).contains(row.totalVotes), (0...1_000_000_000).contains(row.listedVotes),
                  Set(ids).isSubset(of: candidates), ids.count == Set(ids).count,
                  row.results.allSatisfy({ (0...1_000_000_000).contains($0.votes) && ($0.partyID.map(parties.contains) ?? true)
                      && ($0.share.map { (0...1).contains($0) } ?? true) }),
                  row.listedVotes == row.results.reduce(0, { $0 + $1.votes }),
                  row.status != "reported" || (row.totalVotes > 0 && row.totalVotes == row.listedVotes
                      && row.winnerID != nil && row.winnerID == row.results.first?.candidateID),
                  row.winnerID == nil || row.status == "reported" else {
                throw ElectionPackageError.invalid("County result \(row.id)")
            }
        }
        return package
    }
}
