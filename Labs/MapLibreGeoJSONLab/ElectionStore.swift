import Foundation
import SwiftData

@Model
final class InstalledElectionPackage {
    @Attribute(.unique) var id: String
    var slot: Int
    var version: String
    var contentSHA256: String
    var manifestIdentity: String
    var active: Bool

    init(id: String, slot: Int, version: String, hash: String, manifestIdentity: String) {
        self.id = id
        self.slot = slot
        self.version = version
        self.contentSHA256 = hash
        self.manifestIdentity = manifestIdentity
        self.active = true
    }
}

@Model
final class StoredElectionRow {
    var packageID: String
    var year: Int
    var kind: String
    var ordinal: Int
    var data: Data

    init(packageID: String, year: Int, kind: String, ordinal: Int, data: Data) {
        self.packageID = packageID
        self.year = year
        self.kind = kind
        self.ordinal = ordinal
        self.data = data
    }
}

@ModelActor
actor ElectionStore {
    static func open(at url: URL) throws -> ElectionStore {
        let schema = Schema([InstalledElectionPackage.self, StoredElectionRow.self])
        let container = try ModelContainer(for: schema, configurations: ModelConfiguration(schema: schema, url: url))
        return ElectionStore(modelContainer: container)
    }

    func isInstalled(year: Int?) throws -> Bool {
        let version = year == nil ? nil : try active(slot: 0)?.version
        guard year == nil || version != nil else { return false }
        return try active(slot: year ?? 0, version: version) != nil
    }

    func importBundled(_ directory: URL, year: Int?) throws {
        if try isInstalled(year: year) { return }
        let manifest = try JSONDecoder().decode(ElectionManifest.self,
            from: Data(contentsOf: directory.appendingPathComponent("manifest.json")))
        let entry = try manifest.payload(year: year)
        try activate(manifest, year: year, data: Data(contentsOf: directory.appendingPathComponent(entry.file)))
    }

    func downloadPackage(from directory: URL, manifestSHA256: String, year: Int?,
                         geometry: [ElectionManifest.GeographicSource]? = nil,
                         fetch: @Sendable (URL) async throws -> Data = { try await ElectionStore.fetchHTTPS($0) }) async throws {
        let manifestData = try await fetch(directory.appendingPathComponent("manifest.json"))
        guard ElectionPayloadValidation.hash(manifestData) == manifestSHA256 else {
            throw ElectionPackageError.checksum
        }
        let manifest = try JSONDecoder().decode(ElectionManifest.self, from: manifestData)
        let entry = try manifest.payload(year: year)
        if let geometry, geometry != manifest.geographySources {
            throw ElectionPackageError.invalid("Update requires different geographic source files")
        }
        let data = try await fetch(directory.appendingPathComponent(entry.file))
        try Task.checkCancellation()
        try activate(manifest, year: year, data: data)
    }

    static func fetchHTTPS(_ url: URL) async throws -> Data {
        guard url.scheme == "https" else { throw ElectionPackageError.invalid("HTTPS URL required") }
        let (data, response) = try await URLSession.shared.data(from: url)
        guard let http = response as? HTTPURLResponse, http.url?.scheme == "https",
              http.statusCode == 200 else { throw ElectionPackageError.invalid("Package HTTP response") }
        return data
    }

    func activate(_ manifest: ElectionManifest, year: Int?, data: Data) throws {
        let entry = try manifest.payload(year: year)
        try ElectionPayloadValidation.check(data, against: entry)
        let slot = year ?? 0
        let version = manifest.datasetVersion
        let packageID = "\(version):\(slot)"
        let manifestIdentity = try manifest.identity()
        if year != nil {
            guard let national = try active(slot: 0), national.version == version,
                  national.manifestIdentity == manifestIdentity else { throw ElectionPackageError.version }
        }
        if let previous = try modelContext.fetch(FetchDescriptor<InstalledElectionPackage>(
            predicate: #Predicate { $0.id == packageID })).first,
           previous.contentSHA256 != entry.sha256 || previous.manifestIdentity != manifestIdentity {
            throw ElectionPackageError.invalid("Dataset version already identifies different bytes")
        }
        let prior = try modelContext.fetch(FetchDescriptor<InstalledElectionPackage>(
            predicate: #Predicate { $0.slot == slot }))
        if prior.contains(where: { $0.id == packageID && $0.active }) { return }

        modelContext.autosaveEnabled = false
        do {
            if let previous = prior.first(where: { $0.id == packageID }) {
                prior.forEach { $0.active = $0.id == previous.id }
            } else {
                if let year {
                    let (candidates, parties) = try catalog()
                    let package = try ElectionPayloadValidation.county(data, year: year, count: entry.records,
                        candidates: Set(candidates.map(\.id)), parties: Set(parties.map(\.id)))
                    try insert(package.localResults, packageID: packageID, year: year, kind: "county")
                } else {
                    let package = try ElectionPayloadValidation.national(data, count: entry.records)
                    try insert(package.candidates, packageID: packageID, year: 0, kind: "candidate")
                    try insert(package.parties, packageID: packageID, year: 0, kind: "party")
                    for var election in package.elections {
                        try insert(election.allocations, packageID: packageID, year: election.year, kind: "allocation")
                        election.allocations = []
                        try insert([election], packageID: packageID, year: election.year, kind: "election")
                    }
                }
                prior.forEach { $0.active = false }
                modelContext.insert(InstalledElectionPackage(id: packageID, slot: slot, version: version,
                                                             hash: entry.sha256, manifestIdentity: manifestIdentity))
            }
            try Task.checkCancellation()
            try modelContext.save()
        } catch {
            modelContext.rollback()
            throw error
        }
    }

    func election(_ year: Int) throws -> ElectionRecord {
        guard let package = try active(slot: 0),
              var election = try read(ElectionRecord.self, package.id, year, "election").first else {
            throw ElectionPackageError.missing
        }
        election.allocations = try read(ElectoralAllocation.self, package.id, year, "allocation")
        return election
    }

    func county(_ year: Int) throws -> [LocalElectionRecord] {
        guard let national = try active(slot: 0), let package = try active(slot: year, version: national.version) else {
            throw ElectionPackageError.missing
        }
        return try read(LocalElectionRecord.self, package.id, year, "county")
    }

    func catalog() throws -> ([ElectionIdentity], [ElectionIdentity]) {
        guard let package = try active(slot: 0) else { throw ElectionPackageError.missing }
        return (try read(ElectionIdentity.self, package.id, 0, "candidate"),
                try read(ElectionIdentity.self, package.id, 0, "party"))
    }

    private func active(slot: Int, version: String? = nil) throws -> InstalledElectionPackage? {
        // County cache entries are scoped to the active national publication, including after rollback.
        if let version {
            let identity = "\(version):\(slot)"
            return try modelContext.fetch(FetchDescriptor<InstalledElectionPackage>(
                predicate: #Predicate { $0.id == identity })).first
        }
        return try modelContext.fetch(FetchDescriptor<InstalledElectionPackage>(
            predicate: #Predicate { $0.slot == slot && $0.active })).first
    }

    private func insert<T: Encodable>(_ rows: [T], packageID: String, year: Int, kind: String) throws {
        let encoder = JSONEncoder()
        for (index, row) in rows.enumerated() {
            if index.isMultiple(of: 128) { try Task.checkCancellation() }
            modelContext.insert(StoredElectionRow(packageID: packageID, year: year, kind: kind,
                                                  ordinal: index, data: try encoder.encode(row)))
        }
    }

    private func read<T: Decodable>(_ type: T.Type, _ packageID: String, _ year: Int, _ kind: String) throws -> [T] {
        let rows = try modelContext.fetch(FetchDescriptor<StoredElectionRow>(
            predicate: #Predicate { $0.packageID == packageID && $0.year == year && $0.kind == kind },
            sortBy: [SortDescriptor(\.ordinal)]))
        let decoder = JSONDecoder()
        return try rows.map { try decoder.decode(T.self, from: $0.data) }
    }
}
