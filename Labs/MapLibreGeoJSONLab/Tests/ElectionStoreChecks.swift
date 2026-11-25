import Foundation

@main
struct ElectionStoreChecks {
    enum Failure: Error { case check(String), interrupted }

    static func check(_ condition: Bool, _ message: String) throws {
        if !condition { throw Failure.check(message) }
    }

    static func reject(_ label: String, _ operation: () async throws -> Void) async throws {
        do { try await operation() }
        catch { print("PASS \(label): \(type(of: error))"); return }
        throw Failure.check("Accepted \(label)")
    }

    static func measure<T>(_ label: String, _ operation: () async throws -> T) async rethrows -> T {
        let start = ProcessInfo.processInfo.systemUptime
        let value = try await operation()
        print("\(label): \(((ProcessInfo.processInfo.systemUptime - start) * 1000).formatted(.number.precision(.fractionLength(1)))) ms")
        return value
    }

    static func main() async throws {
        let directory = URL(fileURLWithPath: CommandLine.arguments[1], isDirectory: true)
        let work = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString, isDirectory: true)
        try FileManager.default.createDirectory(at: work, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: work) }
        let storeURL = work.appendingPathComponent("checks.sqlite")
        let manifestData = try Data(contentsOf: directory.appendingPathComponent("manifest.json"))
        let manifest = try JSONDecoder().decode(ElectionManifest.self, from: manifestData)
        let national = try Data(contentsOf: directory.appendingPathComponent("national.json"))
        try await firstInstall(directory, storeURL: storeURL, manifest: manifest, manifestData: manifestData, national: national)
        try await offlineReopen(storeURL)
    }

    static func firstInstall(_ directory: URL, storeURL: URL, manifest: ElectionManifest,
                             manifestData: Data, national: Data) async throws {
        let store = try await measure("open empty SwiftData") { try ElectionStore.open(at: storeURL) }
        try await measure("national bundle import") { try await store.importBundled(directory, year: nil) }
        try await measure("duplicate national import") { try await store.importBundled(directory, year: nil) }
        let early = try await store.election(1788)
        let jefferson = try await store.election(1800)
        let adams = try await store.election(1824)
        let lincoln = try await store.election(1864)
        let split = try await store.election(2024)
        try check(early.rules == "twoUndifferentiatedVotes" && early.totals.allocationRows == 69, "Early ballot context")
        try check(jefferson.decision == "houseContingent" && adams.results.first?.winner == false
            && adams.winnerID == "john-quincy-adams", "House decisions")
        try check(lincoln.results.first?.partyID == "national-union", "Lincoln ticket identity")
        try check(split.allocations.filter { $0.kind == "congressionalDistrict" }.count == 5, "ME and NE splits")
        try check(split.geography.snapshotDate == "2000-11-07", "State fallback")
        let initiallyInstalled = try await store.isInstalled(year: 2024)
        try check(!initiallyInstalled, "County installs are lazy")
        let originalNationalKey = try await store.presentationVersion(countyYear: nil)
        let counts = [3152, 3153, 3153, 3153, 3153, 3155, 3154]
        let base = URL(string: "https://fixture.invalid/0.1.0")!
        for (year, count) in zip(ElectionManifest.countyYears, counts) {
            try await measure("county \(year) activation") {
                if year == 2024 {
                    try await store.downloadPackage(from: base, manifestSHA256: ElectionPayloadValidation.hash(manifestData),
                                                    year: year) { url in
                        try Data(contentsOf: directory.appendingPathComponent(url.lastPathComponent))
                    }
                } else {
                    try await store.importBundled(directory, year: year)
                }
            }
            let rows = try await measure("county \(year) query") { try await store.county(year) }
            try check(rows.count == count, "County count \(year)")
            try check(rows.filter { $0.stateCode == "09" && $0.geometryKey != nil }.count == 8, "CT county count")
            let alaska = rows.filter { $0.stateCode == "02" }
            try check(alaska.filter { $0.geometryKey != nil }.count == 40, "AK district count")
            try check(alaska.first { $0.name == "District 99" }?.geometryKey == nil, "District 99 has no polygon")
        }
        let originalCountyKey = try await store.presentationVersion(countyYear: 2024)
        try check(originalCountyKey.count == 2 && originalCountyKey.first == originalNationalKey.first,
                  "County presentation key includes both active releases")
        try await measure("duplicate county import") { try await store.importBundled(directory, year: 2024) }
        let duplicateCountyKey = try await store.presentationVersion(countyYear: 2024)
        try check(duplicateCountyKey == originalCountyKey, "Duplicate activation preserves presentation key")
        try await reject("interrupted download") {
            try await store.downloadPackage(from: base, manifestSHA256: ElectionPayloadValidation.hash(manifestData),
                                            year: nil) { url in
                guard url.lastPathComponent == "manifest.json" else { throw Failure.interrupted }
                return manifestData
            }
        }
        try await reject("truncated payload") {
            try await store.activate(manifest, year: nil, data: national.dropLast())
        }
        var altered = national
        altered[0] = 32
        try await reject("same-size checksum mismatch") {
            try await store.activate(manifest, year: nil, data: altered)
        }
        let invalid = Data(#"{"elections":]"#.utf8)
        let invalidPayload = ElectionManifest.Payload(file: "national.json",
            sha256: ElectionPayloadValidation.hash(invalid), bytes: invalid.count, records: 60, year: nil)
        let replacement = ElectionManifest(schemaVersion: 1, datasetVersion: "0.1.1",
            payloads: manifest.payloads.map { $0.year == nil ? invalidPayload : $0 },
            geographySources: manifest.geographySources, inputHashes: manifest.inputHashes,
            countySourceMetadata: manifest.countySourceMetadata)
        try await reject("invalid JSON with a valid checksum") {
            try await store.activate(replacement, year: nil, data: invalid)
        }
        let unsupported = ElectionManifest(schemaVersion: 2, datasetVersion: manifest.datasetVersion,
            payloads: manifest.payloads, geographySources: manifest.geographySources,
            inputHashes: manifest.inputHashes, countySourceMetadata: manifest.countySourceMetadata)
        try await reject("unsupported schema") { try await store.activate(unsupported, year: nil, data: national) }
        let invalidYear = try await store.election(1824)
        try check(invalidYear.winnerID == adams.winnerID, "Failed replacement retained the original national package")

        let validReplacement = ElectionManifest(schemaVersion: 1, datasetVersion: "0.1.2",
            payloads: manifest.payloads, geographySources: manifest.geographySources,
            inputHashes: manifest.inputHashes, countySourceMetadata: manifest.countySourceMetadata)
        try await store.activate(validReplacement, year: nil, data: national)
        let wrongCountyVersion = try await store.isInstalled(year: 2024)
        try check(!wrongCountyVersion, "County versions never mix")
        let replacementNationalKey = try await store.presentationVersion(countyYear: nil)
        try check(replacementNationalKey != originalNationalKey, "Replacement invalidates cached presentations")
        try await reject("county presentation key from the previous release") {
            _ = try await store.presentationVersion(countyYear: 2024)
        }
        try await store.activate(manifest, year: nil, data: national)
        let countyFromRetainedRelease = try await store.isInstalled(year: 2024)
        try check(countyFromRetainedRelease, "Previously installed county data restored with its national version")
        let restoredKey = try await store.presentationVersion(countyYear: 2024)
        try check(restoredKey == originalCountyKey, "Rollback restores presentation key")
        print("PASS all releases, duplicate import, rollback and version isolation")
    }

    static func offlineReopen(_ url: URL) async throws {
        let store = try await measure("offline reopen SwiftData") { try ElectionStore.open(at: url) }
        let election = try await measure("offline 2024 election query") { try await store.election(2024) }
        let results = try await measure("offline 2024 county query") { try await store.county(2024) }
        try check(election.year == 2024 && results.count == 3154, "Persisted offline data")
        print("PASS offline reopen without a package directory")
    }
}
