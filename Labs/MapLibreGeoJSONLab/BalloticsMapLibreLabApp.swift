import MapLibre
import SwiftUI
import UniformTypeIdentifiers

struct Dataset {
    let geography: Geography
    let url: URL
    let summary: GeoJSON.Summary
    let requestedAt: Double
    let imported: ImportedFile?
    var historical: HistoricalMapFile? = nil

    var albersKey: AlbersCache.Key {
        historical.map { .snapshot($0.manifest.outputSHA256) } ?? .bundled(url)
    }
}

@MainActor
final class AlbersCache {
    enum Key: Equatable {
        case bundled(URL)
        case snapshot(String)
    }

    private var entries: [(Key, AlbersScene, Int)] = []
    private let maximumPositions = 700_000

    func take(_ key: Key) -> AlbersScene? {
        guard let index = entries.firstIndex(where: { $0.0 == key }) else { return nil }
        let entry = entries.remove(at: index)
        entries.append(entry)
        return entry.1
    }

    func put(_ scene: AlbersScene, at key: Key, positions: Int) {
        guard positions <= maximumPositions else { return }
        entries.removeAll { $0.0 == key }
        entries.append((key, scene, positions))
        while entries.count > 2 || entries.reduce(0, { $0 + $1.2 }) > maximumPositions {
            entries.removeFirst()
        }
    }

    func clear() { entries.removeAll() }
}

struct Inspection {
    let title: String
    let properties: [(key: String, value: String)]
    let stateCode: String?

    init(title: String, properties: [(String, String)], stateCode: String?) {
        self.title = title
        self.properties = properties
        self.stateCode = stateCode
    }

    init(attributes: [String: Any]) {
        let fields = attributes.compactMap { key, value -> (key: String, value: String)? in
            if let string = value as? String { return (key, string) }
            if let number = value as? NSNumber { return (key, number.stringValue) }
            return nil
        }
        let title = ["NAMELSAD", "FULL_NAME", "NAME", "name", "GEOID"]
            .compactMap { attributes[$0] as? String }.first(where: { !$0.isEmpty }) ?? "Selected polygon"
        let priority = ["GEOID", "STATE_NAME", "STATE_TERR", "STUSPS", "STATEFP", "NAMELSAD", "NAME",
                        "TERR_TYPE", "ID_NUM", "ID", "VERSION", "START_DATE", "END_DATE",
                        "CD119FP", "COUNTYFP", "LSAD", "CNTY_TYPE", "FIPS", "CHANGE", "CITATION"]
        let sorted = fields.sorted {
            let lhs = priority.firstIndex(of: $0.key) ?? priority.count
            let rhs = priority.firstIndex(of: $1.key) ?? priority.count
            return lhs == rhs ? $0.key < $1.key : lhs < rhs
        }
        self.title = title
        self.properties = sorted
        self.stateCode = attributes["STATEFP"] as? String
    }
}

enum Viewpoint: Equatable {
    case national, alaska, hawaii, selectedState, selectedFeature
}

enum LabRenderer: String, CaseIterable {
    case coreGraphics = "Albers Core Graphics"
    case metal = "Albers Metal"
    case mapLibre = "MapLibre"

    static var initial: Self {
        let arguments = ProcessInfo.processInfo.arguments
        if arguments.contains("--albers-metal") { return .metal }
        return arguments.contains("--albers") ? .coreGraphics : .mapLibre
    }
}

@MainActor
final class LabState: ObservableObject {
    @Published private(set) var geography: Geography = .districts
    @Published private(set) var dataset: Dataset?
    @Published private(set) var imported: ImportedFile?
    @Published var status = "Preparing local geometry…"
    @Published var inspection: Inspection?
    @Published var failure: String?
    @Published private(set) var importing = false
    @Published private(set) var cameraRequest: (Int, Viewpoint) = (0, .national)
    @Published private(set) var alternateColor = false

    @Published private(set) var historicalPending = false
    @Published private(set) var historicalRequest: HistoricalRequest?
    @Published private(set) var election: PresidentialElection?
    @Published private(set) var electionPresentation: ElectionMapPresentation?
    @Published private(set) var electionRevision = 0
    @Published private(set) var renderer = LabRenderer.initial
    var usesAlbers: Bool { renderer != .mapLibre }
    @Published var albersTheme: AlbersTheme = .candidates
    @Published var thickAlbersBorders = false
    @Published private(set) var albersStudyProbe = 0
    @Published private(set) var dismissedStudyMap = false
    let baselineAlbers = ProcessInfo.processInfo.arguments.contains("--albers-baseline")
    let albersCache = AlbersCache()
    let elections = PresidentialElection.available().filter { $0.year <= 2024 }
    private var electionStoreTask: Task<ElectionStore, Error>?
    private var countySource: Dataset?
    private var attemptedRemoteNational = false
    private struct CachedResult {
        let year: Int
        let mode: ElectionMapMode
        let version: [String]
        let record: ElectionRecord
        let presentation: ElectionMapPresentation
    }
    private var cachedResults: [CachedResult] = []
    private var optimizesAlbers: Bool { usesAlbers && !baselineAlbers }

    var electionMode: ElectionMapMode { electionPresentation?.mode ?? .electoralCollege }

    func inspect(_ attributes: [String: Any]) {
        inspection = geography == .presidential
            ? electionPresentation?.inspection(attributes) ?? Inspection(attributes: attributes)
            : Inspection(attributes: attributes)
    }

    private var remotePackage: (URL, String)? {
        let arguments = ProcessInfo.processInfo.arguments
        guard let flag = arguments.firstIndex(of: "--election-package"),
              let resource = arguments.dropFirst(flag + 1).first,
              let sha = arguments.dropFirst(flag + 2).first,
              let url = URL(string: resource), url.scheme == "https" else { return nil }
        return (url, sha)
    }

    private func openElectionStore() async throws -> ElectionStore {
        if let electionStoreTask { return try await electionStoreTask.value }
        let url = Self.documents.appendingPathComponent("PresidentialResults.sqlite")
        let work = Task.detached(priority: .userInitiated) {
            try FileManager.default.createDirectory(at: url.deletingLastPathComponent(), withIntermediateDirectories: true)
            return try ElectionStore.open(at: url)
        }
        electionStoreTask = work
        do { return try await work.value }
        catch {
            electionStoreTask = nil
            throw error
        }
    }
    private var history: HistoricalStore?
    private var electionHistory: HistoricalStore?

    static var documents: URL {
        FileManager.default.urls(for: .documentDirectory, in: .userDomainMask)[0]
    }

    var hasHistoricalArchive: Bool {
        history != nil || FileManager.default.fileExists(
            atPath: Self.documents.appendingPathComponent(HistoricalArchive.filename).path)
    }

    private var validations: [Geography: GeoJSON.Summary] = [:]
    private var loading: Task<Void, Never>?

    var canZoomToSelection: Bool {
        guard geography == .states, let stateCode = inspection?.stateCode else { return false }
        return dataset?.summary.stateBounds[stateCode] != nil
    }

    func start() {
        guard dataset == nil, loading == nil else { return }
        let arguments = ProcessInfo.processInfo.arguments
        if renderer == .metal, arguments.contains("--metal-fallback-probe"),
           let fixture = Bundle.main.url(forResource: "Lab-Metal-Fallback", withExtension: "geojson") {
            importGeoJSON(.success(fixture))
        } else if let index = arguments.firstIndex(of: "--election-year"),
           let year = arguments.dropFirst(index + 1).first.flatMap(Int.init),
           let election = elections.first(where: { $0.year == year }) {
            setAlbers(!arguments.contains("--election-maplibre"))
            applyElection(election, mode: arguments.contains("--county-results") ? .countyResults : .electoralCollege)
        } else if let index = arguments.firstIndex(where: { $0 == "--historical-date" || $0 == "--published-date" }),
           let value = arguments.dropFirst(index + 1).first {
            do {
                let request = try HistoricalRequest(value, provenance: "Benchmark snapshot")
                applyHistory(request, published: arguments[index] == "--published-date")
            } catch {
                failure = error.localizedDescription
            }
        } else if let index = arguments.firstIndex(of: "--dataset"),
           let name = arguments.dropFirst(index + 1).first,
           let geography = Geography.allCases.first(where: { $0.rawValue.lowercased() == name.lowercased() }),
           geography.resource != nil {
            choose(geography)
        } else if arguments.contains("--historical"), let folder = FileManager.default.urls(
            for: .documentDirectory, in: .userDomainMask
        ).first {
            importGeoJSON(.success(folder.appendingPathComponent("US_HistCounties_Gen05.json")))
        } else {
            choose(.districts)
        }
    }

    func choose(_ next: Geography) {
        if next == .historical {
            applyHistory(historicalRequest ?? HistoricalRequest.presets[0])
            return
        }
        if next == .presidential {
            if geography != .presidential {
                setAlbers(!ProcessInfo.processInfo.arguments.contains("--election-maplibre"))
            }
            if let election = election ?? elections.first { applyElection(election, mode: electionMode) }
            return
        }
        guard geography != next || dataset?.geography != next || historicalPending else { return }
        historicalPending = false
        let requestedAt = LabLog.now
        if next == .imported {
            guard let imported else { return }
            loading?.cancel()
            loading = nil
            prepareSwitch(to: next)
            dataset = Dataset(geography: next, url: imported.url, summary: imported.summary,
                              requestedAt: requestedAt, imported: imported)
            return
        }

        guard let name = next.resource, let url = Bundle.main.url(forResource: name, withExtension: "json") else {
            failure = "The bundled \(next.rawValue.lowercased()) file is missing."
            return
        }
        loading?.cancel()
        prepareSwitch(to: next)
        if let summary = validations[next] {
            loading = nil
            dataset = Dataset(geography: next, url: url, summary: summary,
                              requestedAt: requestedAt, imported: nil)
            return
        }

        var baseline = MemorySample.read()?.fields("before_validation") ?? [:]
        baseline["dataset"] = next.rawValue
        LabLog.record("validation_start", baseline)
        loading = Task {
            do {
                let summary = try await prepareGeometry(url, geography: next)
                try Task.checkCancellation()
                validations[next] = summary
                var fields = MemorySample.read()?.fields("after_validation") ?? [:]
                fields["dataset"] = next.rawValue
                fields["validation_ms"] = LabLog.milliseconds(LabLog.now - requestedAt)
                LabLog.record("validation_end", fields)
                dataset = Dataset(geography: next, url: url, summary: summary,
                                  requestedAt: requestedAt, imported: nil)
                loading = nil
            } catch is CancellationError {
                return
            } catch {
                guard !Task.isCancelled else { return }
                status = "Dataset unavailable"
                failure = error.localizedDescription
                dataset = nil
                loading = nil
            }
        }
    }

    func setAlbers(_ enabled: Bool) {
        if enabled, usesAlbers { return }
        setRenderer(enabled ? (LabRenderer.initial == .metal ? .metal : .coreGraphics) : .mapLibre)
    }

    func setRenderer(_ renderer: LabRenderer) {
        guard self.renderer != renderer else { return }
        self.renderer = renderer
        inspection = nil
        if renderer == .mapLibre {
            albersCache.clear()
            cachedResults.removeAll()
        }
    }

    func releaseUnusedAlbersResources() {
        albersCache.clear()
        cachedResults.removeAll()
    }

    func runAlbersStress() async {
        let arguments = ProcessInfo.processInfo.arguments
        guard LabLog.enabled, arguments.contains("--albers-stress") else { return }
        let previouslyDisabled = UIApplication.shared.isIdleTimerDisabled
        UIApplication.shared.isIdleTimerDisabled = true
        defer { UIApplication.shared.isIdleTimerDisabled = previouslyDisabled }
        let seconds = arguments.firstIndex(of: "--albers-seconds")
            .flatMap { arguments.dropFirst($0 + 1).first }.flatMap(Double.init) ?? 1800
        let started = LabLog.now
        var years = 0
        var sources = 0
        var rapid = 0
        var mismatches = 0
        var navigations = 0
        var nextSample = started
        var timedOut = 0
        var attemptedReopen = false
        setAlbers(true)
        LabLog.record("albers_stress_start", ["seconds": seconds])
        guard let first = elections.first(where: { $0.year == 2024 }),
              let second = elections.first(where: { $0.year == 2020 }) else { return }
        while LabLog.now - started < seconds && !Task.isCancelled {
            let window = LabLog.now
            if years < 100 {
                let winner = years.isMultiple(of: 2) ? first : second
                if years.isMultiple(of: 10) {
                    applyElection(winner.year == first.year ? second : first, mode: .countyResults)
                    rapid += 1
                }
                applyElection(winner, mode: .countyResults)
                if !(await awaitStudyPresentation()) { timedOut += 1 }
                if election?.year != winner.year || electionPresentation?.year != winner.year
                    || electionMode != .countyResults || dataset?.geography != .presidential {
                    mismatches += 1
                }
                years += 1
                LabLog.record("albers_stress_year", ["year": winner.year,
                    "li_ms": LabLog.milliseconds(LabLog.now - window), "mismatches": mismatches])
            } else if sources < 50 {
                let sourcesToTest: [Geography] = [.states, .districts, .counties, .historical]
                let expected = sourcesToTest[sources % sourcesToTest.count]
                if expected == .historical {
                    let preset = HistoricalRequest.presets[sources / sourcesToTest.count % HistoricalRequest.presets.count]
                    applyHistory(preset)
                } else {
                    choose(expected)
                }
                let ready = await awaitStudyPresentation()
                if !ready { timedOut += 1 }
                if dataset?.geography != expected { mismatches += 1 }
                sources += 1
                LabLog.record("albers_stress_source", ["sequence": sources, "source": expected.rawValue,
                    "ready": ready, "elapsed_ms": LabLog.milliseconds(LabLog.now - window)])
                if sources == 50 {
                    applyElection(first, mode: .countyResults)
                    if !(await awaitStudyPresentation()) { timedOut += 1 }
                }
            }
            if seconds >= 120, !attemptedReopen, sources >= 50, LabLog.now - started > seconds / 2 {
                attemptedReopen = true
                dismissedStudyMap = true
                releaseUnusedAlbersResources()
                LabLog.record("albers_stress_dismiss")
                try? await Task.sleep(for: .seconds(2))
                status = "Reopening projected geometry…"
                dismissedStudyMap = false
                let reopened = await awaitStudyPresentation()
                LabLog.record("albers_stress_reopen", ["ready": reopened])
                if !reopened { timedOut += 1 }
            }
            let views: [Viewpoint] = [.national, .alaska, .hawaii, .selectedState, .selectedFeature]
            moveCamera(views[navigations % views.count])
            albersStudyProbe += 1
            albersTheme = AlbersTheme.allCases[navigations % AlbersTheme.allCases.count]
            if navigations.isMultiple(of: 3) { thickAlbersBorders.toggle() }
            if LabLog.now >= nextSample {
                var fields = MemorySample.read()?.fields("stress") ?? [:]
                fields["years"] = years
                fields["sources"] = sources
                fields["rapid_requests"] = rapid
                fields["navigation_steps"] = navigations
                fields["mismatches"] = mismatches
                fields["timeouts"] = timedOut
                LabLog.record("albers_stress_checkpoint", fields)
                nextSample = LabLog.now + 15
            }
            navigations += 1
            do { try await Task.sleep(for: .milliseconds(years < 100 || sources < 50 ? 55 : 280)) }
            catch { return }
        }
        LabLog.record("albers_stress_done", ["years": years, "sources": sources, "mismatches": mismatches,
            "rapid_requests": rapid, "navigation_steps": navigations, "timeouts": timedOut,
            "elapsed_ms": LabLog.milliseconds(LabLog.now - started)])
        var retained = MemorySample.read()?.fields("before_release") ?? [:]
        retained["features"] = dataset?.summary.features ?? 0
        LabLog.record("albers_stress_release_start", retained)
        dismissedStudyMap = true
        releaseUnusedAlbersResources()
        dataset = nil
        electionPresentation = nil
        inspection = nil
        try? await Task.sleep(for: .seconds(3))
        LabLog.record("albers_stress_released", MemorySample.read()?.fields("after_release") ?? [:])
    }

    func runMetalHistoricalSupersessionProbe() async {
        guard LabLog.enabled, renderer == .metal,
              ProcessInfo.processInfo.arguments.contains("--historical-rapid-switches"),
              await awaitStudyPresentation() else { return }
        let sequence = [5, 2, 6].map { HistoricalRequest.presets[$0] }
        for request in sequence {
            applyHistory(request)
            guard request != sequence.last else { break }
            let deadline = LabLog.now + 3
            while LabLog.now < deadline && !Task.isCancelled {
                if dataset?.historical?.request.day == request.day,
                   status == "Preparing geographic mesh…" { break }
                do { try await Task.sleep(for: .milliseconds(3)) }
                catch { return }
            }
        }
        let ready = await awaitStudyPresentation()
        LabLog.record("metal_rapid_history_done", ["ready": ready,
            "expected": sequence.last?.day.description ?? "",
            "actual": dataset?.historical?.request.day.description ?? ""])
    }

    private func awaitStudyPresentation() async -> Bool {
        let deadline = LabLog.now + 15
        while LabLog.now < deadline && !Task.isCancelled {
            let ready = renderer == .metal ? status.contains("features · Albers Metal")
                : status.contains("Albers equal area")
            if !historicalPending && ready { return true }
            if status.hasPrefix("Core Graphics fallback") { return false }
            try? await Task.sleep(for: .milliseconds(25))
        }
        LabLog.record("albers_stress_wait_timeout", ["geography": geography.rawValue,
            "year": election?.year ?? 0, "pending": historicalPending])
        return false
    }

    private func prepareGeometry(_ url: URL, geography: Geography) async throws -> GeoJSON.Summary {
        let preproject = optimizesAlbers
        let sampled = MemorySampler()
        let preparationStarted = LabLog.now
        let work = Task.detached(priority: .userInitiated) {
            let start = LabLog.now
            let features = try GeoJSON.readFeatures(url)
            let decoded = LabLog.now
            let summary = try GeoJSON.validate(features, geography: geography)
            let validated = LabLog.now
            let scene = preproject ? try AlbersScene.project(features,
                decodeMilliseconds: LabLog.milliseconds(decoded - start)) : nil
            return (summary, scene, LabLog.milliseconds(decoded - start), LabLog.milliseconds(validated - decoded))
        }
        let (summary, scene, decode, validation) = try await withTaskCancellationHandler {
            try await work.value
        } onCancel: {
            work.cancel()
        }
        try Task.checkCancellation()
        if let scene, optimizesAlbers {
            albersCache.put(scene, at: .bundled(url), positions: summary.positions)
        }
        LabLog.record("geography_decode_validate", ["dataset": geography.rawValue,
            "decode_ms": decode, "validation_ms": validation, "projected_from_same_decode": scene != nil])
        if LabLog.enabled {
            LabLog.record("geography_preparation_memory", sampled.finish(event: "validation", since: preparationStarted))
        }
        return summary
    }

    func importGeoJSON(_ result: Result<URL, Error>) {
        guard !importing else { return }
        let requestedAt = LabLog.now
        let url: URL
        do {
            url = try result.get()
        } catch {
            failure = error.localizedDescription
            return
        }
        importing = true
        failure = nil
        var fields = MemorySample.read()?.fields("before_validation") ?? [:]
        fields["dataset"] = url.lastPathComponent
        LabLog.record("import_start", fields)
        Task {
            defer { importing = false }
            do {
                let file = try await Task.detached(priority: .userInitiated) { try ImportedFile.prepare(url) }.value
                loading?.cancel()
                loading = nil
                historicalPending = false
                imported = file
                prepareSwitch(to: .imported)
                var fields = MemorySample.read()?.fields("after_validation") ?? [:]
                fields["dataset"] = file.displayName
                fields["copy_validation_ms"] = LabLog.milliseconds(file.preparationSeconds)
                LabLog.record("import_end", fields)
                dataset = Dataset(geography: .imported, url: file.url, summary: file.summary,
                                  requestedAt: requestedAt, imported: file)
            } catch {
                failure = "Could not import \(url.lastPathComponent). \(error.localizedDescription)"
            }
        }
    }

    func importHistorical(_ result: Result<URL, Error>) {
        do {
            let source = try result.get()
            let candidate = HistoricalStore(source: source, copyFromFiles: true)
            applyHistory(historicalRequest ?? HistoricalRequest.presets[0], replacement: candidate)
        } catch {
            failure = error.localizedDescription
        }
    }

    func applyHistory(_ request: HistoricalRequest, published: Bool = false,
                      replacement: HistoricalStore? = nil) {
        let source = Self.documents.appendingPathComponent(HistoricalArchive.filename)
        let store = replacement ?? history ?? HistoricalStore(source: source)
        applySnapshot(request, store: store, published: published)
    }

    func applyElection(_ election: PresidentialElection) {
        let mode = ElectionManifest.countyYears.contains(election.year) ? electionMode : .electoralCollege
        applyElection(election, mode: mode)
    }

    func applyElection(_ election: PresidentialElection, mode: ElectionMapMode) {
        loading?.cancel()
        historicalPending = true
        failure = nil
        let started = LabLog.now
        LabLog.record("election_request", ["year": election.year, "mode": mode.rawValue])
        let resources = Bundle.main.bundleURL.appendingPathComponent("ElectionResearch")
        let bundle = resources.appendingPathComponent("0.1.0")
        loading = Task {
            do {
                LabLog.record("election_task_enter", ["year": election.year,
                    "queue_ms": LabLog.milliseconds(LabLog.now - started)])
                let store = try await openElectionStore()
                let bundledManifest = try JSONDecoder().decode(ElectionManifest.self,
                    from: Data(contentsOf: bundle.appendingPathComponent("manifest.json")))
                let installed = try await store.isInstalled(year: nil)
                try await store.importBundled(bundle, year: nil)
                if let (remote, sha) = remotePackage, !attemptedRemoteNational {
                    do {
                        try await store.downloadPackage(from: remote, manifestSHA256: sha, year: nil,
                                                        geometry: bundledManifest.geographySources)
                        attemptedRemoteNational = true
                    } catch is CancellationError { throw CancellationError() }
                    catch {
                        attemptedRemoteNational = true
                        LabLog.record("election_update_rejected", ["scope": "national"])
                    }
                }
                LabLog.record("election_store_ready", ["installed": installed,
                    "elapsed_ms": LabLog.milliseconds(LabLog.now - started)])
                let queryStart = LabLog.now
                if mode == .countyResults {
                    if try await !store.isInstalled(year: election.year) {
                        let importStart = LabLog.now
                        if let (remote, sha) = remotePackage {
                            do {
                                try await store.downloadPackage(from: remote, manifestSHA256: sha, year: election.year,
                                                                geometry: bundledManifest.geographySources)
                            }
                            catch is CancellationError { throw CancellationError() }
                            catch { try await store.importBundled(bundle, year: election.year) }
                        } else {
                            try await store.importBundled(bundle, year: election.year)
                        }
                        LabLog.record("election_county_import", ["year": election.year,
                            "elapsed_ms": LabLog.milliseconds(LabLog.now - importStart)])
                    }
                }
                let version = optimizesAlbers
                    ? try await store.presentationVersion(countyYear: mode == .countyResults ? election.year : nil)
                    : []
                let cached = cachedResults.first {
                    optimizesAlbers && $0.year == election.year && $0.mode == mode && $0.version == version
                }
                let record: ElectionRecord
                let presentation: ElectionMapPresentation
                if let cached {
                    record = cached.record
                    presentation = cached.presentation
                } else {
                    record = try await store.election(election.year)
                    let catalog = try await store.catalog()
                    let county = mode == .countyResults ? try await store.county(election.year) : []
                    let paintStart = LabLog.now
                    presentation = ElectionMapPresentation.make(election: record, mode: mode, catalog: catalog, county: county)
                    LabLog.record("election_presentation_ready", ["year": election.year,
                        "elapsed_ms": LabLog.milliseconds(LabLog.now - paintStart)])
                }
                try Task.checkCancellation()
                if optimizesAlbers {
                    cachedResults.removeAll { $0.year == election.year && $0.mode == mode }
                    cachedResults.append(CachedResult(year: election.year, mode: mode, version: version,
                                                      record: record, presentation: presentation))
                    if cachedResults.count > 2 { cachedResults.removeFirst() }
                }
                LabLog.record("election_query_ready", ["year": election.year, "mode": mode.rawValue,
                    "elapsed_ms": LabLog.milliseconds(LabLog.now - queryStart), "presentation_cached": cached != nil])
                let previous = self.dataset
                let resolved: Dataset
                let geometryStart = LabLog.now
                if mode == .countyResults {
                    if let countySource {
                        resolved = countySource
                    } else {
                        let url = resources.appendingPathComponent("county-geometry-2024.geojson")
                        let summary = try await prepareGeometry(url, geography: .presidential)
                        resolved = Dataset(geography: .presidential, url: url, summary: summary,
                                           requestedAt: geometryStart, imported: nil)
                        countySource = resolved
                    }
                } else {
                    let request = try ElectionGeographyResolver.resolveState(election, record: record)
                    if let previous, previous.geography == .presidential, previous.historical?.request.day == request.day {
                        resolved = previous
                    } else {
                        guard let url = Bundle.main.url(forResource: PresidentialElection.archiveResource,
                                                        withExtension: "json") else { throw ElectionPackageError.missing }
                        let history = electionHistory ?? HistoricalStore(source: url)
                        let (file, _) = try await history.snapshot(request)
                        electionHistory = history
                        resolved = Dataset(geography: .presidential, url: file.url, summary: file.summary,
                                           requestedAt: geometryStart, imported: nil, historical: file)
                    }
                }
                try Task.checkCancellation()
                let reused = resolved.requestedAt == previous?.requestedAt
                LabLog.record("election_geography_ready", ["year": election.year, "mode": mode.rawValue,
                    "reused_source": reused, "elapsed_ms": LabLog.milliseconds(LabLog.now - geometryStart)])
                if !reused {
                    prepareSwitch(to: .presidential)
                    moveCamera(.national)
                }
                self.election = election
                electionPresentation = presentation
                electionRevision += 1
                inspection = nil
                dataset = resolved
                historicalPending = false
                loading = nil
                var fields = MemorySample.read()?.fields("election_switch") ?? [:]
                fields["year"] = election.year
                fields["mode"] = mode.rawValue
                fields["elapsed_ms"] = LabLog.milliseconds(LabLog.now - started)
                LabLog.record("election_switch", fields)
            } catch {
                guard !Task.isCancelled else { return }
                historicalPending = false
                failure = error.localizedDescription
                loading = nil
            }
        }
    }

    private func applySnapshot(_ request: HistoricalRequest, store: HistoricalStore,
                               published: Bool = false) {
        let requestedAt = LabLog.now
        let next: Geography = .historical
        let context: [String: Any] = ["date": request.day.description, "dataset": next.rawValue]
        // Keep the selected dataset and date paired until extraction succeeds.
        loading?.cancel()
        historicalPending = true
        failure = nil
        LabLog.record("historical_request", context.merging(["provenance": request.provenance]) { _, new in new })
        loading = Task {
            do {
                let (file, measurements): (HistoricalMapFile, [[String: Any]])
                if published {
                    let folder = Self.documents
                    let day = request.day
                    let result = try await Task.detached {
                        let peak = MemorySampler()
                        let start = LabLog.now
                        let url = folder.appendingPathComponent("historical-\(day).geojson")
                        let manifestURL = folder.appendingPathComponent("historical-\(day).manifest.json")
                        let (manifest, summary) = try HistoricalArchive.loadPublished(
                            geoJSON: url, manifestURL: manifestURL, date: day)
                        return (HistoricalMapFile(url: url, request: request, summary: summary,
                                                  manifest: manifest, temporary: false),
                                [peak.finish(event: "historical_published_read", since: start)])
                    }.value
                    (file, measurements) = result
                } else {
                    (file, measurements) = try await store.snapshot(request)
                }
                try Task.checkCancellation()
                if !published { history = store }
                historicalRequest = request
                for var fields in measurements {
                    guard let phase = fields.removeValue(forKey: "phase") as? String else { continue }
                    fields.merge(context) { _, new in new }
                    LabLog.record(phase, fields)
                }
                LabLog.record("historical_snapshot", context.merging([
                    "provenance": request.provenance,
                    "features": file.summary.features, "parts": file.summary.parts,
                    "multipart": file.summary.multipart, "rings": file.summary.rings,
                    "positions": file.summary.positions, "bytes": file.manifest.bytes,
                    "output_sha256": file.manifest.outputSHA256, "published": published
                ]) { _, new in new })
                historicalPending = false
                prepareSwitch(to: next)
                dataset = Dataset(geography: next, url: file.url, summary: file.summary,
                                  requestedAt: requestedAt, imported: nil, historical: file)
                loading = nil
            } catch {
                guard !Task.isCancelled else { return }
                historicalPending = false
                failure = "Could not load \(request.day). \(error.localizedDescription)"
                loading = nil
            }
        }
    }

    func enterHistoricalDate(_ date: String, source: String) {
        do {
            let citation = source.trimmingCharacters(in: .whitespacesAndNewlines)
            let provenance = citation.isEmpty ? "Explicit snapshot date" : citation
            applyHistory(try HistoricalRequest(date, provenance: provenance))
        } catch {
            failure = error.localizedDescription
        }
    }

    func moveCamera(_ viewpoint: Viewpoint) {
        cameraRequest = (cameraRequest.0 + 1, viewpoint)
    }

    func recolor() { alternateColor.toggle() }

    private func prepareSwitch(to next: Geography) {
        geography = next
        inspection = nil
        failure = nil
        status = "Preparing \(next.rawValue.lowercased())…"
    }
}

@main
struct BalloticsMapLibreLabApp: App {
    init() {
        _ = LabLog.start
        let configuration = URLSessionConfiguration.ephemeral
        configuration.protocolClasses = [BlockMapHTTP.self]
        MLNNetworkConfiguration.sharedManager.sessionConfiguration = configuration
        LabLog.record("app_init")
    }

    var body: some Scene {
        WindowGroup { LabView() }
    }
}

private struct LabView: View {
    @Environment(\.scenePhase) private var scenePhase
    @StateObject private var state = LabState()
    @State private var showImporter = false
    @State private var showProperties = false
    @State private var importArchive = false
    @State private var showDate = false
    @State private var dateText = ""
    @State private var dateSource = ""
    @State private var showElection = false
    @State private var showGeographySearch = false
    @State private var showElectionResults = false

    private var snapshotDescription: String {
        if state.geography == .presidential, let election = state.election {
            return ElectionGeographyResolver.description(for: election, mode: state.electionMode)
        }
        if state.geography == .historical, let request = state.historicalRequest {
            return "\(request.day) · \(request.provenance)"
        }
        if state.geography.censusSchema != nil { return "2024 Census boundaries · Geometry only" }
        if state.geography == .imported { return "Imported boundaries · Geometry only" }
        return "Historical boundaries · Geometry only"
    }

    private var datasetLabel: String {
        switch state.geography {
        case .historical: return "History"
        case .presidential: return "Elections"
        default: return state.geography.rawValue
        }
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 10) {
            Text("Ballotics GeoJSON Lab").font(.title2.bold())
            Text(verbatim: snapshotDescription)
                .font(.caption).foregroundStyle(.secondary).lineLimit(state.geography == .presidential ? 3 : 1)
                .fixedSize(horizontal: false, vertical: state.geography == .presidential)
                .accessibilityIdentifier("snapshot-date")

            HStack {
                Menu {
                    ForEach(Geography.allCases) { geography in
                        if geography != .imported || state.imported != nil {
                            Button(geography.rawValue) {
                                if geography == .historical && !state.hasHistoricalArchive {
                                    importArchive = true
                                    showImporter = true
                                } else {
                                    state.choose(geography)
                                }
                            }
                        }
                    }
                } label: {
                    Label(datasetLabel, systemImage: "square.3.layers.3d")
                        .frame(minWidth: 96)
                }
                .accessibilityIdentifier("datasets")

                if state.geography == .historical {
                    Menu {
                        ForEach(HistoricalRequest.presets, id: \.day.number) { request in
                            Button(request.day.description) { state.applyHistory(request) }
                        }
                        Button("2024 · archive ends in 2000") {}.disabled(true)
                        Button("Custom date") {
                            dateText = state.historicalRequest?.day.description ?? ""
                            dateSource = ""
                            showDate = true
                        }
                        Button("Load archive") {
                            importArchive = true
                            showImporter = true
                        }
                    } label: {
                        Text("Choose date")
                    }
                    .accessibilityIdentifier("historical-dates")
                } else if state.geography == .presidential, let election = state.election {
                    Button {
                        showElection = true
                    } label: {
                        Label(String(election.year), systemImage: "calendar")
                    }
                    .accessibilityLabel(Text(verbatim: "Election year \(election.year)"))
                    .accessibilityIdentifier("election-years")
                } else {
                    Button("Import file") {
                        importArchive = false
                        showImporter = true
                    }
                    .disabled(state.importing)
                }

                Spacer(minLength: 0)

                Menu {
                    ForEach(LabRenderer.allCases, id: \.self) { renderer in
                        Button {
                            state.setRenderer(renderer)
                        } label: {
                            if state.renderer == renderer {
                                Label(renderer.rawValue, systemImage: "checkmark")
                            } else {
                                Text(renderer.rawValue)
                            }
                        }
                        .disabled(state.geography == .imported && renderer == .coreGraphics)
                    }
                    Divider()
                    Button("National") { state.moveCamera(.national) }
                    Button("Alaska") { state.moveCamera(.alaska) }
                    Button("Hawaii") { state.moveCamera(.hawaii) }
                    if state.usesAlbers {
                        Button("Zoom to selected feature") { state.moveCamera(.selectedFeature) }
                            .disabled(state.inspection == nil)
                        Button("Zoom to selected state") { state.moveCamera(.selectedState) }
                            .disabled(state.inspection?.stateCode == nil)
                        Button("Clear selection") { state.inspection = nil }
                            .disabled(state.inspection == nil)
                        Button("Find geography") { showGeographySearch = true }
                        Toggle("Emphasize borders", isOn: $state.thickAlbersBorders)
                        if state.geography == .presidential {
                            Picker("Map colors", selection: $state.albersTheme) {
                                Text("Candidates").tag(AlbersTheme.candidates)
                                Text("Alternate palette").tag(AlbersTheme.alternate)
                                if state.electionMode == .countyResults {
                                    Text("Reported margin").tag(AlbersTheme.margin)
                                }
                            }
                        }
                    } else {
                        Button("Zoom to selected state") { state.moveCamera(.selectedState) }
                            .disabled(!state.canZoomToSelection)
                    }
                    Button("Recolor polygons") { state.recolor() }.disabled(state.geography == .presidential)
                } label: {
                    Label("Map view", systemImage: "scope").labelStyle(.iconOnly)
                }
                .accessibilityIdentifier("viewpoints")
            }
            .buttonStyle(.bordered)

            if state.geography == .presidential, let election = state.election, let result = state.electionPresentation {
                HStack {
                    Picker("Result mode", selection: Binding(get: { state.electionMode },
                            set: { state.applyElection(election, mode: $0) })) {
                        Text(ElectionMapMode.electoralCollege.rawValue).tag(ElectionMapMode.electoralCollege)
                        if ElectionManifest.countyYears.contains(election.year) {
                            Text(ElectionMapMode.countyResults.rawValue).tag(ElectionMapMode.countyResults)
                        }
                    }
                    .pickerStyle(.segmented)
                    .accessibilityIdentifier("election-mode")
                    Button("Results") { showElectionResults = true }
                        .buttonStyle(.bordered)
                        .accessibilityIdentifier("election-results")
                }
                Text(result.winner)
                    .font(.subheadline.weight(.semibold))
                    .foregroundStyle(.primary)
                    .accessibilityIdentifier("presidential-winner")
                if state.usesAlbers, state.electionMode == .countyResults, state.albersTheme == .margin {
                    Text("Reported margin · darker blue means a larger lead")
                        .font(.caption)
                        .accessibilityIdentifier("albers-margin-legend")
                }
            }

            Group {
                if state.dismissedStudyMap {
                    Text("Albers study finished")
                } else if state.usesAlbers {
                    AlbersMapView(state: state, showGeographySearch: $showGeographySearch)
                } else {
                    MapLibreGeometryView(state: state)
                }
            }
                .onChange(of: state.geography) { _, geography in
                    if geography == .imported, state.renderer == .coreGraphics { state.setAlbers(false) }
                }
                .onChange(of: state.electionMode) { _, mode in
                    if mode != .countyResults && state.albersTheme == .margin { state.albersTheme = .candidates }
                }
                .clipShape(RoundedRectangle(cornerRadius: 12))
                .frame(maxWidth: .infinity, maxHeight: .infinity)
                .layoutPriority(1)

            VStack(alignment: .leading, spacing: 4) {
                Text(state.historicalPending ? "Preparing historical snapshot…" :
                        state.importing ? "Validating imported file…" : state.status)
                    .font(.caption)
                    .foregroundStyle(.secondary)
                    .accessibilityIdentifier("load-status")
                Button {
                    showProperties = true
                } label: {
                    Text(state.inspection?.title ?? "Tap a polygon to inspect it")
                        .font(.subheadline.weight(.semibold)).lineLimit(1)
                }
                .disabled(state.inspection == nil)
                .accessibilityIdentifier("inspection")
            }
        }
        .padding()
        .task {
            state.start()
            await state.runAlbersStress()
            await state.runMetalHistoricalSupersessionProbe()
        }
        .onReceive(NotificationCenter.default.publisher(for: UIApplication.didReceiveMemoryWarningNotification)) { _ in
            state.releaseUnusedAlbersResources()
        }
        .onChange(of: scenePhase) { _, phase in
            if phase == .background { state.releaseUnusedAlbersResources() }
        }
        .sheet(isPresented: $showElectionResults) {
            if let result = state.electionPresentation {
                ElectionResultsPanel(result: result, theme: state.usesAlbers ? state.albersTheme : .candidates)
            }
        }
        .fileImporter(
            isPresented: $showImporter,
            allowedContentTypes: [.json, UTType(filenameExtension: "geojson") ?? .json]
        ) { result in
            if importArchive { state.importHistorical(result) }
            else { state.importGeoJSON(result) }
        }
        .sheet(isPresented: $showElection) {
            ElectionYearPicker(elections: state.elections, selected: state.election?.year,
                               onSelect: state.applyElection)
        }
        .sheet(isPresented: $showDate) {
            NavigationStack {
                Form {
                    TextField("YYYY-MM-DD", text: $dateText).keyboardType(.numbersAndPunctuation).accessibilityIdentifier("custom-date")
                    TextField("Snapshot provenance or election-date source", text: $dateSource)
                        .accessibilityIdentifier("date-provenance")
                }
                .navigationTitle("Historical snapshot")
                .toolbar {
                    ToolbarItem(placement: .cancellationAction) {
                        Button("Cancel") { showDate = false }
                    }
                    ToolbarItem(placement: .confirmationAction) {
                        Button("Apply") {
                            showDate = false
                            state.enterHistoricalDate(dateText, source: dateSource)
                        }
                    }
                }
            }
        }
        .alert("GeoJSON Lab", isPresented: Binding(
            get: { state.failure != nil }, set: { if !$0 { state.failure = nil } }
        )) {
            Button("OK") { state.failure = nil }
        } message: {
            Text(state.failure ?? "")
        }
        .sheet(isPresented: $showProperties) {
            if let inspection = state.inspection {
                NavigationStack {
                    List(Array(inspection.properties.enumerated()), id: \.offset) { _, property in
                        LabeledContent(property.key, value: property.value)
                    }
                    .navigationTitle(inspection.title)
                    .navigationBarTitleDisplayMode(.inline)
                    .toolbar { Button("Done") { showProperties = false } }
                }
            }
        }
    }
}

private struct ElectionYearPicker: View {
    let elections: [PresidentialElection]
    let selected: Int?
    let onSelect: (PresidentialElection) -> Void

    @Environment(\.dismiss) private var dismiss
    @State private var query = ""

    var body: some View {
        NavigationStack {
            List(elections.filter { query.isEmpty || String($0.year).contains(query) }) { election in
                Button {
                    dismiss()
                    onSelect(election)
                } label: {
                    HStack {
                        Text(verbatim: String(election.year))
                        Spacer()
                        if selected == election.year {
                            Image(systemName: "checkmark").accessibilityHidden(true)
                        }
                    }
                }
                .accessibilityIdentifier("election-year-\(election.year)")
                .accessibilityAddTraits(selected == election.year ? .isSelected : [])
            }
            .searchable(text: $query, placement: .navigationBarDrawer(displayMode: .always),
                        prompt: "Search election years")
            .navigationTitle("Presidential election")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .cancellationAction) {
                    Button("Cancel") { dismiss() }
                }
            }
        }
    }
}
