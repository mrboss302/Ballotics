import Foundation

final class HistoricalMapFile {
    let url: URL
    let request: HistoricalRequest
    let summary: GeoJSON.Summary
    let manifest: HistoricalArchive.Manifest
    private let temporary: Bool
    private let store: HistoricalStore?

    init(url: URL, request: HistoricalRequest, summary: GeoJSON.Summary,
         manifest: HistoricalArchive.Manifest, temporary: Bool, store: HistoricalStore? = nil) {
        self.url = url
        self.request = request
        self.summary = summary
        self.manifest = manifest
        self.temporary = temporary
        self.store = store
    }

    func confirm(_ candidates: Set<Int>, longitude: Double, latitude: Double) async throws -> Set<Int> {
        guard !candidates.isEmpty else { return [] }
        let wrapped = (longitude + 180).truncatingRemainder(dividingBy: 360)
        let longitude = (wrapped < 0 ? wrapped + 360 : wrapped) - 180
        if let store {
            return try await store.confirm(on: request.day, candidates: candidates, longitude: longitude, latitude: latitude)
        }
        let local = url
        let read = Task.detached {
            try GeoJSON.matchingHistoricalIDs(in: local, candidates: candidates, longitude: longitude, latitude: latitude)
        }
        return try await withTaskCancellationHandler {
            try await read.value
        } onCancel: {
            read.cancel()
        }
    }

    deinit {
        if temporary { try? FileManager.default.removeItem(at: url) }
    }
}

actor HistoricalStore {
    private let source: URL
    private let copyFromFiles: Bool
    private var localCopy: URL?
    private var index: HistoricalArchive?

    init(source: URL, copyFromFiles: Bool = false) {
        self.source = source
        self.copyFromFiles = copyFromFiles
    }

    deinit {
        if let localCopy { try? FileManager.default.removeItem(at: localCopy) }
    }

    func snapshot(_ request: HistoricalRequest) throws -> (HistoricalMapFile, [[String: Any]]) {
        var measurements: [[String: Any]] = []
        if index == nil {
            let peak = MemorySampler()
            let start = LabLog.now
            let archive = try HistoricalArchive(url: localArchive(), sourceReference: source.lastPathComponent)
            try Task.checkCancellation()
            index = archive
            measurements.append(peak.finish(event: "historical_index", since: start).merging([
                "records": archive.records.count, "source_sha256": archive.sourceSHA256
            ]) { _, new in new })
        }
        guard let index else { preconditionFailure("A historical index must be available.") }

        let peak = MemorySampler()
        let start = LabLog.now
        let snapshot = try index.snapshot(on: request.day)
        guard snapshot.summary.features > 0 else {
            throw GeoJSON.Invalid(message: "No county boundaries are recorded for \(request.day).")
        }
        let destination = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString + ".geojson")
        try snapshot.data.write(to: destination, options: .atomic)
        let file = HistoricalMapFile(url: destination, request: request, summary: snapshot.summary,
                                     manifest: snapshot.manifest, temporary: true, store: self)
        measurements.append(peak.finish(event: "historical_extract", since: start).merging([
            "select_ms": snapshot.selectMilliseconds, "assemble_hash_ms": snapshot.assembleMilliseconds
        ]) { _, new in new })
        return (file, measurements)
    }

    func confirm(on day: HistoricalDay, candidates: Set<Int>, longitude: Double, latitude: Double) throws -> Set<Int> {
        guard let index else { return [] }
        return try index.matchingIDs(on: day, candidates: candidates, longitude: longitude, latitude: latitude)
    }

    private func localArchive() throws -> URL {
        guard copyFromFiles else { return source }
        if let localCopy { return localCopy }
        let access = source.startAccessingSecurityScopedResource()
        defer { if access { source.stopAccessingSecurityScopedResource() } }
        let destination = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString + ".json")
        try FileManager.default.copyItem(at: source, to: destination)
        localCopy = destination
        return destination
    }
}

// Samples process RSS/footprint independently of the actor doing preprocessing.
final class MemorySampler: @unchecked Sendable {
    private let queue = DispatchQueue(label: "org.ballotics.geojson.preprocess-memory")
    private let timer: DispatchSourceTimer?
    private var peak: MemorySample?
    private let baseline: MemorySample?

    init() {
        baseline = LabLog.enabled ? MemorySample.read() : nil
        peak = baseline
        if LabLog.enabled {
            let timer = DispatchSource.makeTimerSource(queue: queue)
            self.timer = timer
            timer.schedule(deadline: .now(), repeating: .milliseconds(10), leeway: .milliseconds(1))
            timer.setEventHandler { [weak self] in self?.sample() }
            timer.resume()
        } else {
            timer = nil
        }
    }

    deinit { timer?.cancel() }

    private func sample() {
        guard let current = MemorySample.read() else { return }
        peak = MemorySample(resident: max(peak?.resident ?? 0, current.resident),
                            footprint: max(peak?.footprint ?? 0, current.footprint))
    }

    func finish(event: String, since start: Double) -> [String: Any] {
        timer?.cancel()
        var result: [String: Any] = ["phase": event, "elapsed_ms": LabLog.milliseconds(LabLog.now - start)]
        if LabLog.enabled {
            if let baseline { result.merge(baseline.fields("before")) { _, new in new } }
            let maximum = queue.sync { sample(); return peak }
            if let maximum { result.merge(maximum.fields("sampled_peak")) { _, new in new } }
            if let after = MemorySample.read() { result.merge(after.fields("after")) { _, new in new } }
        }
        return result
    }
}
