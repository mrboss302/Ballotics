import Darwin
import Foundation
import os

enum LabLog {
    static let log = OSLog(subsystem: "org.ballotics.MapLibreGeoJSONLab", category: "Benchmark")
    static let enabled = ProcessInfo.processInfo.arguments.contains("--measure")
    static let start = ProcessInfo.processInfo.systemUptime

    static var now: Double { ProcessInfo.processInfo.systemUptime }

    static func record(_ event: String, _ fields: [String: Any] = [:]) {
        guard enabled else { return }
        var record = fields
        record["event"] = event
        record["app_ms"] = milliseconds(now - start)
        guard var data = try? JSONSerialization.data(withJSONObject: record, options: .sortedKeys),
              let folder = FileManager.default.urls(for: .cachesDirectory, in: .userDomainMask).first else { return }
        data.append(10)
        let file = folder.appendingPathComponent("benchmark.jsonl")
        if let handle = try? FileHandle(forWritingTo: file) {
            defer { try? handle.close() }
            _ = try? handle.seekToEnd()
            try? handle.write(contentsOf: data)
        } else {
            try? data.write(to: file, options: .atomic)
        }
    }

    static func milliseconds(_ seconds: Double) -> Double {
        (seconds * 10_000).rounded() / 10
    }
}

struct MemorySample {
    let resident: UInt64
    let footprint: UInt64

    static func read() -> MemorySample? {
        var info = task_vm_info_data_t()
        var count = mach_msg_type_number_t(MemoryLayout<task_vm_info_data_t>.size / MemoryLayout<integer_t>.size)
        let status = withUnsafeMutablePointer(to: &info) { pointer in
            pointer.withMemoryRebound(to: integer_t.self, capacity: Int(count)) { words in
                task_info(mach_task_self_, task_flavor_t(TASK_VM_INFO), words, &count)
            }
        }
        guard status == KERN_SUCCESS else { return nil }
        return MemorySample(resident: UInt64(info.resident_size), footprint: UInt64(info.phys_footprint))
    }

    func fields(_ label: String) -> [String: Any] {
        let mib = 1_048_576.0
        return ["\(label)_rss_mib": Double(resident) / mib, "\(label)_footprint_mib": Double(footprint) / mib]
    }
}

struct FrameSamples {
    let started = LabLog.now
    private var timestamps: [Double] = []
    private var peak: MemorySample?
    private var lastSample = -Double.infinity

    mutating func addFrame() {
        let now = LabLog.now
        timestamps.append(now)
        if now - lastSample >= 0.2, let sample = MemorySample.read() {
            lastSample = now
            peak = MemorySample(resident: max(peak?.resident ?? 0, sample.resident),
                                footprint: max(peak?.footprint ?? 0, sample.footprint))
        }
    }

    func fields() -> [String: Any] {
        var fields = peak?.fields("sampled_peak") ?? [:]
        fields["window_ms"] = LabLog.milliseconds(LabLog.now - started)
        fields["frames"] = timestamps.count
        let gaps = zip(timestamps.dropFirst(), timestamps).map { $0 - $1 }.sorted()
        if let worst = gaps.last, let first = timestamps.first, let last = timestamps.last, last > first {
            fields["callback_fps"] = Double(gaps.count) / (last - first)
            fields["p50_gap_ms"] = LabLog.milliseconds(gaps[Int(ceil(Double(gaps.count) * 0.50)) - 1])
            fields["p95_gap_ms"] = LabLog.milliseconds(gaps[Int(ceil(Double(gaps.count) * 0.95)) - 1])
            fields["p99_gap_ms"] = LabLog.milliseconds(gaps[Int(ceil(Double(gaps.count) * 0.99)) - 1])
            fields["worst_gap_ms"] = LabLog.milliseconds(worst)
            fields["gaps_over_25_ms"] = gaps.count { $0 > 0.025 }
        }
        return fields
    }
}

final class BlockMapHTTP: URLProtocol {
    override class func canInit(with request: URLRequest) -> Bool {
        ["http", "https"].contains(request.url?.scheme?.lowercased() ?? "")
    }

    override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }

    override func startLoading() {
        os_log(.error, log: LabLog.log, "Blocked unexpected MapLibre HTTP request")
        client?.urlProtocol(self, didFailWithError: URLError(.notConnectedToInternet))
    }

    override func stopLoading() {}
}
