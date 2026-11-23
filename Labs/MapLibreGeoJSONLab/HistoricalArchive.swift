import CoreFoundation
import CryptoKit
import Foundation

struct HistoricalDay: Comparable, CustomStringConvertible, Sendable {
    let number: Int

    init(_ value: String) throws {
        let bytes = Array(value.utf8)
        guard bytes.count == 10, bytes[4] == 45, bytes[7] == 45,
              bytes.enumerated().allSatisfy({ $0.offset == 4 || $0.offset == 7 || (48...57).contains($0.element) }),
              let year = Int(value.prefix(4)), let month = Int(value.dropFirst(5).prefix(2)),
              let day = Int(value.suffix(2)), year > 0, (1...12).contains(month) else {
            throw GeoJSON.Invalid(message: "Enter a Gregorian date as YYYY-MM-DD.")
        }
        let leap = year.isMultiple(of: 4) && (!year.isMultiple(of: 100) || year.isMultiple(of: 400))
        let days = [31, leap ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
        guard (1...days[month - 1]).contains(day) else {
            throw GeoJSON.Invalid(message: "Enter a valid Gregorian day.")
        }
        number = year * 10_000 + month * 100 + day
    }

    var description: String {
        String(format: "%04d-%02d-%02d", number / 10_000, number / 100 % 100, number % 100)
    }

    static func < (lhs: Self, rhs: Self) -> Bool { lhs.number < rhs.number }
}

struct HistoricalRequest: Equatable, Sendable {
    let day: HistoricalDay
    let provenance: String

    init(_ date: String, provenance: String = "Research snapshot") throws {
        day = try HistoricalDay(date)
        self.provenance = provenance
    }

    static let presets = ["1788", "1800", "1860", "1900", "1950", "1980", "2000"]
        .compactMap { try? HistoricalRequest($0 + "-01-01", provenance: "Research preset") }
}

struct HistoricalArchive {
    static let ruleVersion = "ahcb-inclusive-civil-day-v1"
    static let schemaVersion = 1
    static let filename = "US_HistCounties_Gen05.json"

    struct Record {
        let range: Range<Int>
        let id: Int
        let entity: String
        let version: Int
        let start: HistoricalDay
        let end: HistoricalDay?
        let summary: GeoJSON.Summary
    }

    struct Manifest: Codable, Equatable {
        let schemaVersion: Int
        let ruleVersion: String
        let date: String
        let sourceReference: String
        let sourceSHA256: String
        let outputSHA256: String
        let features: Int
        let parts: Int
        let positions: Int
        let bytes: Int
    }

    struct Snapshot {
        let data: Data
        let summary: GeoJSON.Summary
        let manifest: Manifest
        let selectMilliseconds: Double
        let assembleMilliseconds: Double
    }

    private let url: URL
    private let sourceReference: String
    let sourceSHA256: String
    let records: [Record]
    let coverage: ClosedRange<HistoricalDay>
    private let byteCount: Int
    private let modifiedAt: Date?

    init(url: URL, sourceReference: String? = nil, coverageThrough: HistoricalDay? = nil) throws {
        let modification = try url.resourceValues(forKeys: [.contentModificationDateKey]).contentModificationDate
        let data = try Data(contentsOf: url, options: .mappedIfSafe)
        let ranges = try data.withUnsafeBytes { bytes in
            var cursor = CollectionCursor(bytes: bytes)
            return try cursor.features()
        }
        var recordIDs = Set<Int>()
        var versions: [String: Set<Int>] = [:]
        var records: [Record] = []
        records.reserveCapacity(ranges.count)
        for (offset, range) in ranges.enumerated() {
            try Task.checkCancellation()
            let record = try autoreleasepool {
                do {
                    return try Self.decode(data.subdata(in: range), range: range)
                } catch {
                    throw GeoJSON.Invalid(message: "Historical record \(offset + 1). \(error.localizedDescription)")
                }
            }
            guard recordIDs.insert(record.id).inserted,
                  versions[record.entity, default: []].insert(record.version).inserted else {
                throw GeoJSON.Invalid(message: "Duplicate historical record or entity version (ID_NUM \(record.id)).")
            }
            records.append(record)
        }
        guard let first = records.map(\.start).min(),
              let last = coverageThrough ?? (records.allSatisfy { $0.end != nil } ? records.compactMap(\.end).max() : nil),
              records.allSatisfy({ $0.start <= last && ($0.end ?? last) <= last }) else {
            throw GeoJSON.Invalid(message: "An open-ended archive requires an explicit coverage end.")
        }
        self.url = url
        self.sourceReference = sourceReference ?? url.lastPathComponent
        self.sourceSHA256 = Self.sha256(data)
        self.records = records
        self.coverage = first...last
        self.byteCount = data.count
        self.modifiedAt = modification
    }

    func active(on day: HistoricalDay) throws -> [Record] {
        guard coverage.contains(day) else {
            throw GeoJSON.Invalid(message: "Archive coverage is \(coverage.lowerBound) through \(coverage.upperBound).")
        }
        return records.filter { $0.start <= day && day <= ($0.end ?? coverage.upperBound) }
    }

    func snapshot(on day: HistoricalDay) throws -> Snapshot {
        let start = ProcessInfo.processInfo.systemUptime
        let selected = try active(on: day)
        let selectedAt = ProcessInfo.processInfo.systemUptime
        // The URL is a stable, local archive for the lifetime of the index.
        let source = try archiveBytes()
        var data = Data(#"{"type":"FeatureCollection","features":["#.utf8)
        data.reserveCapacity(selected.reduce(data.count + 2) { $0 + $1.range.count + 1 })
        var summary = GeoJSON.Summary()
        summary.selectionKey = "ID_NUM"
        for (offset, record) in selected.enumerated() {
            try Task.checkCancellation()
            if offset > 0 { data.append(44) }
            data.append(source[record.range])
            summary.include(record.summary)
        }
        data.append(contentsOf: "]}".utf8)
        let manifest = Manifest(schemaVersion: Self.schemaVersion, ruleVersion: Self.ruleVersion,
                                date: day.description, sourceReference: sourceReference, sourceSHA256: sourceSHA256,
                                outputSHA256: Self.sha256(data), features: summary.features, parts: summary.parts,
                                positions: summary.positions, bytes: data.count)
        return Snapshot(data: data, summary: summary, manifest: manifest,
                        selectMilliseconds: (selectedAt - start) * 1_000,
                        assembleMilliseconds: (ProcessInfo.processInfo.systemUptime - selectedAt) * 1_000)
    }

    func matchingIDs(on day: HistoricalDay, candidates: Set<Int>,
                     longitude: Double, latitude: Double) throws -> Set<Int> {
        let data = try archiveBytes()
        var matches = Set<Int>()
        for record in records where candidates.contains(record.id)
            && record.start <= day && day <= (record.end ?? coverage.upperBound) {
            try Task.checkCancellation()
            if let feature = try JSONSerialization.jsonObject(with: data.subdata(in: record.range)) as? [String: Any],
               try GeoJSON.containsFeature(feature, longitude: longitude, latitude: latitude) {
                matches.insert(record.id)
            }
        }
        return matches
    }

    private func archiveBytes() throws -> Data {
        let data = try Data(contentsOf: url, options: .mappedIfSafe)
        let modification = try url.resourceValues(forKeys: [.contentModificationDateKey]).contentModificationDate
        guard data.count == byteCount, modification == modifiedAt else {
            throw GeoJSON.Invalid(message: "The archive changed. Load it again before filtering.")
        }
        return data
    }

    static func sha256(_ data: Data) -> String {
        SHA256.hash(data: data).map { String(format: "%02x", $0) }.joined()
    }

    static func loadPublished(geoJSON: URL, manifestURL: URL, date: HistoricalDay) throws -> (Manifest, GeoJSON.Summary) {
        let manifest = try JSONDecoder().decode(Manifest.self, from: Data(contentsOf: manifestURL))
        let data = try Data(contentsOf: geoJSON, options: .mappedIfSafe)
        guard manifest.schemaVersion == schemaVersion, manifest.ruleVersion == ruleVersion,
              manifest.date == date.description, manifest.bytes == data.count, manifest.outputSHA256 == sha256(data) else {
            throw GeoJSON.Invalid(message: "The snapshot and its manifest disagree.")
        }
        let summary = try GeoJSON.validate(geoJSON, geography: .historical)
        guard summary.features == manifest.features, summary.parts == manifest.parts,
              summary.positions == manifest.positions, summary.selectionKey == "ID_NUM" else {
            throw GeoJSON.Invalid(message: "Snapshot geometry or historical IDs disagree with its manifest.")
        }
        return (manifest, summary)
    }

    private static func decode(_ data: Data, range: Range<Int>) throws -> Record {
        guard let object = try JSONSerialization.jsonObject(with: data) as? [String: Any],
              let properties = object["properties"] as? [String: Any],
              let id = integer(properties["ID_NUM"]), id > 0,
              let entity = properties["ID"] as? String, !entity.isEmpty,
              let version = integer(properties["VERSION"]), version > 0,
              let start = try day(properties, field: "START"),
              properties["END_DATE"] != nil else {
            throw GeoJSON.Invalid(message: "Expected ID, ID_NUM, VERSION, START_DATE and END_DATE.")
        }
        let end = try day(properties, field: "END")
        guard end.map({ start <= $0 }) ?? true else {
            throw GeoJSON.Invalid(message: "END_DATE precedes START_DATE.")
        }
        return Record(range: range, id: id, entity: entity, version: version, start: start, end: end,
                      summary: try GeoJSON.summarizeFeature(object))
    }

    private static func day(_ properties: [String: Any], field: String) throws -> HistoricalDay? {
        let iso = properties[field + "_DATE"]
        let numeric = properties[field + "_N"]
        if iso is NSNull, numeric == nil || numeric is NSNull { return nil }
        guard let value = iso as? String, value.utf8.count == 24, value.hasSuffix("T00:00:00.000Z") else {
            throw GeoJSON.Invalid(message: "Expected the archive's UTC-midnight format for \(field)_DATE.")
        }
        let date = try HistoricalDay(String(value.prefix(10)))
        guard numeric == nil || integer(numeric) == date.number else {
            throw GeoJSON.Invalid(message: "\(field)_N disagrees with \(field)_DATE.")
        }
        return date
    }

    private static func integer(_ value: Any?) -> Int? {
        guard let number = value as? NSNumber, CFGetTypeID(number) != CFBooleanGetTypeID() else { return nil }
        return Int(number.stringValue)
    }
}

// Locate top-level FeatureCollection members and exact feature byte ranges.
// Foundation validates each complete feature and any non-feature root members.
private struct CollectionCursor {
    let bytes: UnsafeRawBufferPointer
    var offset = 0
    var current: UInt8? { offset < bytes.count ? bytes[offset] : nil }

    mutating func features() throws -> [Range<Int>] {
        try take(123)
        var ranges: [Range<Int>]?
        var type: String?
        var keys = Set<String>()
        repeat {
            whitespace()
            let key = try string()
            guard keys.insert(key).inserted else { throw invalid }
            try take(58)
            if key == "features" {
                try take(91)
                var list: [Range<Int>] = []
                whitespace()
                if current != 93 {
                    repeat {
                        whitespace()
                        guard current == 123 else { throw invalid }
                        list.append(try valueRange())
                    } while consume(44)
                }
                try take(93)
                ranges = list
            } else {
                let range = try valueRange()
                let decoded = try JSONSerialization.jsonObject(with: Data(bytes[range]), options: .fragmentsAllowed)
                if key == "type" { type = decoded as? String }
            }
        } while consume(44)
        try take(125)
        whitespace()
        guard current == nil, type == "FeatureCollection", let ranges, !ranges.isEmpty else { throw invalid }
        return ranges
    }

    mutating func valueRange() throws -> Range<Int> {
        whitespace()
        let start = offset
        if current == 123 || current == 91 {
            var ends: [UInt8] = []
            repeat {
                guard let byte = current else { throw invalid }
                switch byte {
                case 34: try skipString()
                case 123: ends.append(125); offset += 1
                case 91: ends.append(93); offset += 1
                case 125, 93:
                    guard ends.popLast() == byte else { throw invalid }
                    offset += 1
                default: offset += 1
                }
            } while !ends.isEmpty
        } else if current == 34 {
            try skipString()
        } else {
            while let byte = current, ![44, 93, 125, 32, 9, 10, 13].contains(byte) { offset += 1 }
        }
        guard offset > start else { throw invalid }
        return start..<offset
    }

    mutating func string() throws -> String {
        let start = offset
        try skipString()
        return try JSONDecoder().decode(String.self, from: Data(bytes[start..<offset]))
    }

    mutating func skipString() throws {
        guard current == 34 else { throw invalid }
        offset += 1
        while let byte = current {
            offset += 1
            if byte == 34 { return }
            if byte == 92, current != nil { offset += 1 }
        }
        throw invalid
    }

    mutating func whitespace() {
        while let byte = current, byte == 32 || byte == 9 || byte == 10 || byte == 13 { offset += 1 }
    }

    mutating func consume(_ byte: UInt8) -> Bool {
        whitespace()
        guard current == byte else { return false }
        offset += 1
        return true
    }

    mutating func take(_ byte: UInt8) throws {
        guard consume(byte) else { throw invalid }
    }

    var invalid: GeoJSON.Invalid { .init(message: "Malformed historical FeatureCollection at byte \(offset).") }
}
