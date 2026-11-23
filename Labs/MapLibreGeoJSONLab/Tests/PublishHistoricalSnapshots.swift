import Foundation

@main
struct PublishHistoricalSnapshots {
    static func main() throws {
        let arguments = CommandLine.arguments.dropFirst()
        guard arguments.count >= 3 else {
            FileHandle.standardError.write(Data("Usage: publish-historical ARCHIVE OUTPUT_DIRECTORY YYYY-MM-DD ...\n".utf8))
            Foundation.exit(2)
        }
        let source = URL(fileURLWithPath: arguments[arguments.startIndex])
        let destination = URL(fileURLWithPath: arguments[arguments.startIndex + 1], isDirectory: true)
        let dates = try arguments.dropFirst(2).map { try HistoricalDay($0) }
        let archive = try HistoricalArchive(url: source)
        for date in dates { _ = try archive.active(on: date) }
        try FileManager.default.createDirectory(at: destination, withIntermediateDirectories: true)
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.prettyPrinted, .sortedKeys]
        for date in dates {
            let snapshot = try archive.snapshot(on: date)
            let base = "historical-" + snapshot.manifest.date
            try snapshot.data.write(to: destination.appendingPathComponent(base + ".geojson"), options: .atomic)
            let manifest = try encoder.encode(snapshot.manifest)
            try manifest.write(to: destination.appendingPathComponent(base + ".manifest.json"), options: .atomic)
            print(String(decoding: manifest, as: UTF8.self))
        }
    }
}
