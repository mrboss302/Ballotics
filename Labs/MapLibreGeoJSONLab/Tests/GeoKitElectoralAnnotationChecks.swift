import AppKit
import Foundation

@main
struct GeoKitElectoralAnnotationChecks {
    private static func fixtureUnit(_ id: String, recipient: String? = nil, quantity: Int,
                                    kind: ElectoralAnnotation.Kind = .statewide,
                                    status: ElectoralAnnotation.Status = .reported) -> ElectoralAnnotation.Unit {
        .init(id: id, recipientID: recipient, name: recipient, affiliation: recipient.map { _ in "Fixture affiliation" },
              color: nil, quantity: quantity, kind: kind, status: status, source: "Synthetic fixture")
    }

    private static func model(_ units: [ElectoralAnnotation.Unit], basis: ElectoralAnnotation.Basis = .presidential,
                              status: ElectoralAnnotation.Status = .reported, assigned: Int? = nil) -> ElectoralAnnotation {
        .init(jurisdictionID: "fixture", name: "Fixture", basis: basis, status: status, assigned: assigned,
              units: units, notes: [])
    }

    private static func fixtures() {
        let a = fixtureUnit("a", recipient: "Aster", quantity: 2, kind: .atLarge)
        let b = fixtureUnit("b", recipient: "Birch", quantity: 1, kind: .district("1"))
        let c = fixtureUnit("c", recipient: "Cedar", quantity: 1, kind: .historicalSplit("2"))
        let faithless = fixtureUnit("f", recipient: "Cedar", quantity: 1, kind: .faithless)
        let rejected = fixtureUnit("r", quantity: 1, kind: .rejected)
        let uncast = fixtureUnit("u", quantity: 1, kind: .uncast)
        let plain = fixtureUnit("s", recipient: "Aster", quantity: 4)
        precondition(model([plain]).squares == nil && model([plain]).reported == 4)
        precondition(model([a, b]).squares?.map(\.id) == ["a", "a", "b"])
        precondition(model([a, b, c]).recipients == ["Aster", "Birch", "Cedar"])
        let exceptional = model([a, b, c, faithless, rejected, uncast], basis: .allocationSlots, assigned: 7)
        precondition(exceptional.squares?.count == 7 && exceptional.basis.countSuffix == "slots")
        precondition(exceptional.groupedQuantities.reduce(0) { $0 + $1.quantity } == 7)
        precondition(model([a, b, c, faithless, rejected, uncast]).accessibilityDescription.contains("Congressional district 1"))
        precondition(model([faithless, rejected, uncast]).accessibilityDescription.contains("Rejected"))
        precondition(model([a, b], basis: .earlyBallot).squares == nil)
        precondition(model([], status: .missing).displayCount == nil)
        precondition(model([], status: .absent).accessibilityDescription.contains("Absent"))
        precondition(model([], status: .nonparticipating).accessibilityDescription.contains("Nonparticipating"))
        precondition(model([a], status: .unresolved, assigned: 4).displayCount == "4?")
        precondition(model([a], assigned: 4).squares == nil)
        precondition(model([fixtureUnit("large", recipient: "Aster", quantity: 25), b]).squares == nil)
        precondition(model([fixtureUnit("large", recipient: "Aster", quantity: 25), b]).reported == 26)
        for width in [320.0, 350, 396, 840] {
            let grid = ElectoralEditorialLayout.keyGrid(count: 9, width: width)
            let region = CGRect(x: 0, y: 300, width: width, height: grid.height)
            let frames = grid.frames(in: region)
            precondition(frames.count == 9 && frames.allSatisfy { region.contains($0) && $0.width == 44 })
            for (index, frame) in frames.enumerated() {
                precondition(!frames.dropFirst(index + 1).contains { $0.intersects(frame) && $0.intersection(frame).width > 0
                    && $0.intersection(frame).height > 0 })
            }
        }
    }

    private static func sizes(_ specs: [MapLabel]) -> [String: LabelMeasurements] {
        let font = NSFont.monospacedDigitSystemFont(ofSize: 8, weight: .medium)
        let paragraph = NSMutableParagraphStyle()
        paragraph.alignment = .center
        paragraph.minimumLineHeight = 9
        paragraph.maximumLineHeight = 9
        func measure(_ text: String) -> CGSize {
            let rect = (text as NSString).boundingRect(with: CGSize(width: 500,height: 100), options: [.usesLineFragmentOrigin],
                attributes: [.font: font, .paragraphStyle: paragraph])
            return CGSize(width: ceil(rect.width) + 2, height: ceil(rect.height) + 1)
        }
        return Dictionary(uniqueKeysWithValues: specs.map {
            ($0.id, LabelMeasurements(full: measure($0.text(for: .stacked)), compact: measure($0.primary),
                                      inline: $0.secondary.isEmpty ? nil : measure($0.text(for: .inline))))
        })
    }

    static func main() throws {
        fixtures()
        let args = CommandLine.arguments
        precondition(args.count == 3, "Supply the historical state archive and normalized national package")
        let archive = try HistoricalArchive(url: URL(fileURLWithPath: args[1]))
        let data = try Data(contentsOf: URL(fileURLWithPath: args[2]))
        let package = try ElectionPayloadValidation.national(data, count: 60)
        let names = Dictionary(uniqueKeysWithValues: package.candidates.map { ($0.id, $0.name) })
        let colors = names.mapValues { _ in "#34456A" }
        let parties = Dictionary(uniqueKeysWithValues: package.parties.map { ($0.id, $0.name) })
        var previousDate = ""
        var scene: AlbersScene?
        for election in package.elections {
            let date = election.geography.snapshotDate
            if previousDate != date {
                let snap = try archive.snapshot(on: HistoricalDay(date))
                let collection = try JSONSerialization.jsonObject(with: snap.data) as! [String: Any]
                scene = try AlbersScene.project(collection["features"] as! [[String: Any]]).composed(.thematic)
                previousDate = date
            }
            let scene = scene!
            let records = ElectoralAnnotation.from(election, names: names, parties: parties, colors: colors)
            let sourceRows = election.allocations.filter { $0.geometryKey != nil }
            precondition(Set(records.values.flatMap(\.units).map(\.id)) == Set(sourceRows.map(\.id)))
            precondition(records.values.flatMap(\.units).reduce(0) { $0 + $1.quantity } == sourceRows.reduce(0) { $0 + $1.electoralVotes })
            for record in records.values {
                if let squares = record.squares {
                    precondition(record.basis != .earlyBallot && record.matchesCountingBasis && squares.count == record.reported)
                }
                if record.status != .reported {
                    precondition(record.squares == nil && !record.allocationArtwork)
                }
            }
            let votes = records.compactMapValues { $0.matchesCountingBasis ? $0.reported : nil }
            let statuses = records.mapValues { $0.status.rawValue }
            let input = ElectoralCartography.specifications(scene: scene, votes: votes, statuses: statuses)
            let (specs, content) = ElectoralEditorialLayout.content(scene: scene, specifications: input, records: records)
            let byID = Dictionary(uniqueKeysWithValues: specs.map { ($0.id, $0) })
            let anchorCount = scene.labelCache.count
            let anchors = Dictionary(uniqueKeysWithValues: specs.map {
                ($0.id, scene.labelCache.anchors(in: scene, features: $0.features, editorial: true))
            })
            let measures = sizes(specs)
            let artwork = specs.filter { !$0.external && content[$0.id]?.squares != nil }
            let callouts = AnnotationCallouts(scene: scene)
            let key = specs.filter(\.external)
            for (width, height) in [(350, 450), (370, 380), (850, 620), (680, 220)] {
                let bounds = CGRect(x: 0, y: 0, width: width, height: height)
                let reserve = ElectoralEditorialLayout.reservedHeight(count: key.count, width: bounds.width, height: bounds.height)
                let outer = GeographicMapLayout(bounds: bounds, externalHeight: reserve, idealMapAspect: scene.bounds.width / scene.bounds.height)
                let camera = GeographicCamera(viewport: outer.viewport, center: CGPoint(x: scene.bounds.midX, y: scene.bounds.midY),
                    scale: GeographicCamera.fit(scene.bounds, in: outer.viewport))
                let t = ProcessInfo.processInfo.systemUptime
                let decisions = LabelResolver.resolve(specs, anchors: anchors, sizes: measures, scene: scene,
                    camera: camera, zoom: 1, policy: .nationalEditorial)
                let layout = outer.externalAnnotations.map {
                    ElectoralEditorialLayout.resolve(decisions, specs: byID, artworkSpecs: artwork, allocations: content,
                        anchors: anchors, callouts: callouts, scene: scene, camera: camera, region: $0, zoom: 1)
                }
                let elapsed = (ProcessInfo.processInfo.systemUptime - t) * 1000
                let art = layout?.artwork ?? [], tiles = layout?.tiles ?? []
                let seen = Set((art + tiles).map(\.id))
                precondition(seen.count == art.count + tiles.count)
                let full = decisions.filter { $0.mode == .full && !byID[$0.id]!.secondary.isEmpty }
                let qualified = Set(specs.filter { !$0.secondary.isEmpty }.map(\.id))
                let visible = qualified.intersection(seen.union(full.map(\.id)))
                let missing = decisions.filter { $0.mode == .suppressed }
                let artworkIDs = Set(art.map(\.id))
                let internalFrames = decisions.filter { ($0.mode == .full || $0.mode == .compact) && !artworkIDs.contains($0.id) }
                let frames = internalFrames.map(\.frame) + art.map(\.frame) + tiles.map(\.frame)
                for (index, frame) in frames.enumerated() {
                    precondition(!frames.dropFirst(index + 1).contains { $0.intersection(frame).width > 0.1
                        && $0.intersection(frame).height > 0.1 && $0.intersects(frame) })
                }
                if let region = outer.externalAnnotations {
                    precondition(tiles.allSatisfy { region.contains($0.frame) && $0.frame.width == 44 })
                    precondition(tiles.allSatisfy { !$0.frame.intersects(camera.viewport) || $0.frame.minY == camera.viewport.maxY })
                }
                let details = missing.map { p -> [String: Any] in
                    let spec = byID[p.id]!, size = measures[p.id]!, candidates = anchors[p.id]?.candidates ?? []
                    return ["id": p.id, "reason": p.reason, "visible": seen.contains(p.id), "count": spec.secondary,
                        "status": content[p.id]?.status.rawValue ?? "Allocation unavailable",
                        "candidates": candidates.count, "stacked": [size.full.width, size.full.height],
                        "compact": [size.compact.width, size.compact.height],
                        "inline": size.inline.map { [$0.width, $0.height] } ?? []]
                }
                let study: [String: Any] = ["year": election.year, "basis": election.rules, "viewport": [width,height],
                    "keys": tiles.map(\.id), "artwork": art.map(\.id), "callouts": art.filter { $0.leader != nil }.map(\.id),
                    "available": qualified.count, "visible": visible.sorted(), "suppressed": details,
                    "anchorGroupsCreated": scene.labelCache.count - anchorCount, "layout_ms": elapsed,
                    "sourceRows": sourceRows.count, "sourceQuantity": sourceRows.reduce(0) { $0 + $1.electoralVotes }]
                print(String(decoding: try JSONSerialization.data(withJSONObject: study, options: [.sortedKeys]), as: UTF8.self))
            }
        }
        let base = package.elections.first { $0.year == 2020 }!
        for updated in [false, true] {
            let (caseRecord, candidates, parties) = ElectoralAnnotationFixture.make(from: base, updated: updated)!
            let annotations = ElectoralAnnotation.from(caseRecord,
                names: Dictionary(uniqueKeysWithValues: candidates.map { ($0.id, $0.name) }),
                parties: Dictionary(uniqueKeysWithValues: parties.map { ($0.id, $0.name) }),
                colors: Dictionary(uniqueKeysWithValues: candidates.map { ($0.id, "fixture-color") }))
            precondition(caseRecord.geography.snapshotDate == base.geography.snapshotDate)
            precondition(annotations.values.first?.reported == (updated ? 5 : 4))
            precondition(annotations.values.first?.recipients.count == (updated ? 3 : 2))
        }
    }
}
