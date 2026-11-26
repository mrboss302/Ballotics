import Foundation

/// Election-owned content. Each unit preserves a source row and its counting semantics.
struct ElectoralAnnotation {
    enum Basis: String {
        case earlyBallot = "Reported early-ballot entries"
        case presidential = "Presidential electoral votes"
        case allocationSlots = "Reported allocation slots, including uncast or rejected votes"

        var countSuffix: String {
            switch self {
            case .earlyBallot: "reported"
            case .presidential: "EV"
            case .allocationSlots: "slots"
            }
        }
    }

    enum Status: String {
        case reported = "Reported allocation"
        case unresolved = "Unresolved allocation"
        case nonparticipating = "Nonparticipating"
        case missing = "Allocation unavailable"
        case absent = "Absent from the geographic snapshot"
    }

    enum Kind: Equatable {
        case statewide, atLarge, district(String), historicalSplit(String?)
        case faithless, uncast, rejected, unknown(String)

        var label: String {
            switch self {
            case .statewide: "Statewide"
            case .atLarge: "At large"
            case .district(let part): "Congressional district \(part)"
            case .historicalSplit: "Historical statewide allocation"
            case .faithless: "Faithless elector"
            case .uncast: "Uncast"
            case .rejected: "Rejected"
            case .unknown(let raw): raw
            }
        }
    }

    struct Unit {
        let id: String
        let recipientID: String?
        let name: String?
        let affiliation: String?
        let color: String?
        let quantity: Int
        let kind: Kind
        let status: Status
        let source: String

        var description: String {
            let recipient = status == .reported ? [name, affiliation].compactMap { $0 }.joined(separator: ", ") : ""
            let outcome = !recipient.isEmpty ? recipient : status == .reported ? kind.label : status.rawValue
            return "\(kind.label), \(quantity), \(outcome)"
        }
    }

    let jurisdictionID: String
    let name: String
    let basis: Basis
    let status: Status
    let assigned: Int?
    let units: [Unit]
    let notes: [String]

    var matchesCountingBasis: Bool {
        status == .reported && (assigned == nil || assigned == reported)
    }
    var reported: Int? {
        let positive = units.filter { $0.quantity > 0 }
        return positive.isEmpty ? nil : positive.reduce(0) { $0 + $1.quantity }
    }
    var recipients: Set<String> { Set(units.filter { $0.quantity > 0 && $0.status == .reported }.compactMap(\.recipientID)) }
    var allocationArtwork: Bool {
        basis != .earlyBallot && matchesCountingBasis && (recipients.count > 1 || units.contains {
            switch $0.kind {
            case .atLarge, .district, .faithless, .uncast, .rejected: $0.quantity > 0
            default: false
            }
        })
    }
    var squares: [Unit]? {
        guard allocationArtwork, let reported, reported <= 9 else { return nil }
        return units.flatMap { Array(repeating: $0, count: max(0, $0.quantity)) }
    }
    struct QuantityGroup {
        let unit: Unit
        var quantity: Int
    }

    var groupedQuantities: [QuantityGroup] {
        var result: [QuantityGroup] = []
        for unit in units where unit.quantity > 0 {
            let exceptional = [Kind.faithless, .uncast, .rejected].contains(unit.kind)
            if let index = result.firstIndex(where: {
                $0.unit.recipientID == unit.recipientID &&
                    (exceptional ? $0.unit.kind == unit.kind : ![Kind.faithless, .uncast, .rejected].contains($0.unit.kind))
            }) {
                result[index].quantity += unit.quantity
            } else {
                result.append(.init(unit: unit, quantity: unit.quantity))
            }
        }
        return result
    }

    var displayCount: String? {
        if matchesCountingBasis { return reported.map(String.init) }
        return assigned.map { "\($0)?" }
    }
    var accessibilityDescription: String {
        var description = [name, basis.rawValue, status.rawValue]
        if let assigned { description.append("\(assigned) assigned") }
        if let reported { description.append("\(reported) in source rows") }
        if let assigned, let reported, assigned != reported {
            description.append("Reported and assigned totals differ")
        }
        if recipients.count > 1 && matchesCountingBasis { description.append("Split allocation") }
        description += units.filter { $0.quantity > 0 }.map(\.description)
        description += notes
        return description.joined(separator: ". ")
    }

    static func from(_ election: ElectionRecord, names: [String: String], parties: [String: String],
                     colors: [String: String]) -> [String: Self] {
        let earlyBallot = election.rules == "twoUndifferentiatedVotes"
        let notesByName = Dictionary(grouping: election.reviewNotes) { $0.jurisdiction ?? "" }
        let rows = Dictionary(grouping: election.allocations.compactMap { row in
            row.geometryKey.map { ($0, row) }
        }, by: { $0.0 })
        return rows.compactMapValues { records in
            let group = records.map(\.1)
            guard let head = group.first else { return nil }
            let units = group.map { row in
                let kind: Kind
                switch row.kind {
                case "statewide": kind = .statewide
                case "atLarge": kind = .atLarge
                case "congressionalDistrict": kind = row.part.map(Kind.district) ?? .unknown(row.kind)
                case "historicalSplit": kind = .historicalSplit(row.part)
                case "faithless": kind = .faithless
                case "uncast": kind = .uncast
                case "rejected": kind = .rejected
                default: kind = .unknown(row.kind)
                }
                let status: Status = switch row.status {
                case "nonparticipating": .nonparticipating
                case "reported":
                    row.candidateID.flatMap { colors[$0] } != nil || [.uncast, .rejected].contains(kind)
                        ? .reported : .unresolved
                default: .unresolved
                }
                return Unit(id: row.id, recipientID: row.candidateID,
                            name: row.candidateID.flatMap { names[$0] },
                            affiliation: row.partyID.flatMap { parties[$0] },
                            color: row.candidateID.flatMap { colors[$0] }, quantity: row.electoralVotes,
                            kind: kind, status: status, source: election.decision == "syntheticFixture"
                                ? "Synthetic allocation \(row.sourceLine)" : "State CSV row \(row.sourceLine)")
            }
            let containsUncastOrRejected = units.contains { $0.quantity > 0 && [.uncast, .rejected].contains($0.kind) }
            let basis: Basis = earlyBallot ? .earlyBallot : containsUncastOrRejected ? .allocationSlots : .presidential
            let notes = notesByName[head.stateName] ?? []
            // Apportionment notes are the normalized contract's explicit assigned-count evidence.
            let assigned = notes.first { $0.kind == "apportionment" }?.votes
            let status: Status = units.contains { $0.status == .unresolved } ? .unresolved
                : units.contains { $0.quantity > 0 && $0.status == .reported } ? .reported : .nonparticipating
            return Self(jurisdictionID: head.jurisdictionID, name: head.stateName, basis: basis,
                        status: status, assigned: assigned, units: units, notes: notes.map(\.text))
        }
    }
}

/// A launch-only research fixture on the same geographic revision.
enum ElectoralAnnotationFixture {
    static func make(from base: ElectionRecord, updated: Bool) -> (ElectionRecord, [ElectionIdentity], [ElectionIdentity])? {
        guard let geography = base.allocations.first(where: { $0.stateName == "Maine" })?.geometryKey else { return nil }
        let candidates = [
            ElectionIdentity(id: "fixture-aster", name: "Aster"),
            ElectionIdentity(id: "fixture-birch", name: "Birch"),
            ElectionIdentity(id: "fixture-cedar", name: "Cedar")
        ]
        let party = ElectionIdentity(id: "fixture-affiliation", name: "Fixture affiliation")
        let recipients = updated ? [0, 2, 1, 1] : [0, 0, 1]
        let kinds = updated ? ["atLarge", "congressionalDistrict", "congressionalDistrict", "faithless"]
            : ["atLarge", "congressionalDistrict", "congressionalDistrict"]
        let quantities = updated ? [2, 1, 1, 1] : [2, 1, 1]
        let allocations = zip(recipients.indices, recipients).map { index, candidate in
            ElectoralAllocation(id: "fixture:\(index)", jurisdictionID: "state:23", stateName: "Maine",
                sourceLabel: "Synthetic unit \(index + 1)", sourceName: candidates[candidate].name, sourceStyle: "Synthetic",
                candidateID: candidates[candidate].id, partyID: party.id, electoralVotes: quantities[index],
                kind: kinds[index], part: kinds[index] == "congressionalDistrict" ? String(index) : nil,
                status: "reported", geometryKey: geography, sourceLine: index + 1)
        }
        let totals = Dictionary(grouping: allocations, by: \.candidateID).mapValues { $0.reduce(0) { $0 + $1.electoralVotes } }
        let results = candidates.enumerated().compactMap { index, candidate -> NationalVoteRecord? in
            guard let count = totals[candidate.id] else { return nil }
            return NationalVoteRecord(candidateID: candidate.id, name: candidate.name, partyID: party.id,
                kind: "candidate", sourceParty: party.name, sourceStyle: "Synthetic", electoralVotes: count,
                popularVotes: nil, sourcePopularVotes: nil, winner: index == 0, sourceIndex: index)
        }
        let total = quantities.reduce(0, +)
        let record = ElectionRecord(id: "synthetic:electoral", year: base.year, winnerID: candidates[0].id,
            decision: "syntheticFixture", rules: "separatePresidentialBallot", reportedMajority: total / 2 + 1,
            totals: .init(reported: total, nationalRows: total, allocationRows: total), geography: base.geography,
            results: results, reviewNotes: [], allocations: allocations)
        return (record, candidates, [party])
    }
}
