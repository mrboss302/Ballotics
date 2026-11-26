import BalloticsGeoKitApple
import SwiftUI

struct ElectionMapPresentation {
    struct Legend: Identifiable {
        let id: String
        let title: String
        let color: String
        let votes: Int?
    }

    static let noData = "#cbd2d7"
    static let nonparticipating = "#e8e5df"
    static let unresolved = "#a58f7c"
    static let tied = "#706383"

    struct StateSummary: Identifiable {
        let id: String
        let name: String
        let records: Int
        let joined: Int
        let reportedVotes: Int
    }

    struct ElectoralKeyEntry: Identifiable {
        let id: String
        let jurisdiction: String
        let votes: Int?
        let status: String
    }

    let states: [StateSummary]
    let electoralKey: [ElectoralKeyEntry]
    let year: Int
    let mode: ElectionMapMode
    let winner: String
    let totals: String
    let legend: [Legend]
    let fills: [String: [String]]
    let electoralVotesByGeometry: [String: Int]
    let margins: [String: Double]
    let details: [String: [(String, String)]]
    let unmapped: [LocalElectionRecord]
    let hasSplit: Bool
    let reviewNotes: [ElectionRecord.ReviewNote]
    let unmappedAllocations: [ElectoralAllocation]

    static func make(election: ElectionRecord, mode: ElectionMapMode, catalog: ([ElectionIdentity], [ElectionIdentity]),
                     county: [LocalElectionRecord]) -> Self {
        let names = Dictionary(uniqueKeysWithValues: catalog.0.map { ($0.id, $0.name) })
        let parties = Dictionary(uniqueKeysWithValues: catalog.1.map { ($0.id, $0.name) })
        var affiliations: [String: String] = [:]
        for row in election.results {
            if let id = row.candidateID { affiliations[id] = row.partyID }
        }
        if mode == .countyResults {
            for result in county {
                for vote in result.results {
                    if let id = vote.candidateID, affiliations[id] == nil { affiliations[id] = vote.partyID }
                }
            }
        }
        let orderedCandidates = election.results.compactMap(\.candidateID)
        let candidates = orderedCandidates + Set(county.compactMap(\.winnerID)).subtracting(orderedCandidates).sorted()
        let colors = candidateColors(candidates, affiliations: affiliations)
        let declaredWinner = names[election.winnerID] ?? election.winnerID
        let method = election.decision == "houseContingent" ? " · elected by the House" : ""
        var legend: [Legend] = []
        var electoralKey: [ElectoralKeyEntry] = []
        var fills: [String: [String]] = [:]
        var electoralVotesByGeometry: [String: Int] = [:]
        var margins: [String: Double] = [:]
        var details: [String: [(String, String)]] = [:]
        let totals: String
        let unmapped: [LocalElectionRecord]
        if mode == .electoralCollege {
            totals = "Electoral votes · reported \(election.totals.reported) · national rows \(election.totals.nationalRows) · state rows \(election.totals.allocationRows)\nReported majority \(election.reportedMajority)"
                + (election.rules == "twoUndifferentiatedVotes" ? " · pre-12th Amendment ballot" : "")
            for (offset, row) in election.results.enumerated() {
                let party = row.partyID.flatMap { parties[$0] }
                let name = row.name + (party.map { " · \($0)" } ?? "")
                legend.append(Legend(id: row.candidateID ?? "aggregate:\(offset)", title: name,
                                     color: row.candidateID.flatMap { colors[$0] } ?? unresolved, votes: row.electoralVotes))
            }
            let groups = Dictionary(grouping: election.allocations.filter { $0.geometryKey != nil }) {
                $0.geometryKey ?? ""
            }
            for (key, group) in groups {
                let positive = group.filter { $0.electoralVotes > 0 }
                let ids = orderedCandidates.filter { id in positive.contains { $0.candidateID == id } }
                let status: String
                let paint: [String]
                if group.contains(where: { $0.status == "unresolved" }) || positive.contains(where: { row in
                    row.candidateID.map { !ids.contains($0) } ?? true
                }) {
                    status = "Unresolved allocation"
                    paint = [unresolved]
                } else if ids.isEmpty {
                    status = "Nonparticipating"
                    paint = [nonparticipating]
                } else {
                    status = ids.count > 1 ? "Split electoral allocation" : "Reported electoral allocation"
                    paint = ids.compactMap { colors[$0] }
                }
                if !positive.isEmpty, status != "Unresolved allocation" {
                    electoralVotesByGeometry[key] = positive.reduce(0) { $0 + $1.electoralVotes }
                }
                if let name = group.first?.stateName {
                    electoralKey.append(.init(id: key, jurisdiction: name, votes: electoralVotesByGeometry[key], status: status))
                }
                fills[key] = paint
                details[key] = [("Result", status)] + group.map { row in
                    let name = row.candidateID.flatMap { names[$0] } ?? "Unassigned"
                    let party = row.partyID.flatMap { parties[$0] }.map { " · \($0)" } ?? ""
                    return ("\(row.sourceLabel) · CSV row \(row.sourceLine)", "\(name)\(party) · \(row.electoralVotes) EV")
                } + election.reviewNotes.filter { $0.jurisdiction == group.first?.stateName }.map {
                    ("Historical review", $0.text)
                }
            }
            unmapped = []
        } else {
            unmapped = county.filter { $0.geometryKey == nil }
            totals = "\(county.count.formatted()) reporting records · \(unmapped.count.formatted()) without polygons"
            let winners = Set(county.compactMap(\.winnerID))
            legend = candidates.filter(winners.contains).map { id in
                let party = affiliations[id].flatMap { parties[$0] }.map { " · \($0)" } ?? ""
                return Legend(id: id, title: (names[id] ?? id) + party, color: colors[id] ?? unresolved, votes: nil)
            }
            for result in county {
                guard let key = result.geometryKey else { continue }
                fills[key] = [countyColor(result, colors: colors)]
                details[key] = countyDetails(result, names: names, parties: parties)
                if result.status == "reported", let margin = result.marginVotes, result.totalVotes > 0 {
                    let share = Double(margin) / Double(result.totalVotes)
                    switch result.winnerID.flatMap({ affiliations[$0] }) {
                    case "democrat": margins[key] = -share
                    case "republican": margins[key] = share
                    default: break
                    }
                }
            }
        }
        let states = Dictionary(grouping: county, by: \.stateCode).compactMap { code, rows -> StateSummary? in
            guard let name = rows.first?.stateName, !code.isEmpty else { return nil }
            return StateSummary(id: code, name: name, records: rows.count,
                                joined: rows.filter { $0.geometryKey != nil }.count,
                                reportedVotes: rows.filter { $0.status == "reported" }.reduce(0) { $0 + $1.totalVotes })
        }.sorted { $0.name < $1.name }
        return Self(states: states, electoralKey: electoralKey.sorted { $0.jurisdiction < $1.jurisdiction }, year: election.year, mode: mode, winner: declaredWinner + method, totals: totals,
                    legend: legend, fills: fills, electoralVotesByGeometry: electoralVotesByGeometry, margins: margins, details: details, unmapped: unmapped,
                    hasSplit: fills.values.contains { $0.count > 1 },
                    reviewNotes: mode == .electoralCollege ? election.reviewNotes : [],
                    unmappedAllocations: mode == .electoralCollege
                        ? election.allocations.filter { $0.geometryKey == nil && $0.electoralVotes > 0 } : [])
    }

    func inspection(_ attributes: [String: Any]) -> Inspection {
        let base = Inspection(attributes: attributes)
        let key = ElectionGeographyResolver.featureKey(attributes, mode: mode)
        let fallback = mode == .electoralCollege
            && attributes["TERR_TYPE"] as? String != "State"
            && attributes["TERR_TYPE"] as? String != "District of Columbia" ? "Nonparticipating territory" : "No joined result"
        return Inspection(title: base.title, properties: (key.flatMap { details[$0] } ?? [("Result", fallback)])
                          + base.properties, stateCode: base.stateCode)
    }

    static func countyDetails(_ row: LocalElectionRecord, names: [String: String] = [:],
                              parties: [String: String] = [:]) -> [(String, String)] {
        let leader = row.results.first
        let label: String
        switch row.status {
        case "tied": label = "Tied"
        case "unresolved": label = "Unresolved vote totals or candidate identity"
        case "noData": label = "No reported votes"
        default:
            if let id = row.winnerID, let leader {
                label = (names[id] ?? leader.name) + " · " + (leader.partyID.flatMap { parties[$0] } ?? leader.sourceParty)
            } else {
                label = "No joined result"
            }
        }
        var fields = [("Result", label), ("Reporting unit", "\(row.name), \(row.stateName)"),
                      ("Reporting code", row.sourceCode), ("Geographic join", joinLabel(row.joinStatus)),
                      ("Reported votes", row.totalVotes.formatted()), ("Listed votes", row.listedVotes.formatted())]
        if let review = row.reportingReview {
            fields.append(("Official Alaska return", review.reportedVotes.formatted()))
            fields.append(("Review source", review.source))
        }
        if let margin = row.marginVotes, row.totalVotes > 0 {
            let share = Double(margin) / Double(row.totalVotes)
            fields.append(("Margin", "\(margin.formatted()) votes · \(share.formatted(.percent.precision(.fractionLength(1))))"))
        }
        for vote in row.results {
            let name = vote.candidateID.flatMap { names[$0] } ?? vote.name
            let share = vote.share.map { " · " + $0.formatted(.percent.precision(.fractionLength(1))) } ?? ""
            fields.append((name, vote.votes.formatted() + share))
        }
        return fields
    }

    static func joinLabel(_ status: String) -> String {
        switch status {
        case "fallback": "2024 boundary fallback"
        case "traditional": "Traditional Connecticut county"
        case "matched": "2024 boundary"
        case "nonGeographic": "Non-geographic reporting"
        case "overlappingReportingJurisdiction": "Overlapping reporting jurisdiction"
        case "federalOverseasAbsentee": "Federal overseas absentee"
        case "classificationUnresolved": "Reporting classification under review"
        case "boundaryChange": "Boundary change under review"
        default: "Unmatched geographic identifier"
        }
    }

    private static func countyColor(_ row: LocalElectionRecord, colors: [String: String]) -> String {
        switch row.status {
        case "tied": tied
        case "unresolved": unresolved
        case "reported": row.winnerID.flatMap { colors[$0] } ?? noData
        default: noData
        }
    }

    private static func candidateColors(_ ids: [String], affiliations: [String: String]) -> [String: String] {
        // Palette is a presentation choice. Candidates sharing an affiliation get distinct colors in the same election.
        let partyColors = ["democrat": "#326baa", "republican": "#b34e48", "federalist": "#5d397c",
                           "democratic-republican": "#3f795c", "whig": "#9f6824", "free-soil": "#527278",
                           "progressive": "#695393", "libertarian": "#b98618", "green": "#367e45",
                           "independent": "#515c63", "american-independent": "#aa7451"]
        let secondary = ["#d48641", "#8e5796", "#327e8c", "#827543", "#4c547c", "#8b674b", "#6b8453", "#846e8f"]
        var used: Set<String> = []
        return Dictionary(uniqueKeysWithValues: ids.enumerated().map { index, id in
            let preferred = affiliations[id].flatMap { partyColors[$0] }
            let color = preferred.flatMap { used.contains($0) ? nil : $0 }
                ?? secondary.first { !used.contains($0) } ?? secondary[index % secondary.count]
            used.insert(color)
            return (id, color)
        })
    }
}

extension UIColor {
    convenience init(electionHex hex: String) {
        let value = UInt32(hex.dropFirst(), radix: 16) ?? 0xCBD2D7
        self.init(red: CGFloat((value >> 16) & 255) / 255, green: CGFloat((value >> 8) & 255) / 255,
                  blue: CGFloat(value & 255) / 255, alpha: 1)
    }
}

struct ElectionResultsPanel: View {
    let result: ElectionMapPresentation
    var theme: AlbersTheme = .candidates

    var body: some View {
        NavigationStack {
            List {
                Section("Presidential outcome") { Text(result.winner) }
                Section(result.mode.rawValue) {
                    Text(result.totals)
                    if theme == .margin, result.mode == .countyResults {
                        CountyMarginLegend()
                    } else {
                        ForEach(result.legend) { entry in
                            HStack {
                                Circle().fill(Color(uiColor: theme.color(entry.color))).frame(width: 12, height: 12)
                                Text(entry.title)
                                Spacer()
                                if let votes = entry.votes { Text(votes.formatted()).monospacedDigit() }
                            }
                        }
                    }
                    if result.hasSplit { Text("Striped fill · multiple reported allocations") }
                    statusKey("No data", color: ElectionMapPresentation.noData)
                    statusKey("Unresolved", color: ElectionMapPresentation.unresolved)
                    if result.mode == .electoralCollege {
                        statusKey("Nonparticipating", color: ElectionMapPresentation.nonparticipating)
                    } else {
                        statusKey("Tied", color: ElectionMapPresentation.tied)
                    }
                }
                if !result.electoralKey.isEmpty {
                    Section("Electoral vote key") {
                        ForEach(result.electoralKey) { entry in
                            NavigationLink {
                                List(Array((result.details[entry.id] ?? []).enumerated()), id: \.offset) { _, field in
                                    LabeledContent(field.0, value: field.1)
                                }
                                .navigationTitle(entry.jurisdiction)
                            } label: {
                                LabeledContent(entry.jurisdiction, value: entry.votes.map { "\($0) EV" } ?? entry.status)
                            }
                        }
                    }
                }
                if !result.reviewNotes.isEmpty {
                    Section("Historical review") {
                        ForEach(Array(result.reviewNotes.enumerated()), id: \.offset) { _, note in
                            VStack(alignment: .leading, spacing: 4) {
                                Text(note.text)
                                if let source = URL(string: note.source) { Link("Source", destination: source).font(.caption) }
                            }
                        }
                    }
                }
                if !result.unmappedAllocations.isEmpty {
                    Section("Allocations outside this snapshot") {
                        ForEach(result.unmappedAllocations, id: \.id) { row in
                            LabeledContent(row.sourceLabel, value: "\(row.sourceName) · \(row.electoralVotes) EV")
                        }
                    }
                }
                if !result.unmapped.isEmpty {
                    Section("Reporting records without polygons") {
                        ForEach(result.unmapped, id: \.id) { row in
                            NavigationLink("\(row.name), \(row.stateName)") {
                                List(Array(ElectionMapPresentation.countyDetails(row).enumerated()), id: \.offset) { _, field in
                                    LabeledContent(field.0, value: field.1)
                                        .accessibilityElement(children: .combine)
                                        .accessibilityLabel("\(field.0), \(field.1)")
                                        .accessibilityIdentifier(field.0 == "Official Alaska return" ? "district-official-votes" : field.0)
                                }
                                .navigationTitle(row.name)
                                .navigationBarTitleDisplayMode(.inline)
                            }
                        }
                    }
                }
            }
            .navigationTitle(Text(verbatim: "\(result.year) results"))
            .navigationBarTitleDisplayMode(.inline)
            .toolbar { CloseElectionResults() }
        }
    }

    private func statusKey(_ title: String, color: String) -> some View {
        Label { Text(title) } icon: {
            Circle().fill(Color(uiColor: UIColor(electionHex: color))).frame(width: 12, height: 12)
        }
    }
}

private struct CloseElectionResults: View {
    @Environment(\.dismiss) private var dismiss
    var body: some View { Button("Done") { dismiss() } }
}


struct CountyMarginLegend: View {
    var body: some View {
        VStack(alignment: .leading, spacing: 3) {
            HStack {
                Text("Dem. +100")
                Spacer()
                Text("0")
                Spacer()
                Text("Rep. +100")
            }
            LinearGradient(colors: [-1.0, 0, 1].map { Color(cgColor: AlbersTheme.marginScale.color($0)) },
                           startPoint: .leading, endPoint: .trailing)
                .frame(height: 7)
                .clipShape(Capsule())
        }
        .font(.caption2)
        .accessibilityElement(children: .ignore)
        .accessibilityLabel("Reported leader to runner-up margin as a share of reported total votes. Democratic and Republican colors. Other winners remain categorical.")
        .accessibilityIdentifier("albers-margin-legend")
    }
}
