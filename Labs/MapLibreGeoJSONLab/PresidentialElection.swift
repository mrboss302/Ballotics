import Foundation

struct PresidentialElection: Identifiable, Sendable {
    private enum Weekday: Int {
        case tuesday = 3
        case wednesday = 4
    }

    static let archiveResource = "US_HistStateTerr_Gen05"
    private static let archiveElectionYear = 2000
    private static let archiveElectionDay = day(year: archiveElectionYear, month: 11, weekday: .tuesday, startingOn: 2)

    let year: Int
    let electionDay: HistoricalDay
    let boundaries: HistoricalRequest

    var id: Int { year }
    var uses2000Boundaries: Bool { year > Self.archiveElectionYear }

    var snapshotDescription: String {
        if uses2000Boundaries {
            return "Election \(electionDay) · 2000 boundaries (\(boundaries.day))"
        }
        return "\(boundaries.day) · \(boundaries.provenance)"
    }

    private init(year: Int) {
        self.year = year
        if year == 1788 {
            // Confederation Congress resolution of September 13, 1788.
            electionDay = Self.day(year: 1789, month: 2, weekday: .wednesday, startingOn: 1)
        } else if year < 1848 {
            // 1 Stat. 239, sections 1–2. State appointment dates varied.
            electionDay = Self.day(year: year, month: 12, weekday: .wednesday, startingOn: 1)
        } else {
            // 5 Stat. 721. The first Tuesday after the first Monday falls on November 2–8.
            electionDay = Self.day(year: year, month: 11, weekday: .tuesday, startingOn: 2)
        }
        let boundaryDay = year > Self.archiveElectionYear ? Self.archiveElectionDay : electionDay
        let provenance = year < 1848 ? "Scheduled Electoral College vote" : "Presidential Election Day"
        boundaries = HistoricalRequest(day: boundaryDay, provenance: provenance)
    }

    static func available(asOf day: HistoricalDay) -> [Self] {
        let year = day.number / 10_000
        guard year >= 1788 else { return [] }
        return stride(from: 1788, through: year, by: 4)
            .map(Self.init(year:))
            .filter { $0.electionDay <= day }
            .reversed()
    }

    static func available(now: Date = .now) -> [Self] {
        var calendar = Calendar(identifier: .gregorian)
        calendar.timeZone = TimeZone(identifier: "America/New_York")!
        let today = calendar.dateComponents([.year, .month, .day], from: now)
        return available(asOf: civilDay(year: today.year!, month: today.month!, day: today.day!))
    }

    private static func day(year: Int, month: Int, weekday: Weekday, startingOn firstDay: Int) -> HistoricalDay {
        var calendar = Calendar(identifier: .gregorian)
        calendar.timeZone = .gmt
        let start = calendar.date(from: DateComponents(year: year, month: month, day: firstDay))!
        let offset = (weekday.rawValue - calendar.component(.weekday, from: start) + 7) % 7
        return civilDay(year: year, month: month, day: firstDay + offset)
    }

    private static func civilDay(year: Int, month: Int, day: Int) -> HistoricalDay {
        // Callers supply Gregorian calendar components or compute a day within the first eight.
        try! HistoricalDay(String(format: "%04d-%02d-%02d", year, month, day))
    }
}
