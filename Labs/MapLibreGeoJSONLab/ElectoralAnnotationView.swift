import UIKit

/// Camera-independent artwork backed by a single accessible geographic action.
@MainActor
final class ElectoralAnnotationView: UIControl {
    private typealias Unit = ElectoralAnnotation.Unit
    private let square = UILabel()
    private let abbreviation = UILabel()
    private var symbols: [UIView] = []
    private let palette = UIColor(red: 0.93, green: 0.90, blue: 0.83, alpha: 1)
    private var compact = false
    private var grouped = false

    override init(frame: CGRect) {
        super.init(frame: frame)
        isAccessibilityElement = true
        accessibilityTraits = .button
        for label in [square, abbreviation] {
            label.textAlignment = .center
            label.isAccessibilityElement = false
            addSubview(label)
        }
        square.font = .monospacedDigitSystemFont(ofSize: 13, weight: .medium)
        abbreviation.font = .systemFont(ofSize: 10, weight: .medium)
        abbreviation.textColor = .label
    }

    required init?(coder: NSCoder) { nil }

    func configure(code: String, count: String?, fill: UIColor, ink: UIColor,
                   allocation: ElectoralAnnotation?, artwork: Bool, compact: Bool, unitColor: (String) -> UIColor) {
        self.compact = compact
        grouped = false
        abbreviation.text = code
        accessibilityLabel = allocation?.accessibilityDescription ?? "\(code), allocation unavailable"
        accessibilityHint = "Selects this jurisdiction on the map"
        square.backgroundColor = fill
        square.textColor = ink
        refreshTypography()
        square.text = count ?? "?"
        for view in symbols { view.removeFromSuperview() }
        symbols = []
        if artwork, let allocation, allocation.allocationArtwork {
            square.backgroundColor = palette
            square.textColor = .black
            square.text = nil
            if let units = allocation.squares, !compact || units.count <= 6 {
                symbols = units.map { unit in makeSymbol(unit, mark: Self.mark(for: unit, in: allocation), palette: unitColor) }
            } else if allocation.groupedQuantities.count <= 3 {
                grouped = true
                symbols = allocation.groupedQuantities.map { group in
                    makeSymbol(group.unit, mark: Self.mark(for: group.unit, in: allocation) + String(group.quantity), palette: unitColor)
                }
            } else {
                let label = UILabel()
                label.textAlignment = .center
                label.font = .monospacedDigitSystemFont(ofSize: compact ? 8 : 10, weight: .medium)
                label.textColor = .black
                label.text = (count ?? "?") + "·"
                abbreviation.text = code + " split"
                abbreviation.font = .systemFont(ofSize: 8, weight: .medium)
                symbols = [label]
            }
            symbols.forEach { view in
                view.isAccessibilityElement = false
                view.isUserInteractionEnabled = false
                addSubview(view)
            }
        }
        setNeedsLayout()
    }

    private static func mark(for unit: Unit, in allocation: ElectoralAnnotation) -> String {
        switch unit.kind {
        case .uncast: return "–"
        case .rejected: return "×"
        case .faithless: return "!"
        default: break
        }
        guard let id = unit.recipientID, let first = unit.name?.split(separator: " ").last?.first else { return "?" }
        let matching = allocation.units.filter { $0.name?.split(separator: " ").last?.first == first }
        guard Set(matching.compactMap(\.recipientID)).count > 1 else { return String(first) }
        let order = allocation.recipients.sorted()
        return String((order.firstIndex(of: id) ?? 0) + 1)
    }

    private func makeSymbol(_ unit: Unit, mark: String, palette: (String) -> UIColor) -> UIView {
        let label = UILabel()
        let color = unit.color.map(palette) ?? .systemGray3
        label.backgroundColor = color
        label.textColor = color.annotationInk
        label.textAlignment = .center
        label.font = .systemFont(ofSize: 8, weight: .semibold)
        label.text = mark
        return label
    }

    private func refreshTypography() {
        square.font = UIFontMetrics(forTextStyle: .caption1).scaledFont(
            for: .monospacedDigitSystemFont(ofSize: compact ? 9 : 13, weight: .medium),
            maximumPointSize: compact ? 10 : 15, compatibleWith: traitCollection)
        abbreviation.font = UIFontMetrics(forTextStyle: .caption2).scaledFont(
            for: .systemFont(ofSize: 10, weight: .medium), maximumPointSize: 12, compatibleWith: traitCollection)
    }

    override func accessibilityActivate() -> Bool {
        guard isEnabled, !isHidden else { return false }
        sendActions(for: .touchUpInside)
        return true
    }

    override var isSelected: Bool {
        didSet {
            accessibilityTraits = isSelected ? [.button, .selected] : .button
            layer.borderWidth = isSelected ? 1 : 0
            layer.borderColor = UIColor.label.cgColor
        }
    }

    override func layoutSubviews() {
        super.layoutSubviews()
        refreshTypography()
        let side: CGFloat = compact ? 24 : 28
        let origin = CGPoint(x: (bounds.width - side) / 2, y: 1)
        square.frame = CGRect(origin: origin, size: CGSize(width: side, height: side))
        abbreviation.frame = CGRect(x: 0, y: side + 1, width: bounds.width, height: 13)
        if grouped {
            let height = floor((side - CGFloat(symbols.count + 1)) / CGFloat(symbols.count))
            for (index, view) in symbols.enumerated() {
                view.frame = CGRect(x: origin.x + 1, y: origin.y + 1 + CGFloat(index) * (height + 1),
                                    width: side - 2, height: height)
            }
            return
        }
        let columns = symbols.count <= 4 ? 2 : 3
        let gap: CGFloat = 1
        let size = floor((side - CGFloat(columns + 1) * gap) / CGFloat(columns))
        let rows = (symbols.count + columns - 1) / columns
        for (index, view) in symbols.enumerated() {
            let y = origin.y + (side - CGFloat(rows) * (size + gap) + gap) / 2
            if symbols.count == 1 { view.frame = square.frame }
            else {
                view.frame = CGRect(x: origin.x + gap + CGFloat(index % columns) * (size + gap),
                                    y: y + CGFloat(index / columns) * (size + gap), width: size, height: size)
            }
        }
    }
}

extension UIColor {
    var annotationInk: UIColor {
        var r: CGFloat = 0, g: CGFloat = 0, b: CGFloat = 0, a: CGFloat = 0
        guard getRed(&r, green: &g, blue: &b, alpha: &a) else { return .label }
        func linear(_ v: CGFloat) -> CGFloat { v <= 0.04045 ? v / 12.92 : pow((v + 0.055) / 1.055, 2.4) }
        return 0.2126 * linear(r) + 0.7152 * linear(g) + 0.0722 * linear(b) > 0.179 ? .black : .white
    }
}
