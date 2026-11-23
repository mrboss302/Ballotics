import SwiftUI
import MapLibre
import CoreLocation
import UniformTypeIdentifiers

private enum Geography: String, CaseIterable, Identifiable {
    case districts = "Districts"
    case counties = "Counties"
    case states = "States"
    case imported = "Imported file"

    var id: String { rawValue }

    var resource: String? {
        switch self {
        case .districts: return "cb_2024_us_cd119_5m"
        case .counties: return "cb_2024_us_county_5m"
        case .states: return "cb_2024_us_state_5m"
        case .imported: return nil
        }
    }
}

private final class LabState: ObservableObject {
    @Published var geography: Geography = .districts
    @Published var importedURL: URL?
    @Published var status = "Loading local map style…"
    @Published var firstIdleSeconds: Double?
    @Published var selection = "Tap a polygon to inspect its GEOID."
    @Published var error: String?

    var currentURL: URL? {
        if geography == .imported { return importedURL }
        guard let name = geography.resource else { return nil }
        return Bundle.main.url(forResource: name, withExtension: "json")
    }

    func importGeoJSON(_ result: Result<URL, Error>) {
        do {
            let url = try result.get()
            let access = url.startAccessingSecurityScopedResource()
            defer { if access { url.stopAccessingSecurityScopedResource() } }
            let directory = FileManager.default.urls(for: .documentDirectory, in: .userDomainMask)[0]
                .appendingPathComponent("GeoJSONImports", isDirectory: true)
            try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
            // Unique URL forces MapLibre to replace its source on reimport.
            let destination = directory.appendingPathComponent(
                UUID().uuidString + "-" + url.lastPathComponent
            )
            try FileManager.default.copyItem(at: url, to: destination)
            importedURL = destination
            error = nil
            geography = .imported
        } catch {
            self.error = error.localizedDescription
        }
    }
}

@main
struct BalloticsMapLibreLabApp: App {
    var body: some Scene {
        WindowGroup {
            LabView()
        }
    }
}

private struct LabView: View {
    @StateObject private var state = LabState()
    @State private var showImporter = false

    var body: some View {
        VStack(spacing: 12) {
            VStack(alignment: .leading, spacing: 4) {
                Text("Ballotics MapLibre GeoJSON Lab").font(.headline)
                Text("Direct GeoJSON · offline style · research only")
                    .font(.caption).foregroundStyle(.secondary)
            }
            .frame(maxWidth: .infinity, alignment: .leading)

            HStack {
                Picker("Geometry", selection: $state.geography) {
                    ForEach(Geography.allCases) { geography in
                        if geography != .imported || state.importedURL != nil {
                            Text(geography.rawValue).tag(geography)
                        }
                    }
                }
                .pickerStyle(.menu)
                Button("Import file") { showImporter = true }
                    .buttonStyle(.bordered)
            }

            MapLibreGeometryView(state: state)
                .frame(maxWidth: .infinity, maxHeight: .infinity)

            VStack(alignment: .leading, spacing: 5) {
                Text(state.status).font(.subheadline)
                if let seconds = state.firstIdleSeconds {
                    Text(String(format: "Added source → first idle: %.3f s (indicative)", seconds))
                        .font(.caption).monospacedDigit()
                }
                Text(state.selection).font(.caption).lineLimit(3)
                if let error = state.error {
                    Text(error).font(.caption).foregroundStyle(.red)
                }
                Text("Use Xcode Instruments for actual FPS and resident memory.")
                    .font(.caption2).foregroundStyle(.secondary)
            }
            .frame(maxWidth: .infinity, alignment: .leading)
        }
        .padding()
        .fileImporter(
            isPresented: $showImporter,
            allowedContentTypes: [.json, UTType(filenameExtension: "geojson") ?? .json]
        ) { state.importGeoJSON($0) }
    }
}

private struct MapLibreGeometryView: UIViewRepresentable {
    @ObservedObject var state: LabState

    func makeCoordinator() -> Coordinator { Coordinator(state: state) }

    func makeUIView(context: Context) -> MLNMapView {
        let map = MLNMapView(
            frame: .zero,
            styleURL: Bundle.main.url(forResource: "offline-style", withExtension: "json")
        )
        map.delegate = context.coordinator
        map.setCenter(
            CLLocationCoordinate2D(latitude: 39.5, longitude: -97),
            zoomLevel: 2.5,
            animated: false
        )
        let tap = UITapGestureRecognizer(
            target: context.coordinator,
            action: #selector(Coordinator.handleTap(_:))
        )
        tap.cancelsTouchesInView = false
        map.addGestureRecognizer(tap)
        return map
    }

    func updateUIView(_ map: MLNMapView, context: Context) {
        // Avoid publishing ObservableObject changes during SwiftUI's view update.
        let coordinator = context.coordinator
        DispatchQueue.main.async {
            coordinator.applyIfNeeded(on: map)
        }
    }

    final class Coordinator: NSObject, MLNMapViewDelegate {
        private let sourceID = "ballotics-geojson"
        private let fillID = "ballotics-fill"
        private let lineID = "ballotics-border"
        private let selectID = "ballotics-selection"
        private let state: LabState
        private var loadedURL: URL?
        private var started: TimeInterval?

        init(state: LabState) { self.state = state }

        func mapView(_ mapView: MLNMapView, didFinishLoading style: MLNStyle) {
            loadedURL = nil
            applyIfNeeded(on: mapView)
        }

        func applyIfNeeded(on map: MLNMapView) {
            guard let style = map.style else { return }
            let url = state.currentURL
            guard loadedURL != url else { return }
            loadedURL = url
            started = nil
            state.firstIdleSeconds = nil
            state.selection = "Tap a polygon to inspect its GEOID."

            for identifier in [selectID, lineID, fillID] {
                if let layer = style.layer(withIdentifier: identifier) {
                    style.removeLayer(layer)
                }
            }
            if let source = style.source(withIdentifier: sourceID) {
                style.removeSource(source)
            }
            guard let url, FileManager.default.fileExists(atPath: url.path) else {
                state.status = "No local GeoJSON file found."
                return
            }
            state.status = "Loading " + url.lastPathComponent
            started = ProcessInfo.processInfo.systemUptime

            // The unmodified GeoJSON file URL is handed straight to MapLibre.
            let source = MLNShapeSource(identifier: sourceID, url: url, options: nil)
            style.addSource(source)

            let fill = MLNFillStyleLayer(identifier: fillID, source: source)
            fill.fillColor = NSExpression(forConstantValue: UIColor(
                red: 0.30, green: 0.55, blue: 0.76, alpha: 1
            ))
            fill.fillOpacity = NSExpression(forConstantValue: 0.88)
            style.addLayer(fill)

            let line = MLNLineStyleLayer(identifier: lineID, source: source)
            line.lineColor = NSExpression(forConstantValue: UIColor.white)
            line.lineWidth = NSExpression(forConstantValue: 0.65)
            style.addLayer(line)

            let selected = MLNFillStyleLayer(identifier: selectID, source: source)
            selected.fillColor = NSExpression(forConstantValue: UIColor.systemYellow)
            selected.fillOpacity = NSExpression(forConstantValue: 0.75)
            selected.predicate = NSPredicate(value: false)
            style.addLayer(selected)

            map.setCenter(
                CLLocationCoordinate2D(latitude: 39.5, longitude: -97),
                zoomLevel: 2.5,
                animated: false
            )
        }

        func mapViewDidBecomeIdle(_ map: MLNMapView) {
            guard let started else { return }
            self.started = nil
            state.firstIdleSeconds = ProcessInfo.processInfo.systemUptime - started
            state.status = "Map idle. Visually verify polygons, zoom and tapping."
        }

        func mapViewDidFailLoadingMap(_ map: MLNMapView, withError error: Error) {
            started = nil
            state.status = "Map loading failed: " + error.localizedDescription
        }

        @objc func handleTap(_ recognizer: UITapGestureRecognizer) {
            guard let map = recognizer.view as? MLNMapView,
                  let selectionLayer = map.style?.layer(withIdentifier: selectID)
                    as? MLNFillStyleLayer else { return }
            let features = map.visibleFeatures(
                at: recognizer.location(in: map),
                styleLayerIdentifiers: Set([fillID])
            )
            guard let feature = features.first else {
                selectionLayer.predicate = NSPredicate(value: false)
                state.selection = "No polygon at this location."
                return
            }
            let fields = ["NAME", "NAMELSAD", "GEOID", "STATEFP", "CD119FP"]
            let parts = fields.compactMap { key -> String? in
                guard let value = feature.attributes[key] else { return nil }
                return key + ": " + String(describing: value)
            }
            state.selection = parts.isEmpty ? "Polygon found, no known properties" :
                parts.joined(separator: " • ")
            if let geoid = feature.attributes["GEOID"] as? String {
                selectionLayer.predicate = NSPredicate(format: "GEOID == %@", geoid)
            } else {
                selectionLayer.predicate = NSPredicate(value: false)
            }
        }
    }
}
