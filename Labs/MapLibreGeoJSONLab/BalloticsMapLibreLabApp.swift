import MapLibre
import SwiftUI
import UniformTypeIdentifiers

struct Dataset {
    let geography: Geography
    let url: URL
    let summary: GeoJSON.Summary
    let requestedAt: Double
    let imported: ImportedFile?
}

struct Inspection {
    let title: String
    let properties: [(key: String, value: String)]
    let stateCode: String?
}

enum Viewpoint: Equatable {
    case national, alaska, hawaii, selectedState
}

@MainActor
final class LabState: ObservableObject {
    @Published private(set) var geography: Geography = .districts
    @Published private(set) var dataset: Dataset?
    @Published private(set) var imported: ImportedFile?
    @Published var status = "Preparing local geometry…"
    @Published var inspection: Inspection?
    @Published var failure: String?
    @Published private(set) var importing = false
    @Published private(set) var cameraRequest: (Int, Viewpoint) = (0, .national)
    @Published private(set) var alternateColor = false

    private var validations: [Geography: GeoJSON.Summary] = [:]
    private var loading: Task<Void, Never>?

    var canZoomToSelection: Bool {
        guard geography == .states, let stateCode = inspection?.stateCode else { return false }
        return dataset?.summary.stateBounds[stateCode] != nil
    }

    func start() {
        guard dataset == nil, loading == nil else { return }
        let arguments = ProcessInfo.processInfo.arguments
        if let index = arguments.firstIndex(of: "--dataset"),
           let name = arguments.dropFirst(index + 1).first,
           let geography = Geography.allCases.first(where: { $0.rawValue.lowercased() == name.lowercased() }),
           geography != .imported {
            choose(geography)
        } else if arguments.contains("--historical"), let folder = FileManager.default.urls(
            for: .documentDirectory, in: .userDomainMask
        ).first {
            importGeoJSON(.success(folder.appendingPathComponent("US_HistCounties_Gen05.json")))
        } else {
            choose(.districts)
        }
    }

    func choose(_ next: Geography) {
        guard geography != next || dataset?.geography != next else { return }
        let requestedAt = LabLog.now
        if next == .imported {
            guard let imported else { return }
            loading?.cancel()
            loading = nil
            prepareSwitch(to: next)
            dataset = Dataset(geography: next, url: imported.url, summary: imported.summary,
                              requestedAt: requestedAt, imported: imported)
            return
        }

        guard let name = next.resource, let url = Bundle.main.url(forResource: name, withExtension: "json") else {
            failure = "The bundled \(next.rawValue.lowercased()) file is missing."
            return
        }
        loading?.cancel()
        prepareSwitch(to: next)
        if let summary = validations[next] {
            loading = nil
            dataset = Dataset(geography: next, url: url, summary: summary,
                              requestedAt: requestedAt, imported: nil)
            return
        }

        var baseline = MemorySample.read()?.fields("before_validation") ?? [:]
        baseline["dataset"] = next.rawValue
        LabLog.record("validation_start", baseline)
        loading = Task {
            let validate = Task.detached(priority: .userInitiated) { try GeoJSON.validate(url, geography: next) }
            defer { validate.cancel() }
            do {
                let summary = try await withTaskCancellationHandler {
                    try await validate.value
                } onCancel: {
                    validate.cancel()
                }
                try Task.checkCancellation()
                validations[next] = summary
                var fields = MemorySample.read()?.fields("after_validation") ?? [:]
                fields["dataset"] = next.rawValue
                fields["validation_ms"] = LabLog.milliseconds(LabLog.now - requestedAt)
                LabLog.record("validation_end", fields)
                dataset = Dataset(geography: next, url: url, summary: summary,
                                  requestedAt: requestedAt, imported: nil)
                loading = nil
            } catch is CancellationError {
                return
            } catch {
                guard !Task.isCancelled else { return }
                status = "Dataset unavailable"
                failure = error.localizedDescription
                dataset = nil
                loading = nil
            }
        }
    }

    func importGeoJSON(_ result: Result<URL, Error>) {
        guard !importing else { return }
        let requestedAt = LabLog.now
        let url: URL
        do {
            url = try result.get()
        } catch {
            failure = error.localizedDescription
            return
        }
        importing = true
        failure = nil
        var fields = MemorySample.read()?.fields("before_validation") ?? [:]
        fields["dataset"] = url.lastPathComponent
        LabLog.record("import_start", fields)
        Task {
            defer { importing = false }
            do {
                let file = try await Task.detached(priority: .userInitiated) { try ImportedFile.prepare(url) }.value
                loading?.cancel()
                loading = nil
                imported = file
                prepareSwitch(to: .imported)
                var fields = MemorySample.read()?.fields("after_validation") ?? [:]
                fields["dataset"] = file.displayName
                fields["copy_validation_ms"] = LabLog.milliseconds(file.preparationSeconds)
                LabLog.record("import_end", fields)
                dataset = Dataset(geography: .imported, url: file.url, summary: file.summary,
                                  requestedAt: requestedAt, imported: file)
            } catch {
                failure = "Could not import \(url.lastPathComponent). \(error.localizedDescription)"
            }
        }
    }

    func moveCamera(_ viewpoint: Viewpoint) {
        cameraRequest = (cameraRequest.0 + 1, viewpoint)
    }

    func recolor() { alternateColor.toggle() }

    private func prepareSwitch(to next: Geography) {
        geography = next
        inspection = nil
        failure = nil
        status = "Preparing \(next.rawValue.lowercased())…"
    }
}

@main
struct BalloticsMapLibreLabApp: App {
    init() {
        _ = LabLog.start
        let configuration = URLSessionConfiguration.ephemeral
        configuration.protocolClasses = [BlockMapHTTP.self]
        MLNNetworkConfiguration.sharedManager.sessionConfiguration = configuration
        LabLog.record("app_init")
    }

    var body: some Scene {
        WindowGroup { LabView() }
    }
}

private struct LabView: View {
    @StateObject private var state = LabState()
    @State private var showImporter = false
    @State private var showProperties = false

    var body: some View {
        VStack(alignment: .leading, spacing: 10) {
            Text("Ballotics GeoJSON Lab").font(.title2.bold())
            Text("Historical boundaries · Geometry only")
                .font(.caption).foregroundStyle(.secondary)

            HStack {
                Menu {
                    ForEach(Geography.allCases) { geography in
                        if geography != .imported || state.imported != nil {
                            Button(geography.rawValue) { state.choose(geography) }
                        }
                    }
                } label: {
                    Label(state.geography.rawValue, systemImage: "square.3.layers.3d")
                        .frame(minWidth: 96)
                }
                .accessibilityIdentifier("datasets")

                Button("Import file") { showImporter = true }
                    .disabled(state.importing)

                Spacer(minLength: 0)

                Menu {
                    Button("National") { state.moveCamera(.national) }
                    Button("Alaska") { state.moveCamera(.alaska) }
                    Button("Hawaii") { state.moveCamera(.hawaii) }
                    Button("Zoom to selected state") { state.moveCamera(.selectedState) }
                        .disabled(!state.canZoomToSelection)
                    Button("Recolor polygons") { state.recolor() }
                } label: {
                    Label("Map view", systemImage: "scope").labelStyle(.iconOnly)
                }
                .accessibilityIdentifier("viewpoints")
            }
            .buttonStyle(.bordered)

            MapLibreGeometryView(state: state)
                .clipShape(RoundedRectangle(cornerRadius: 12))
                .frame(maxWidth: .infinity, maxHeight: .infinity)
                .layoutPriority(1)

            VStack(alignment: .leading, spacing: 4) {
                Text(state.importing ? "Validating imported file…" : state.status)
                    .font(.caption)
                    .foregroundStyle(.secondary)
                    .accessibilityIdentifier("load-status")
                Button {
                    showProperties = true
                } label: {
                    Text(state.inspection?.title ?? "Tap a polygon to inspect it")
                        .font(.subheadline.weight(.semibold)).lineLimit(1)
                }
                .disabled(state.inspection == nil)
                .accessibilityIdentifier("inspection")
            }
        }
        .padding()
        .task { state.start() }
        .fileImporter(
            isPresented: $showImporter,
            allowedContentTypes: [.json, UTType(filenameExtension: "geojson") ?? .json]
        ) { state.importGeoJSON($0) }
        .alert("GeoJSON Lab", isPresented: Binding(
            get: { state.failure != nil }, set: { if !$0 { state.failure = nil } }
        )) {
            Button("OK") { state.failure = nil }
        } message: {
            Text(state.failure ?? "")
        }
        .sheet(isPresented: $showProperties) {
            if let inspection = state.inspection {
                NavigationStack {
                    List(inspection.properties, id: \.key) { property in
                        LabeledContent(property.key, value: property.value)
                    }
                    .navigationTitle(inspection.title)
                    .navigationBarTitleDisplayMode(.inline)
                    .toolbar { Button("Done") { showProperties = false } }
                }
            }
        }
    }
}
