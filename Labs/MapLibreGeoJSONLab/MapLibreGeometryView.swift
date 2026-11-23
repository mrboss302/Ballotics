import MapLibre
import SwiftUI
import os

struct MapLibreGeometryView: UIViewRepresentable {
    @ObservedObject var state: LabState

    func makeCoordinator() -> Coordinator { Coordinator(state: state) }

    func makeUIView(context: Context) -> MLNMapView {
        // Avoid MLNMapView's remote default style if the bundled resource is missing.
        let localStyle = Bundle.main.url(forResource: "offline-style", withExtension: "json")
            ?? Bundle.main.bundleURL.appendingPathComponent("offline-style.json")
        let map = MLNMapView(frame: .zero, styleURL: localStyle)
        map.delegate = context.coordinator
        map.isRotateEnabled = false
        map.isPitchEnabled = false
        map.accessibilityIdentifier = "geometry-map"
        context.coordinator.installTap(on: map)
        return map
    }

    func updateUIView(_ map: MLNMapView, context: Context) {
        let coordinator = context.coordinator
        DispatchQueue.main.async { [weak map, weak coordinator] in
            guard let map, let coordinator else { return }
            coordinator.update(map)
        }
    }

    static func dismantleUIView(_ map: MLNMapView, coordinator: Coordinator) {
        map.delegate = nil
        coordinator.stop()
    }

    @MainActor
    final class Coordinator: NSObject, @preconcurrency MLNMapViewDelegate, UIGestureRecognizerDelegate {
        private let state: LabState
        private var style: MLNStyle?
        private var active: Dataset?
        private var nextRevision = 0
        private var layers: Layers?
        private var lastCameraRequest = 0
        private var didApplyCamera = false
        private var selectionTask: Task<Void, Never>?
        private var loading: Load?
        private var route: Task<Void, Never>?
        private var frameSamples: FrameSamples?
        private var completedRoutes = 0
        private var completedLoads = 0
        private var switchCount = 0
        private var nationalZoom: Double?
        private let measureRoute = ProcessInfo.processInfo.arguments.contains("--navigation-benchmark")
        private let measureSwitches = ProcessInfo.processInfo.arguments.contains("--switch-benchmark")
        private let sourceProfile = SourceProfile.fromArguments
        private let historicalMatched = ProcessInfo.processInfo.arguments.contains("--historical-matched")
        private let historicalSwitches = ProcessInfo.processInfo.arguments.contains("--historical-date-switches")
        private let historicalProbes = ProcessInfo.processInfo.arguments.contains("--historical-probes")
        private let rapidHistory = ProcessInfo.processInfo.arguments.contains("--historical-rapid-switches")

        private enum SourceProfile: String {
            case defaults, narrowBuffer = "narrow-buffer", coarseTolerance = "coarse-tolerance", capZoom = "cap-zoom"

            static var fromArguments: Self {
                let args = ProcessInfo.processInfo.arguments
                guard let index = args.firstIndex(of: "--source-option"),
                      let name = args.dropFirst(index + 1).first else { return .defaults }
                return Self(rawValue: name) ?? .defaults
            }

            var options: [MLNShapeSourceOption: Any]? {
                switch self {
                case .defaults: return nil
                case .narrowBuffer: return [.buffer: 32]
                case .coarseTolerance: return [.simplificationTolerance: 1.5]
                case .capZoom: return [.maximumZoomLevel: 8]
                }
            }
        }

        private struct Layers {
            let source: String
            let fill: String
            let border: String
            let highlight: String
            let highlightBorder: String
            let overlaySource: String
            let overlayFill: String
            let overlayBorder: String

            init(_ token: Int) {
                source = "geojson-\(token)"
                fill = "\(source)-fill"
                border = "\(source)-border"
                highlight = "\(source)-highlight"
                highlightBorder = "\(source)-highlight-border"
                overlaySource = "\(source)-overlay"
                overlayFill = "\(overlaySource)-fill"
                overlayBorder = "\(overlaySource)-border"
            }
        }

        private struct Load {
            let start: Double
            let signpost: OSSignpostID
            var firstFrame: Double?
        }

        init(state: LabState) { self.state = state }

        func installTap(on map: MLNMapView) {
            let tap = UITapGestureRecognizer(target: self, action: #selector(handleTap(_:)))
            tap.cancelsTouchesInView = false
            tap.delegate = self
            for gesture in map.gestureRecognizers ?? [] {
                if let doubleTap = gesture as? UITapGestureRecognizer, doubleTap.numberOfTapsRequired == 2,
                   doubleTap.numberOfTouchesRequired == 1 {
                    tap.require(toFail: doubleTap)
                } else if let quickZoom = gesture as? UILongPressGestureRecognizer, quickZoom.numberOfTapsRequired == 1 {
                    tap.require(toFail: quickZoom)
                }
            }
            map.addGestureRecognizer(tap)
        }

        func gestureRecognizer(_ gestureRecognizer: UIGestureRecognizer,
                               shouldRecognizeSimultaneouslyWith other: UIGestureRecognizer) -> Bool {
            guard let tap = other as? UITapGestureRecognizer else { return false }
            return tap.numberOfTapsRequired == 1 && tap.numberOfTouchesRequired == 1
        }

        func stop() {
            selectionTask?.cancel()
            route?.cancel()
            endLoad(reason: "dismantled")
            style = nil
        }

        func mapView(_ mapView: MLNMapView, didFinishLoading style: MLNStyle) {
            endLoad(reason: "style changed")
            self.style = style
            active = nil
            layers = nil
            didApplyCamera = false
            let baseline = MemorySample.read()?.fields("style_ready") ?? [:]
            LabLog.record("style_ready", baseline)
            update(mapView)
        }

        func update(_ map: MLNMapView) {
            guard let style, !map.bounds.isEmpty else { return }
            if active?.requestedAt != state.dataset?.requestedAt {
                let oldGeography = active?.geography
                selectionTask?.cancel()
                route?.cancel()
                frameSamples = nil
                endLoad(reason: "replaced")
                if let layers {
                    removeOverlay(layers, from: style)
                    for id in [layers.highlightBorder, layers.highlight, layers.border, layers.fill] {
                        if let layer = style.layer(withIdentifier: id) { style.removeLayer(layer) }
                    }
                    if let source = style.source(withIdentifier: layers.source) { style.removeSource(source) }
                }
                active = state.dataset
                layers = nil
                if let active {
                    if active.geography == .imported && !historicalMatched {
                        fit(active.summary.bounds, on: map, animated: false)
                        didApplyCamera = true
                    } else if !didApplyCamera || oldGeography == .imported {
                        move(.national, on: map, animated: false)
                        didApplyCamera = true
                    } else if active.geography == .historical,
                              !intersects(active.summary.bounds, map.visibleCoordinateBounds) {
                        move(.national, on: map, animated: false)
                    }
                    add(active, to: style)
                }
            }

            let (requestID, viewpoint) = state.cameraRequest
            if requestID != lastCameraRequest {
                lastCameraRequest = requestID
                move(viewpoint, on: map, animated: true)
            }
            if let layers,
               let fill = style.layer(withIdentifier: layers.fill) as? MLNFillStyleLayer {
                let color = NSExpression(forConstantValue: baseColor)
                if fill.fillColor != color { fill.fillColor = color }
            }
        }

        private var baseColor: UIColor {
            if state.alternateColor { return UIColor(red: 0.23, green: 0.51, blue: 0.40, alpha: 1) }
            return UIColor(red: 0.29, green: 0.49, blue: 0.68, alpha: 1)
        }

        private var datasetName: String {
            if let history = active?.historical { return history.request.day.description }
            return active?.imported?.displayName ?? active?.geography.rawValue ?? ""
        }

        private func add(_ dataset: Dataset, to style: MLNStyle) {
            nextRevision += 1
            let layers = Layers(nextRevision)
            self.layers = layers
            state.status = "Rendering \(dataset.geography.rawValue.lowercased())…"
            let signpost = OSSignpostID(log: LabLog.log)
            let start = LabLog.now
            os_signpost(.begin, log: LabLog.log, name: "GeoJSON to useful frame", signpostID: signpost)
            loading = Load(start: start, signpost: signpost)
            var fields = MemorySample.read()?.fields("before_source") ?? [:]
            fields["dataset"] = datasetName
            fields["revision"] = nextRevision
            fields["source_profile"] = sourceProfile.rawValue
            LabLog.record("source_start", fields)

            let source = MLNShapeSource(identifier: layers.source, url: dataset.url, options: sourceProfile.options)
            style.addSource(source)
            let fill = MLNFillStyleLayer(identifier: layers.fill, source: source)
            fill.fillColor = NSExpression(forConstantValue: baseColor)
            style.addLayer(fill)

            let border = MLNLineStyleLayer(identifier: layers.border, source: source)
            border.lineColor = NSExpression(forConstantValue: UIColor(white: 0.96, alpha: 0.85))
            border.lineWidth = NSExpression(forConstantValue: 0.65)
            style.addLayer(border)

            addHighlight(fill: layers.highlight, border: layers.highlightBorder, source: source,
                         visible: false, to: style)
            LabLog.record("source_submitted", ["dataset": datasetName, "revision": nextRevision,
                                                "submit_ms": LabLog.milliseconds(LabLog.now - start)])
        }

        private func addHighlight(fill fillID: String, border borderID: String, source: MLNShapeSource,
                                  visible: Bool, to style: MLNStyle) {
            let fill = MLNFillStyleLayer(identifier: fillID, source: source)
            fill.fillColor = NSExpression(forConstantValue: UIColor(red: 1, green: 0.76, blue: 0.29, alpha: 1))
            fill.fillOpacity = NSExpression(forConstantValue: 0.72)
            fill.isVisible = visible
            style.addLayer(fill)
            let border = MLNLineStyleLayer(identifier: borderID, source: source)
            border.lineColor = NSExpression(forConstantValue: UIColor(red: 0.36, green: 0.21, blue: 0.06, alpha: 1))
            border.lineWidth = NSExpression(forConstantValue: 2)
            border.isVisible = visible
            style.addLayer(border)
        }

        private func removeOverlay(_ layers: Layers, from style: MLNStyle) {
            for id in [layers.overlayBorder, layers.overlayFill] {
                if let layer = style.layer(withIdentifier: id) { style.removeLayer(layer) }
            }
            if let source = style.source(withIdentifier: layers.overlaySource) { style.removeSource(source) }
        }

        func mapViewDidFinishRenderingFrame(_ mapView: MLNMapView, fullyRendered: Bool) {
            frameSamples?.addFrame()
            guard let active, active.geography == state.geography,
                  active.requestedAt == state.dataset?.requestedAt, !state.historicalPending,
                  let layers, var load = loading, load.firstFrame == nil else { return }
            let now = LabLog.now
            let bounds = mapView.bounds
            let layerIDs: Set<String> = [layers.fill]

            // Cheap interior probes usually find land without materializing a full viewport
            // of features. Try a viewport query only after MapLibre reports a complete frame.
            let found = [0.5, 0.33, 0.67].contains { y in
                [0.5, 0.33, 0.67].contains { x in
                    let point = CGPoint(x: bounds.width * x, y: bounds.height * y)
                    return !mapView.visibleFeatures(at: point, styleLayerIdentifiers: layerIDs).isEmpty
                }
            }
            guard found || (fullyRendered && !mapView.visibleFeatures(in: bounds, styleLayerIdentifiers: layerIDs).isEmpty)
            else { return }
            load.firstFrame = now
            loading = load
            os_signpost(.end, log: LabLog.log, name: "GeoJSON to useful frame", signpostID: load.signpost)
            var fields = MemorySample.read()?.fields("useful") ?? [:]
            fields["dataset"] = datasetName
            fields["revision"] = nextRevision
            fields["source_ms"] = LabLog.milliseconds(now - load.start)
            fields["request_ms"] = LabLog.milliseconds(now - active.requestedAt)
            fields["frame_fully_rendered"] = fullyRendered
            LabLog.record("first_useful_frame", fields)
            let count = active.summary.features.formatted()
            state.status = "\(count) validated features · polygons visible"
        }

        func mapViewDidBecomeIdle(_ mapView: MLNMapView) {
            guard let active, active.requestedAt == state.dataset?.requestedAt,
                  !state.historicalPending, let load = loading, load.firstFrame != nil else { return }
            loading = nil
            completedLoads += 1
            var fields = MemorySample.read()?.fields("idle") ?? [:]
            fields["dataset"] = datasetName
            fields["revision"] = nextRevision
            fields["source_ms"] = LabLog.milliseconds(LabLog.now - load.start)
            fields["request_ms"] = LabLog.milliseconds(LabLog.now - active.requestedAt)
            LabLog.record("idle_with_geometry", fields)
            if rapidHistory && completedLoads == 1 {
                Task {
                    for index in [0, 5, 1] {
                        state.applyHistory(HistoricalRequest.presets[index])
                        try? await Task.sleep(for: .milliseconds(18))
                    }
                }
            } else if historicalProbes, completedLoads == 1 {
                runHistoricalProbes(on: mapView)
            } else if historicalSwitches {
                continueHistoricalSwitches()
            } else if measureSwitches {
                continueSwitchBenchmark()
            } else if measureRoute, completedRoutes == 0 {
                runNavigation(on: mapView)
            }
        }

        func mapViewDidFailLoadingMap(_ mapView: MLNMapView, withError error: Error) {
            endLoad(reason: "MapLibre error")
            state.status = "Map unavailable"
            state.failure = error.localizedDescription
        }

        private func endLoad(reason: String) {
            guard let load = loading else { return }
            loading = nil
            if load.firstFrame == nil {
                os_signpost(.end, log: LabLog.log, name: "GeoJSON to useful frame", signpostID: load.signpost,
                            "%{public}@", reason)
            }
        }

        @objc private func handleTap(_ recognizer: UITapGestureRecognizer) {
            guard let map = recognizer.view as? MLNMapView,
                  let active, active.requestedAt == state.dataset?.requestedAt,
                  active.geography == state.geography, !state.historicalPending, let style, let layers else { return }
            selectionTask?.cancel()
            removeOverlay(layers, from: style)
            let point = recognizer.location(in: map)
            let hits = map.visibleFeatures(at: point, styleLayerIdentifiers: [layers.fill])
            let coordinate = map.convert(point, toCoordinateFrom: map)
            if LabLog.enabled {
                LabLog.record("map_tap", ["dataset": datasetName, "hit": !hits.isEmpty, "zoom": map.zoomLevel,
                                          "longitude": coordinate.longitude, "latitude": coordinate.latitude])
            }
            if let file = active.historical, !hits.isEmpty {
                let candidates = Set(hits.compactMap { ($0.attributes["ID_NUM"] as? NSNumber)?.intValue })
                selectionTask = Task {
                    do {
                        let matches = try await file.confirm(candidates, longitude: coordinate.longitude,
                                                             latitude: coordinate.latitude)
                        try Task.checkCancellation()
                        guard state.dataset?.requestedAt == active.requestedAt,
                              state.geography == active.geography, !state.historicalPending else { return }
                        let hit = hits.first {
                            ($0.attributes["ID_NUM"] as? NSNumber).map { matches.contains($0.intValue) } ?? false
                        }
                        applyTap(hit, active: active, coordinate: coordinate, style: style, layers: layers)
                    } catch {
                        guard !Task.isCancelled else { return }
                        state.failure = "Could not inspect the historical boundary. \(error.localizedDescription)"
                    }
                }
            } else {
                applyTap(hits.first, active: active, coordinate: coordinate, style: style, layers: layers)
            }
        }

        private func applyTap(_ hit: MLNFeature?, active: Dataset, coordinate: CLLocationCoordinate2D,
                              style: MLNStyle, layers: Layers) {
            let key = active.summary.selectionKey
            var predicate: NSPredicate?
            if let hit, let key {
                let id = key == "$id" ? hit.identifier : hit.attributes[key]
                if let id = id as? String {
                    predicate = NSPredicate(format: "%K == %@", key, id)
                } else if let id = id as? NSNumber {
                    predicate = NSPredicate(format: "%K == %@", key, id)
                }
            }
            for id in [layers.highlight, layers.highlightBorder] {
                if let layer = style.layer(withIdentifier: id) as? MLNVectorStyleLayer {
                    layer.predicate = predicate
                    layer.isVisible = predicate != nil
                }
            }

            guard let hit else {
                state.inspection = nil
                return
            }
            let inspection = inspect(hit)
            state.inspection = inspection
            if key != nil {
                if LabLog.enabled {
                    var fields: [String: Any] = ["dataset": datasetName, "title": inspection.title,
                                                  "key": key ?? "", "revision": nextRevision]
                    if let id = hit.attributes["ID_NUM"] as? NSNumber { fields["record_id"] = id }
                    LabLog.record("selection", fields)
                }
                return
            }
            let x = (coordinate.longitude + 540).truncatingRemainder(dividingBy: 360) - 180
            let y = coordinate.latitude
            selectionTask = Task {
                let resolution = Task.detached(priority: .userInitiated) {
                    try GeoJSON.originalPolygon(in: active.url, longitude: x, latitude: y)
                }
                do {
                    let data = try await withTaskCancellationHandler {
                        try await resolution.value
                    } onCancel: {
                        resolution.cancel()
                    }
                    try Task.checkCancellation()
                    guard let data else { return }
                    let shape = try MLNShape(data: data, encoding: String.Encoding.utf8.rawValue)
                    let source = MLNShapeSource(identifier: layers.overlaySource, shape: shape, options: nil)
                    style.addSource(source)
                    addHighlight(fill: layers.overlayFill, border: layers.overlayBorder, source: source,
                                 visible: true, to: style)
                } catch is CancellationError {
                    return
                } catch {
                    guard !Task.isCancelled else { return }
                    state.failure = "Could not highlight this polygon. \(error.localizedDescription)"
                }
            }
        }

        private func inspect(_ feature: MLNFeature) -> Inspection {
            let attributes = feature.attributes
            let fields = attributes.compactMap { key, value -> (key: String, value: String)? in
                if let string = value as? String { return (key, string) }
                if let number = value as? NSNumber { return (key, number.stringValue) }
                return nil
            }
            let title = ["NAMELSAD", "FULL_NAME", "NAME", "name", "GEOID"]
                .compactMap { attributes[$0] as? String }.first(where: { !$0.isEmpty }) ?? "Selected polygon"
            let priority = ["GEOID", "STATE_NAME", "STATE_TERR", "STUSPS", "STATEFP", "NAMELSAD", "NAME",
                            "ID_NUM", "ID", "VERSION", "START_DATE", "END_DATE",
                            "CD119FP", "COUNTYFP", "LSAD", "CNTY_TYPE", "FIPS", "CHANGE", "CITATION"]
            let sorted = fields.sorted {
                let lhs = priority.firstIndex(of: $0.key) ?? priority.count
                let rhs = priority.firstIndex(of: $1.key) ?? priority.count
                return lhs == rhs ? $0.key < $1.key : lhs < rhs
            }
            return Inspection(title: title, properties: sorted, stateCode: attributes["STATEFP"] as? String)
        }

        private func move(_ viewpoint: Viewpoint, on map: MLNMapView, animated: Bool) {
            switch viewpoint {
            case .national:
                fit(GeoBounds(west: -125, south: 24, east: -66.5, north: 50), on: map, animated: animated)
                if !animated { nationalZoom = map.zoomLevel }
            case .alaska:
                map.setCenter(CLLocationCoordinate2D(latitude: 61, longitude: -152), zoomLevel: 3.4, animated: animated)
            case .hawaii:
                map.setCenter(CLLocationCoordinate2D(latitude: 20.7, longitude: -157.3), zoomLevel: 5.5, animated: animated)
            case .selectedState:
                guard let code = state.inspection?.stateCode, let bounds = active?.summary.stateBounds[code] else { return }
                if code == "02" { move(.alaska, on: map, animated: animated) }
                else if code == "15" { move(.hawaii, on: map, animated: animated) }
                else { fit(bounds, on: map, animated: animated) }
            }
        }

        private func intersects(_ bounds: GeoBounds, _ visible: MLNCoordinateBounds) -> Bool {
            bounds.south <= visible.ne.latitude && bounds.north >= visible.sw.latitude
                && bounds.west <= visible.ne.longitude && bounds.east >= visible.sw.longitude
        }

        private func fit(_ bounds: GeoBounds, on map: MLNMapView, animated: Bool) {
            let sw = CLLocationCoordinate2D(latitude: max(-85, bounds.south), longitude: bounds.west)
            let ne = CLLocationCoordinate2D(latitude: min(85, bounds.north), longitude: bounds.east)
            let inset = UIEdgeInsets(top: 24, left: 24, bottom: 24, right: 24)
            map.setVisibleCoordinateBounds(MLNCoordinateBounds(sw: sw, ne: ne),
                                           edgePadding: inset, animated: animated, completionHandler: nil)
        }

        private func continueSwitchBenchmark() {
            // Initial load followed by 21 switches. Wait for a useful frame and idle
            // before submitting the next dataset so each pass reaches the renderer.
            guard switchCount < 21 else {
                LabLog.record("switch_benchmark_done", ["switches": switchCount, "loads": completedLoads])
                return
            }
            let order: [Geography] = [.districts, .counties, .states]
            switchCount += 1
            let next = order[switchCount % order.count]
            Task { state.choose(next) }
        }

        private func runHistoricalProbes(on map: MLNMapView) {
            guard let file = active?.historical else { return }
            let day = file.request.day
            typealias Probe = (label: String, lon: Double, lat: Double, expected: [Int])
            let probes: [Probe]
            if day.number == 17880101 {
                probes = [("Bristol colonial coastal county", -71.270, 41.679, [12833]),
                          ("Augusta before city formation", -78.9, 38.0967, [15846])]
            } else if day.number == 20000101 {
                probes = [("Nantucket land", -70.09, 41.28, [7215]),
                          ("Nantucket offshore", -70.09, 41.40, []),
                          ("Charlottesville inside Albemarle hole", -78.491, 38.032, [15946]),
                          ("Albemarle outside its hole", -78.55, 38.043, [15811]),
                          ("Albemarle 40m outside city boundary", -78.51954, 38.032, [15811]),
                          ("Charlottesville 40m inside city boundary", -78.51854, 38.032, [15946]),
                          ("Tooele west of internal tile meridian", -112.50005, 40.72, [15727]),
                          ("Tooele east of internal tile meridian", -112.49995, 40.72, [15727]),
                          ("Augusta after city formation", -78.9, 38.0967, [15858])]
            } else {
                return
            }
            route = Task { [weak map] in
                guard let map, let layers else { return }
                for (label, lon, lat, expected) in probes {
                    for zoom in [6.0, 10.0] {
                        let coordinate = CLLocationCoordinate2D(latitude: lat, longitude: lon)
                        map.setCenter(coordinate, zoomLevel: zoom, animated: false)
                        do { try await Task.sleep(for: .milliseconds(750)) }
                        catch { return }
                        let point = map.convert(coordinate, toPointTo: map)
                        let hits = map.visibleFeatures(at: point, styleLayerIdentifiers: [layers.fill])
                        let ids = Set(hits.compactMap { ($0.attributes["ID_NUM"] as? NSNumber)?.intValue }).sorted()
                        let began = LabLog.now
                        let verified = try? await file.confirm(Set(ids), longitude: lon, latitude: lat)
                        LabLog.record("historical_probe", ["date": day.description, "label": label,
                                       "zoom": zoom, "expected": expected, "actual": ids,
                                       "verified": verified?.sorted() ?? [],
                                       "verify_ms": LabLog.milliseconds(LabLog.now - began),
                                       "source_profile": sourceProfile.rawValue])
                    }
                }
                LabLog.record("historical_probes_done", ["date": day.description])
            }
        }

        private func continueHistoricalSwitches() {
            let sequence = [6, 1, 5, 2, 4, 3, 0]  // Revisit the starting date after each sweep.
            guard switchCount < 21 else {
                LabLog.record("historical_switches_done", ["switches": switchCount, "loads": completedLoads])
                return
            }
            let next = HistoricalRequest.presets[sequence[switchCount % sequence.count]]
            switchCount += 1
            Task { state.applyHistory(next) }
        }

        private func runNavigation(on map: MLNMapView) {
            completedRoutes += 1
            if nationalZoom == nil { move(.national, on: map, animated: false) }
            let normal = nationalZoom ?? map.zoomLevel
            let longitude = historicalMatched ? -81.0 : -97.0
            route = Task { [weak map] in
                guard let map else { return }
                for (pass, level) in [0, 1, 2, 3, 3].enumerated() {
                    let zoom = normal + Double(level)
                    map.setCenter(CLLocationCoordinate2D(latitude: 39, longitude: longitude), zoomLevel: zoom, animated: false)
                    do {
                        try await Task.sleep(for: .milliseconds(650))
                        frameSamples = FrameSamples()
                        let scale = pow(2.0, Double(level))
                        for (index, offset) in [(-8.0, 2.5), (7.0, 1.0), (5.0, -2.0),
                                                (-4.0, -2.5), (0.0, 0.0)].enumerated() {
                            let center = CLLocationCoordinate2D(latitude: 39 + offset.1 / scale,
                                                                longitude: longitude + offset.0 / scale)
                            let targetZoom = zoom + (index.isMultiple(of: 2) ? 0 : 0.25)
                            let camera = map.camera
                            camera.centerCoordinate = center
                            camera.altitude *= pow(2, map.zoomLevel - targetZoom)
                            await map.setCamera(camera, withDuration: 1.2,
                                                animationTimingFunction: CAMediaTimingFunction(name: .linear))
                            try Task.checkCancellation()
                        }
                        var fields = frameSamples?.fields() ?? [:]
                        frameSamples = nil
                        fields["dataset"] = datasetName
                        fields["zoom_multiplier"] = 1 << level
                        fields["pass"] = pass == 4 ? 2 : 1
                        fields["base_zoom"] = zoom
                        if let sample = MemorySample.read() {
                            fields.merge(sample.fields("after_navigation")) { _, new in new }
                        }
                        LabLog.record("navigation", fields)
                    } catch {
                        frameSamples = nil
                        return
                    }
                }
                LabLog.record("navigation_benchmark_done", ["dataset": datasetName])
                state.status = "Navigation benchmark finished"
            }
        }
    }
}
