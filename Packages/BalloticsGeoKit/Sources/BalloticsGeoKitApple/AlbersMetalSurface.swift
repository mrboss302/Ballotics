import BalloticsGeoKit
import BalloticsGeoKitMesh
import MetalKit
import QuartzCore

public struct AlbersMetalPreparedScene {
    struct Panel {
        let origin: CGPoint
        let vertices: MTLBuffer
        let indices: MTLBuffer
        let indexCount: Int
        let parts: MTLBuffer
        let partCount: Int
        let segments: MTLBuffer
        let uniqueSegments: MTLBuffer
        let uniqueSegmentCount: Int
        let featureSegments: [Int: [Range<Int>]]
    }

    let panels: [Panel]
    public let identity: GeographicMeshIdentity
    public let featureCount: Int
    public let statistics: AlbersMesh.Statistics
    public let cpuMeshBytes: Int
    public let regionalCount: Int
    public let metadataBytes: Int
    public let preparationMilliseconds: Double
    public let allocationMilliseconds: Double

    /// Prepare geometry and allocate immutable buffers off the main actor.
    public static func prepare(_ scene: AlbersScene, revision: GeographicRevision,
                               records: GeographicRecords? = nil, device: MTLDevice) throws -> Self {
        let start = RenderTiming.now
        let mesh = try AlbersMesh.prepare(scene, revision: revision, records: records)
        return try Self(mesh, device: device, preparationMilliseconds: RenderTiming.milliseconds(RenderTiming.now - start))
    }

    public static func prepareAsync(_ scene: AlbersScene, revision: GeographicRevision,
                                    records: GeographicRecords? = nil, device: MTLDevice) async throws -> Self {
        let work = Task.detached(priority: .userInitiated) {
            try prepare(scene, revision: revision, records: records, device: device)
        }
        return try await withTaskCancellationHandler {
            let prepared = try await work.value
            try Task.checkCancellation()
            return prepared
        } onCancel: { work.cancel() }
    }

    public init(_ mesh: AlbersMesh, device: MTLDevice) throws {
        try self.init(mesh, device: device, preparationMilliseconds: 0)
    }

    private init(_ mesh: AlbersMesh, device: MTLDevice, preparationMilliseconds: Double) throws {
        let start = RenderTiming.now
        panels = try mesh.panels.map { panel in
            try Task.checkCancellation()
            return try Panel(origin: panel.origin,
                      vertices: Self.buffer(panel.vertices, on: device, label: "Regional positions"),
                      indices: Self.buffer(panel.indices, on: device, label: "Geographic triangle indices"),
                      indexCount: panel.indices.count,
                      parts: Self.buffer(panel.parts, on: device, label: "Part feature and stripe origin"),
                      partCount: panel.parts.count,
                      segments: Self.buffer(panel.segments, on: device, label: "Geographic ring segments"),
                      uniqueSegments: Self.buffer(panel.uniqueSegmentIndices, on: device, label: "Unique edge indices"),
                      uniqueSegmentCount: panel.uniqueSegmentIndices.count,
                      featureSegments: panel.featureSegments)
        }
        try Task.checkCancellation()
        statistics = mesh.statistics
        identity = mesh.identity
        featureCount = mesh.featureIDs.count
        regionalCount = panels.count
        cpuMeshBytes = mesh.payloadBytes
        metadataBytes = panels.reduce(0) { sum, panel in
            sum + panel.featureSegments.values.reduce(0) { $0 + $1.count * MemoryLayout<Range<Int>>.stride }
        }
        self.preparationMilliseconds = preparationMilliseconds
        allocationMilliseconds = RenderTiming.milliseconds(RenderTiming.now - start)
    }

    public var byteCounts: [String: Int] {
        panels.reduce(into: [:]) { fields, panel in
            fields["vertex_bytes", default: 0] += panel.vertices.length
            fields["index_bytes", default: 0] += panel.indices.length
            fields["part_bytes", default: 0] += panel.parts.length
            fields["boundary_bytes", default: 0] += panel.segments.length + panel.uniqueSegments.length
        }
    }

    static func buffer<T>(_ values: [T], on device: MTLDevice, label: String) throws -> MTLBuffer {
        guard let buffer = values.withUnsafeBytes({ bytes -> MTLBuffer? in
            guard let base = bytes.baseAddress, !bytes.isEmpty else { return nil }
            return device.makeBuffer(bytes: base, length: bytes.count, options: .storageModeShared)
        }) else {
            throw GeographicRenderError(message: "Metal could not allocate \(label.lowercased()).")
        }
        buffer.label = label
        return buffer
    }
}

public final class AlbersMetalSurface: MTKView, MTKViewDelegate {
    private struct Camera {
        let center: SIMD2<Float>
        let viewSize: SIMD2<Float>
        let scale: Float
        let depthScale: Float
        let strokeWidth: Float
        let selected: UInt32
        let hasTheme: UInt32
        var selectionPass: UInt32
        let showsFrame: UInt32
        let reserved: UInt32 = 0
        let baseColor: SIMD4<Float>
    }

    private struct Style {
        let offset: UInt32
        let count: UInt32
    }

    private struct Window {
        var cpu: [Double] = []
        var encode: [Double] = []
        var drawable: [Double] = []
        var gpu: [Double] = []
        var presentation: [Double] = []
        var drawCalls = 0
        var indices = 0
        var vertices = 0
        var segments = 0
        var inFlightSkips = 0
        var unavailableDrawables = 0

        func fields() -> [String: Any] {
            var result: [String: Any] = ["draw_calls_total": drawCalls, "submitted_indices_total": indices,
                "referenced_vertices_total": vertices, "submitted_segments_total": segments,
                "in_flight_skips": inFlightSkips, "unavailable_drawables": unavailableDrawables]
            let presentedTimes = presentation.sorted()
            let gaps = zip(presentedTimes.dropFirst(), presentedTimes).map { RenderTiming.milliseconds($0 - $1) }
            for (name, values) in [("cpu", cpu), ("encode", encode), ("drawable", drawable), ("gpu", gpu),
                                   ("present_gap", gaps)] {
                result["\(name)_samples"] = values.count
                let sorted = values.sorted()
                for percentile in [50, 95, 99, 100] where !sorted.isEmpty {
                    let index = Int(ceil(Double(sorted.count * percentile) / 100)) - 1
                    result["\(percentile == 100 ? "max" : "p\(percentile)")_\(name)_ms"] = sorted[index]
                }
            }
            result["presented_samples"] = presentation.count
            if let first = presentedTimes.first, let last = presentedTimes.last, first < last {
                result["presented_fps"] = Double(presentation.count - 1) / (last - first)
                result["presented_gaps_over_25_ms"] = gaps.count { $0 > 25 }
            }
            return result
        }
    }

    private let diagnostics: ((GeographicRenderDiagnostic) -> Void)?
    private let diagnosticsStart: Double
    private var generationsInFlight: [Int: Int] = [:]
    private var attachmentBytes = 0
    private var framePhase: GeographicRenderReadiness.Phase = .uploaded
    public var meshIdentity: GeographicMeshIdentity? { buffers?.identity }
    public var readiness: GeographicRenderReadiness? {
        meshIdentity.map { .init(mesh: $0, appearance: presentationTicket, phase: framePhase) }
    }
    public var resources: GeographicRenderResources {
        .init(geographicBufferBytes: sceneBytes, selectionRangeBytes: buffers?.metadataBytes ?? 0,
              styleBufferBytes: styleBuffer.length + colorBuffer.length, inFlightCommands: activeCommands,
              activeMeshGenerations: buffers == nil ? 0 : 1,
              retiredMeshGenerations: generationsInFlight.keys.filter { $0 != generation || buffers == nil }.count,
              estimatedAttachmentBytes: attachmentBytes)
    }

    private func emit(_ event: GeographicRenderDiagnostic.Kind, _ fields: @autoclosure () -> [String: Any]) {
        guard let diagnostics else { return }
        diagnostics(.init(kind: event, fields: fields()))
    }

    private let config: AlbersMetalConfiguration
    private let queue: MTLCommandQueue
    private let fillPipeline: MTLRenderPipelineState
    private let strokePipeline: MTLRenderPipelineState?
    private let fillDepth: MTLDepthStencilState
    private let strokeDepth: MTLDepthStencilState
    private let selectionDepth: MTLDepthStencilState
    private let inFlight = DispatchSemaphore(value: 3)
    private var generation = 0
    private var buffers: AlbersMetalPreparedScene?
    private var focusedRegion: AlbersRegion?
    private var layout: [AlbersScene.Panel] = []
    private let neutralStyle: MTLBuffer
    private let neutralColors: MTLBuffer
    private var styleBuffer: MTLBuffer
    private var colorBuffer: MTLBuffer
    private var hasTheme = false
    private var mapCenter = CGPoint.zero
    private var viewport = CGRect.zero
    private var scale: CGFloat = 1
    private var stroke: CGFloat = 0.48
    private var selected: Int?
    private var useAlternate = false
    private var dirty = false
    private var measurements: Window?
    private var measurementID = 0
    private var activeCommands = 0
    private var submissions = 0
    private var sceneBytes = 0
    private var submittedFirst = false
    private var announcedReady = false
    private var presentedFirst = false
    private var appearanceMetric: (start: Double, name: String)?
    public var onFirstFrame: (() -> Void)?
    public var onFailure: ((String) -> Void)?
    public var onFrame: ((Double) -> Void)?
    public var renderAllowed = true {
        didSet {
            if oldValue != renderAllowed {
                emit(.sceneActivity, ["active": renderAllowed, "submissions": submissions])
                if renderAllowed {
                    requestDraw()
                } else {
                    advanceAppearance()
                    attachmentBytes = 0
                    releaseDrawables()
                }
            }
        }
    }

    public init(configuration: AlbersMetalConfiguration = .init(),
                diagnostics: ((GeographicRenderDiagnostic) -> Void)? = nil,
                diagnosticTimeOrigin: Double? = nil) throws {
        let started = RenderTiming.now
        self.diagnostics = diagnostics
        diagnosticsStart = diagnosticTimeOrigin ?? started
        guard let device = MTLCreateSystemDefaultDevice(),
              configuration.samples > 0, device.supportsTextureSampleCount(configuration.samples),
              let queue = device.makeCommandQueue() else {
            throw GeographicRenderError(message: "The requested Metal device or sample count is unavailable.")
        }
        config = configuration
        self.queue = queue
        let libraryStart = RenderTiming.now
        let library: MTLLibrary
        let runtime: Bool
        switch configuration.shaders {
        case .package:
            let bundle = try Self.shaderBundle()
            library = try device.makeDefaultLibrary(bundle: bundle)
            runtime = false
        case .compiledFile(let url):
            library = try device.makeLibrary(URL: url)
            runtime = false
        case .runtimeSource(let url):
            library = try device.makeLibrary(source: String(contentsOf: url, encoding: .utf8), options: nil)
            runtime = true
        }
        let pipelineStart = RenderTiming.now
        let pipeline = MTLRenderPipelineDescriptor()
        pipeline.label = "Albers polygon fills"
        guard let fillVertex = library.makeFunction(name: configuration.polygonVertexFunction) else {
            throw GeographicRenderError(message: "Metal library is missing the polygon vertex function.")
        }
        pipeline.vertexFunction = fillVertex
        pipeline.fragmentFunction = library.makeFunction(name: "albers_fill_fragment")
        pipeline.colorAttachments[0].pixelFormat = .bgra8Unorm
        pipeline.depthAttachmentPixelFormat = .depth32Float
        pipeline.rasterSampleCount = configuration.samples
        fillPipeline = try device.makeRenderPipelineState(descriptor: pipeline)
        if configuration.boundaries {
            pipeline.label = "Screen-space geographic strokes"
            pipeline.vertexFunction = library.makeFunction(name: "albers_stroke_vertex")
            pipeline.fragmentFunction = library.makeFunction(name: "albers_stroke_fragment")
            strokePipeline = try device.makeRenderPipelineState(descriptor: pipeline)
        } else {
            strokePipeline = nil
        }
        let pipelineEnd = RenderTiming.now
        let depth = MTLDepthStencilDescriptor()
        depth.depthCompareFunction = .less
        depth.isDepthWriteEnabled = true
        guard let fillDepth = device.makeDepthStencilState(descriptor: depth) else {
            throw GeographicRenderError(message: "Metal could not create the Albers fill depth state.")
        }
        self.fillDepth = fillDepth
        depth.isDepthWriteEnabled = false
        depth.depthCompareFunction = .lessEqual
        guard let strokeDepth = device.makeDepthStencilState(descriptor: depth) else {
            throw GeographicRenderError(message: "Metal could not create the Albers stroke depth state.")
        }
        self.strokeDepth = strokeDepth
        depth.depthCompareFunction = .always
        guard let selectionDepth = device.makeDepthStencilState(descriptor: depth) else {
            throw GeographicRenderError(message: "Metal could not create the Albers selection depth state.")
        }
        self.selectionDepth = selectionDepth
        styleBuffer = try AlbersMetalPreparedScene.buffer([Style(offset: 0, count: 0)], on: device, label: "Neutral feature style")
        colorBuffer = try AlbersMetalPreparedScene.buffer([SIMD4<Float>.zero], on: device, label: "Neutral palette")
        neutralStyle = styleBuffer
        neutralColors = colorBuffer
        super.init(frame: .zero, device: device)
        colorPixelFormat = .bgra8Unorm
        depthStencilPixelFormat = .depth32Float
        sampleCount = configuration.samples
        clearColor = MTLClearColor(red: 0.93, green: 0.95, blue: 0.96, alpha: 1)
        clearDepth = 1
        isPaused = true
        enableSetNeedsDisplay = true
        #if os(iOS)
        isUserInteractionEnabled = false
        accessibilityElementsHidden = true
        #endif
        delegate = self
        emit(.pipeline, ["samples": configuration.samples, "boundaries": configuration.boundaries,
            "library": runtime ? "runtime" : "compiled", "library_ms": RenderTiming.milliseconds(pipelineStart - libraryStart),
            "pipeline_ms": RenderTiming.milliseconds(pipelineEnd - pipelineStart),
            "elapsed_ms": RenderTiming.milliseconds(RenderTiming.now - started)])
    }

    private static func shaderBundle() throws -> Bundle {
        // SwiftPM's synthesized Bundle.module traps when deployment drops the entire bundle.
        // Use the same app, framework and executable locations with a recoverable error.
        let name = "BalloticsGeoKit_BalloticsGeoKitApple.bundle"
        let locations = [Bundle.main.resourceURL, Bundle(for: AlbersMetalSurface.self).resourceURL,
                         Bundle.main.bundleURL].compactMap { $0 }
        for location in locations {
            if let bundle = Bundle(url: location.appendingPathComponent(name)) { return bundle }
        }
        throw GeographicRenderError(message: "The BalloticsGeoKit Metal resource bundle is missing.")
    }

    public required init(coder: NSCoder) { fatalError("Storyboard construction is unavailable.") }

    public func beginWindow() {
        measurementID += 1
        measurements = Window()
    }

    public func finishWindow() -> [String: Any] {
        var fields = measurements?.fields() ?? [:]
        fields["samples"] = config.samples
        fields["boundaries"] = config.boundaries
        measurements = nil
        return fields
    }

    /// Install immutable geometry and its initial style table as one main-actor operation.
    public func publish(_ buffers: AlbersMetalPreparedScene, scene: AlbersScene, palette: [[CGColor]]?) throws {
        guard buffers.identity.matches(scene), buffers.panels.first?.vertices.device === device else {
            throw GeographicRenderError(message: "Prepared Metal buffers belong to a different scene or device.")
        }
        try setPalette(palette, featureCount: buffers.featureCount)
        generation += 1
        advanceAppearance()
        acknowledge = nil
        selected = nil
        self.buffers = buffers
        sceneBytes = buffers.byteCounts.values.reduce(0, +)
        layout = scene.panels
        submittedFirst = false
        announcedReady = false
        presentedFirst = false
        appearanceMetric = nil
        requestDraw()
    }

    public func updateLayout(_ scene: AlbersScene) throws {
        guard buffers?.identity.matches(scene) == true else {
            throw GeographicRenderError(message: "Presentation belongs to a different prepared scene.")
        }
        layout = scene.panels
        advanceAppearance()
        emit(.layout, ["generation": generation, "mesh_bytes": sceneBytes, "commands_in_flight": activeCommands])
        requestDraw()
    }

    // Only a successful command with the current presentation ticket can acknowledge this appearance.
    private var presentationTicket = 0
    private var acknowledge: (() -> Void)?
    /// Acknowledge the latest appearance after a successful GPU command.
    public func expectPresentation(_ completion: @escaping () -> Void) {
        advanceAppearance()
        acknowledge = completion
        requestDraw()
    }

    private func advanceAppearance() {
        presentationTicket += 1
        framePhase = .uploaded
    }

    public func clear() {
        presentationTicket += 1
        acknowledge = nil
        generation += 1
        let releasedBytes = sceneBytes
        sceneBytes = 0
        attachmentBytes = 0
        styleBuffer = neutralStyle
        colorBuffer = neutralColors
        hasTheme = false
        selected = nil
        buffers = nil
        layout = []
        dirty = false
        appearanceMetric = nil
        releaseDrawables()
        if releasedBytes > 0 {
            emit(.release, ["scene_bytes": releasedBytes, "commands_in_flight": activeCommands,
                "generation": generation, "surface_submissions": submissions,
                "active_mesh_generations": resources.activeMeshGenerations,
                "retired_mesh_generations": resources.retiredMeshGenerations])
        }
    }

    public func updatePalette(_ palette: [[CGColor]]?) throws {
        try setPalette(palette, featureCount: buffers?.featureCount)
        advanceAppearance()
        requestDraw()
    }

    private func setPalette(_ palette: [[CGColor]]?, featureCount: Int?) throws {
        guard let palette, let device else {
            styleBuffer = neutralStyle
            colorBuffer = neutralColors
            hasTheme = false
            return
        }
        guard let featureCount, palette.count == featureCount else {
            throw GeographicRenderError(message: "The style table must have one entry per scene feature.")
        }
        let start = RenderTiming.now
        let space = CGColorSpace(name: CGColorSpace.sRGB)!
        var styles: [Style] = []
        var colors: [SIMD4<Float>] = []
        for feature in palette {
            guard feature.count <= Int(UInt32.max) - colors.count else {
                throw GeographicRenderError(message: "Palette exceeds the UInt32 style range.")
            }
            styles.append(Style(offset: UInt32(colors.count), count: UInt32(feature.count)))
            for color in feature {
                let rgba = color.converted(to: space, intent: .defaultIntent, options: nil)?.components ?? []
                colors.append(rgba.count == 4 ? SIMD4(Float(rgba[0]), Float(rgba[1]), Float(rgba[2]), 1)
                                             : SIMD4(0.29, 0.49, 0.68, 1))
            }
        }
        if colors.isEmpty { colors.append(.zero) }
        let prepared = RenderTiming.now
        let newStyle = try AlbersMetalPreparedScene.buffer(styles, on: device, label: "Feature styles")
        let newColors = try AlbersMetalPreparedScene.buffer(colors, on: device, label: "Style colors")
        styleBuffer = newStyle
        colorBuffer = newColors
        hasTheme = true
        emit(.styleUpload, ["allocated_buffers": 2, "style_bytes": newStyle.length + newColors.length,
            "prepare_ms": RenderTiming.milliseconds(prepared - start), "copy_ms": RenderTiming.milliseconds(RenderTiming.now - prepared)])
    }

    public func markAppearance(_ name: String, at start: Double) {
        appearanceMetric = (start, name)
    }

    public func move(camera: GeographicCamera, boundary: GeographicBoundaryStyle = .init(),
                     selected: Int? = nil, alternate: Bool = false) {
        let selection = selected.flatMap { index in
            buffers.map { (0..<$0.featureCount).contains(index) } == true ? index : nil
        }
        if viewport != camera.viewport || focusedRegion != camera.region || stroke != boundary.width
            || self.selected != selection || useAlternate != alternate {
            advanceAppearance()
        }
        viewport = camera.viewport
        focusedRegion = camera.region
        mapCenter = camera.center
        scale = camera.scale
        stroke = boundary.width
        self.selected = selection
        useAlternate = alternate
        requestDraw()
    }

    private func requestDraw() {
        dirty = true
        guard renderAllowed, window != nil, !isHidden else { return }
        #if os(macOS)
        needsDisplay = true
        #else
        setNeedsDisplay()
        #endif
    }

    public func mtkView(_ view: MTKView, drawableSizeWillChange size: CGSize) { requestDraw() }

    public func draw(in view: MTKView) {
        guard let buffers, renderAllowed, !isHidden, window != nil, bounds.width > 0, bounds.height > 0,
              scale > 0, scale.isFinite, mapCenter.x.isFinite, mapCenter.y.isFinite else { return }
        guard inFlight.wait(timeout: .now()) == .success else {
            measurements?.inFlightSkips += 1
            return
        }
        let start = RenderTiming.now
        guard let descriptor = currentRenderPassDescriptor, let drawable = currentDrawable,
              let command = queue.makeCommandBuffer(),
              let encoder = command.makeRenderCommandEncoder(descriptor: descriptor) else {
            inFlight.signal()
            measurements?.unavailableDrawables += 1
            return
        }
        dirty = false
        let encodingStarted = RenderTiming.now
        let acquired = RenderTiming.milliseconds(encodingStarted - start)
        let windowID = measurementID
        let measuring = measurements != nil
        var calls = 0
        var submittedIndices = 0
        var submittedSegments = 0
        var referencedVertices = 0
        let region = CGRect(x: mapCenter.x - bounds.width / (2 * scale), y: mapCenter.y - bounds.height / (2 * scale),
                            width: bounds.width / scale, height: bounds.height / scale)
        encoder.setRenderPipelineState(fillPipeline)
        encoder.setDepthStencilState(fillDepth)
        encoder.setCullMode(.none)
        encoder.setFragmentBuffer(styleBuffer, offset: 0, index: 0)
        encoder.setFragmentBuffer(colorBuffer, offset: 0, index: 1)
        for (buffer, panel) in zip(buffers.panels, layout) where (focusedRegion == nil || focusedRegion == panel.region) && panel.frame.intersects(region) {
            var camera = camera(for: buffer, panel: panel)
            encoder.setVertexBuffer(buffer.vertices, offset: 0, index: 0)
            encoder.setVertexBuffer(buffer.parts, offset: 0, index: 1)
            setCamera(&camera, on: encoder)
            encoder.drawIndexedPrimitives(type: .triangle, indexCount: buffer.indexCount,
                                          indexType: .uint32, indexBuffer: buffer.indices, indexBufferOffset: 0)
            calls += 1
            submittedIndices += buffer.indexCount
            referencedVertices += buffer.vertices.length / MemoryLayout<AlbersMesh.Vertex>.stride
        }
        if let strokePipeline {
            encoder.setRenderPipelineState(strokePipeline)
            encoder.setDepthStencilState(strokeDepth)
            for (buffer, panel) in zip(buffers.panels, layout) where (focusedRegion == nil || focusedRegion == panel.region) && panel.frame.intersects(region) {
                var camera = camera(for: buffer, panel: panel)
                encoder.setVertexBuffer(buffer.segments, offset: 0, index: 0)
                encoder.setVertexBuffer(buffer.parts, offset: 0, index: 1)
                encoder.setVertexBuffer(buffer.uniqueSegments, offset: 0, index: 3)
                setCamera(&camera, on: encoder)
                encoder.drawPrimitives(type: .triangle, vertexStart: 0, vertexCount: 6,
                                       instanceCount: buffer.uniqueSegmentCount)
                calls += 1
                submittedSegments += buffer.uniqueSegmentCount
            }
            if let selected {
                encoder.setDepthStencilState(selectionDepth)
                for (buffer, panel) in zip(buffers.panels, layout) where (focusedRegion == nil || focusedRegion == panel.region) && panel.frame.intersects(region) {
                    guard let ranges = buffer.featureSegments[selected] else { continue }
                    var camera = camera(for: buffer, panel: panel)
                    camera.selectionPass = 1
                    encoder.setVertexBuffer(buffer.parts, offset: 0, index: 1)
                    encoder.setVertexBuffer(buffer.uniqueSegments, offset: 0, index: 3)
                    setCamera(&camera, on: encoder)
                    for range in ranges {
                        encoder.setVertexBuffer(buffer.segments,
                                                offset: range.lowerBound * MemoryLayout<AlbersMesh.Segment>.stride,
                                                index: 0)
                        encoder.drawPrimitives(type: .triangle, vertexStart: 0, vertexCount: 6, instanceCount: range.count)
                        calls += 1
                        submittedSegments += range.count
                    }
                }
            }
        }
        encoder.endEncoding()
        let ticket = presentationTicket
        let stamp = generation
        let first = !submittedFirst
        let operation = appearanceMetric
        appearanceMetric = nil
        let gate = inFlight
        let encode = RenderTiming.milliseconds(RenderTiming.now - encodingStarted)
        #if !targetEnvironment(simulator)
        if framePhase.rawValue < GeographicRenderReadiness.Phase.presented.rawValue || operation != nil || measuring {
            drawable.addPresentedHandler { [weak self] drawable in
                let time = drawable.presentedTime
                guard time > 0 else { return }
                DispatchQueue.main.async { [weak self] in
                    guard let self, self.generation == stamp, self.presentationTicket == ticket else { return }
                    self.framePhase = .presented
                    if measuring, self.measurementID == windowID { self.measurements?.presentation.append(time) }
                    if !self.presentedFirst {
                        self.presentedFirst = true
                        self.emit(.firstPresent, ["app_elapsed_ms": RenderTiming.milliseconds(time - self.diagnosticsStart)])
                    }
                    if let operation {
                        self.emit(.appearancePresent, ["operation": operation.name,
                            "elapsed_ms": RenderTiming.milliseconds(time - operation.start)])
                    }
                }
            }
        }
        #endif
        command.addCompletedHandler { [weak self] result in
            gate.signal()
            let gpu = result.gpuEndTime > result.gpuStartTime
                ? RenderTiming.milliseconds(result.gpuEndTime - result.gpuStartTime) : nil
            let failed = result.error?.localizedDescription
            DispatchQueue.main.async { [weak self] in
                guard let self else { return }
                self.activeCommands -= 1
                if let count = self.generationsInFlight[stamp], count > 1 {
                    self.generationsInFlight[stamp] = count - 1
                } else {
                    self.generationsInFlight.removeValue(forKey: stamp)
                }
                if self.dirty { self.requestDraw() }
                guard self.generation == stamp else { return }
                if let failed {
                    if self.presentationTicket == ticket { self.onFailure?(failed) }
                    return
                }
                if self.presentationTicket == ticket {
                    if self.framePhase != .presented { self.framePhase = .completed }
                    let completion = self.acknowledge
                    self.acknowledge = nil
                    completion?()
                    guard self.generation == stamp, self.presentationTicket == ticket else { return }
                    if !self.announcedReady {
                        self.announcedReady = true
                        self.onFirstFrame?()
                    }
                }
                if measuring, self.measurementID == windowID, let gpu { self.measurements?.gpu.append(gpu) }
                if first {
                    var fields: [String: Any] = ["submitted_indices": submittedIndices]
                    fields["gpu_ms"] = gpu
                    self.emit(.firstComplete, fields)
                }
            }
        }
        let pixels = drawable.texture.width * drawable.texture.height
        let resolveBytes = pixels * 4
        let depthBytes = pixels * 4 * config.samples
        let multisampleBytes = config.samples > 1 ? depthBytes : 0
        attachmentBytes = resolveBytes + depthBytes + multisampleBytes
        if framePhase == .uploaded { framePhase = .submitted }
        command.present(drawable)
        activeCommands += 1
        generationsInFlight[stamp, default: 0] += 1
        submissions += 1
        command.commit()
        let duration = RenderTiming.milliseconds(RenderTiming.now - start)
        measurements?.cpu.append(duration)
        measurements?.encode.append(encode)
        measurements?.drawable.append(acquired)
        measurements?.drawCalls += calls
        measurements?.indices += submittedIndices
        measurements?.vertices += referencedVertices
        measurements?.segments += submittedSegments
        if first {
            submittedFirst = true
            emit(.firstSubmit, ["cpu_ms": duration, "encode_ms": encode, "acquire_ms": acquired,
                "draw_calls": calls, "submitted_indices": submittedIndices, "submitted_segments": submittedSegments,
                "referenced_vertices": referencedVertices, "drawable_width": drawable.texture.width,
                "drawable_height": drawable.texture.height, "samples": config.samples,
                "resolve_color_bytes_estimate": resolveBytes, "depth_bytes_estimate": depthBytes,
                "multisample_color_bytes_estimate": multisampleBytes,
                "active_mesh_generations": resources.activeMeshGenerations,
                "retired_mesh_generations": resources.retiredMeshGenerations, "commands_in_flight": activeCommands])
        }
        if let operation {
            emit(.appearanceSubmit, ["operation": operation.name,
                "cpu_ms": duration, "drawable_ms": acquired, "encode_ms": encode,
                "before_draw_ms": RenderTiming.milliseconds(start - operation.start),
                "elapsed_ms": RenderTiming.milliseconds(RenderTiming.now - operation.start)])
        }
        onFrame?(duration)
    }

    private func camera(for buffer: AlbersMetalPreparedScene.Panel, panel: AlbersScene.Panel) -> Camera {
        let local = panel.local(mapCenter)
        return Camera(center: SIMD2(Float(local.x - buffer.origin.x), Float(local.y - buffer.origin.y)),
                      viewSize: SIMD2(Float(bounds.width), Float(bounds.height)), scale: Float(scale * panel.scale),
                      depthScale: 1 / Float(buffer.partCount + 1), strokeWidth: Float(stroke),
                      selected: selected.map(UInt32.init) ?? .max, hasTheme: hasTheme ? 1 : 0, selectionPass: 0, showsFrame: panel.showsFrame ? 1 : 0,
                      baseColor: useAlternate ? SIMD4(0.23, 0.51, 0.40, 1) : SIMD4(0.29, 0.49, 0.68, 1))
    }

    private func setCamera(_ camera: inout Camera, on encoder: MTLRenderCommandEncoder) {
        assert(MemoryLayout<Camera>.stride == 64)
        encoder.setVertexBytes(&camera, length: MemoryLayout<Camera>.stride, index: 2)
        encoder.setFragmentBytes(&camera, length: MemoryLayout<Camera>.stride, index: 2)
    }
}
