import MetalKit
import QuartzCore

struct AlbersMetalConfiguration {
    let samples: Int
    let boundaries: Bool

    static let launch = Self(samples: ProcessInfo.processInfo.arguments.contains("--metal-samples-1") ? 1 : 4,
                             boundaries: !ProcessInfo.processInfo.arguments.contains("--metal-fill-only"))
}

struct AlbersMetalBuffers {
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
    let statistics: AlbersMesh.Statistics

    init(_ mesh: AlbersMesh, device: MTLDevice) throws {
        panels = try mesh.panels.map { panel in
            try Panel(origin: panel.origin,
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
        statistics = mesh.statistics
    }

    var byteCounts: [String: Int] {
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
            throw GeoJSON.Invalid(message: "Metal could not allocate \(label.lowercased()).")
        }
        buffer.label = label
        return buffer
    }
}

final class AlbersMetalSurface: MTKView, MTKViewDelegate {
    private struct Camera {
        let center: SIMD2<Float>
        let viewSize: SIMD2<Float>
        let scale: Float
        let depthScale: Float
        let strokeWidth: Float
        let selected: UInt32
        let hasTheme: UInt32
        var selectionPass: UInt32
        let reserved: SIMD2<UInt32> = .zero
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
            let gaps = zip(presentedTimes.dropFirst(), presentedTimes).map { LabLog.milliseconds($0 - $1) }
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

    private let config: AlbersMetalConfiguration
    private let queue: MTLCommandQueue
    private let fillPipeline: MTLRenderPipelineState
    private let strokePipeline: MTLRenderPipelineState?
    private let fillDepth: MTLDepthStencilState
    private let strokeDepth: MTLDepthStencilState
    private let selectionDepth: MTLDepthStencilState
    private let inFlight = DispatchSemaphore(value: 3)
    private var generation = 0
    private var buffers: AlbersMetalBuffers?
    private var layout: [AlbersScene.Panel] = []
    private let neutralStyle: MTLBuffer
    private let neutralColors: MTLBuffer
    private var styleBuffer: MTLBuffer
    private var colorBuffer: MTLBuffer
    private var hasTheme = false
    private var mapCenter = CGPoint.zero
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
    private var presentedFirst = false
    private var appearance: (start: Double, name: String)?
    var onFirstFrame: (() -> Void)?
    var onFailure: ((String) -> Void)?
    var onFrame: ((Double) -> Void)?
    var renderAllowed = true {
        didSet {
            if oldValue != renderAllowed {
                LabLog.record("metal_scene_activity", ["active": renderAllowed, "submissions": submissions])
            }
        }
    }

    init(configuration: AlbersMetalConfiguration) throws {
        let started = LabLog.now
        guard let device = MTLCreateSystemDefaultDevice(),
              device.supportsTextureSampleCount(configuration.samples),
              let queue = device.makeCommandQueue(),
              let sourceURL = Bundle.main.url(forResource: "AlbersShaders", withExtension: "msl") else {
            throw GeoJSON.Invalid(message: "The requested Metal device, shader or sample count is unavailable.")
        }
        config = configuration
        self.queue = queue
        let library = try device.makeLibrary(source: String(contentsOf: sourceURL, encoding: .utf8), options: nil)
        let pipeline = MTLRenderPipelineDescriptor()
        pipeline.label = "Albers polygon fills"
        pipeline.vertexFunction = library.makeFunction(name: "albers_fill_vertex")
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
        let depth = MTLDepthStencilDescriptor()
        depth.depthCompareFunction = .less
        depth.isDepthWriteEnabled = true
        guard let fillDepth = device.makeDepthStencilState(descriptor: depth) else {
            throw GeoJSON.Invalid(message: "Metal could not create the Albers fill depth state.")
        }
        self.fillDepth = fillDepth
        depth.isDepthWriteEnabled = false
        depth.depthCompareFunction = .lessEqual
        guard let strokeDepth = device.makeDepthStencilState(descriptor: depth) else {
            throw GeoJSON.Invalid(message: "Metal could not create the Albers stroke depth state.")
        }
        self.strokeDepth = strokeDepth
        depth.depthCompareFunction = .always
        guard let selectionDepth = device.makeDepthStencilState(descriptor: depth) else {
            throw GeoJSON.Invalid(message: "Metal could not create the Albers selection depth state.")
        }
        self.selectionDepth = selectionDepth
        styleBuffer = try AlbersMetalBuffers.buffer([Style(offset: 0, count: 0)], on: device, label: "Neutral feature style")
        colorBuffer = try AlbersMetalBuffers.buffer([SIMD4<Float>.zero], on: device, label: "Neutral palette")
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
        isUserInteractionEnabled = false
        accessibilityElementsHidden = true
        delegate = self
        LabLog.record("metal_pipeline", ["samples": configuration.samples, "boundaries": configuration.boundaries,
            "elapsed_ms": LabLog.milliseconds(LabLog.now - started)])
    }

    required init(coder: NSCoder) { fatalError("Storyboard construction is unavailable.") }

    func beginWindow() {
        measurementID += 1
        measurements = Window()
    }

    func finishWindow() -> [String: Any] {
        var fields = measurements?.fields() ?? [:]
        fields["samples"] = config.samples
        fields["boundaries"] = config.boundaries
        measurements = nil
        return fields
    }

    func publish(_ buffers: AlbersMetalBuffers, layout: [AlbersScene.Panel]) {
        generation += 1
        self.buffers = buffers
        sceneBytes = buffers.byteCounts.values.reduce(0, +)
        self.layout = layout
        submittedFirst = false
        presentedFirst = false
        appearance = nil
    }

    func clear() {
        generation += 1
        let releasedBytes = sceneBytes
        sceneBytes = 0
        styleBuffer = neutralStyle
        colorBuffer = neutralColors
        hasTheme = false
        buffers = nil
        layout = []
        dirty = false
        appearance = nil
        releaseDrawables()
        if releasedBytes > 0 {
            LabLog.record("metal_release", ["scene_bytes": releasedBytes, "commands_in_flight": activeCommands,
                "generation": generation, "surface_submissions": submissions])
        }
    }

    func updatePalette(_ palette: [[CGColor]]?) throws {
        guard let palette, let device else {
            styleBuffer = neutralStyle
            colorBuffer = neutralColors
            hasTheme = false
            return
        }
        let start = LabLog.now
        let space = CGColorSpace(name: CGColorSpace.sRGB)!
        var styles: [Style] = []
        var colors: [SIMD4<Float>] = []
        for feature in palette {
            styles.append(Style(offset: UInt32(colors.count), count: UInt32(feature.count)))
            for color in feature {
                let rgba = color.converted(to: space, intent: .defaultIntent, options: nil)?.components ?? []
                colors.append(rgba.count == 4 ? SIMD4(Float(rgba[0]), Float(rgba[1]), Float(rgba[2]), 1)
                                             : SIMD4(0.29, 0.49, 0.68, 1))
            }
        }
        if colors.isEmpty { colors.append(.zero) }
        let prepared = LabLog.now
        let newStyle = try AlbersMetalBuffers.buffer(styles, on: device, label: "Feature styles")
        let newColors = try AlbersMetalBuffers.buffer(colors, on: device, label: "Style colors")
        styleBuffer = newStyle
        colorBuffer = newColors
        hasTheme = true
        LabLog.record("metal_style_upload", ["style_bytes": newStyle.length + newColors.length,
            "prepare_ms": LabLog.milliseconds(prepared - start), "copy_ms": LabLog.milliseconds(LabLog.now - prepared)])
    }

    func markAppearance(_ name: String, at start: Double) {
        appearance = (start, name)
    }

    func move(center: CGPoint, scale: CGFloat, stroke: CGFloat, selected: Int?, alternate: Bool) {
        mapCenter = center
        self.scale = scale
        self.stroke = stroke
        self.selected = selected
        useAlternate = alternate
        requestDraw()
    }

    private func requestDraw() {
        dirty = true
        if renderAllowed, window != nil, !isHidden { setNeedsDisplay() }
    }

    func mtkView(_ view: MTKView, drawableSizeWillChange size: CGSize) { requestDraw() }

    func draw(in view: MTKView) {
        guard let buffers, renderAllowed, !isHidden, window != nil, bounds.width > 0, bounds.height > 0 else { return }
        guard inFlight.wait(timeout: .now()) == .success else {
            measurements?.inFlightSkips += 1
            return
        }
        let start = LabLog.now
        guard let descriptor = currentRenderPassDescriptor, let drawable = currentDrawable,
              let command = queue.makeCommandBuffer(),
              let encoder = command.makeRenderCommandEncoder(descriptor: descriptor) else {
            inFlight.signal()
            measurements?.unavailableDrawables += 1
            return
        }
        dirty = false
        let encodingStarted = LabLog.now
        let acquired = LabLog.milliseconds(encodingStarted - start)
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
        for (buffer, panel) in zip(buffers.panels, layout) where panel.frame.intersects(region) {
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
            for (buffer, panel) in zip(buffers.panels, layout) where panel.frame.intersects(region) {
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
                for (buffer, panel) in zip(buffers.panels, layout) where panel.frame.intersects(region) {
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
        let stamp = generation
        let first = !submittedFirst
        let operation = appearance
        appearance = nil
        let gate = inFlight
        let encode = LabLog.milliseconds(LabLog.now - encodingStarted)
        #if !targetEnvironment(simulator)
        if LabLog.enabled, !presentedFirst || operation != nil || measuring {
            drawable.addPresentedHandler { [weak self] drawable in
                let time = drawable.presentedTime
                guard time > 0 else { return }
                DispatchQueue.main.async { [weak self] in
                    guard let self, self.generation == stamp else { return }
                    if measuring, self.measurementID == windowID { self.measurements?.presentation.append(time) }
                    if !self.presentedFirst {
                        self.presentedFirst = true
                        LabLog.record("metal_first_present", ["app_elapsed_ms": LabLog.milliseconds(time - LabLog.start)])
                    }
                    if let operation {
                        LabLog.record("metal_appearance_present", ["operation": operation.name,
                            "elapsed_ms": LabLog.milliseconds(time - operation.start)])
                    }
                }
            }
        }
        #endif
        command.addCompletedHandler { [weak self] result in
            gate.signal()
            let gpu = result.gpuEndTime > result.gpuStartTime
                ? LabLog.milliseconds(result.gpuEndTime - result.gpuStartTime) : nil
            let failed = result.error?.localizedDescription
            DispatchQueue.main.async { [weak self] in
                guard let self else { return }
                self.activeCommands -= 1
                if self.dirty { self.requestDraw() }
                guard self.generation == stamp else { return }
                if let failed { self.onFailure?(failed); return }
                if measuring, self.measurementID == windowID, let gpu { self.measurements?.gpu.append(gpu) }
                if first {
                    var fields: [String: Any] = ["submitted_indices": submittedIndices]
                    fields["gpu_ms"] = gpu
                    LabLog.record("metal_first_complete", fields)
                }
            }
        }
        command.present(drawable)
        activeCommands += 1
        submissions += 1
        command.commit()
        let duration = LabLog.milliseconds(LabLog.now - start)
        measurements?.cpu.append(duration)
        measurements?.encode.append(encode)
        measurements?.drawable.append(acquired)
        measurements?.drawCalls += calls
        measurements?.indices += submittedIndices
        measurements?.vertices += referencedVertices
        measurements?.segments += submittedSegments
        if first {
            submittedFirst = true
            let pixels = drawable.texture.width * drawable.texture.height
            var fields = MemorySample.read()?.fields("first_submit") ?? [:]
            fields.merge(["cpu_ms": duration, "encode_ms": encode, "acquire_ms": acquired,
                "draw_calls": calls, "submitted_indices": submittedIndices, "submitted_segments": submittedSegments,
                "referenced_vertices": referencedVertices, "drawable_width": drawable.texture.width,
                "drawable_height": drawable.texture.height, "samples": config.samples,
                "resolve_color_bytes_estimate": pixels * 4, "depth_bytes_estimate": pixels * 4 * config.samples,
                "multisample_color_bytes_estimate": config.samples > 1 ? pixels * 4 * config.samples : 0]) { _, new in new }
            LabLog.record("metal_first_submit", fields)
            onFirstFrame?()
        }
        if let operation {
            LabLog.record("metal_appearance_submit", ["operation": operation.name,
                "elapsed_ms": LabLog.milliseconds(LabLog.now - operation.start)])
        }
        onFrame?(duration)
    }

    private func camera(for buffer: AlbersMetalBuffers.Panel, panel: AlbersScene.Panel) -> Camera {
        let local = panel.local(mapCenter)
        return Camera(center: SIMD2(Float(local.x - buffer.origin.x), Float(local.y - buffer.origin.y)),
                      viewSize: SIMD2(Float(bounds.width), Float(bounds.height)), scale: Float(scale),
                      depthScale: 1 / Float(buffer.partCount + 1), strokeWidth: Float(stroke),
                      selected: selected.map(UInt32.init) ?? .max, hasTheme: hasTheme ? 1 : 0, selectionPass: 0,
                      baseColor: useAlternate ? SIMD4(0.23, 0.51, 0.40, 1) : SIMD4(0.29, 0.49, 0.68, 1))
    }

    private func setCamera(_ camera: inout Camera, on encoder: MTLRenderCommandEncoder) {
        assert(MemoryLayout<Camera>.stride == 64)
        encoder.setVertexBytes(&camera, length: MemoryLayout<Camera>.stride, index: 2)
        encoder.setFragmentBytes(&camera, length: MemoryLayout<Camera>.stride, index: 2)
    }
}
