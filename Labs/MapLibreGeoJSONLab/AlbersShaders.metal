#include <metal_stdlib>
using namespace metal;

struct Vertex {
    float2 position;
    uint part;
    uint reserved;
};

struct Part {
    uint feature;
    float stripeOriginX;
};

struct Style {
    uint offset;
    uint count;
};

struct Camera {
    float2 center;
    float2 viewSize;
    float scale;
    float depthScale;
    float strokeWidth;
    uint selected;
    uint hasTheme;
    uint selectionPass;
    uint showsFrame;
    uint reserved;
    float4 baseColor;
};

float4 project(float2 point, float depth, constant Camera &camera) {
    float2 clip = (point - camera.center) * (2.0f * camera.scale) / camera.viewSize;
    return float4(clip.x, -clip.y, depth, 1.0f);
}

float layerDepth(uint part, constant Camera &camera) {
    return 1.0f - (float(part) + 1.0f) * camera.depthScale;
}

struct FillVertex {
    float4 position [[position]];
    uint feature [[flat]];
    float stripePoints;
};

vertex FillVertex albers_fill_vertex(uint id [[vertex_id]],
                                     const device Vertex *vertices [[buffer(0)]],
                                     const device Part *parts [[buffer(1)]],
                                     constant Camera &camera [[buffer(2)]]) {
    Vertex v = vertices[id];
    Part part = parts[v.part];
    return { project(v.position, layerDepth(v.part, camera), camera), part.feature,
             (v.position.x - part.stripeOriginX) * camera.scale };
}

fragment float4 albers_fill_fragment(FillVertex v [[stage_in]],
                                    const device Style *styles [[buffer(0)]],
                                    const device float4 *colors [[buffer(1)]],
                                    constant Camera &camera [[buffer(2)]]) {
    if (v.feature == 0xffffffffu && !camera.showsFrame) discard_fragment();
    if (v.feature == 0xffffffffu) return float4(0.89f, 0.93f, 0.95f, 1.0f);
    if (!camera.hasTheme) {
        if (v.feature == camera.selected) return float4(1.0f, 0.76f, 0.29f, 1.0f);
        return camera.baseColor;
    }
    Style style = styles[v.feature];
    if (!style.count) return camera.baseColor;
    uint stripe = uint(max(0.0f, floor(v.stripePoints / 5.0f))) % style.count;
    return colors[style.offset + stripe];
}

struct Segment {
    float2 start;
    float2 end;
    uint part;
    uint reserved;
};

struct StrokeVertex {
    float4 position [[position]];
    float2 uv [[sample_no_perspective]];
    float segmentLength [[flat]];
    float radius [[flat]];
    uint decoration [[flat]];
};

vertex StrokeVertex albers_stroke_vertex(uint vertexID [[vertex_id]],
                                         uint instanceID [[instance_id]],
                                         const device Segment *segments [[buffer(0)]],
                                         const device Part *parts [[buffer(1)]],
                                         constant Camera &camera [[buffer(2)]],
                                         const device uint *order [[buffer(3)]]) {
    Segment edge = segments[camera.selectionPass ? instanceID : order[instanceID]];
    bool decoration = parts[edge.part].feature == 0xffffffffu;
    float radius = 0.5f * (camera.selectionPass ? 1.5f : decoration ? 1.0f : camera.strokeWidth);
    float2 delta = (edge.end - edge.start) * camera.scale;
    float segmentLength = length(delta);
    float2 direction = segmentLength > 0.0f ? delta / segmentLength : float2(1.0f, 0.0f);
    float2 normal = float2(-direction.y, direction.x);
    constexpr float2 corners[6] = {{0, -1}, {1, -1}, {0, 1}, {0, 1}, {1, -1}, {1, 1}};
    float2 corner = corners[vertexID];
    float2 uv = float2(mix(-radius, segmentLength + radius, corner.x), corner.y * radius);
    float2 offset = uv.x * direction + uv.y * normal;
    float4 position = project(edge.start, layerDepth(edge.part, camera), camera);
    position.xy += offset * float2(2.0f, -2.0f) / camera.viewSize;
    return { position, uv, segmentLength, radius, uint(decoration) };
}

fragment float4 albers_stroke_fragment(StrokeVertex v [[stage_in]], constant Camera &camera [[buffer(2)]]) {
    float longitudinal = max(0.0f, max(-v.uv.x, v.uv.x - v.segmentLength));
    if (dot(float2(longitudinal, v.uv.y), float2(longitudinal, v.uv.y)) > v.radius * v.radius) discard_fragment();
    if (camera.selectionPass) return float4(0.36f, 0.21f, 0.06f, 1.0f);
    if (v.decoration && !camera.showsFrame) discard_fragment();
    if (v.decoration) return float4(0.72f, 0.79f, 0.83f, 1.0f);
    return float4(0.91f, 0.92f, 0.93f, 1.0f);
}
