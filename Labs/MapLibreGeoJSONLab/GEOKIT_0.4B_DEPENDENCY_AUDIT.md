# BalloticsGeoKit 0.4B dependency audit

Baseline `617ef28b6768c47b6ea08a83cfa262d5f4668053` matches the research checkout and GitHub. The 0.4B worktree branches directly from that commit.

## Original graph

| Component | Direct dependencies | Extraction boundary |
| --- | --- | --- |
| BalloticsGeoKit | Foundation, CoreFoundation and Core Graphics | Keep source validation, regional Albers, indexed picking, composition, camera coordinates and label placement here. |
| AlbersMesh | Projected scene and paths, vendored LibTess2, lab GeoJSON error wrapper | Move the Swift implementation and C sources together. Replace the error wrapper with a mesh error. Preserve the vertex, index, part and segment layouts. |
| AlbersMetalSurface | Mesh records, Albers scene panels, MetalKit, QuartzCore, main-bundle MSL/metallib, GeoJSON errors, LabLog, process memory samples and launch arguments | Move buffer allocation, shader lookup, pipelines, frame scheduling and revision tickets into Apple rendering. Pass configuration and diagnostics explicitly. |
| AlbersCanvas in AlbersMapView | UIKit, gestures and camera animation, Core Graphics, Metal surface and preparation, request timestamps, LabLog, benchmark routes, ElectoralLabelOverlay | Share path drawing, camera calculations and resource preparation. Keep research routes, the source-publication gate and electoral content in the lab. |
| AlbersMapView and ElectoralLabelOverlay | SwiftUI, LabState, ElectionMapPresentation, party colors, electoral values, native text and accessibility | Translate election palettes and selection to feature indices. Keep search, inspector, external key and election label construction at the consumer boundary. |
| LabState and lab app | Historical archive, ElectionStore, SwiftData, election geography resolver, SwiftUI navigation, MapLibre Native | Keep source selection, snapshot filtering, persistence and the MapLibre control in the lab. |
| DistrictAtlas | AppKit, geographic record keys, AlbersScene, Core Graphics path primitive, native text | Use the same public mesh and Apple rendering adapters as the lab. Keep Census keys, styles, search and inspector in DistrictAtlas. |
| Qualification tools | Host Swift probes, XCTest routes, JSONL benchmark schema, simulator/device orchestration | Keep orchestration and evidence in the lab. Translate adapter events to the existing log schema. |

## Proposed boundary

`BalloticsGeoKitApple` depends on `BalloticsGeoKitMesh` and `BalloticsGeoKit`. The mesh target depends on the core and `LibTess2`. Apple rendering groups Metal and Core Graphics so they can share a camera and feature appearance contract. Both consumers own their surrounding UI and supply native annotations from the resulting camera and label placements. CPU mesh preparation and geographic qualification remain runnable without MetalKit. The package supplies cancellable preparation and the Metal view owns GPU publication, frame tickets and retirement. Consumers coordinate when a requested source may become visible.

## Resulting application dependencies

The package imports no election, political, MapLibre Native, SwiftData, XCTest or lab module in its production targets. The lab keeps its request gate, result-oriented UI, placement of the external electoral key, accessibility, memory sampling, launch flags and benchmark routes. Its runtime shader comparison points to the package shader source. DistrictAtlas links the standalone package and validates the 441 Census records directly. Census schema keys and U.S. region policy in the geographic core remain useful boundaries for a future general geographic product.
