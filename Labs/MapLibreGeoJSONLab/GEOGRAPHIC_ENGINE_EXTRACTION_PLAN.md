# Geographic engine extraction plan

Extract in dependency order after the qualification restrictions have owners. The first consumer should be the existing lab. Preserve the controls and the host fixture runner throughout the move.

| Component today | Dependencies and contract to retain | Proposed boundary |
| --- | --- | --- |
| GeoJSON and imported files | Foundation, JSON validation, source-specific schemas, stable source identifiers, temporary-file ownership | Source adapter validates coordinates and produces a revision plus typed feature properties. Keep file access and lifetime with its caller. |
| HistoricalArchive and HistoricalStore | Inclusive civil dates, byte ranges, hashes, archive-specific fields, snapshot cache, temporary files | Temporal source supplies immutable geographic revisions. The election calendar chooses a date outside the engine. |
| AlbersProjection, region assignment and paths | Scalar WGS 84 math, longitude wrapping, densification, Core Graphics, U.S. jurisdiction metadata | Separate projection math and configurable region policy from the Apple path representation. Preserve Double regional coordinates. |
| AlbersScene and spatial index | Ordered parts, projected bounds, Core Graphics even–odd membership, bounding-volume hierarchy, source property strings | Scene exposes identities, ordered hits and bounds. Isolate Census and historical key lookups behind source adapters. |
| National composition and camera | Region extents, uniform transforms, viewport size, selection framing, UIKit gesture and animation clocks | Pure layout/camera values shared by renderers. Inject the clock, motion preference and geographic focus target at the view boundary. |
| Mesh and tessellation | Regional paths, libtess2 C module and license, Double origins, Float32 positions, UInt32 indices, original edges and part metadata | Immutable CPU mesh artifact, keyed by geographic revision. Keep validation, cancellation and documented precision limits at this boundary. |
| Paint and labels | ElectionMapPresentation, electoral/county joins, historical participation, candidate colors, margin/stripe rules, UIKit text | Consumer supplies feature-indexed paint, optional label content and accessibility text. Preserve split allocations and absence of verified counts. |
| Search and selection | Ordered membership, stable feature identity, historical overlaps, source-specific inspector strings | Engine returns ranked identities. The app chooses the picker, inspector and result navigation. |
| Metal surface and Core Graphics control | MetalKit, shaders, device capabilities, GPU retention, main actor, Core Graphics reference | Apple render adapters consume the same scene, layout, camera and style revisions. Keep completion ownership separate from source-model readiness. |
| MapLibre control and instrumentation | Mercator map camera, delegate callbacks, XCTest, simulator/device logging | Keep the control and benchmark orchestration in the lab as external clients. |

Today the canvas knows LabState, Viewpoint, AlbersTheme, election revision, search and inspector callbacks. Scene construction also knows Census and historical property keys. Metal exposes AlbersScene.Panel directly, including path/index data it does not need to encode a draw. A first extraction should introduce narrow read-only contracts at these points. Keep election archive selection, SwiftData packages, candidate identities and historical reconciliation with the application.

A useful first consumer test is a Census district map with no presidential data linked. It should support the same region transforms, indexed overlap picking, fallback and retained mesh behavior as the election map. Run the existing fourteen inputs and GPU lifecycle route after each boundary moves. Cross-platform interchange, source redistribution and Android rendering require separate decisions.
