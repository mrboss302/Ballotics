# Ballotics MapLibre Native GeoJSON Lab

This is an isolated SwiftUI iOS research project. It does not modify Ballotics production source code.

## Setup

1. Check out the research/maplibre-native-geojson-lab branch from the mrboss302/Ballotics repository.
2. Open Labs/MapLibreGeoJSONLab/MapLibreGeoJSONLab.xcodeproj in Xcode.
3. Resolve the official MapLibre Swift package (pinned to 6.31.0).
4. Run the MapLibreGeoJSONLab scheme on iPhone simulator, then on a physical iPhone.
5. Choose Districts, Counties, States, or Import file. Tap polygons and pan/zoom.

The project directly references three EXISTING files at the Ballotics repository root. Xcode bundles the raw JSON resources, with no conversion to SVG, MVT or PMTiles:

- cb_2024_us_cd119_5m.json (4,749,558 bytes)
- cb_2024_us_county_5m.json (10,167,949 bytes)
- cb_2024_us_state_5m.json (2,349,868 bytes)

Files must remain at the repository root. If extracting this lab out of the repository, update the Xcode file references.

Import file supports additional GeoJSON files from the Files app. To test the much larger historical-county asset, copy US_HistCounties_Gen05.json from the preserved iOS repository to the iPhone Files app, then select Import file.

## Test protocol

- Confirm no basemap download is required: offline-style.json is a LOCAL MapLibre style with no external sources.
- Confirm the three original GeoJSON files load and draw all expected geographic areas.
- Tap and identify polygons by GEOID; inspect multipart coastal geometry.
- Run the same sequence at 1x, 2x, 4x and 8x zoom, with repeated panning.
- Cycle districts → counties → states → districts 20 times. Watch memory growth and stale geometry.
- Use a physical device in Release configuration with Xcode Instruments Time Profiler, Allocations/VM Tracker and Core Animation to measure actual CPU, peak/resident memory and frame pacing.
- Use a fixed device/OS/camera path and the SAME geometry when comparing against the native Ballotics Maps Lab.
- Check accessibility separately using VoiceOver. This test app is not a production accessibility qualification.

The on-screen time means source addition to the first MapLibre idle callback. It is a diagnostic, NOT a trustworthy end-to-end load, frame-rate, or memory benchmark. Only the device measurements establish speed. This project has not been built or run in Xcode by the author.

The bundled files describe 2024 geographic boundaries. Do not treat them as validated 2026 district boundaries or election results. This experiment contains no election results.

MapLibre sources:
https://maplibre.org/maplibre-native/ios/latest/documentation/maplibre-native-for-ios/gettingstarted/
https://maplibre.org/maplibre-native/ios/latest/documentation/maplibre-native-for-ios/geojson/
