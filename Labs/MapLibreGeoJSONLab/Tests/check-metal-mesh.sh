#!/bin/bash
set -euo pipefail

lab=$(cd "$(dirname "$0")/.." && pwd)
kit="$lab/../../Packages/BalloticsGeoKit"
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
objects=()
swift_flags=()
sources=()
if [[ -n ${METAL_BASELINE_ROOT:-} ]]; then
    baseline="$METAL_BASELINE_ROOT/Labs/MapLibreGeoJSONLab"
    vendor="$baseline/Vendor/libtess2"
    for source in "$vendor"/Source/*.c; do
        object="$work/$(basename "$source" .c).o"
        xcrun clang -O3 -I "$vendor/Include" -c "$source" -o "$object"
        objects+=("$object")
    done
    swift_flags+=(-I "$vendor/Include")
    sources+=("$METAL_BASELINE_ROOT"/Packages/BalloticsGeoKit/Sources/BalloticsGeoKit/*.swift
        "$baseline/GeoJSON.swift" "$baseline/AlbersMesh.swift")
else
    scratch=${GEOKIT_SWIFT_SCRATCH:-"$work/swift"}
    swift build --package-path "$kit" --scratch-path "$scratch" -c release --target BalloticsGeoKitMesh >&2
    bin=$(swift build --package-path "$kit" --scratch-path "$scratch" -c release --show-bin-path)
    swift_flags+=(-D GEOKIT_PACKAGE -package-name balloticsgeokit -I "$bin/Modules" -I "$kit/Sources/LibTess2/Include")
    objects+=("$bin"/BalloticsGeoKit.build/*.swift.o "$bin"/BalloticsGeoKitMesh.build/*.swift.o "$bin"/LibTess2.build/Source/*.o)
fi

if [[ -n ${METAL_EARCUT_ROOT:-} ]]; then
    expected=2fbafada15dbe5350c606426e17111b91a964e55
    if [[ $(git -C "$METAL_EARCUT_ROOT" rev-parse HEAD) != "$expected" ]] ||
       [[ -n $(git -C "$METAL_EARCUT_ROOT" status --porcelain -- include) ]]; then
        echo "Use an unmodified Earcut checkout at $expected." >&2
        exit 2
    fi
    shim="$lab/Tests/EarcutComparison"
    object="$work/earcut.o"
    xcrun clang++ -std=c++17 -O3 -I "$METAL_EARCUT_ROOT/include" -c "$shim/earcut.cpp" -o "$object"
    objects+=("$object")
    swift_flags+=(-D COMPARE_EARCUT -I "$shim" -lc++)
fi

xcrun swiftc -O -module-cache-path "$work/modules" "${swift_flags[@]}" ${sources[@]+"${sources[@]}"} \
    "$lab/Benchmarks.swift" "$lab/Tests/MetalMeshChecks.swift" "${objects[@]}" -o "$work/check"
METAL_FALLBACK_FIXTURE="$lab/Tests/Fixtures/Lab-Metal-Fallback.geojson" "$work/check" "$@"
