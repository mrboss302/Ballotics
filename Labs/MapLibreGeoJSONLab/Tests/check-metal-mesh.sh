#!/bin/bash
set -euo pipefail

lab=$(cd "$(dirname "$0")/.." && pwd)
vendor="$lab/Vendor/libtess2"
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
objects=()
swift_flags=()
for source in "$vendor"/Source/*.c; do
    object="$work/$(basename "$source" .c).o"
    xcrun clang -O3 -I "$vendor/Include" -c "$source" -o "$object"
    objects+=("$object")
done

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
    swift_flags=(-D COMPARE_EARCUT -I "$shim" -lc++)
fi

xcrun swiftc -O -I "$vendor/Include" -module-cache-path "$work/modules" \
    ${swift_flags[@]+"${swift_flags[@]}"} \
    "$lab"/../../Packages/BalloticsGeoKit/Sources/BalloticsGeoKit/*.swift "$lab/GeoJSON.swift"  "$lab/Benchmarks.swift" \
    "$lab/AlbersMesh.swift" "$lab/Tests/MetalMeshChecks.swift" "${objects[@]}" -o "$work/check"
METAL_FALLBACK_FIXTURE="$lab/Tests/Fixtures/Lab-Metal-Fallback.geojson" "$work/check" "$@"
