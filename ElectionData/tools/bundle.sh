#!/bin/sh
set -eu
root="$SRCROOT/../.."
destination="$TARGET_BUILD_DIR/$UNLOCALIZED_RESOURCES_FOLDER_PATH/ElectionResearch"
staging="$DERIVED_FILE_DIR/ElectionResearch"
python3 "$root/ElectionData/tools/normalize.py" --source "$root" --output "$staging" --geometry
mkdir -p "$destination"
cp -R "$staging/0.1.0" "$destination"
cp "$staging/county-geometry-2024.geojson" "$destination/"
