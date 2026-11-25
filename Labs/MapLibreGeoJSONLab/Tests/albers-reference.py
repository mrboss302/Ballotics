"""Regenerate the projection fixture with pyproj and PROJ."""

import json

import pyproj


REGIONS = {
    "mainland": ((-96, 23, 29.5, 45.5), [
        (-124.4, 40.4), (-100, 40), (-77.0365, 38.8977), (-66.9, 44.8), (-80.3, 25.5),
    ]),
    "alaska": ((-154, 50, 55, 65), [
        (-149.9003, 61.2181), (-156.8, 71.3), (179.8, 52), (-179.8, 52), (172.4, 52.8),
    ]),
    "hawaii": ((-157, 3, 8, 18), [
        (-157.8583, 21.3069), (-155.55, 19.6), (-159.5, 22), (-154.81, 19.51),
    ]),
}


def main():
    points = []
    patches = []
    geod = pyproj.Geod(ellps="WGS84")
    for region, (parameters, probes) in REGIONS.items():
        lon0, lat0, lat1, lat2 = parameters
        transform = pyproj.Proj(
            proj="aea", ellps="WGS84", lon_0=lon0, lat_0=lat0, lat_1=lat1, lat_2=lat2,
        )
        for lon, lat in probes:
            easting, northing = transform(lon, lat)
            points.append(dict(region=region, lon=lon, lat=lat, easting=easting, northing=northing))
        lon, lat = probes[0]
        # Small geodesic quadrilateral, integrated independently on the ellipsoid.
        ring = [(lon - .01, lat - .01), (lon + .01, lat - .01),
                (lon + .01, lat + .01), (lon - .01, lat + .01)]
        area, _ = geod.polygon_area_perimeter(*zip(*ring))
        patches.append(dict(region=region, ring=ring, area=area))
    print(json.dumps(dict(proj=pyproj.proj_version_str, points=points, patches=patches), indent=2))


if __name__ == "__main__":
    main()
