// swift-tools-version: 5.9
import PackageDescription

let package = Package(
    name: "BalloticsGeoKit",
    platforms: [.iOS(.v17), .macOS(.v13)],
    products: [.library(name: "BalloticsGeoKit", targets: ["BalloticsGeoKit"]),
               .executable(name: "DistrictAtlas", targets: ["DistrictAtlas"])],
    targets: [.target(name: "BalloticsGeoKit"),
              .executableTarget(name: "DistrictAtlas", dependencies: ["BalloticsGeoKit"]),
              .testTarget(name: "BalloticsGeoKitTests", dependencies: ["BalloticsGeoKit"], resources: [.copy("Fixtures")])]
)
