// swift-tools-version: 5.9
import PackageDescription

let package = Package(
    name: "BalloticsGeoKit",
    platforms: [.iOS(.v17), .macOS(.v13)],
    products: [.library(name: "BalloticsGeoKit", targets: ["BalloticsGeoKit"]),
               .library(name: "BalloticsGeoKitMesh", targets: ["BalloticsGeoKitMesh"]),
               .library(name: "BalloticsGeoKitApple", targets: ["BalloticsGeoKitApple"]),
               .executable(name: "DistrictAtlas", targets: ["DistrictAtlas"])],
    targets: [.target(name: "BalloticsGeoKit"),
              .target(name: "LibTess2", publicHeadersPath: "Include"),
              .target(name: "BalloticsGeoKitMesh", dependencies: ["BalloticsGeoKit", "LibTess2"],
                      resources: [.copy("LICENSE-libtess2.txt")]),
              .target(name: "BalloticsGeoKitApple", dependencies: ["BalloticsGeoKit", "BalloticsGeoKitMesh"],
                      resources: [.process("Shaders")]),
              .executableTarget(name: "DistrictAtlas", dependencies: ["BalloticsGeoKit", "BalloticsGeoKitApple"]),
              .testTarget(name: "BalloticsGeoKitTests", dependencies: ["BalloticsGeoKit"], resources: [.copy("Fixtures")]),
              .testTarget(name: "BalloticsGeoKitRenderingTests",
                          dependencies: ["BalloticsGeoKit", "BalloticsGeoKitMesh", "BalloticsGeoKitApple"])]
)
