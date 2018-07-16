// swift-tools-version:4.1

import PackageDescription

let package = Package(
        name: "Aerospike",
        products: [
            .executable(
                name: "Aerospike",
                targets: ["Aerospike"]
            )
        ],
        dependencies: [
            .package(url: "https://github.com/1024jp/GzipSwift.git", from: "4.0.0"),
            .package(url: "https://github.com/Zig1375/libaerospike.git", from: "0.9.17")
        ],
        targets: [
            .target(
                name: "Aerospike",
                dependencies: [
                    "Gzip",
                    "libaerospike"
                ],
                path: "Sources"
            )
        ]
)
