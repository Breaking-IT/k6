#!/bin/bash
# Build k6 Linux and macOS binaries (amd64 and arm64) from the current code.
# Usage: ./build-linux-binaries.sh [output-dir]
# Default output dir is "dist". Binaries end in -breakingit-YYYYMMDDTHHMMSSZ (UTC).

set -euo pipefail

OUT_DIR="${1:-dist}"
VERSION="${VERSION:-$(git describe --tags --always --dirty)}"
BUILD_TIMESTAMP="$(date -u +%Y%m%dT%H%M%SZ)"

echo "--- Building k6 binaries: ${VERSION}"
echo "--- Build timestamp (UTC): ${BUILD_TIMESTAMP}"
echo "--- Output directory: ${OUT_DIR}"

mkdir -p "$OUT_DIR"

LDFLAGS='-s -w'

build_linux() {
    local arch="$1"
    local name="k6-linux-${arch}-breakingit-${BUILD_TIMESTAMP}"
    echo "Building ${name}..."
    GOOS=linux GOARCH="${arch}" CGO_ENABLED=0 go build -tags lean -trimpath -ldflags "${LDFLAGS}" -o "${OUT_DIR}/${name}" .
    echo "  -> ${OUT_DIR}/${name}"
}

build_darwin() {
    local arch="$1"
    local name="k6-darwin-${arch}-breakingit-${BUILD_TIMESTAMP}"
    echo "Building ${name}..."
    GOOS=darwin GOARCH="${arch}" CGO_ENABLED=0 go build -tags lean -trimpath -ldflags "${LDFLAGS}" -o "${OUT_DIR}/${name}" .
    echo "  -> ${OUT_DIR}/${name}"
}

build_linux amd64
build_linux arm64
build_darwin amd64
build_darwin arm64

echo "--- Done. Binaries in ${OUT_DIR}/"
