#!/bin/bash
set -e

echo "Building Snapbase for Linux..."
echo

# Extract version from Cargo.toml (handle both Unix and Windows line endings)
VERSION=$(grep '^version = ' Cargo.toml | head -1 | sed 's/version = "\(.*\)"/\1/' | tr -d '\r')

# Detect architecture
ARCH=$(uname -m)
case $ARCH in
    x86_64) ARCH="x86_64" ;;
    aarch64) ARCH="arm64" ;;
    arm64) ARCH="arm64" ;;
    i386|i686) ARCH="x86" ;;
    *) ARCH="$ARCH" ;;
esac

echo "Building version: $VERSION for $ARCH"
echo

echo "Step 1: Building CLI (Rust)..."
cargo build --release
echo "CLI build completed successfully"
echo

echo "Step 2: Building Java bindings..."
cd java-bindings
cargo build --release --features jni
cd ..
echo "Java bindings build completed successfully"
echo

echo "Step 3: Building Java JAR..."
cd java
mvn clean package -DskipTests
cd ..
echo "Java JAR build completed successfully"
echo

echo "Step 4: Building Python bindings..."
# Clean wheel directory to avoid contamination from previous builds
rm -rf target/wheels/*
cd python
rm -rf .venv
uv sync
uv run --with maturin maturin build --release
cd ..
echo "Python bindings build completed successfully"
echo

echo "Step 5: Creating distribution directories..."
mkdir -p dist/linux
echo "Distribution directories created"
echo

echo "Step 6: Copying artifacts to distribution..."
cp target/release/snapbase dist/linux/snapbase-linux-${ARCH}-v${VERSION}
# Copy JAR file and rename it (contains platform-specific native libraries)
cp java/target/snapbase-java-*-fat.jar dist/linux/
for jar in dist/linux/snapbase-java-*-fat.jar; do
    mv "$jar" "dist/linux/snapbase-java-linux-${ARCH}-v${VERSION}.jar"
    break  # Only rename the first jar found
done
# Copy Linux-specific wheel file and rename it
cp target/wheels/*linux_x86_64.whl dist/linux/ 2>/dev/null || cp target/wheels/*manylinux*.whl dist/linux/ 2>/dev/null || cp target/wheels/*.whl dist/linux/
for wheel in dist/linux/*.whl; do
    mv "$wheel" "dist/linux/snapbase-linux-${ARCH}-v${VERSION}.whl"
    break  # Only rename the first wheel found
done
echo "Artifacts copied to distribution"
echo

echo "All builds completed successfully!"
echo
echo "Distribution outputs:"
echo "- Linux CLI: dist/linux/snapbase-linux-${ARCH}-v${VERSION}"
echo "- Linux JAR: dist/linux/snapbase-java-linux-${ARCH}-v${VERSION}.jar"
echo "- Linux wheel: dist/linux/snapbase-linux-${ARCH}-v${VERSION}.whl"