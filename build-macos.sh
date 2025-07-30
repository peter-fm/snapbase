#!/bin/bash
set -e

echo "Building Snapbase for macOS..."
echo

# Extract version from Cargo.toml (handle both Unix and Windows line endings)
VERSION=$(grep '^version = ' Cargo.toml | head -1 | sed 's/version = "\(.*\)"/\1/' | tr -d '\r')

# Detect architecture
ARCH=$(uname -m)
case $ARCH in
    x86_64) ARCH="x86_64" ;;
    arm64) ARCH="arm64" ;;
    aarch64) ARCH="arm64" ;;
    i386|i686) ARCH="x86" ;;
    *) ARCH="$ARCH" ;;
esac

# Use more descriptive names for macOS
if [ "$ARCH" = "arm64" ]; then
    ARCH_DESC="apple-silicon"
else
    ARCH_DESC="intel"
fi

echo "Building version: $VERSION for macOS $ARCH_DESC ($ARCH)"
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
mkdir -p dist/macos
echo "Distribution directories created"
echo

echo "Step 6: Copying artifacts to distribution..."
cp target/release/snapbase dist/macos/snapbase-macos-${ARCH_DESC}-v${VERSION}
# Copy JAR file and rename it (contains platform-specific native libraries)
cp java/target/snapbase-java-*-fat.jar dist/macos/
for jar in dist/macos/snapbase-java-*-fat.jar; do
    mv "$jar" "dist/macos/snapbase-java-macos-${ARCH_DESC}-v${VERSION}.jar"
    break  # Only rename the first jar found
done
# Copy macOS-specific wheel file and rename it
cp target/wheels/*macosx*.whl dist/macos/ 2>/dev/null || cp target/wheels/*.whl dist/macos/
for wheel in dist/macos/*.whl; do
    mv "$wheel" "dist/macos/snapbase-macos-${ARCH_DESC}-v${VERSION}.whl"
    break  # Only rename the first wheel found
done
echo "Artifacts copied to distribution"
echo

echo "All builds completed successfully!"
echo
echo "Distribution outputs:"
echo "- macOS CLI: dist/macos/snapbase-macos-${ARCH_DESC}-v${VERSION}"
echo "- macOS JAR: dist/macos/snapbase-java-macos-${ARCH_DESC}-v${VERSION}.jar"
echo "- macOS wheel: dist/macos/snapbase-macos-${ARCH_DESC}-v${VERSION}.whl"