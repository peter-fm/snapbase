#!/bin/bash
set -e

echo "Building Snapbase for Linux..."
echo

# Extract version from Cargo.toml
VERSION=$(grep '^version = ' Cargo.toml | head -1 | sed 's/version = "\(.*\)"/\1/')
echo "Building version: $VERSION"
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
cd python
uv run --with maturin maturin build --release
cd ..
echo "Python bindings build completed successfully"
echo

echo "Step 5: Creating distribution directories..."
mkdir -p dist/linux
echo "Distribution directories created"
echo

echo "Step 6: Copying artifacts to distribution..."
cp target/release/snapbase dist/linux/snapbase-linux-v${VERSION}
cp java/target/snapbase-*fat.jar dist/linux/snapbase-linux-v${VERSION}.jar
cp target/wheels/*.whl dist/linux/snapbase-linux-v${VERSION}.whl
echo "Artifacts copied to distribution"
echo

echo "All builds completed successfully!"
echo
echo "Distribution outputs:"
echo "- Linux CLI: dist/linux/snapbase-linux-v${VERSION}"
echo "- Linux JAR: dist/linux/snapbase-linux-v${VERSION}.jar"
echo "- Linux wheel: dist/linux/snapbase-linux-v${VERSION}.whl"