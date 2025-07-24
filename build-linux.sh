#!/usr/bin/env bash
set -e

echo "Building Snapbase for Linux..."
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
cp target/release/snapbase-cli dist/linux/
cp java/target/snapbase-*fat.jar dist/linux/
cp target/wheels/*.whl dist/python/
echo "Artifacts copied to distribution"
echo

echo "All builds completed successfully!"
echo
echo "Distribution outputs:"
echo "- Linux CLI: dist/linux/snapbase-cli"
echo "- Linux JAR: dist/linux/snapbase-*.jar"
echo "- Python wheel: dist/python/*.whl"