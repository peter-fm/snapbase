#!/bin/bash

# Build script for Snapbase Java API
# This script builds the Rust JNI bindings and Java wrapper

set -e

# Extract version from Cargo.toml (handle both Unix and Windows line endings)
VERSION=$(grep '^version = ' Cargo.toml | head -1 | sed 's/version = "\(.*\)"/\1/' | tr -d '\r')

echo "🦀 Building Snapbase Java API v${VERSION}..."

# Check if required tools are installed
if ! command -v cargo &> /dev/null; then
    echo "❌ Cargo (Rust) is required but not installed. Please install Rust."
    exit 1
fi

if ! command -v mvn &> /dev/null; then
    echo "❌ Maven is required but not installed. Please install Maven."
    exit 1
fi

# Check Java version
if ! java -version 2>&1 | grep -q "11\|17\|21"; then
    echo "⚠️  Java 11+ is recommended. Current version:"
    java -version
fi

echo "📦 Building Rust JNI bindings..."
cd java-bindings
cargo build --release --features jni

echo "☕ Building Java components..."
cd ../java

# Clean previous builds
mvn clean

# Compile and package
mvn compile
mvn test
mvn package

echo "✅ Build completed successfully!"
echo ""
echo "📁 Artifacts created:"
echo "   - JAR: java/target/snapbase-java-${VERSION}.jar"
echo "   - Sources: java/target/snapbase-java-${VERSION}-sources.jar"
echo "   - Javadoc: java/target/snapbase-java-${VERSION}-javadoc.jar"
echo "   - Native library: java-bindings/target/release/libsnapbase_java.so (Linux/macOS)"
echo ""
echo "🚀 To use in your project, add the JAR to your classpath:"
echo "   mvn install:install-file -Dfile=java/target/snapbase-java-${VERSION}.jar \\"
echo "                           -DgroupId=com.snapbase \\"
echo "                           -DartifactId=snapbase-java \\"
echo "                           -Dversion=${VERSION} \\"
echo "                           -Dpackaging=jar"