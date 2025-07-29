#!/bin/bash

# Compliance Dashboard Runner
# This script runs the Spring Boot dashboard with the required JVM arguments for Arrow support

echo "🚀 Starting Snapbase Compliance Dashboard..."
echo ""

rm -rf compliance_workspace

# Check if Maven is available
if ! command -v mvn &> /dev/null; then
    echo "❌ Error: Maven is not installed or not in PATH"
    echo "Please install Maven and try again"
    exit 1
fi

# Compile the project first
echo "🔨 Building project..."
mvn clean compile -q

if [ $? -ne 0 ]; then
    echo "❌ Build failed. Please check the output above."
    exit 1
fi

echo "✅ Build successful"
echo ""

# Set the JVM arguments for Arrow support
export MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED --enable-native-access=ALL-UNNAMED"

echo "🌐 Starting web dashboard..."
echo "📱 Dashboard will be available at: http://localhost:8080"
echo ""

# Run the Spring Boot application with proper JVM arguments
mvn spring-boot:run \
    -Dspring-boot.run.jvmArguments="--add-opens=java.base/java.nio=ALL-UNNAMED --enable-native-access=ALL-UNNAMED"

echo ""
if [ $? -eq 0 ]; then
    echo "👋 Dashboard stopped successfully!"
else
    echo "❌ Dashboard failed. Check the output above for details."
fi