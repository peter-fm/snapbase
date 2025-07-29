#!/bin/bash

# Compliance Audit Demo Runner
# This script runs the demo with the required JVM arguments for Arrow support

echo "🚀 Starting Snapbase Compliance Audit Demo..."
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

# Run the demo with proper JVM arguments
echo "🏃 Running compliance audit demo..."
echo ""

export MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED --enable-native-access=ALL-UNNAMED"

mvn exec:java \
    -Dexec.mainClass="com.snapbase.demos.compliance.ComplianceAuditDemo" \
    -Dexec.args="--add-opens=java.base/java.nio=ALL-UNNAMED --enable-native-access=ALL-UNNAMED" \
    -q

echo ""
if [ $? -eq 0 ]; then
    echo "🎉 Demo completed successfully!"
    echo ""
    echo "📁 Check the 'compliance_workspace' directory for:"
    echo "   • Snapshots in .snapbase/ directory"
    echo "   • Audit exports in audit_exports/ directory" 
    echo "   • Customer data CSV file"
else
    echo "❌ Demo failed. Check the output above for details."
fi