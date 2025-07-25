#!/bin/bash

# Version update script for Snapbase project
# Updates version across Rust, Java, and Python components

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored output
print_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
print_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
print_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# Function to show usage
usage() {
    echo "Usage: $0 <new_version>"
    echo "Example: $0 0.1.1"
    echo ""
    echo "This script updates the version in:"
    echo "  - Cargo.toml (Rust workspace)"
    echo "  - java/pom.xml (Java Maven project)"
    echo "  - python/pyproject.toml (Python project)"
    exit 1
}

# Check if version argument is provided
if [ $# -ne 1 ]; then
    print_error "Version argument is required"
    usage
fi

NEW_VERSION="$1"

# Validate version format (basic semver check)
if ! echo "$NEW_VERSION" | grep -qE '^[0-9]+\.[0-9]+\.[0-9]+(-[a-zA-Z0-9\.-]+)?(\+[a-zA-Z0-9\.-]+)?$'; then
    print_error "Invalid version format. Please use semantic versioning (e.g., 0.1.1)"
    exit 1
fi

# Get current directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

print_info "Updating Snapbase project version to $NEW_VERSION"

# Function to get current version from a file
get_current_version() {
    local file="$1"
    local pattern="$2"
    
    if [ ! -f "$file" ]; then
        print_error "File not found: $file"
        return 1
    fi
    
    grep -o "$pattern" "$file" | head -1 || echo "unknown"
}

# Update Rust workspace version
print_info "Updating Rust workspace version..."
RUST_FILE="$SCRIPT_DIR/Cargo.toml"
CURRENT_RUST_VERSION=$(get_current_version "$RUST_FILE" 'version = "[^"]*"' | sed 's/version = "\([^"]*\)"/\1/')

if [ "$CURRENT_RUST_VERSION" != "unknown" ]; then
    print_info "Current Rust version: $CURRENT_RUST_VERSION"
    sed -i.bak "s/version = \"$CURRENT_RUST_VERSION\"/version = \"$NEW_VERSION\"/" "$RUST_FILE"
    print_info "✓ Updated $RUST_FILE"
else
    print_warn "Could not detect current Rust version"
fi

# Update Java Maven version
print_info "Updating Java Maven version..."
JAVA_FILE="$SCRIPT_DIR/java/pom.xml"
CURRENT_JAVA_VERSION=$(get_current_version "$JAVA_FILE" '<version>[^<]*</version>' | head -1 | sed 's/<version>\([^<]*\)<\/version>/\1/')

if [ "$CURRENT_JAVA_VERSION" != "unknown" ]; then
    print_info "Current Java version: $CURRENT_JAVA_VERSION"
    sed -i.bak "0,/<version>$CURRENT_JAVA_VERSION<\/version>/s//<version>$NEW_VERSION<\/version>/" "$JAVA_FILE"
    print_info "✓ Updated $JAVA_FILE"
else
    print_warn "Could not detect current Java version"
fi

# Update Python version
print_info "Updating Python version..."
PYTHON_FILE="$SCRIPT_DIR/python/pyproject.toml"
CURRENT_PYTHON_VERSION=$(get_current_version "$PYTHON_FILE" 'version = "[^"]*"' | sed 's/version = "\([^"]*\)"/\1/')

if [ "$CURRENT_PYTHON_VERSION" != "unknown" ]; then
    print_info "Current Python version: $CURRENT_PYTHON_VERSION"
    sed -i.bak "s/version = \"$CURRENT_PYTHON_VERSION\"/version = \"$NEW_VERSION\"/" "$PYTHON_FILE"
    print_info "✓ Updated $PYTHON_FILE"
else
    print_warn "Could not detect current Python version"
fi

# Clean up backup files
print_info "Cleaning up backup files..."
find "$SCRIPT_DIR" -name "*.bak" -delete

print_info "Version update complete!"
print_info ""
print_info "Summary of changes:"
print_info "  Rust workspace: $CURRENT_RUST_VERSION → $NEW_VERSION"
print_info "  Java Maven: $CURRENT_JAVA_VERSION → $NEW_VERSION"
print_info "  Python: $CURRENT_PYTHON_VERSION → $NEW_VERSION"
print_info ""
print_info "Next steps:"
print_info "  1. Review the changes: git diff"
print_info "  2. Test the build: cargo build && cd java && mvn clean package && cd ../python && uv run --with maturin maturin develop"
print_info "  3. Commit the changes: git add . && git commit -m 'Bump version to $NEW_VERSION'"
print_info "  4. Create a tag: git tag v$NEW_VERSION"