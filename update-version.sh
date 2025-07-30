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
    echo "  - java/dependency-reduced-pom.xml (Generated Maven file)"
    echo "  - python/pyproject.toml (Python project)"
    echo "  - python/Cargo.toml (Python Rust bindings)"
    echo "  - python/src/snapbase/__init__.py (Python module)"
    echo "  - .github/workflows/release.yml (CI/CD pipeline)"
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
CURRENT_JAVA_VERSION=$(grep -o '<version>[^<]*</version>' "$JAVA_FILE" | head -1 | sed 's/<version>\([^<]*\)<\/version>/\1/')

if [ "$CURRENT_JAVA_VERSION" != "" ] && [ "$CURRENT_JAVA_VERSION" != "unknown" ]; then
    print_info "Current Java version: $CURRENT_JAVA_VERSION"
    # Use a simpler sed approach - replace the first occurrence
    sed -i.bak "s/<version>$CURRENT_JAVA_VERSION<\/version>/<version>$NEW_VERSION<\/version>/" "$JAVA_FILE"
    print_info "✓ Updated $JAVA_FILE"
else
    print_warn "Could not detect current Java version"
fi

# Update Java dependency-reduced-pom.xml (if exists)
print_info "Updating Java dependency-reduced-pom.xml..."
JAVA_DEP_FILE="$SCRIPT_DIR/java/dependency-reduced-pom.xml"
if [ -f "$JAVA_DEP_FILE" ]; then
    CURRENT_JAVA_DEP_VERSION=$(grep -o '<version>[^<]*</version>' "$JAVA_DEP_FILE" | head -1 | sed 's/<version>\([^<]*\)<\/version>/\1/')
    
    if [ "$CURRENT_JAVA_DEP_VERSION" != "" ] && [ "$CURRENT_JAVA_DEP_VERSION" != "unknown" ]; then
        print_info "Current Java dependency-reduced version: $CURRENT_JAVA_DEP_VERSION"
        # Use a simpler sed approach - replace the first occurrence
        sed -i.bak "s/<version>$CURRENT_JAVA_DEP_VERSION<\/version>/<version>$NEW_VERSION<\/version>/" "$JAVA_DEP_FILE"
        print_info "✓ Updated $JAVA_DEP_FILE"
    else
        print_warn "Could not detect current Java dependency-reduced version"
    fi
else
    print_info "Java dependency-reduced-pom.xml not found (this is normal)"
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

# Update Python Cargo.toml
print_info "Updating Python Cargo.toml..."
PYTHON_CARGO_FILE="$SCRIPT_DIR/python/Cargo.toml"
CURRENT_PYTHON_CARGO_VERSION=$(get_current_version "$PYTHON_CARGO_FILE" 'version = "[^"]*"' | sed 's/version = "\([^"]*\)"/\1/')

if [ "$CURRENT_PYTHON_CARGO_VERSION" != "unknown" ]; then
    print_info "Current Python Cargo version: $CURRENT_PYTHON_CARGO_VERSION"
    sed -i.bak "s/version = \"$CURRENT_PYTHON_CARGO_VERSION\"/version = \"$NEW_VERSION\"/" "$PYTHON_CARGO_FILE"
    print_info "✓ Updated $PYTHON_CARGO_FILE"
else
    print_warn "Could not detect current Python Cargo version"
fi

# Update Python __init__.py
print_info "Updating Python __init__.py..."
PYTHON_INIT_FILE="$SCRIPT_DIR/python/src/snapbase/__init__.py"
CURRENT_INIT_VERSION=$(get_current_version "$PYTHON_INIT_FILE" '__version__ = "[^"]*"' | sed 's/__version__ = "\([^"]*\)"/\1/')

if [ "$CURRENT_INIT_VERSION" != "unknown" ]; then
    print_info "Current __init__.py version: $CURRENT_INIT_VERSION"
    sed -i.bak "s/__version__ = \"$CURRENT_INIT_VERSION\"/__version__ = \"$NEW_VERSION\"/" "$PYTHON_INIT_FILE"
    print_info "✓ Updated $PYTHON_INIT_FILE"
else
    print_warn "Could not detect current __init__.py version"
fi

# Update GitHub Actions workflow
print_info "Updating GitHub Actions workflow..."
WORKFLOW_FILE="$SCRIPT_DIR/.github/workflows/release.yml"
if [ -f "$WORKFLOW_FILE" ]; then
    # Update multiple occurrences of version in the workflow file
    sed -i.bak "s/snapbase-java-[0-9]\+\.[0-9]\+\.[0-9]\+-/snapbase-java-$NEW_VERSION-/g" "$WORKFLOW_FILE"
    print_info "✓ Updated $WORKFLOW_FILE"
else
    print_warn "GitHub Actions workflow file not found"
fi

# Update README files
print_info "Updating README files..."

# Update main README.md
MAIN_README="$SCRIPT_DIR/README.md"
if [ -f "$MAIN_README" ]; then
    # Update CLI download links with architecture-based versioned filenames (fix for macOS sed)
    sed -i.bak "s/snapbase-linux-[a-z0-9_]*-v[0-9][0-9]*\.[0-9][0-9]*\.[0-9][0-9]*/snapbase-linux-x86_64-v$NEW_VERSION/g" "$MAIN_README"
    sed -i.bak "s/snapbase-linux-arm64-v[0-9][0-9]*\.[0-9][0-9]*\.[0-9][0-9]*/snapbase-linux-arm64-v$NEW_VERSION/g" "$MAIN_README"
    sed -i.bak "s/snapbase-macos-[a-z-]*-v[0-9][0-9]*\.[0-9][0-9]*\.[0-9][0-9]*/snapbase-macos-apple-silicon-v$NEW_VERSION/g" "$MAIN_README"
    sed -i.bak "s/snapbase-macos-intel-v[0-9][0-9]*\.[0-9][0-9]*\.[0-9][0-9]*/snapbase-macos-intel-v$NEW_VERSION/g" "$MAIN_README"
    sed -i.bak "s/snapbase-windows-[a-z0-9_]*-v[0-9][0-9]*\.[0-9][0-9]*\.[0-9][0-9]*/snapbase-windows-x86_64-v$NEW_VERSION/g" "$MAIN_README"
    # Update Java Maven version
    sed -i.bak "s/<version>[0-9][0-9]*\.[0-9][0-9]*\.[0-9][0-9]*<\/version>/<version>$NEW_VERSION<\/version>/" "$MAIN_README"
    print_info "✓ Updated $MAIN_README"
else
    print_warn "Main README.md not found"
fi

# Update CLI README.md
CLI_README="$SCRIPT_DIR/cli/README.md"
if [ -f "$CLI_README" ]; then
    # Update CLI download links with architecture-based versioned filenames (fix for macOS sed)
    sed -i.bak "s/snapbase-linux-[a-z0-9_]*-v[0-9][0-9]*\.[0-9][0-9]*\.[0-9][0-9]*/snapbase-linux-x86_64-v$NEW_VERSION/g" "$CLI_README"
    sed -i.bak "s/snapbase-linux-arm64-v[0-9][0-9]*\.[0-9][0-9]*\.[0-9][0-9]*/snapbase-linux-arm64-v$NEW_VERSION/g" "$CLI_README"
    sed -i.bak "s/snapbase-macos-[a-z-]*-v[0-9][0-9]*\.[0-9][0-9]*\.[0-9][0-9]*/snapbase-macos-apple-silicon-v$NEW_VERSION/g" "$CLI_README"
    sed -i.bak "s/snapbase-macos-intel-v[0-9][0-9]*\.[0-9][0-9]*\.[0-9][0-9]*/snapbase-macos-intel-v$NEW_VERSION/g" "$CLI_README"
    sed -i.bak "s/snapbase-windows-[a-z0-9_]*-v[0-9][0-9]*\.[0-9][0-9]*\.[0-9][0-9]*/snapbase-windows-x86_64-v$NEW_VERSION/g" "$CLI_README"
    print_info "✓ Updated $CLI_README"
else
    print_warn "CLI README.md not found"
fi

# Update Python README.md
PYTHON_README="$SCRIPT_DIR/python/README.md"
if [ -f "$PYTHON_README" ]; then
    # Python README likely has version references in examples or installation instructions (fix for macOS sed)
    sed -i.bak "s/version=[0-9][0-9]*\.[0-9][0-9]*\.[0-9][0-9]*/version=$NEW_VERSION/g" "$PYTHON_README"
    print_info "✓ Updated $PYTHON_README"
else
    print_warn "Python README.md not found"
fi

# Update Java README.md
JAVA_README="$SCRIPT_DIR/java/README.md"
if [ -f "$JAVA_README" ]; then
    # Update Java Maven version references (fix for macOS sed)
    sed -i.bak "s/<version>[0-9][0-9]*\.[0-9][0-9]*\.[0-9][0-9]*<\/version>/<version>$NEW_VERSION<\/version>/g" "$JAVA_README"
    sed -i.bak "s/snapbase-java-[0-9][0-9]*\.[0-9][0-9]*\.[0-9][0-9]*/snapbase-java-$NEW_VERSION/g" "$JAVA_README"
    # Update architecture-specific JAR download links
    sed -i.bak "s/snapbase-java-linux-[a-z0-9_]*-v[0-9][0-9]*\.[0-9][0-9]*\.[0-9][0-9]*/snapbase-java-linux-x86_64-v$NEW_VERSION/g" "$JAVA_README"
    sed -i.bak "s/snapbase-java-linux-arm64-v[0-9][0-9]*\.[0-9][0-9]*\.[0-9][0-9]*/snapbase-java-linux-arm64-v$NEW_VERSION/g" "$JAVA_README"
    sed -i.bak "s/snapbase-java-macos-[a-z-]*-v[0-9][0-9]*\.[0-9][0-9]*\.[0-9][0-9]*/snapbase-java-macos-apple-silicon-v$NEW_VERSION/g" "$JAVA_README"
    sed -i.bak "s/snapbase-java-macos-intel-v[0-9][0-9]*\.[0-9][0-9]*\.[0-9][0-9]*/snapbase-java-macos-intel-v$NEW_VERSION/g" "$JAVA_README"
    sed -i.bak "s/snapbase-java-windows-[a-z0-9_]*-v[0-9][0-9]*\.[0-9][0-9]*\.[0-9][0-9]*/snapbase-java-windows-x86_64-v$NEW_VERSION/g" "$JAVA_README"
    print_info "✓ Updated $JAVA_README"
else
    print_warn "Java README.md not found"
fi

# Clean up backup files
print_info "Cleaning up backup files..."
find "$SCRIPT_DIR" -name "*.bak" -delete

print_info "Version update complete!"
print_info ""
print_info "Summary of changes:"
print_info "  Rust workspace: $CURRENT_RUST_VERSION → $NEW_VERSION"
print_info "  Java Maven: $CURRENT_JAVA_VERSION → $NEW_VERSION"
if [ -f "$JAVA_DEP_FILE" ]; then
    print_info "  Java dependency-reduced: $CURRENT_JAVA_DEP_VERSION → $NEW_VERSION"
fi
print_info "  Python project: $CURRENT_PYTHON_VERSION → $NEW_VERSION"
print_info "  Python Cargo: $CURRENT_PYTHON_CARGO_VERSION → $NEW_VERSION"
print_info "  Python module: $CURRENT_INIT_VERSION → $NEW_VERSION"
print_info "  GitHub Actions: Updated JAR references"
print_info "  README files: Updated installation instructions with new version"
print_info ""
print_info "Next steps:"
print_info "  1. Review the changes: git diff"
print_info "  2. Test the build: cargo build && cd java && mvn clean package && cd ../python && uv run --with maturin maturin develop"
print_info "  3. Commit the changes: git add . && git commit -m 'Bump version to $NEW_VERSION'"
print_info "  4. Create a tag: git tag v$NEW_VERSION"