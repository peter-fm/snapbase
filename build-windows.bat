@echo off
echo Building Snapbase for Windows...
echo.

REM Extract version from Cargo.toml
for /f "tokens=3 delims= " %%a in ('findstr "^version = " Cargo.toml') do set VERSION=%%a
set VERSION=%VERSION:"=%
echo Building version: %VERSION%
echo.

echo Step 1: Building CLI (Rust)...
cargo build --release
if %ERRORLEVEL% neq 0 (
    echo ERROR: CLI build failed
    exit /b 1
)
echo CLI build completed successfully
echo.

echo Step 2: Building Java bindings...
cd java-bindings
cargo build --release --features jni
if %ERRORLEVEL% neq 0 (
    echo ERROR: Java bindings build failed
    exit /b 1
)
cd ..
echo Java bindings build completed successfully
echo.

echo Step 3: Building Java JAR...
cd java
call mvn clean package -DskipTests
if %ERRORLEVEL% neq 0 (
    echo ERROR: Java JAR build failed
    exit /b 1
)
cd ..
echo Java JAR build completed successfully
echo.

echo Step 4: Building Python bindings...
cd python
uv run --with maturin maturin build --release
if %ERRORLEVEL% neq 0 (
    echo ERROR: Python bindings build failed
    exit /b 1
)
cd ..
echo Python bindings build completed successfully
echo.

echo Step 5: Creating distribution directories...
if not exist "dist" mkdir dist
if not exist "dist\windows" mkdir dist\windows
echo Distribution directories created
echo.

echo Step 6: Copying artifacts to distribution...
copy "target\release\snapbase.exe" "dist\windows\snapbase-windows-v%VERSION%.exe"
copy "java\target\snapbase*fat.jar" "dist\windows\snapbase-windows-v%VERSION%.jar"
copy "target\wheels\*.whl" "dist\windows\snapbase-windows-v%VERSION%.whl"
echo Artifacts copied to distribution
echo.

echo All builds completed successfully!
echo.
echo Distribution outputs:
echo - Windows CLI: dist\windows\snapbase-windows-v%VERSION%.exe
echo - Windows JAR: dist\windows\snapbase-windows-v%VERSION%.jar
echo - Python wheel: dist\windows\snapbase-windows-v%VERSION%.whl