@echo off
echo Building Snapbase for Windows...
echo.

REM Extract version from Cargo.toml (get first match only)
for /f "tokens=3 delims= " %%a in ('findstr /B "version = " Cargo.toml') do (
    if not defined VERSION set VERSION=%%a
)
set VERSION=%VERSION:"=%

REM Detect architecture
if "%PROCESSOR_ARCHITECTURE%"=="AMD64" (
    set ARCH=x86_64
) else if "%PROCESSOR_ARCHITECTURE%"=="ARM64" (
    set ARCH=arm64
) else (
    set ARCH=x86
)

echo Building version: %VERSION% for %ARCH%
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
REM Clean wheel directory to avoid contamination from previous builds
if exist "target\wheels" del /q "target\wheels\*"
cd python
rmdir /s /q .venv
uv sync
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
copy "target\release\snapbase.exe" "dist\windows\snapbase-windows-%ARCH%-v%VERSION%.exe"
REM Copy JAR file and rename it (contains platform-specific native libraries)
copy "java\target\snapbase-java-*-fat.jar" "dist\windows\"
for %%f in (dist\windows\snapbase-java-*-fat.jar) do (
    move "%%f" "dist\windows\snapbase-java-windows-%ARCH%-v%VERSION%.jar"
    goto :jar_done
)
:jar_done
REM Copy Windows-specific wheel file and rename it
copy "target\wheels\*win_amd64.whl" "dist\windows\" 2>nul || copy "target\wheels\*win32.whl" "dist\windows\" 2>nul || copy "target\wheels\*.whl" "dist\windows\"
for %%f in (dist\windows\*.whl) do (
    move "%%f" "dist\windows\snapbase-windows-%ARCH%-v%VERSION%.whl"
    goto :wheel_done
)
:wheel_done
echo Artifacts copied to distribution
echo.

echo All builds completed successfully!
echo.
echo Distribution outputs:
echo - Windows CLI: dist\windows\snapbase-windows-%ARCH%-v%VERSION%.exe
echo - Windows JAR: dist\windows\snapbase-java-windows-%ARCH%-v%VERSION%.jar
echo - Windows wheel: dist\windows\snapbase-windows-%ARCH%-v%VERSION%.whl