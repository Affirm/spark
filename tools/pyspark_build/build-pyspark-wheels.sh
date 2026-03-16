#!/usr/bin/env bash
# PySpark wheel builder — clone → checkout upstream tag → apply Affirm commit →
# set version → mvn compile → package 3 wheels → (optional) upload to Artifactory.
# Run on Thor; use --upload only when ready to publish.
#
# All Affirm changes must live in a single commit in Affirm/spark (not merged to master).
# Productive releases require the commit SHA to be recorded in VERSIONS.md (this repo).

set -euo pipefail

echo "meminfo:"; grep -E 'MemTotal|MemAvailable' /proc/meminfo

# Defaults
OUTPUT_DIR="${OUTPUT_DIR:-/build/dist}"
UPLOAD="${UPLOAD:-false}"
APPLY_COMMIT=""

usage() {
    cat <<EOF
Usage: $0 --spark-version VER --affirm-version VER --apply-commit SHA [options]

Required:
  --spark-version VER    Upstream Spark version tag (e.g. 4.0.0)
  --affirm-version VER   Affirm suffix (e.g. dev1, 1) → 2815!VER+affirm.VER
  --apply-commit SHA     SHA of the Affirm/spark commit containing all Affirm changes

Optional:
  --output-dir DIR      Where to write .whl files (default: /build/dist)
  --upload              Upload wheels to Artifactory (requires ARTIFACTORY_* env vars)

Environment variables:
  GITHUB_TOKEN          GitHub PAT for authenticated repo access (optional)

Credentials (only when --upload):
  ARTIFACTORY_USER      Affirm email for Artifactory
  ARTIFACTORY_TOKEN     JFrog API token
EOF
    exit 1
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --spark-version)   SPARK_VERSION="$2"; shift 2 ;;
        --affirm-version)  AFFIRM_VERSION="$2"; shift 2 ;;
        --apply-commit)    APPLY_COMMIT="$2"; shift 2 ;;
        --output-dir)      OUTPUT_DIR="$2"; shift 2 ;;
        --upload)          UPLOAD="true"; shift ;;
        -h|--help)         usage ;;
        *) echo "Unknown option: $1"; usage ;;
    esac
done

if [[ -z "${SPARK_VERSION:-}" || -z "${AFFIRM_VERSION:-}" || -z "${APPLY_COMMIT:-}" ]]; then
    echo "ERROR: --spark-version, --affirm-version, and --apply-commit are all required"
    usage
fi

VERSION_STRING="2815!${SPARK_VERSION}+affirm.${AFFIRM_VERSION}"
REPO_URL="${REPO_URL:-https://affirmprod.jfrog.io/artifactory/api/pypi/pypi-local}"
VERSIONS_FILE="/build/VERSIONS.md"

# --- AFFIRM_VERSION and Commit SHA validation ---
# Productive versions are numeric-only (e.g. 1, 2, 3). Development versions (e.g. dev1, dev2)
# are exempt from both checks. For productive versions the script enforces:
#   1. A section "## <AFFIRM_VERSION>" must exist in VERSIONS.md.
#   2. That section must include a "Commit SHA: <sha>" line matching the --apply-commit argument.
if [[ "${AFFIRM_VERSION}" =~ ^[0-9]+$ ]]; then
    if [[ ! -f "${VERSIONS_FILE}" ]]; then
        echo "ERROR: Productive AFFIRM_VERSION '${AFFIRM_VERSION}' must be documented in ${VERSIONS_FILE}, but the file does not exist."
        exit 1
    fi

    # Check section exists
    if ! grep -qE "^#{1,3} +(Version +)?${AFFIRM_VERSION}( |$)" "${VERSIONS_FILE}"; then
        echo "ERROR: Productive AFFIRM_VERSION '${AFFIRM_VERSION}' must be documented in ${VERSIONS_FILE}."
        echo "       Add a section '## ${AFFIRM_VERSION}' with a 'Commit SHA: <sha>' line and a description of the changes."
        exit 1
    fi

    # Extract the Commit SHA recorded in the section for this AFFIRM_VERSION.
    # Reads from "## N" (or "## Version N") up to the next heading (## ...) or EOF,
    # then looks for a line matching "Commit SHA: <sha>" (case-insensitive key, any-case value).
    RECORDED_SHA=$(awk "
        /^#+ +(Version +)?${AFFIRM_VERSION}( |\$)/ { in_section=1; next }
        in_section && /^#+ / { exit }
        in_section && /[Cc]ommit [Ss][Hh][Aa]:/ { line=\$0; sub(/.*[Cc]ommit [Ss][Hh][Aa]:[ \t]*/, \"\", line); sub(/^[^a-fA-F0-9]*/, \"\", line); sub(/[^a-fA-F0-9]*\$/, \"\", line); if (line != \"\") print line }
    " "${VERSIONS_FILE}")

    if [[ -z "${RECORDED_SHA}" ]]; then
        echo "ERROR: VERSIONS.md section '## ${AFFIRM_VERSION}' does not contain a 'Commit SHA: <sha>' line."
        echo "       Add a line like '**Commit SHA:** ${APPLY_COMMIT}' inside that section."
        exit 1
    fi

    # Normalize both to lowercase for comparison
    RECORDED_SHA_LOWER=$(echo "${RECORDED_SHA}" | tr '[:upper:]' '[:lower:]')
    APPLY_COMMIT_LOWER=$(echo "${APPLY_COMMIT}" | tr '[:upper:]' '[:lower:]')

    if [[ "${RECORDED_SHA_LOWER}" != "${APPLY_COMMIT_LOWER}" ]]; then
        echo "ERROR: Commit SHA mismatch for AFFIRM_VERSION '${AFFIRM_VERSION}'."
        echo "       VERSIONS.md records: ${RECORDED_SHA}"
        echo "       --apply-commit arg:  ${APPLY_COMMIT}"
        echo "       Update the 'Commit SHA:' line in the '## ${AFFIRM_VERSION}' section, or pass the correct SHA."
        exit 1
    fi

    echo "OK: AFFIRM_VERSION '${AFFIRM_VERSION}' is documented in ${VERSIONS_FILE} with matching commit SHA '${APPLY_COMMIT}'."
else
    echo "Development AFFIRM_VERSION '${AFFIRM_VERSION}' — skipping VERSIONS.md check."
fi

# Set git user.email and user.name
if [[ -z "${GIT_USER_EMAIL:-}" ]]; then
    echo "ERROR: GIT_USER_EMAIL env var is not set"
    exit 1
fi
if [[ -z "${GIT_USER_NAME:-}" ]]; then
    echo "ERROR: GIT_USER_NAME env var is not set"
    exit 1
fi
git config --global user.email "${GIT_USER_EMAIL}"
git config --global user.name "${GIT_USER_NAME}"

# Optional: use GITHUB_TOKEN for authenticated repo access via GIT_ASKPASS
# so the token never appears in URLs, git config, or process listings.
if [[ -n "${GITHUB_TOKEN:-}" ]]; then
    ASKPASS_SCRIPT=$(mktemp)
    chmod +x "$ASKPASS_SCRIPT"
    cat > "$ASKPASS_SCRIPT" <<'ASKPASS'
#!/usr/bin/env bash
if [[ "$1" == *"Username"* ]]; then
    echo "x-access-token"
else
    echo "$GITHUB_TOKEN"
fi
ASKPASS
    export GIT_ASKPASS="$ASKPASS_SCRIPT"
    export GIT_TERMINAL_PROMPT=0
    echo "OK: GITHUB_TOKEN detected — GIT_ASKPASS configured for authenticated GitHub access."
else
    echo "NOTICE: GITHUB_TOKEN not set."
fi

# --- Phase 0: Environment Validation ---
# Verify Python version is 3.9.x or 3.12.x (supported by affirm-base images)
PYTHON_VERSION=$(python3 --version 2>&1)
echo "Python version: ${PYTHON_VERSION}"
if [[ ! "$PYTHON_VERSION" == *"3.9"* && ! "$PYTHON_VERSION" == *"3.12"* ]]; then
    echo "ERROR: Expected Python 3.9.x or 3.12.x but found ${PYTHON_VERSION}"
    exit 1
fi
echo "OK: Supported Python version detected."

# Verify Java version
JAVA_VERSION_OUTPUT=$(java -version 2>&1 | head -1)
echo "Java version: ${JAVA_VERSION_OUTPUT}"

# --- Phase 1: Clone & Apply Affirm Changes ---
echo "=== Phase 1: Clone & Apply Affirm Changes ==="
if [[ -d /build/affirm-spark ]]; then
    echo "Removing existing /build/affirm-spark"
    rm -rf /build/affirm-spark
fi
# Clone the Affirm Spark fork
git clone https://github.com/Affirm/spark.git /build/affirm-spark
cd /build/affirm-spark

# Base new branch on a clean copy of apache/spark's tag (e.g. v4.0.0)
echo "Fetching tag v${SPARK_VERSION} from upstream (apache/spark)..."
git remote add upstream https://github.com/apache/spark.git 2>/dev/null || true
git fetch --quiet upstream tag "v${SPARK_VERSION}"

# Checkout the upstream release tag
git checkout -b "affirm-${SPARK_VERSION}-${AFFIRM_VERSION}" "v${SPARK_VERSION}"

# Apply the Affirm commit from Affirm/spark via git diff | git apply
echo "=== Applying changes from commit ${APPLY_COMMIT} via git diff (Affirm/spark) ==="
git fetch origin "${APPLY_COMMIT}"
if ! git diff "${APPLY_COMMIT}~1" "${APPLY_COMMIT}" | git apply --verbose; then
    echo "ERROR: Failed to apply commit ${APPLY_COMMIT}"
    exit 1
fi
echo "Applied commit ${APPLY_COMMIT}"

# Set PySpark version in version.py
VERSION_PY="/build/affirm-spark/python/pyspark/version.py"
if [[ ! -f "$VERSION_PY" ]]; then
    echo "ERROR: ${VERSION_PY} not found"
    exit 1
fi
sed -i "s/^__version__.*/__version__: str = \"${VERSION_STRING}\"/" "$VERSION_PY"
echo "Set PySpark version to: ${VERSION_STRING}"

# Version string validation
if grep -q "${VERSION_STRING}" "$VERSION_PY"; then
    echo "Version string verified in ${VERSION_PY}"
else
    echo "ERROR: Version string was not set correctly in ${VERSION_PY}"
    exit 1
fi

# --- Phase 2: Compile ---
echo "=== Phase 2: Compile ==="
echo "=== Compiling Spark (this takes 20-40 min) ==="
cd /build/affirm-spark
if ! ./build/mvn -DskipTests -Pkubernetes clean package -T 1C; then
    echo "ERROR: Maven compilation failed"
    exit 2
fi

# --- Phase 3: Package 3 wheels ---
echo "=== Phase 3: Package 3 Wheels ==="
mkdir -p "${OUTPUT_DIR}"

for subpkg in classic client connect; do
    echo "=== Building wheel: ${subpkg} ==="
    cd /build/affirm-spark/python/packaging/${subpkg}
    python3 setup.py bdist_wheel --dist-dir "${OUTPUT_DIR}" || { echo "Wheel build failed: ${subpkg}"; exit 3; }
done

echo "=== Built wheels ==="
ls -lh "${OUTPUT_DIR}"/*.whl

# Wheel count validation (expect exactly 3)
WHL_COUNT=$(find "${OUTPUT_DIR}" -maxdepth 1 -name '*.whl' | wc -l)
if [[ "$WHL_COUNT" -ne 3 ]]; then
    echo "ERROR: Expected 3 wheels, found ${WHL_COUNT}"
    exit 3
fi

# --- Phase 4: Upload (only with --upload) ---
if [[ "$UPLOAD" == "true" ]]; then
    if [[ -z "${ARTIFACTORY_USER:-}" || -z "${ARTIFACTORY_TOKEN:-}" ]]; then
        echo "ERROR: --upload requires ARTIFACTORY_USER and ARTIFACTORY_TOKEN env vars"
        exit 4
    fi
    echo "=== Uploading wheels to Artifactory ==="
    for whl in "${OUTPUT_DIR}"/*.whl; do
        echo "Uploading: $(basename "$whl")"
        if ! twine upload \
            --repository-url "${REPO_URL}" \
            --username "${ARTIFACTORY_USER}" \
            --password "${ARTIFACTORY_TOKEN}" \
            "$whl"; then
            echo "ERROR: Upload failed for $(basename "$whl")"
            exit 4
        fi
    done
    echo "=== Upload complete ==="
else
    echo "=== DRY RUN: Wheels built but NOT uploaded ==="
    echo "To upload, re-run with --upload"
fi

exit 0
