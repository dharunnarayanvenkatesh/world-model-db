#!/usr/bin/env bash
set -Eeuo pipefail

# Run the complete WQMDB SpyTime SF1 workflow in a Google Colab runtime.
# The generated CSV, database, and reports stay under /content by default.

if [[ ! -d /content && "${ALLOW_NON_COLAB:-0}" != "1" ]]; then
  echo "Refusing to run outside a Colab-style /content runtime." >&2
  echo "Set ALLOW_NON_COLAB=1 only if you intentionally want a local run." >&2
  exit 2
fi

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="${WQMDB_REPO_DIR:-$(cd -- "${SCRIPT_DIR}/../.." && pwd)}"
OUTPUT_DIR="${WQMDB_OUTPUT_DIR:-/content/wqmdb-output}"
RECORDS="${WQMDB_RECORDS:-10000}"
REPETITIONS="${WQMDB_REPETITIONS:-10}"
SEED="${WQMDB_SEED:-6003396521826796849}"
ARCHIVE="${WQMDB_ARCHIVE:-/content/wqmdb-spytime-sf1.tar.gz}"

for value_name in RECORDS REPETITIONS SEED; do
  value="${!value_name}"
  if [[ ! "${value}" =~ ^[0-9]+$ || "${value}" == "0" ]]; then
    echo "${value_name} must be a positive integer; got '${value}'." >&2
    exit 2
  fi
done

if [[ ! -f "${REPO_DIR}/Cargo.toml" ]]; then
  echo "WQMDB checkout not found at ${REPO_DIR}." >&2
  exit 2
fi

if ! command -v cargo >/dev/null 2>&1; then
  echo "Rust is not installed; installing the minimal stable toolchain..."
  RUSTUP_INSTALLER="/content/rustup-init.sh"
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs -o "${RUSTUP_INSTALLER}"
  sh "${RUSTUP_INSTALLER}" -y --profile minimal --default-toolchain stable
fi

if [[ -f "${HOME}/.cargo/env" ]]; then
  # shellcheck disable=SC1091
  source "${HOME}/.cargo/env"
fi

mkdir -p "${OUTPUT_DIR}"
rm -f \
  "${OUTPUT_DIR}/spytime-sf1.redb" \
  "${OUTPUT_DIR}/dataset-sf1.csv" \
  "${OUTPUT_DIR}/spytime-wqmdb-sf1.json" \
  "${OUTPUT_DIR}/spytime-wqmdb-sf1.md" \
  "${ARCHIVE}"

cd "${REPO_DIR}"

echo "Validating SpyTime runner..."
cargo test -p wm-bench --bin wm-spytime

echo "Building optimized native binary..."
RUSTFLAGS="${RUSTFLAGS:--C target-cpu=native}" \
  cargo build --release -p wm-bench --bin wm-spytime

{
  echo "run_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "git_revision=$(git rev-parse HEAD)"
  echo "git_status_begin"
  git status --short
  echo "git_status_end"
  rustc --version --verbose
  cargo --version
  uname -a
  if command -v lscpu >/dev/null 2>&1; then
    lscpu
  fi
  if command -v nvidia-smi >/dev/null 2>&1; then
    echo "GPU information (SpyTime does not use the GPU):"
    nvidia-smi --query-gpu=name,memory.total,driver_version --format=csv,noheader
  fi
} > "${OUTPUT_DIR}/environment.txt"

echo "Running SpyTime with ${RECORDS} rows and ${REPETITIONS} repetitions..."
"${REPO_DIR}/target/release/wm-spytime" \
  --records "${RECORDS}" \
  --repetitions "${REPETITIONS}" \
  --seed "${SEED}" \
  --database "${OUTPUT_DIR}/spytime-sf1.redb" \
  --dataset "${OUTPUT_DIR}/dataset-sf1.csv" \
  --json "${OUTPUT_DIR}/spytime-wqmdb-sf1.json" \
  --markdown "${OUTPUT_DIR}/spytime-wqmdb-sf1.md"

echo "Verifying result artifacts..."
test -s "${OUTPUT_DIR}/dataset-sf1.csv"
test -s "${OUTPUT_DIR}/spytime-wqmdb-sf1.json"
test -s "${OUTPUT_DIR}/spytime-wqmdb-sf1.md"
test -s "${OUTPUT_DIR}/environment.txt"

tar -czf "${ARCHIVE}" -C "${OUTPUT_DIR}" \
  dataset-sf1.csv \
  spytime-wqmdb-sf1.json \
  spytime-wqmdb-sf1.md \
  environment.txt

echo
echo "Benchmark complete."
echo "Results: ${OUTPUT_DIR}/spytime-wqmdb-sf1.md"
echo "Dataset: ${OUTPUT_DIR}/dataset-sf1.csv"
echo "Download archive: ${ARCHIVE}"
echo
echo "In a new Colab Python cell, run:"
echo "from google.colab import files"
echo "files.download('${ARCHIVE}')"
