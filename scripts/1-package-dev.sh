#!/usr/bin/env bash

set -euo pipefail

# Creates a deployable dev bundle from local source.
# Run from anywhere on your laptop:
#   ./1-package-dev.sh

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
BUILD_DIR="${SCRIPT_DIR}/build"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
APP_ARCHIVE_NAME="evergreen-dev-app-${TIMESTAMP}.tar.gz"
PACKAGE_DIR_NAME="evergreen-dev-package-${TIMESTAMP}"
ARCHIVE_NAME="${PACKAGE_DIR_NAME}.tar.gz"
APP_ARCHIVE_PATH="${BUILD_DIR}/${APP_ARCHIVE_NAME}"
PACKAGE_DIR="${BUILD_DIR}/${PACKAGE_DIR_NAME}"
ARCHIVE_PATH="${BUILD_DIR}/${ARCHIVE_NAME}"
CHECKSUM_PATH="${ARCHIVE_PATH}.sha256"
LATEST_PACKAGE_PATH="${BUILD_DIR}/evergreen-dev-package.tar.gz"
ZIP_PATH="${BUILD_DIR}/${PACKAGE_DIR_NAME}.zip"
LATEST_ZIP_PATH="${BUILD_DIR}/evergreen-dev-package.zip"

mkdir -p "${BUILD_DIR}"

BLUE='\033[0;34m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${BLUE}======================================================${NC}"
echo -e "${BLUE}   Step 1/3 - DEV Package (Run on local laptop)${NC}"
echo -e "${BLUE}======================================================${NC}"
echo -e "${YELLOW}Creating app payload:${NC} ${APP_ARCHIVE_PATH}"

tar \
  --exclude="evergreen-client/.git" \
  --exclude="evergreen-client/.next" \
  --exclude="evergreen-client/node_modules" \
  --exclude="evergreen-client/.idea" \
  --exclude="evergreen-client/.cursor" \
  --exclude="evergreen-client/.vscode" \
  --exclude="evergreen-server/.git" \
  --exclude="evergreen-server/__pycache__" \
  --exclude="evergreen-server/.pytest_cache" \
  --exclude="evergreen-server/.venv" \
  --exclude="evergreen-server/venv" \
  --exclude="evergreen-server/scripts/build" \
  -czf "${APP_ARCHIVE_PATH}" \
  -C "${REPO_ROOT}" \
  evergreen-client evergreen-server

rm -rf "${PACKAGE_DIR}"
mkdir -p "${PACKAGE_DIR}"

# Keep transfer/deploy scripts at package root for VM/Linux handoff.
cp "${SCRIPT_DIR}/2-transfer-dev.sh" "${PACKAGE_DIR}/2-transfer-dev.sh"
cp "${SCRIPT_DIR}/3-deploy-dev.sh" "${PACKAGE_DIR}/3-deploy-dev.sh"
cp "${APP_ARCHIVE_PATH}" "${PACKAGE_DIR}/${APP_ARCHIVE_NAME}"

echo -e "${YELLOW}Creating distributable package:${NC} ${ARCHIVE_PATH}"
tar -czf "${ARCHIVE_PATH}" -C "${BUILD_DIR}" "${PACKAGE_DIR_NAME}"
cp "${ARCHIVE_PATH}" "${LATEST_PACKAGE_PATH}"

echo -e "${YELLOW}Creating single dev zip:${NC} ${ZIP_PATH}"
(
  cd "${BUILD_DIR}"
  if command -v zip >/dev/null 2>&1; then
    zip -rq "${ZIP_PATH}" "${PACKAGE_DIR_NAME}"
  else
    BUILD_DIR="${BUILD_DIR}" PACKAGE_DIR_NAME="${PACKAGE_DIR_NAME}" ZIP_PATH="${ZIP_PATH}" python - <<'PY'
import os, zipfile
build_dir = os.environ["BUILD_DIR"]
pkg = os.environ["PACKAGE_DIR_NAME"]
zip_path = os.environ["ZIP_PATH"]
root = os.path.join(build_dir, pkg)
with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
    for base, _, files in os.walk(root):
        for f in files:
            p = os.path.join(base, f)
            zf.write(p, os.path.relpath(p, build_dir))
PY
  fi
)
cp "${ZIP_PATH}" "${LATEST_ZIP_PATH}"

if command -v sha256sum >/dev/null 2>&1; then
  sha256sum "${ARCHIVE_PATH}" > "${CHECKSUM_PATH}"
elif command -v shasum >/dev/null 2>&1; then
  shasum -a 256 "${ARCHIVE_PATH}" > "${CHECKSUM_PATH}"
else
  echo "[dev-package] WARNING: no sha256 utility found; checksum file not created"
fi

echo -e "${GREEN}Package created successfully.${NC}"
echo "Archive : ${ARCHIVE_PATH}"
echo "Latest  : ${LATEST_PACKAGE_PATH}"
echo "Zip     : ${ZIP_PATH}"
echo "ZipLatest: ${LATEST_ZIP_PATH}"
if [[ -f "${CHECKSUM_PATH}" ]]; then
  echo "Checksum: ${CHECKSUM_PATH}"
fi
echo ""
echo "Important: Use the single zip for VM handoff."
echo "  Correct file: ${LATEST_ZIP_PATH}"
echo ""
echo "When extracted in Windows VM, run:"
echo "  ./2-transfer-dev.sh"
echo ""
echo -e "${YELLOW}Next step (run from Windows VM):${NC}"
echo "./2-transfer-dev.sh"
