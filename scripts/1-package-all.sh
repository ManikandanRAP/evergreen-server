#!/usr/bin/env bash

set -euo pipefail

# Creates a single migration zip for Windows VM handoff.
# Output includes:
# - 2-transfer-prod.sh
# - 2-transfer-dev.sh
# - 3-deploy-prod.sh
# - 3-deploy-dev.sh
# - one app payload tar.gz (client + server)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
BUILD_DIR="${SCRIPT_DIR}/build"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"

APP_ARCHIVE_NAME="evergreen-app-${TIMESTAMP}.tar.gz"
APP_ARCHIVE_PATH="${BUILD_DIR}/${APP_ARCHIVE_NAME}"

PACKAGE_DIR_NAME="evergreen-migration-package-${TIMESTAMP}"
PACKAGE_DIR="${BUILD_DIR}/${PACKAGE_DIR_NAME}"
ZIP_PATH="${BUILD_DIR}/${PACKAGE_DIR_NAME}.zip"
LATEST_ZIP_PATH="${BUILD_DIR}/evergreen-migration-package.zip"

BLUE='\033[0;34m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

mkdir -p "${BUILD_DIR}"

echo -e "${BLUE}======================================================${NC}"
echo -e "${BLUE}   Step 1/3 - Package All (Run on local laptop)${NC}"
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

cp "${SCRIPT_DIR}/2-transfer-prod.sh" "${PACKAGE_DIR}/2-transfer-prod.sh"
cp "${SCRIPT_DIR}/2-transfer-dev.sh" "${PACKAGE_DIR}/2-transfer-dev.sh"
cp "${SCRIPT_DIR}/3-deploy-prod.sh" "${PACKAGE_DIR}/3-deploy-prod.sh"
cp "${SCRIPT_DIR}/3-deploy-dev.sh" "${PACKAGE_DIR}/3-deploy-dev.sh"
cp "${APP_ARCHIVE_PATH}" "${PACKAGE_DIR}/${APP_ARCHIVE_NAME}"

cat > "${PACKAGE_DIR}/README-NEXT-STEPS.txt" <<EOF
Single package prepared for VM -> Linux deployment.

1) Copy this folder content to Windows VM and run one or both:
   SERVER_HOST=<linux-ip> ./2-transfer-prod.sh
   SERVER_HOST=<linux-ip> ./2-transfer-dev.sh

2) On Linux server, run:
   chmod +x /tmp/evergreen-migrate-prod/3-deploy-prod.sh
   /tmp/evergreen-migrate-prod/3-deploy-prod.sh

   chmod +x /tmp/evergreen-migrate-dev/3-deploy-dev.sh
   /tmp/evergreen-migrate-dev/3-deploy-dev.sh

Note: Place SQL dumps in VM/Linux path expected by scripts
defaults:
  /tmp/evergreen_prod.sql
  /tmp/evergreen_dev.sql
EOF

echo -e "${YELLOW}Creating single zip:${NC} ${ZIP_PATH}"
(
  cd "${BUILD_DIR}"
  if command -v zip >/dev/null 2>&1; then
    zip -rq "${ZIP_PATH}" "${PACKAGE_DIR_NAME}"
  else
    python - <<'PY'
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

echo -e "${GREEN}Single migration zip created.${NC}"
echo "Zip     : ${ZIP_PATH}"
echo "Latest  : ${LATEST_ZIP_PATH}"
echo ""
echo -e "${YELLOW}Next step (Windows VM):${NC}"
echo "Extract: ${LATEST_ZIP_PATH}"
echo "Run    : ./2-transfer-prod.sh  and/or  ./2-transfer-dev.sh"
