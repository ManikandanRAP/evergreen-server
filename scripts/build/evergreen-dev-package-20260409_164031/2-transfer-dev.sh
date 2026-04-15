#!/usr/bin/env bash

set -e

# DEV transfer script (same style as original scripts/dev/2-transfer.sh)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ============ CONFIGURATION ============
LINUX_HOST="10.10.30.10"
LINUX_USER="rapadmin"
LINUX_PASSWORD="tubDbdjsyzAcdZCo"
LINUX_PORT="22"
REMOTE_TEMP="/tmp/evergreen-migrate-dev"
# =======================================

ARCHIVE_PATH=""

if [ -z "${ARCHIVE_PATH}" ]; then
  ARCHIVE_PATH="$(ls -1t "${SCRIPT_DIR}"/evergreen-dev-app-*.tar.gz 2>/dev/null | head -n 1 || true)"
fi

if [ -z "${ARCHIVE_PATH}" ]; then
  ARCHIVE_PATH="$(ls -1t "${SCRIPT_DIR}"/build/evergreen-dev-app-*.tar.gz 2>/dev/null | head -n 1 || true)"
fi

if [ -z "${ARCHIVE_PATH}" ] || [ ! -f "${ARCHIVE_PATH}" ]; then
  echo "ERROR: Could not find dev app archive. Set ARCHIVE_PATH explicitly."
  exit 1
fi

CHECKSUM_PATH="${ARCHIVE_PATH}.sha256"
REMOTE="${LINUX_USER}@${LINUX_HOST}"

BLUE='\033[0;34m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${BLUE}======================================================${NC}"
echo -e "${BLUE}   Step 2/3 - DEV Transfer (Run on Windows VM)${NC}"
echo -e "${BLUE}======================================================${NC}"
echo "[dev-transfer] SSH user    : ${LINUX_USER}"
echo "[dev-transfer] SSH password: ${LINUX_PASSWORD}"

echo "[dev-transfer] Creating remote temp dir: ${REMOTE_TEMP}"
if command -v sshpass >/dev/null 2>&1; then
  sshpass -p "${LINUX_PASSWORD}" ssh -p "${LINUX_PORT}" "${REMOTE}" "mkdir -p '${REMOTE_TEMP}'"
else
  ssh -p "${LINUX_PORT}" "${REMOTE}" "mkdir -p '${REMOTE_TEMP}'"
fi

echo "[dev-transfer] Copying setup files"
if command -v sshpass >/dev/null 2>&1; then
  sshpass -p "${LINUX_PASSWORD}" scp -P "${LINUX_PORT}" "${ARCHIVE_PATH}" "${REMOTE}:${REMOTE_TEMP}/"
  sshpass -p "${LINUX_PASSWORD}" scp -P "${LINUX_PORT}" "${SCRIPT_DIR}/3-deploy-dev.sh" "${REMOTE}:${REMOTE_TEMP}/3-deploy-dev.sh"
else
  scp -P "${LINUX_PORT}" "${ARCHIVE_PATH}" "${REMOTE}:${REMOTE_TEMP}/"
  scp -P "${LINUX_PORT}" "${SCRIPT_DIR}/3-deploy-dev.sh" "${REMOTE}:${REMOTE_TEMP}/3-deploy-dev.sh"
fi
if [ -f "${CHECKSUM_PATH}" ]; then
  if command -v sshpass >/dev/null 2>&1; then
    sshpass -p "${LINUX_PASSWORD}" scp -P "${LINUX_PORT}" "${CHECKSUM_PATH}" "${REMOTE}:${REMOTE_TEMP}/"
  else
    scp -P "${LINUX_PORT}" "${CHECKSUM_PATH}" "${REMOTE}:${REMOTE_TEMP}/"
  fi
fi

echo "[dev-transfer] Transfer complete"
echo "[dev-transfer] Remote location: ${REMOTE_TEMP}"
echo "[dev-transfer] Normalizing script line endings on Linux server"
if command -v sshpass >/dev/null 2>&1; then
  sshpass -p "${LINUX_PASSWORD}" ssh -p "${LINUX_PORT}" "${REMOTE}" "sed -i 's/\r$//' '${REMOTE_TEMP}/3-deploy-dev.sh' && chmod +x '${REMOTE_TEMP}/3-deploy-dev.sh'"
else
  ssh -p "${LINUX_PORT}" "${REMOTE}" "sed -i 's/\r$//' '${REMOTE_TEMP}/3-deploy-dev.sh' && chmod +x '${REMOTE_TEMP}/3-deploy-dev.sh'"
fi
echo -e "${GREEN}Transfer completed successfully.${NC}"
echo ""
echo -e "${YELLOW}Next step (run on Linux server):${NC}"
echo "chmod +x ${REMOTE_TEMP}/3-deploy-dev.sh"
echo "${REMOTE_TEMP}/3-deploy-dev.sh"
