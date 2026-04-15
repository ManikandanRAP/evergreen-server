#!/usr/bin/env bash

set -euo pipefail

# Runs on Linux target server.
# Deploys prod stack (frontend + backend). DB restore is separate.
#
# Example:
#   ./3-deploy-prod.sh

SERVER_APP_DIR="${SERVER_APP_DIR:-/opt/evergreen}"
SOURCE_DIR="${SOURCE_DIR:-/tmp/evergreen-migrate-prod}"
PROD_ARCHIVE_PATH="${PROD_ARCHIVE_PATH:-}"
SERVER_COMPOSE_FILE="${SERVER_COMPOSE_FILE:-docker-compose-live.yml}"
SERVER_DB_SERVICE="${SERVER_DB_SERVICE:-db}"
SERVER_BACKEND_SERVICE="${SERVER_BACKEND_SERVICE:-backend}"
CLIENT_COMPOSE_FILE="${CLIENT_COMPOSE_FILE:-docker-compose-prod.yml}"
CLIENT_SERVICE="${CLIENT_SERVICE:-evergreen_client}"

BLUE='\033[0;34m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${BLUE}======================================================${NC}"
echo -e "${BLUE}   Step 3/3 - PROD Deploy (Run on Linux server)${NC}"
echo -e "${BLUE}======================================================${NC}"

mkdir -p "${SERVER_APP_DIR}"

if [[ -z "${PROD_ARCHIVE_PATH}" ]]; then
  PROD_ARCHIVE_PATH="$(ls -1t "${SOURCE_DIR}"/evergreen-prod-app-*.tar.gz 2>/dev/null | head -n 1 || true)"
fi

if [[ -z "${PROD_ARCHIVE_PATH}" || ! -f "${PROD_ARCHIVE_PATH}" ]]; then
  echo "ERROR: Prod archive not found in ${SOURCE_DIR}"
  exit 1
fi

echo "[prod-deploy] Extracting ${PROD_ARCHIVE_PATH} to /opt/evergreen"
tar -xzf "${PROD_ARCHIVE_PATH}" -C "$(dirname "${SERVER_APP_DIR}")"

if [[ -d "${SERVER_APP_DIR}/evergreen-server" && -d "${SERVER_APP_DIR}/evergreen-client" ]]; then
  SERVER_ROOT="${SERVER_APP_DIR}"
elif [[ -d "$(dirname "${SERVER_APP_DIR}")/evergreen-server" && -d "$(dirname "${SERVER_APP_DIR}")/evergreen-client" ]]; then
  SERVER_ROOT="$(dirname "${SERVER_APP_DIR}")"
else
  echo "ERROR: Could not locate extracted evergreen-server/evergreen-client directories."
  echo "Checked:"
  echo "  ${SERVER_APP_DIR}"
  echo "  $(dirname "${SERVER_APP_DIR}")"
  exit 1
fi

cd "${SERVER_ROOT}/evergreen-server"

if [[ ! -f "${SERVER_COMPOSE_FILE}" ]]; then
  echo "ERROR: Server compose file not found: ${SERVER_COMPOSE_FILE}"
  exit 1
fi

echo "[prod-deploy] Starting DB + backend services"
docker compose -f "${SERVER_COMPOSE_FILE}" up -d --build "${SERVER_DB_SERVICE}" "${SERVER_BACKEND_SERVICE}"

cd "${SERVER_ROOT}/evergreen-client"

if [[ ! -f "${CLIENT_COMPOSE_FILE}" ]]; then
  echo "ERROR: Client compose file not found: ${CLIENT_COMPOSE_FILE}"
  exit 1
fi

echo "[prod-deploy] Starting frontend service"
docker compose -f "${CLIENT_COMPOSE_FILE}" up -d --build "${CLIENT_SERVICE}"

echo "[prod-deploy] Completed successfully"
echo -e "${GREEN}PROD deployment completed.${NC}"
echo ""
echo -e "${YELLOW}Next step:${NC}"
echo "Verify backend logs: docker logs evergreen-server-backend --tail 50"
echo "Verify frontend logs: docker logs evergreen-client-container --tail 50"
echo "Run DB import separately when ready."
