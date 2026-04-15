#!/usr/bin/env bash

set -euo pipefail

# Runs on Linux target server.
# Deploys dev stack (frontend + backend). DB restore is separate.
#
# Example:
#   ./3-deploy-dev.sh

SERVER_APP_DIR="${SERVER_APP_DIR:-/opt/evergreen}"
SOURCE_DIR="${SOURCE_DIR:-/tmp/evergreen-migrate-dev}"
DEV_ARCHIVE_PATH="${DEV_ARCHIVE_PATH:-}"
SERVER_COMPOSE_FILE="${SERVER_COMPOSE_FILE:-docker-compose-dev.yml}"
SERVER_BACKEND_SERVICE="${SERVER_BACKEND_SERVICE:-backend-dev}"
CLIENT_COMPOSE_FILE="${CLIENT_COMPOSE_FILE:-docker-compose-dev.yml}"
CLIENT_SERVICE="${CLIENT_SERVICE:-client-dev}"
DEV_NETWORK="${DEV_NETWORK:-evergreen_dev_net}"
DEV_DB_CONTAINER="${DEV_DB_CONTAINER:-evergreen-mysql-dev}"
DEV_DB_PORT="${DEV_DB_PORT:-3307}"
DEV_DB_VOLUME="${DEV_DB_VOLUME:-/opt/evergreen/db_data}"
DEV_DB_ROOT_PASSWORD="${DEV_DB_ROOT_PASSWORD:-rootpassword}"
DEV_DB_NAME="${DEV_DB_NAME:-evergreen_dev}"

BLUE='\033[0;34m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${BLUE}======================================================${NC}"
echo -e "${BLUE}   Step 3/3 - DEV Deploy (Run on Linux server)${NC}"
echo -e "${BLUE}======================================================${NC}"

mkdir -p "${SERVER_APP_DIR}"

if [[ -z "${DEV_ARCHIVE_PATH}" ]]; then
  DEV_ARCHIVE_PATH="$(ls -1t "${SOURCE_DIR}"/evergreen-dev-app-*.tar.gz 2>/dev/null | head -n 1 || true)"
fi

if [[ -z "${DEV_ARCHIVE_PATH}" || ! -f "${DEV_ARCHIVE_PATH}" ]]; then
  echo "ERROR: Dev archive not found in ${SOURCE_DIR}"
  exit 1
fi

echo "[dev-deploy] Extracting ${DEV_ARCHIVE_PATH} to /opt/evergreen"
tar -xzf "${DEV_ARCHIVE_PATH}" -C "$(dirname "${SERVER_APP_DIR}")"

if ! docker network ls | grep -q "${DEV_NETWORK}"; then
  echo "[dev-deploy] Creating Docker network ${DEV_NETWORK}"
  docker network create "${DEV_NETWORK}"
fi

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

if ! docker ps --format '{{.Names}}' | grep -q "^${DEV_DB_CONTAINER}$"; then
  if docker ps -a --format '{{.Names}}' | grep -q "^${DEV_DB_CONTAINER}$"; then
    echo "[dev-deploy] Starting existing DB container ${DEV_DB_CONTAINER}"
    docker start "${DEV_DB_CONTAINER}"
  else
    echo "[dev-deploy] Creating DB container ${DEV_DB_CONTAINER}"
    mkdir -p "${DEV_DB_VOLUME}"
    docker run -d \
      --name "${DEV_DB_CONTAINER}" \
      --network "${DEV_NETWORK}" \
      -e MYSQL_ROOT_PASSWORD="${DEV_DB_ROOT_PASSWORD}" \
      -e MYSQL_DATABASE="${DEV_DB_NAME}" \
      -p "${DEV_DB_PORT}:3306" \
      -v "${DEV_DB_VOLUME}:/var/lib/mysql" \
      --restart unless-stopped \
      mysql:8.0 \
      --bind-address=0.0.0.0 --default-authentication-plugin=mysql_native_password
  fi
fi

if [[ ! -f ".env.dev" ]]; then
  echo "[dev-deploy] Creating default .env.dev"
  cat > .env.dev <<EOF
DB_HOST=${DEV_DB_CONTAINER}
DB_USER=root
DB_PASSWORD=${DEV_DB_ROOT_PASSWORD}
DB_NAME=${DEV_DB_NAME}
DB_PORT=3306
SECRET_KEY=dev_secret_key_change_in_production
ENVIRONMENT=development
DEBUG=true
MYSQL_CONTAINER_NAME=${DEV_DB_CONTAINER}
EOF
fi

if [[ ! -f "${SERVER_COMPOSE_FILE}" ]]; then
  echo "ERROR: Server compose file not found: ${SERVER_COMPOSE_FILE}"
  exit 1
fi

echo "[dev-deploy] Starting backend service"
docker compose -f "${SERVER_COMPOSE_FILE}" up -d --build "${SERVER_BACKEND_SERVICE}"

cd "${SERVER_ROOT}/evergreen-client"

if [[ ! -f "${CLIENT_COMPOSE_FILE}" ]]; then
  echo "ERROR: Client compose file not found: ${CLIENT_COMPOSE_FILE}"
  exit 1
fi

echo "[dev-deploy] Starting frontend service"
docker compose -f "${CLIENT_COMPOSE_FILE}" up -d --build "${CLIENT_SERVICE}"

echo "[dev-deploy] Completed successfully"
echo -e "${GREEN}DEV deployment completed.${NC}"
echo ""
echo -e "${YELLOW}Next step:${NC}"
echo "Verify backend logs: docker logs evergreen-server-backend-dev --tail 50"
echo "Verify frontend logs: docker logs evergreen-client-container-dev --tail 50"
echo "Run DB import separately when ready."
