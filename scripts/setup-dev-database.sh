#!/bin/bash

#######################################################
# Evergreen Dev Database Setup Script
# 
# This script exports the production database and 
# imports it into a new dev database.
#######################################################

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration - Update these values as needed
PROD_BACKEND_CONTAINER="evergreen-server-backend"
DEV_BACKEND_CONTAINER="evergreen-server-backend-dev"
DEV_SERVER_DIR="/opt/evergreen-dev/evergreen-server"
DUMP_FILE="/tmp/evergreen_prod_dump_$(date +%Y%m%d_%H%M%S).sql"

echo -e "${BLUE}======================================================${NC}"
echo -e "${BLUE}     Evergreen Dev Database Setup Script${NC}"
echo -e "${BLUE}======================================================${NC}"
echo ""

#######################################################
# Step 1: Get production database details
#######################################################
echo -e "${YELLOW}Step 1: Getting production database details...${NC}"

# Try to get DB details from production container
if docker ps --format '{{.Names}}' | grep -q "^${PROD_BACKEND_CONTAINER}$"; then
    PROD_DB_HOST=$(docker exec ${PROD_BACKEND_CONTAINER} printenv DB_HOST 2>/dev/null || echo "")
    PROD_DB_USER=$(docker exec ${PROD_BACKEND_CONTAINER} printenv DB_USER 2>/dev/null || echo "")
    PROD_DB_PASSWORD=$(docker exec ${PROD_BACKEND_CONTAINER} printenv DB_PASSWORD 2>/dev/null || echo "")
    PROD_DB_NAME=$(docker exec ${PROD_BACKEND_CONTAINER} printenv DB_NAME 2>/dev/null || echo "")
    PROD_DB_PORT=$(docker exec ${PROD_BACKEND_CONTAINER} printenv DB_PORT 2>/dev/null || echo "3306")
    
    echo -e "${GREEN}Found production DB settings:${NC}"
    echo "  DB_HOST: ${PROD_DB_HOST}"
    echo "  DB_USER: ${PROD_DB_USER}"
    echo "  DB_NAME: ${PROD_DB_NAME}"
    echo "  DB_PORT: ${PROD_DB_PORT}"
else
    echo -e "${RED}Production backend container not found. Please enter details manually:${NC}"
    read -p "DB_HOST: " PROD_DB_HOST
    read -p "DB_USER: " PROD_DB_USER
    read -s -p "DB_PASSWORD: " PROD_DB_PASSWORD
    echo ""
    read -p "DB_NAME: " PROD_DB_NAME
    read -p "DB_PORT [3306]: " PROD_DB_PORT
    PROD_DB_PORT=${PROD_DB_PORT:-3306}
fi

# Validate we have required values
if [ -z "$PROD_DB_HOST" ] || [ -z "$PROD_DB_USER" ] || [ -z "$PROD_DB_PASSWORD" ] || [ -z "$PROD_DB_NAME" ]; then
    echo -e "${RED}Error: Missing required database configuration.${NC}"
    echo "Please enter the production database details manually:"
    read -p "DB_HOST: " PROD_DB_HOST
    read -p "DB_USER: " PROD_DB_USER
    read -s -p "DB_PASSWORD: " PROD_DB_PASSWORD
    echo ""
    read -p "DB_NAME: " PROD_DB_NAME
    read -p "DB_PORT [3306]: " PROD_DB_PORT
    PROD_DB_PORT=${PROD_DB_PORT:-3306}
fi

echo ""

#######################################################
# Step 2: Configure dev database settings
#######################################################
echo -e "${YELLOW}Step 2: Configuring dev database settings...${NC}"

DEV_DB_NAME="evergreen_dev"
DEV_DB_USER="${PROD_DB_USER}"  # Use same user or create new one
DEV_DB_PASSWORD="${PROD_DB_PASSWORD}"  # Use same password or set new one
DEV_DB_HOST="${PROD_DB_HOST}"
DEV_DB_PORT="${PROD_DB_PORT}"

echo "Dev database will be named: ${DEV_DB_NAME}"
read -p "Use different dev database name? [${DEV_DB_NAME}]: " INPUT_DEV_DB_NAME
DEV_DB_NAME=${INPUT_DEV_DB_NAME:-$DEV_DB_NAME}

echo ""

#######################################################
# Step 3: Export production database
#######################################################
echo -e "${YELLOW}Step 3: Exporting production database...${NC}"

# Check if we can connect to MySQL directly or need to use a container
MYSQL_CMD=""

# Try to find MySQL container
MYSQL_CONTAINER=$(docker ps --format '{{.Names}}' | grep -E 'mysql|mariadb' | head -1 || echo "")

if [ -n "$MYSQL_CONTAINER" ]; then
    echo "Found MySQL container: ${MYSQL_CONTAINER}"
    MYSQL_CMD="docker exec ${MYSQL_CONTAINER}"
    
    # When running mysqldump from inside the MySQL container, use localhost instead of Docker network hostname
    MYSQL_HOST_FOR_DUMP="localhost"
    if [ "${PROD_DB_HOST}" = "localhost" ] || [ "${PROD_DB_HOST}" = "127.0.0.1" ]; then
        MYSQL_HOST_FOR_DUMP="${PROD_DB_HOST}"
    fi
    
    # Export using mysqldump
    echo "Exporting database ${PROD_DB_NAME} (connecting to ${MYSQL_HOST_FOR_DUMP} from inside container)..."
    ${MYSQL_CMD} mysqldump -h ${MYSQL_HOST_FOR_DUMP} -u ${PROD_DB_USER} -p${PROD_DB_PASSWORD} --single-transaction --routines --triggers ${PROD_DB_NAME} > ${DUMP_FILE}
elif command -v mysqldump &> /dev/null; then
    echo "Using local mysqldump..."
    mysqldump -h ${PROD_DB_HOST} -P ${PROD_DB_PORT} -u ${PROD_DB_USER} -p${PROD_DB_PASSWORD} --single-transaction --routines --triggers ${PROD_DB_NAME} > ${DUMP_FILE}
else
    echo -e "${RED}Error: Cannot find mysqldump. Please ensure MySQL client is installed or a MySQL container is running.${NC}"
    exit 1
fi

# Check if dump was successful
if [ -f "$DUMP_FILE" ] && [ -s "$DUMP_FILE" ]; then
    DUMP_SIZE=$(du -h ${DUMP_FILE} | cut -f1)
    echo -e "${GREEN}Database exported successfully!${NC}"
    echo "  File: ${DUMP_FILE}"
    echo "  Size: ${DUMP_SIZE}"
else
    echo -e "${RED}Error: Database export failed or file is empty.${NC}"
    exit 1
fi

echo ""

#######################################################
# Step 4: Create dev database and import data
#######################################################
echo -e "${YELLOW}Step 4: Creating dev database and importing data...${NC}"

# Create the dev database
if [ -n "$MYSQL_CONTAINER" ]; then
    # When running from inside the MySQL container, use localhost
    MYSQL_HOST_FOR_IMPORT="localhost"
    
    echo "Creating database ${DEV_DB_NAME}..."
    ${MYSQL_CMD} mysql -h ${MYSQL_HOST_FOR_IMPORT} -u ${PROD_DB_USER} -p${PROD_DB_PASSWORD} -e "DROP DATABASE IF EXISTS ${DEV_DB_NAME}; CREATE DATABASE ${DEV_DB_NAME};"
    
    echo "Importing data into ${DEV_DB_NAME}..."
    # For import, we need to copy the dump file into the container first
    docker cp ${DUMP_FILE} ${MYSQL_CONTAINER}:/tmp/dump.sql
    ${MYSQL_CMD} mysql -h ${MYSQL_HOST_FOR_IMPORT} -u ${PROD_DB_USER} -p${PROD_DB_PASSWORD} ${DEV_DB_NAME} -e "source /tmp/dump.sql"
    # Clean up the dump file from container
    ${MYSQL_CMD} rm -f /tmp/dump.sql
else
    echo "Creating database ${DEV_DB_NAME}..."
    mysql -h ${PROD_DB_HOST} -P ${PROD_DB_PORT} -u ${PROD_DB_USER} -p${PROD_DB_PASSWORD} -e "DROP DATABASE IF EXISTS ${DEV_DB_NAME}; CREATE DATABASE ${DEV_DB_NAME};"
    
    echo "Importing data into ${DEV_DB_NAME}..."
    mysql -h ${PROD_DB_HOST} -P ${PROD_DB_PORT} -u ${PROD_DB_USER} -p${PROD_DB_PASSWORD} ${DEV_DB_NAME} < ${DUMP_FILE}
fi

echo -e "${GREEN}Database imported successfully!${NC}"
echo ""

#######################################################
# Step 5: Create .env.dev file for dev backend
#######################################################
echo -e "${YELLOW}Step 5: Creating .env.dev configuration file...${NC}"

# Get or generate SECRET_KEY
PROD_SECRET_KEY=$(docker exec ${PROD_BACKEND_CONTAINER} printenv SECRET_KEY 2>/dev/null || echo "")
if [ -z "$PROD_SECRET_KEY" ]; then
    DEV_SECRET_KEY=$(openssl rand -hex 32)
    echo "Generated new SECRET_KEY for dev environment"
else
    DEV_SECRET_KEY="${PROD_SECRET_KEY}"
    echo "Using production SECRET_KEY"
fi

# For .env.dev, use the same DB_HOST as production since we're connecting 
# to the same MySQL server, just a different database
# The production DB_HOST (e.g., 'db') is the Docker network hostname

# Create .env.dev file
mkdir -p ${DEV_SERVER_DIR}
cat > ${DEV_SERVER_DIR}/.env.dev << EOF
# Evergreen Dev Environment Configuration
# Generated on $(date)

DB_HOST=${PROD_DB_HOST}
DB_USER=${DEV_DB_USER}
DB_PASSWORD=${DEV_DB_PASSWORD}
DB_NAME=${DEV_DB_NAME}
DB_PORT=${DEV_DB_PORT}
SECRET_KEY=${DEV_SECRET_KEY}
ENVIRONMENT=development
DEBUG=true
EOF

echo -e "${GREEN}.env.dev file created at ${DEV_SERVER_DIR}/.env.dev${NC}"
echo ""

#######################################################
# Step 6: Restart dev backend container
#######################################################
echo -e "${YELLOW}Step 6: Restarting dev backend container...${NC}"

if docker ps --format '{{.Names}}' | grep -q "^${DEV_BACKEND_CONTAINER}$"; then
    docker restart ${DEV_BACKEND_CONTAINER}
    echo "Waiting for container to start..."
    sleep 5
    
    # Check if container is healthy
    if docker ps --format '{{.Names}}' | grep -q "^${DEV_BACKEND_CONTAINER}$"; then
        echo -e "${GREEN}Dev backend container restarted successfully!${NC}"
    else
        echo -e "${RED}Warning: Container may not have started properly.${NC}"
    fi
else
    echo -e "${YELLOW}Dev backend container not running. You may need to start it manually.${NC}"
fi

echo ""

#######################################################
# Step 7: Verify the setup
#######################################################
echo -e "${YELLOW}Step 7: Verifying setup...${NC}"

sleep 3

if docker ps --format '{{.Names}}' | grep -q "^${DEV_BACKEND_CONTAINER}$"; then
    echo "Checking backend logs..."
    docker logs ${DEV_BACKEND_CONTAINER} --tail 10 2>&1
    
    # Check if there are any database errors
    if docker logs ${DEV_BACKEND_CONTAINER} --tail 20 2>&1 | grep -q "Database connection verified successfully"; then
        echo ""
        echo -e "${GREEN}======================================================${NC}"
        echo -e "${GREEN}  SUCCESS! Dev database setup completed!${NC}"
        echo -e "${GREEN}======================================================${NC}"
        echo ""
        echo "Summary:"
        echo "  - Production DB: ${PROD_DB_NAME}"
        echo "  - Dev DB: ${DEV_DB_NAME}"
        echo "  - Dump file: ${DUMP_FILE}"
        echo "  - Config file: ${DEV_SERVER_DIR}/.env.dev"
        echo ""
    else
        echo ""
        echo -e "${YELLOW}Warning: Could not verify database connection.${NC}"
        echo "Please check the logs manually:"
        echo "  docker logs ${DEV_BACKEND_CONTAINER} --tail 50"
    fi
else
    echo -e "${YELLOW}Dev backend container is not running.${NC}"
fi

#######################################################
# Cleanup
#######################################################
echo ""
read -p "Do you want to delete the dump file? [y/N]: " DELETE_DUMP
if [[ "$DELETE_DUMP" =~ ^[Yy]$ ]]; then
    rm -f ${DUMP_FILE}
    echo "Dump file deleted."
else
    echo "Dump file kept at: ${DUMP_FILE}"
fi

echo ""
echo -e "${BLUE}Script completed!${NC}"

