#!/bin/bash

#######################################################
# Evergreen Dev Deployment Script
# 
# Usage: ./deploy-dev.sh [frontend|backend|all]
#######################################################

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Directories
CLIENT_DIR="/opt/evergreen-dev/evergreen-client"
SERVER_DIR="/opt/evergreen-dev/evergreen-server"

# What to deploy
DEPLOY_TARGET="${1:-all}"

echo -e "${BLUE}======================================================${NC}"
echo -e "${BLUE}     Evergreen Dev Deployment Script${NC}"
echo -e "${BLUE}======================================================${NC}"
echo ""

deploy_frontend() {
    echo -e "${YELLOW}Deploying Frontend...${NC}"
    cd ${CLIENT_DIR}
    
    # Pull latest code (if using git)
    if [ -d ".git" ]; then
        echo "Pulling latest code..."
        git pull
    fi
    
    # Stop and remove old container
    echo "Stopping old container..."
    docker stop evergreen-client-container-dev 2>/dev/null || true
    docker rm evergreen-client-container-dev 2>/dev/null || true
    
    # Rebuild with no cache (important for NEXT_PUBLIC_* env vars)
    echo "Rebuilding frontend (this may take a few minutes)..."
    docker compose -f docker-compose-dev.yml build --no-cache
    
    # Start new container
    echo "Starting new container..."
    docker compose -f docker-compose-dev.yml up -d
    
    echo -e "${GREEN}Frontend deployed successfully!${NC}"
}

deploy_backend() {
    echo -e "${YELLOW}Deploying Backend...${NC}"
    cd ${SERVER_DIR}
    
    # Pull latest code (if using git)
    if [ -d ".git" ]; then
        echo "Pulling latest code..."
        git pull
    fi
    
    # Stop and remove old container
    echo "Stopping old container..."
    docker stop evergreen-server-backend-dev 2>/dev/null || true
    docker rm evergreen-server-backend-dev 2>/dev/null || true
    
    # Rebuild
    echo "Rebuilding backend..."
    docker compose -f docker-compose-dev.yml build
    
    # Start new container
    echo "Starting new container..."
    docker compose -f docker-compose-dev.yml up -d
    
    # Wait for startup
    sleep 3
    
    # Verify
    echo "Checking backend logs..."
    docker logs evergreen-server-backend-dev --tail 10
    
    echo -e "${GREEN}Backend deployed successfully!${NC}"
}

case $DEPLOY_TARGET in
    frontend)
        deploy_frontend
        ;;
    backend)
        deploy_backend
        ;;
    all)
        deploy_frontend
        echo ""
        deploy_backend
        ;;
    *)
        echo -e "${RED}Usage: $0 [frontend|backend|all]${NC}"
        echo ""
        echo "Examples:"
        echo "  $0 frontend  - Deploy only frontend"
        echo "  $0 backend   - Deploy only backend"
        echo "  $0 all       - Deploy both (default)"
        exit 1
        ;;
esac

echo ""
echo -e "${BLUE}======================================================${NC}"
echo -e "${GREEN}Deployment complete!${NC}"
echo -e "${BLUE}======================================================${NC}"
echo ""
echo "URLs:"
echo "  Frontend: https://myco-dev.evergreenpodcasts.com"
echo "  API Docs: https://myco-dev.evergreenpodcasts.com/api/docs"
echo ""
echo "To check logs:"
echo "  Frontend: docker logs evergreen-client-container-dev -f"
echo "  Backend:  docker logs evergreen-server-backend-dev -f"

