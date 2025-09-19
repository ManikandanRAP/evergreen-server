#!/bin/bash

# Production Database Fix Script
# This script will properly import the database dump with correct DEFINER handling

set -e  # Exit on any error

echo "=== Production Database Fix ==="
echo "This will fix the incomplete database import in production"
echo ""

# Configuration
DB_CONTAINER="evergreen-mysql"
DB_NAME="evergreen"
DUMP_FILE="/srv/evergreen/backend/eg_dump_9_19.sql"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}Step 1: Backup current database${NC}"
# Create backup of current state
docker exec $DB_CONTAINER mysqldump -uroot -prootpassword $DB_NAME > /tmp/evergreen_backup_$(date +%Y%m%d_%H%M%S).sql
echo -e "${GREEN}✓ Backup created${NC}"

echo -e "${YELLOW}Step 2: Drop and recreate database${NC}"
docker exec -it $DB_CONTAINER mysql -uroot -prootpassword -e \
"DROP DATABASE IF EXISTS $DB_NAME; CREATE DATABASE $DB_NAME CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
echo -e "${GREEN}✓ Database recreated${NC}"

echo -e "${YELLOW}Step 3: Import dump with DEFINER fixes${NC}"
# Import with proper DEFINER handling
sed -E '
  s/CREATE[[:space:]]+DEFINER=`[^`]+`@`[^`]+`[[:space:]]+(TRIGGER|FUNCTION|PROCEDURE|EVENT)/CREATE \1/g;
  s/ALGORITHM=UNDEFINED[[:space:]]+DEFINER=`[^`]+`@`[^`]+`[[:space:]]+/ALGORITHM=UNDEFINED /g;
  s/CREATE[[:space:]]+OR[[:space:]]+REPLACE[[:space:]]+ALGORITHM=UNDEFINED[[:space:]]+DEFINER=`[^`]+`@`[^`]+`[[:space:]]+/CREATE OR REPLACE ALGORITHM=UNDEFINED /g;
  s/[[:space:]]+DEFINER=`[^`]+`@`[^`]+`[[:space:]]+//g;
  s/SQL SECURITY DEFINER/SQL SECURITY INVOKER/g;
  s/USE[[:space:]]+`egp`;/USE `evergreen`;/g;
  s/`egp`/`evergreen`/g
' $DUMP_FILE | docker exec -i $DB_CONTAINER mysql -uroot -prootpassword $DB_NAME

echo -e "${GREEN}✓ Database import completed${NC}"

echo -e "${YELLOW}Step 4: Verify import${NC}"
# Check tables
echo "Tables in database:"
docker exec -it $DB_CONTAINER mysql -uroot -prootpassword -e "USE $DB_NAME; SHOW TABLES;"

# Check views
echo ""
echo "Views in database:"
docker exec -it $DB_CONTAINER mysql -uroot -prootpassword -e "USE $DB_NAME; SHOW FULL TABLES WHERE Table_type='VIEW';"

# Check ledger_partnerpayouts data
echo ""
echo "Checking ledger_partnerpayouts data:"
docker exec -it $DB_CONTAINER mysql -uroot -prootpassword -e "USE $DB_NAME; SELECT COUNT(*) as total_records, SUM(effective_billed_amount_paid) as total_effective_paid FROM ledger_partnerpayouts;"

# Check sample data
echo ""
echo "Sample ledger_partnerpayouts records:"
docker exec -it $DB_CONTAINER mysql -uroot -prootpassword -e "USE $DB_NAME; SELECT docnumber, bill_amount, effective_billed_amount_paid, payment_id FROM ledger_partnerpayouts WHERE effective_billed_amount_paid > 0 LIMIT 5;"

echo ""
echo -e "${GREEN}=== Fix Complete ===${NC}"
echo "The database has been properly imported with all data."
echo "Please test the Revenue Ledger page to verify the fix."
