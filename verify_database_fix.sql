-- Verification queries to check if the database fix worked
-- Run these after the fix to confirm everything is working

-- 1. Check total records and effective payments
SELECT 
    'ledger_partnerpayouts' as table_name,
    COUNT(*) as total_records,
    SUM(effective_billed_amount_paid) as total_effective_paid,
    COUNT(CASE WHEN effective_billed_amount_paid > 0 THEN 1 END) as records_with_payments
FROM ledger_partnerpayouts;

-- 2. Check revenue_ledger for comparison
SELECT 
    'revenue_ledger' as table_name,
    COUNT(*) as total_records,
    SUM(partner_compensation) as total_partner_compensation
FROM revenue_ledger;

-- 3. Sample records with actual payments
SELECT 
    docnumber,
    bill_date,
    partner_name,
    bill_amount,
    effective_billed_amount_paid,
    payment_id,
    date_of_payment
FROM ledger_partnerpayouts 
WHERE effective_billed_amount_paid > 0
ORDER BY effective_billed_amount_paid DESC
LIMIT 10;

-- 4. Check for any remaining 0 values (should be minimal)
SELECT 
    COUNT(*) as zero_payment_records,
    COUNT(CASE WHEN payment_id IS NOT NULL AND payment_id != '' THEN 1 END) as records_with_payment_id_but_zero_amount
FROM ledger_partnerpayouts 
WHERE effective_billed_amount_paid = 0;

-- 5. Verify view definition is correct
SHOW CREATE TABLE ledger_partnerpayouts;

-- 6. Check if there are any NULL values that should have amounts
SELECT 
    COUNT(*) as null_payment_records
FROM ledger_partnerpayouts 
WHERE effective_billed_amount_paid IS NULL;

-- 7. Summary statistics
SELECT 
    COUNT(*) as total_records,
    MIN(effective_billed_amount_paid) as min_payment,
    MAX(effective_billed_amount_paid) as max_payment,
    AVG(effective_billed_amount_paid) as avg_payment,
    SUM(effective_billed_amount_paid) as total_payments
FROM ledger_partnerpayouts;
