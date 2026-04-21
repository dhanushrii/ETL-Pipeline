IF OBJECT_ID('final_customer','U') IS NOT NULL DROP TABLE final_customer;
IF OBJECT_ID('error_table','U') IS NOT NULL DROP TABLE error_table;

CREATE TABLE final_customer (
    invoice_no    BIGINT,
    invoice_dt    DATETIME,
    cust_name     NVARCHAR(1000),
    warehouse_loc NVARCHAR(1000),
    qty           BIGINT,
    price_amt     FLOAT,
    partner_code  BIGINT
);

CREATE TABLE error_table (
    invoice_no    NVARCHAR(1000),
    invoice_dt    NVARCHAR(1000),
    cust_name     NVARCHAR(1000),
    warehouse_loc NVARCHAR(1000),
    qty           NVARCHAR(1000),
    price_amt     NVARCHAR(1000),
    partner_code  NVARCHAR(1000),
    error_message NVARCHAR(2000)
);

-- ================= AVERAGES =================

DECLARE @avg_qty FLOAT, @avg_price FLOAT;

SELECT
    @avg_qty   = AVG(TRY_CAST(qty AS BIGINT)),
    @avg_price = AVG(TRY_CAST(price_amt AS FLOAT))
FROM stg_customer;

-- ================= VALIDATION + DEDUP INTO TEMP TABLE =================

IF OBJECT_ID('tempdb..#validated','U') IS NOT NULL DROP TABLE #validated;

SELECT
    s.*,

    TRY_CAST(invoice_no   AS BIGINT)   AS invoice_no_c,
    TRY_CAST(invoice_dt   AS DATETIME) AS invoice_dt_c,
    TRY_CAST(qty          AS BIGINT)   AS qty_c,
    TRY_CAST(price_amt    AS FLOAT)    AS price_amt_c,
    TRY_CAST(partner_code AS BIGINT)   AS partner_code_c,

    ROW_NUMBER() OVER (
        PARTITION BY
            CASE
                WHEN TRY_CAST(invoice_no AS BIGINT) IS NULL THEN invoice_no
                ELSE CAST(TRY_CAST(invoice_no AS BIGINT) AS NVARCHAR(100))
            END
        ORDER BY TRY_CAST(invoice_dt AS DATETIME) DESC
    ) AS rn,

    CONCAT_WS(' | ',

        CASE
            WHEN invoice_no IS NULL OR invoice_no = '' THEN 'error: null invoice number'
            WHEN TRY_CAST(invoice_no AS BIGINT) IS NULL THEN 'error: datatype mismatch - invoice number'
        END,

        CASE
            WHEN invoice_dt IS NULL OR invoice_dt = '' THEN 'error: null invoice date'
            WHEN TRY_CAST(invoice_dt AS DATETIME) IS NULL THEN 'error: datatype mismatch - invoice date'
            WHEN TRY_CAST(invoice_dt AS DATETIME) > GETDATE() THEN 'warning: future invoice date'
        END,

        CASE WHEN cust_name IS NULL OR cust_name = '' THEN 'error: null customer name' END,
        CASE WHEN warehouse_loc IS NULL OR warehouse_loc = '' THEN 'error: null warehouse location' END,

        CASE
            WHEN partner_code IS NULL OR partner_code = '' THEN 'error: null partner code'
            WHEN TRY_CAST(partner_code AS BIGINT) IS NULL THEN 'error: datatype mismatch - partner code'
        END,

        CASE
            WHEN price_amt IS NULL OR price_amt = '' THEN 'error: null price'
            WHEN TRY_CAST(price_amt AS FLOAT) IS NULL THEN 'error: datatype mismatch - price'
            WHEN TRY_CAST(price_amt AS FLOAT) > @avg_price THEN 'warning: price out of range'
        END,

        CASE
            WHEN qty IS NULL OR qty = '' THEN 'error: null quantity'
            WHEN TRY_CAST(qty AS BIGINT) IS NULL THEN 'error: datatype mismatch - quantity'
            WHEN TRY_CAST(qty AS BIGINT) > @avg_qty THEN 'warning: quantity out of range'
        END,

        CASE
            WHEN cust_name IS NOT NULL
             AND NOT EXISTS (
                    SELECT 1 FROM PartnerCode p
                    WHERE p.[distributor name] = cust_name
             )
            THEN 'error: distributor not in master'
        END,

        CASE
            WHEN TRY_CAST(partner_code AS BIGINT) IS NOT NULL
             AND warehouse_loc IS NOT NULL
             AND NOT EXISTS (
                    SELECT 1 FROM Location l
                    WHERE l.[partner code] = TRY_CAST(partner_code AS BIGINT)
                      AND l.[copy of location] = warehouse_loc
             )
            THEN 'error: partner-location mismatch'
        END

    ) AS error_message

INTO #validated
FROM stg_customer s;

-- ================= LOAD FINAL =================

INSERT INTO final_customer
SELECT
    invoice_no_c,
    invoice_dt_c,
    cust_name,
    warehouse_loc,
    qty_c,
    price_amt_c,
    partner_code_c
FROM #validated
WHERE ISNULL(error_message, '') NOT LIKE '%error:%'
AND rn = 1;

-- ================= LOAD ERROR =================

INSERT INTO error_table
SELECT
    invoice_no,
    invoice_dt,
    cust_name,
    warehouse_loc,
    qty,
    price_amt,
    partner_code,
    error_message
FROM #validated
WHERE error_message LIKE '%error:%'
AND rn = 1;

-- ================= DEBUG =================

SELECT COUNT(*) AS total_staging FROM stg_customer;
SELECT COUNT(*) AS total_final   FROM final_customer;
SELECT COUNT(*) AS total_error   FROM error_table;