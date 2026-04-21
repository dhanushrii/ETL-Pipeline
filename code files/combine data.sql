USE casestudy;

-- ================= STAGING =================

IF OBJECT_ID('stg_customer','U') IS NOT NULL DROP TABLE stg_customer;

CREATE TABLE stg_customer (
    invoice_no     NVARCHAR(100),
    invoice_dt     NVARCHAR(100),
    cust_name      NVARCHAR(1000),
    warehouse_loc  NVARCHAR(1000),
    qty            NVARCHAR(100),
    price_amt      NVARCHAR(100),
    partner_code   NVARCHAR(100)
);

-- ================= COMBINE RAW TABLES =================

INSERT INTO stg_customer (
    invoice_no,
    invoice_dt,
    cust_name,
    warehouse_loc,
    qty,
    price_amt,
    partner_code
)

SELECT invoice_no, invoice_dt, cust_name, warehouse_loc, qty, price_amt, partner_code
FROM raw_customer1

UNION ALL
SELECT inv_no, inv_date, customer, location, quantity, price, partner
FROM raw_customer2

UNION ALL
SELECT invoiceNumber, date, cust, warehouse, qty_units, amount, partner_code
FROM raw_customer3

UNION ALL
SELECT inv, invoice_date, customer_name, warehouse_location, quantity, price, partner_code
FROM raw_customer4

UNION ALL
SELECT invoice_no, inv_dt, cust_name, warehouse_loc, qty, price_amt, partner_code
FROM raw_customer5

UNION ALL
SELECT inv_no, invoice_date, customer, location, quantity, price, partner
FROM raw_customer6

UNION ALL
SELECT invoiceNumber, date, cust, warehouse, qty_units, amount, partner_code
FROM raw_customer7

UNION ALL
SELECT invoice_no, inv_dt, cust_name, warehouse_loc, qty, price_amt, partner_code
FROM raw_customer8

UNION ALL
SELECT inv_no, invoice_date, customer, location, quantity, price, partner
FROM raw_customer9

UNION ALL
SELECT invoiceNumber, date, cust, warehouse, qty_units, amount, partner_code
FROM raw_customer10;
