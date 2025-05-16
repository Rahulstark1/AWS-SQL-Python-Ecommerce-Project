
---- Data Modeling and Snowpipe Pipeline Setup ----

-- Create and use database
CREATE DATABASE ECOM_AWS;
USE DATABASE ECOM_AWS;

-- Create customer table
CREATE OR REPLACE TABLE ECO_CONSUMER (
  customer_id STRING,
  customer_unique_id STRING,
  customer_zip_code_prefix STRING,  -- Use STRING to preserve leading zeros in ZIP codes
  customer_city STRING,
  customer_state STRING
);

-- Create file format for CSV data
CREATE OR REPLACE FILE FORMAT CSV_FORMAT
  TYPE = CSV
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1;

-- Create storage integration for secure access to S3
CREATE OR REPLACE STORAGE INTEGRATION S3_INT_ECO
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::345594606365:role/ecom_role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://ecommercedt/');

DESC INTEGRATION S3_INT_ECO;

-- Create external stage referencing the S3 bucket
CREATE OR REPLACE STAGE ECOM_STAGE
  URL = 's3://ecommercedt'
  FILE_FORMAT = CSV_FORMAT
  STORAGE_INTEGRATION = S3_INT_ECO;

LIST @ECOM_STAGE;
SHOW STAGES;

-- Create Snowpipe to auto-ingest customer data from S3
CREATE OR REPLACE PIPE PIPE_CUSTOMER AUTO_INGEST = TRUE AS
COPY INTO "ECOM_AWS"."PUBLIC"."ECO_CONSUMER"
FROM '@ECOM_STAGE/customers/'
FILE_FORMAT = CSV_FORMAT;

SHOW PIPES;

-- Manually refresh pipe (trigger data load)
ALTER PIPE PIPE_CUSTOMER REFRESH;

-- Validate data load
SELECT COUNT(CUSTOMER_ID) FROM ECO_CONSUMER;
SELECT * FROM ECO_CONSUMER;

-- Create seller table
CREATE OR REPLACE TABLE ECO_SELLERS (
  seller_id STRING,
  seller_zip_code_prefix STRING,
  seller_city STRING,
  seller_state STRING
);

-- Create pipe for seller data ingestion
CREATE OR REPLACE PIPE PIPE_SELLERS AUTO_INGEST = TRUE AS
COPY INTO "ECOM_AWS"."PUBLIC"."ECO_SELLERS"
FROM '@ECOM_STAGE/sellers/'
FILE_FORMAT = csv_utf8;

ALTER PIPE PIPE_SELLERS REFRESH;

-- Validate seller data load
SELECT COUNT(*) FROM ECO_SELLERS;
SELECT * FROM ECO_SELLERS;

-- Troubleshooting steps:
-- 1. Confirm data exists in stage
LIST @ECOM_STAGE/sellers/;
-- 2. Check file format compatibility
COPY INTO ECOM_AWS.PUBLIC.ECO_CONSUMER
FROM @ECOM_STAGE/customers/
FILE_FORMAT = (FORMAT_NAME = 'CSV_FORMAT');

-- Create geolocation table
CREATE OR REPLACE TABLE ECO_GEO (
  geolocation_zip_code_prefix INT,
  geolocation_lat FLOAT,
  geolocation_lng FLOAT,
  geolocation_city STRING,
  geolocation_state STRING
);

-- Create order items table
CREATE OR REPLACE TABLE ECO_ORDERITEM (
  order_id STRING,
  order_item_id INT,
  product_id STRING,
  seller_id STRING,
  shipping_limit_date DATE,
  price FLOAT,
  freight_value FLOAT,
  shipping_limit_time TIME
);

-- Create orders table
CREATE OR REPLACE TABLE ECO_ORDERS (
  order_id STRING,
  customer_id STRING,
  order_status STRING,
  order_purchase_timestamp TIMESTAMP,
  order_approved_at TIMESTAMP,
  order_delivered_carrier_date TIMESTAMP,
  order_delivered_customer_date TIMESTAMP,
  order_estimated_delivery_date TIMESTAMP
);

-- Create payments table
CREATE OR REPLACE TABLE ECO_PAYMENTS (
  order_id STRING,
  payment_sequential NUMBER,
  payment_type STRING,
  payment_installments NUMBER,
  payment_value FLOAT
);

-- Create products table
CREATE OR REPLACE TABLE ECO_PRODUCTS (
  product_id STRING,
  product_category STRING,
  product_name_length NUMBER,
  product_description_length NUMBER,
  product_photos_qty NUMBER,
  product_weight_g NUMBER,
  product_length_cm NUMBER,
  product_height_cm NUMBER,
  product_width_cm NUMBER
);

-- Create UTF-8 compatible file format
CREATE OR REPLACE FILE FORMAT csv_utf8
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
  ENCODING = 'UTF8';

-- Create and refresh pipes for remaining tables
CREATE OR REPLACE PIPE PIPE_ECO_GEO AUTO_INGEST = TRUE AS 
COPY INTO "ECOM_AWS"."PUBLIC"."ECO_GEO"
FROM '@ECOM_STAGE/geolocation/'
FILE_FORMAT = csv_utf8;

ALTER PIPE PIPE_ECO_GEO REFRESH;
SELECT COUNT(*) FROM ECO_GEO;

CREATE OR REPLACE PIPE PIPE_ECO_ORDERITEM AUTO_INGEST = TRUE AS 
COPY INTO "ECOM_AWS"."PUBLIC"."ECO_ORDERITEM"
FROM '@ECOM_STAGE/orderitem/'
FILE_FORMAT = csv_utf8;

ALTER PIPE PIPE_ECO_ORDERITEM REFRESH;
SELECT COUNT(*) FROM ECO_ORDERITEM;
SELECT * FROM ECO_ORDERITEM LIMIT 10;

CREATE OR REPLACE PIPE PIPE_ECO_ORDERS AUTO_INGEST = TRUE AS 
COPY INTO "ECOM_AWS"."PUBLIC"."ECO_ORDERS"
FROM '@ECOM_STAGE/orders/'
FILE_FORMAT = CSV_FORMAT;

ALTER PIPE PIPE_ECO_ORDERS REFRESH;
SELECT COUNT(*) FROM ECO_ORDERS;

CREATE OR REPLACE PIPE PIPE_ECO_PAYMENTS AUTO_INGEST = TRUE AS 
COPY INTO "ECOM_AWS"."PUBLIC"."ECO_PAYMENTS"
FROM '@ECOM_STAGE/payemnts/'
FILE_FORMAT = CSV_FORMAT;

ALTER PIPE PIPE_ECO_PAYMENTS REFRESH;
SELECT COUNT(*) FROM ECO_PAYMENTS;

CREATE OR REPLACE PIPE PIPE_ECO_PRODUCTS AUTO_INGEST = TRUE AS 
COPY INTO "ECOM_AWS"."PUBLIC"."ECO_PRODUCTS"
FROM '@ECOM_STAGE/products/'
FILE_FORMAT = CSV_FORMAT;

ALTER PIPE PIPE_ECO_PRODUCTS REFRESH;
SELECT COUNT(*) FROM ECO_PRODUCTS;

-- Final table validations
SELECT * FROM ECO_CONSUMER LIMIT 10;
SELECT * FROM ECO_GEO LIMIT 10;
SELECT * FROM ECO_ORDERITEM LIMIT 100;
SELECT * FROM ECO_ORDERS LIMIT 10;
SELECT * FROM ECO_PAYMENTS LIMIT 10;
SELECT * FROM ECO_PRODUCTS LIMIT 10;
SELECT * FROM ECO_SELLERS LIMIT 10;
