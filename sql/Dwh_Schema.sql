-- Database: ecommerce_dwh
-- Purpose: Schema Initialization Script (staging and dwh schemas)
-- Architecture: Kimball Star Schema with Staging Layer

CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS dwh;

-- 1. STAGING LAYER (staging.*)


-- STAGING CUSTOMERS
CREATE TABLE IF NOT EXISTS staging.stg_customers (
    customer_id              VARCHAR(50)     NOT NULL,
    customer_unique_id       VARCHAR(50)     NOT NULL,
    customer_zip_code_prefix VARCHAR(10)     NOT NULL,
    customer_city            VARCHAR(100)    NOT NULL,
    customer_state           VARCHAR(10)     NOT NULL,
    _loaded_at               TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
    _source_file             VARCHAR(255),
    CONSTRAINT pk_stg_customers PRIMARY KEY (customer_id)
);

-- STAGING ORDERS
CREATE TABLE IF NOT EXISTS staging.stg_orders (
    order_id                      VARCHAR(50)     NOT NULL,
    customer_id                   VARCHAR(50)     NOT NULL,
    order_status                  VARCHAR(30)     NOT NULL,
    order_purchase_timestamp      TIMESTAMP       NOT NULL,
    order_approved_at             TIMESTAMP,
    order_delivered_carrier_date  TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP       NOT NULL,
    _loaded_at                    TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
    _source_file                  VARCHAR(255),
    CONSTRAINT pk_stg_orders PRIMARY KEY (order_id)
);

-- STAGING ORDER ITEMS
CREATE TABLE IF NOT EXISTS staging.stg_order_items (
    order_id            VARCHAR(50)     NOT NULL,
    order_item_id       INTEGER         NOT NULL,
    product_id          VARCHAR(50)     NOT NULL,
    seller_id           VARCHAR(50)     NOT NULL,
    shipping_limit_date TIMESTAMP       NOT NULL,
    price               NUMERIC(14,2)   NOT NULL,
    freight_value       NUMERIC(14,2)   NOT NULL,
    _loaded_at          TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
    _source_file        VARCHAR(255),
    CONSTRAINT pk_stg_order_items PRIMARY KEY (order_id, order_item_id)
);

-- STAGING ORDER PAYMENTS
CREATE TABLE IF NOT EXISTS staging.stg_order_payments (
    order_id             VARCHAR(50)    NOT NULL,
    payment_sequential   INTEGER        NOT NULL,
    payment_type         VARCHAR(30)    NOT NULL,
    payment_installments INTEGER        NOT NULL,
    payment_value        NUMERIC(14,2)  NOT NULL,
    _loaded_at           TIMESTAMP      DEFAULT CURRENT_TIMESTAMP,
    _source_file         VARCHAR(255),
    CONSTRAINT pk_stg_order_payments PRIMARY KEY (order_id, payment_sequential)
);

-- STAGING ORDER REVIEWS
CREATE TABLE IF NOT EXISTS staging.stg_order_reviews (
    review_id               VARCHAR(50)     NOT NULL,
    order_id                VARCHAR(50)     NOT NULL,
    review_score            INTEGER         NOT NULL,
    review_comment_title    TEXT,
    review_comment_message  TEXT,
    review_creation_date    TIMESTAMP       NOT NULL,
    review_answer_timestamp TIMESTAMP       NOT NULL,
    _loaded_at              TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
    _source_file            VARCHAR(255)
);

-- STAGING PRODUCTS
CREATE TABLE IF NOT EXISTS staging.stg_products (
    product_id                  VARCHAR(50)     NOT NULL,
    product_category_name       VARCHAR(100),
    product_category_name_en    VARCHAR(100),
    product_name_length         INTEGER,
    product_description_length  INTEGER,
    product_photos_qty          INTEGER,
    product_weight_g            NUMERIC(12,2),
    product_length_cm           NUMERIC(12,2),
    product_height_cm           NUMERIC(12,2),
    product_width_cm            NUMERIC(12,2),
    _loaded_at                  TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
    _source_file                VARCHAR(255),
    CONSTRAINT pk_stg_products PRIMARY KEY (product_id)
);

-- STAGING SELLERS
CREATE TABLE IF NOT EXISTS staging.stg_sellers (
    seller_id              VARCHAR(50)     NOT NULL,
    seller_zip_code_prefix VARCHAR(10)     NOT NULL,
    seller_city            VARCHAR(100)    NOT NULL,
    seller_state           VARCHAR(10)     NOT NULL,
    _loaded_at             TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
    _source_file           VARCHAR(255),
    CONSTRAINT pk_stg_sellers PRIMARY KEY (seller_id)
);


-- 2. DIMENSIONAL SERVING LAYER (dwh.*)


-- DIM_DATE
CREATE TABLE IF NOT EXISTS dwh.dim_date (
    date_key INTEGER PRIMARY KEY,
    calendar_date DATE NOT NULL UNIQUE,
    day INTEGER NOT NULL,
    day_name VARCHAR(10) NOT NULL,
    week_of_year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    month_name VARCHAR(10) NOT NULL,
    quarter INTEGER NOT NULL,
    year INTEGER NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    is_month_end BOOLEAN NOT NULL,
    CONSTRAINT chk_dim_date_day CHECK (day BETWEEN 1 AND 31),
    CONSTRAINT chk_dim_date_week CHECK (week_of_year BETWEEN 1 AND 53),
    CONSTRAINT chk_dim_date_month CHECK (month BETWEEN 1 AND 12),
    CONSTRAINT chk_dim_date_quarter CHECK (quarter BETWEEN 1 AND 4)
);

-- DIM_CUSTOMERS: SCD Type 2
CREATE TABLE IF NOT EXISTS dwh.dim_customers (
    customer_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    customer_unique_id VARCHAR(50) NOT NULL,
    city VARCHAR(100),
    state VARCHAR(10),
    zip_code_prefix VARCHAR(10),
    valid_from TIMESTAMP NOT NULL,
    valid_to TIMESTAMP NOT NULL,
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    CONSTRAINT chk_customer_validity CHECK (valid_to > valid_from)
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_customers_current
ON dwh.dim_customers (customer_unique_id)
WHERE is_current = TRUE;

CREATE INDEX IF NOT EXISTS ix_dim_customers_business_key
ON dwh.dim_customers (customer_unique_id);

-- DIM_PRODUCTS: SCD Type 2
CREATE TABLE IF NOT EXISTS dwh.dim_products (
    product_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    product_id VARCHAR(50) NOT NULL,
    product_category_name VARCHAR(100),
    product_category_name_en VARCHAR(100),
    product_name_length INTEGER,
    product_description_length INTEGER,
    product_photos_qty INTEGER,
    product_weight_g NUMERIC(12,2),
    product_length_cm NUMERIC(12,2),
    product_height_cm NUMERIC(12,2),
    product_width_cm NUMERIC(12,2),
    valid_from TIMESTAMP NOT NULL,
    valid_to TIMESTAMP NOT NULL,
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    CONSTRAINT chk_product_validity CHECK (valid_to > valid_from),
    CONSTRAINT chk_product_name_length CHECK (product_name_length IS NULL OR product_name_length >= 0),
    CONSTRAINT chk_product_description_length CHECK (product_description_length IS NULL OR product_description_length >= 0),
    CONSTRAINT chk_product_photos CHECK (product_photos_qty IS NULL OR product_photos_qty >= 0),
    CONSTRAINT chk_product_weight CHECK (product_weight_g IS NULL OR product_weight_g >= 0),
    CONSTRAINT chk_product_length CHECK (product_length_cm IS NULL OR product_length_cm >= 0),
    CONSTRAINT chk_product_height CHECK (product_height_cm IS NULL OR product_height_cm >= 0),
    CONSTRAINT chk_product_width CHECK (product_width_cm IS NULL OR product_width_cm >= 0)
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_products_current
ON dwh.dim_products (product_id)
WHERE is_current = TRUE;

CREATE INDEX IF NOT EXISTS ix_dim_products_business_key
ON dwh.dim_products (product_id);

-- DIM_SELLERS: SCD Type 1
CREATE TABLE IF NOT EXISTS dwh.dim_sellers (
    seller_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    seller_id VARCHAR(50) NOT NULL UNIQUE,
    seller_city VARCHAR(100),
    seller_state VARCHAR(10),
    seller_zip_code VARCHAR(10)
);

-- FACT_ORDERS: Accumulating Snapshot
CREATE TABLE IF NOT EXISTS dwh.fact_orders (
    order_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_id VARCHAR(50) NOT NULL UNIQUE,
    customer_key BIGINT NOT NULL,
    purchase_date_key INTEGER,
    approved_date_key INTEGER,
    carrier_date_key INTEGER,
    delivered_date_key INTEGER,
    estimated_date_key INTEGER,
    order_status VARCHAR(30),
    purchase_timestamp TIMESTAMP,
    approved_at TIMESTAMP,
    delivered_carrier_date TIMESTAMP,
    delivered_customer_date TIMESTAMP,
    estimated_delivery_date TIMESTAMP,
    total_order_value NUMERIC(14,2),
    delivery_days INTEGER,
    is_delivered_on_time BOOLEAN,
    CONSTRAINT fk_fact_orders_customer FOREIGN KEY (customer_key) REFERENCES dwh.dim_customers(customer_key),
    CONSTRAINT fk_fact_orders_purchase_date FOREIGN KEY (purchase_date_key) REFERENCES dwh.dim_date(date_key),
    CONSTRAINT fk_fact_orders_approved_date FOREIGN KEY (approved_date_key) REFERENCES dwh.dim_date(date_key),
    CONSTRAINT fk_fact_orders_carrier_date FOREIGN KEY (carrier_date_key) REFERENCES dwh.dim_date(date_key),
    CONSTRAINT fk_fact_orders_delivered_date FOREIGN KEY (delivered_date_key) REFERENCES dwh.dim_date(date_key),
    CONSTRAINT fk_fact_orders_estimated_date FOREIGN KEY (estimated_date_key) REFERENCES dwh.dim_date(date_key),
    CONSTRAINT chk_fact_orders_value CHECK (total_order_value IS NULL OR total_order_value >= 0),
    CONSTRAINT chk_fact_orders_delivery_days CHECK (delivery_days IS NULL OR delivery_days >= 0)
);

CREATE INDEX IF NOT EXISTS ix_fact_orders_customer
ON dwh.fact_orders (customer_key);

CREATE INDEX IF NOT EXISTS ix_fact_orders_purchase_date
ON dwh.fact_orders (purchase_date_key);

-- FACT_ORDER_ITEMS: Transaction Fact
CREATE TABLE IF NOT EXISTS dwh.fact_order_items (
    order_item_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_id VARCHAR(50) NOT NULL,
    order_item_id INTEGER NOT NULL,
    customer_key BIGINT NOT NULL,
    product_key BIGINT NOT NULL,
    seller_key BIGINT NOT NULL,
    order_date_key INTEGER NOT NULL,
    quantity INTEGER NOT NULL,
    price NUMERIC(14,2) NOT NULL,
    freight_value NUMERIC(14,2) NOT NULL,
    total_item_value NUMERIC(14,2) NOT NULL,
    CONSTRAINT uq_fact_order_items_source UNIQUE (order_id, order_item_id),
    CONSTRAINT fk_fact_order_items_customer FOREIGN KEY (customer_key) REFERENCES dwh.dim_customers(customer_key),
    CONSTRAINT fk_fact_order_items_product FOREIGN KEY (product_key) REFERENCES dwh.dim_products(product_key),
    CONSTRAINT fk_fact_order_items_seller FOREIGN KEY (seller_key) REFERENCES dwh.dim_sellers(seller_key),
    CONSTRAINT fk_fact_order_items_date FOREIGN KEY (order_date_key) REFERENCES dwh.dim_date(date_key),
    CONSTRAINT chk_order_item_quantity CHECK (quantity > 0),
    CONSTRAINT chk_order_item_price CHECK (price >= 0),
    CONSTRAINT chk_order_item_freight CHECK (freight_value >= 0),
    CONSTRAINT chk_order_item_total CHECK (total_item_value >= 0)
);

CREATE INDEX IF NOT EXISTS ix_fact_order_items_customer
ON dwh.fact_order_items (customer_key);

CREATE INDEX IF NOT EXISTS ix_fact_order_items_product
ON dwh.fact_order_items (product_key);

CREATE INDEX IF NOT EXISTS ix_fact_order_items_seller
ON dwh.fact_order_items (seller_key);

CREATE INDEX IF NOT EXISTS ix_fact_order_items_date
ON dwh.fact_order_items (order_date_key);

-- FACT_PAYMENTS: Transaction Fact
CREATE TABLE IF NOT EXISTS dwh.fact_payments (
    payment_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_id VARCHAR(50) NOT NULL,
    payment_sequential INTEGER NOT NULL,
    customer_key BIGINT NOT NULL,
    payment_type VARCHAR(30),
    payment_installments INTEGER,
    payment_value NUMERIC(14,2) NOT NULL,
    CONSTRAINT uq_fact_payments_source UNIQUE (order_id, payment_sequential),
    CONSTRAINT fk_fact_payments_customer FOREIGN KEY (customer_key) REFERENCES dwh.dim_customers(customer_key),
    CONSTRAINT chk_payment_sequential CHECK (payment_sequential > 0),
    CONSTRAINT chk_payment_installments CHECK (payment_installments IS NULL OR payment_installments > 0),
    CONSTRAINT chk_payment_value CHECK (payment_value >= 0)
);

CREATE INDEX IF NOT EXISTS ix_fact_payments_customer
ON dwh.fact_payments (customer_key);

CREATE INDEX IF NOT EXISTS ix_fact_payments_order
ON dwh.fact_payments (order_id);

-- FACT_ORDER_REVIEWS: Transaction Fact
CREATE TABLE IF NOT EXISTS dwh.fact_order_reviews (
    review_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    review_id VARCHAR(50) NOT NULL UNIQUE,
    order_id VARCHAR(50) NOT NULL,
    customer_key BIGINT NOT NULL,
    creation_date_key INTEGER,
    answer_date_key INTEGER,
    review_score INTEGER,
    review_comment_title TEXT,
    review_comment_message TEXT,
    response_time_hours NUMERIC(14,2),
    CONSTRAINT fk_fact_reviews_customer FOREIGN KEY (customer_key) REFERENCES dwh.dim_customers(customer_key),
    CONSTRAINT fk_fact_reviews_creation_date FOREIGN KEY (creation_date_key) REFERENCES dwh.dim_date(date_key),
    CONSTRAINT fk_fact_reviews_answer_date FOREIGN KEY (answer_date_key) REFERENCES dwh.dim_date(date_key),
    CONSTRAINT chk_review_score CHECK (review_score IS NULL OR review_score BETWEEN 1 AND 5),
    CONSTRAINT chk_response_time CHECK (response_time_hours IS NULL OR response_time_hours >= 0)
);

CREATE INDEX IF NOT EXISTS ix_fact_reviews_customer
ON dwh.fact_order_reviews (customer_key);

CREATE INDEX IF NOT EXISTS ix_fact_reviews_creation_date
ON dwh.fact_order_reviews (creation_date_key);

CREATE INDEX IF NOT EXISTS ix_fact_reviews_answer_date
ON dwh.fact_order_reviews (answer_date_key);

CREATE INDEX IF NOT EXISTS ix_fact_reviews_order
ON dwh.fact_order_reviews (order_id);
