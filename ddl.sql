-- =========================
-- CUSTOMERS
-- =========================
DROP TABLE IF EXISTS customers CASCADE;

CREATE TABLE customers (
    customer_pk SERIAL PRIMARY KEY,
    customer_id VARCHAR(100),
    full_name TEXT,
    email TEXT,
    country VARCHAR(50),
    language VARCHAR(10),
    birth_date DATE,
    registration_date TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    
    CONSTRAINT uq_customer_id UNIQUE(customer_id)
);

-- =========================
-- ORDERS
-- =========================
DROP TABLE IF EXISTS orders CASCADE;

CREATE TABLE orders (
    order_pk SERIAL PRIMARY KEY,
    order_id VARCHAR(100),
    customer_pk INTEGER NOT NULL REFERENCES customers(customer_pk),
    total_amount NUMERIC(12,2),
    currency VARCHAR(10),
    order_date TIMESTAMP,
    status VARCHAR(50),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    
    CONSTRAINT uq_order_id UNIQUE(order_id)
);

-- =========================
-- REVIEWS
-- =========================
DROP TABLE IF EXISTS reviews CASCADE;

CREATE TABLE reviews (
    review_pk SERIAL PRIMARY KEY,
    review_id VARCHAR(100) UNIQUE,
    customer_pk INTEGER REFERENCES customers(customer_pk),
    product_id VARCHAR(100),
    transaction_id VARCHAR(100),
    title TEXT,
    rating NUMERIC(2,1),
    comment TEXT,
    review_date TIMESTAMP,
    verified_purchase BOOLEAN,
    helpful_votes INT,
    unhelpful_votes INT,
    created_at TIMESTAMP DEFAULT NOW()
);

-- =========================
-- COMPETITOR PRICING
-- =========================
DROP TABLE IF EXISTS competitor_pricing CASCADE;

CREATE TABLE competitor_pricing (
    pricing_pk SERIAL PRIMARY KEY,
    snapshot_id VARCHAR(100) UNIQUE,
    product_id VARCHAR(100),
    snapshot_date TIMESTAMP,
    our_price NUMERIC(12,2),
    competitor_price NUMERIC(12,2),
    competitor_name TEXT,
    stock BOOLEAN,
    num_reviews NUMERIC(12,2),
    rating NUMERIC(3,2),
    competitor_url TEXT
);

-- =========================
-- SUPPORT TICKETS
-- =========================
DROP TABLE IF EXISTS support_tickets CASCADE;

CREATE TABLE support_tickets (
    ticket_pk SERIAL PRIMARY KEY,
    ticket_id VARCHAR(100) UNIQUE,
    customer_pk INTEGER REFERENCES customers(customer_pk),
    transaction_id VARCHAR(100),
    subject TEXT,
    description TEXT,
    priority VARCHAR(20),
    status VARCHAR(20),
    category VARCHAR(50),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    resolved_at TIMESTAMP,
    assigned_to TEXT,
    resolution TEXT
);

-- =========================
-- MARKETING SENDS
-- =========================
DROP TABLE IF EXISTS marketing_sends CASCADE;

CREATE TABLE marketing_sends (
    send_pk SERIAL PRIMARY KEY,
    send_id VARCHAR(100) UNIQUE,
    customer_pk INTEGER REFERENCES customers(customer_pk),
    campaign_id VARCHAR(100),
    sent_date TIMESTAMP,
    open_date TIMESTAMP,
    click_date TIMESTAMP,
    conversion_date TIMESTAMP,
    bounced BOOLEAN,
    bounce_reason TEXT,
    opened BOOLEAN,
    clicked BOOLEAN,
    converted BOOLEAN,
    unsubscribed BOOLEAN
);

-- =========================
-- CAMPAIGNS
-- =========================
DROP TABLE IF EXISTS campaigns CASCADE;

CREATE TABLE campaigns (
    campaign_pk SERIAL PRIMARY KEY,
    campaign_id VARCHAR(100) UNIQUE,
    name TEXT,
    channel VARCHAR(50),
    budget NUMERIC(12,2),
    impressions INT,
    clicks INT,
    conversions INT,
    revenue_generated NUMERIC(12,2),
    start_date TIMESTAMP,
    end_date TIMESTAMP,
    age_min INT,
    age_max INT
);

-- =========================
-- INVENTORY ADJUSTMENTS
-- =========================
DROP TABLE IF EXISTS inventory_adjustments CASCADE;

CREATE TABLE inventory_adjustments (
    inv_pk SERIAL PRIMARY KEY,
    adjustment_id VARCHAR(100) UNIQUE,
    product_id VARCHAR(100),
    movement_type VARCHAR(20),
    quantity_change INT,
    previous_stock INT,
    new_stock INT,
    warehouse VARCHAR(50),
    adjustment_date TIMESTAMP,
    user_name TEXT,
    reason TEXT
);

-- =========================
-- PRODUCTS (DIM PRODUCTO)
-- =========================
DROP TABLE IF EXISTS products CASCADE;

CREATE TABLE products (
    product_id      VARCHAR(100) PRIMARY KEY,  -- ID de negocio (ya existe en tus tablas)
    product_name    TEXT,
    category        TEXT,
    subcategory     TEXT,
    brand           TEXT,
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW()
);

INSERT INTO products (product_id)
SELECT DISTINCT product_id
FROM (
    SELECT product_id FROM reviews
    UNION ALL
    SELECT product_id FROM competitor_pricing
    UNION ALL
    SELECT product_id FROM inventory_adjustments
) t
WHERE product_id IS NOT NULL
ON CONFLICT (product_id) DO NOTHING;


-- REVIEW → PRODUCT
ALTER TABLE reviews
ADD CONSTRAINT fk_reviews_product
FOREIGN KEY (product_id)
REFERENCES products(product_id);

-- COMPETITOR_PRICING → PRODUCT
ALTER TABLE competitor_pricing
ADD CONSTRAINT fk_competitor_pricing_product
FOREIGN KEY (product_id)
REFERENCES products(product_id);

-- INVENTORY_ADJUSTMENTS → PRODUCT
ALTER TABLE inventory_adjustments
ADD CONSTRAINT fk_inventory_adjustments_product
FOREIGN KEY (product_id)
REFERENCES products(product_id);

DROP TABLE IF EXISTS dim_date CASCADE;

CREATE TABLE dim_date (
    date_pk         DATE PRIMARY KEY,
    year            INT,
    quarter         INT,
    month           INT,
    month_name      TEXT,
    week_of_year    INT,
    day_of_month    INT,
    day_name        TEXT,
    is_weekend      BOOLEAN
);




INSERT INTO dim_date (date_pk, year, quarter, month, month_name, week_of_year, day_of_month, day_name, is_weekend)
SELECT
    d::date AS date_pk,
    EXTRACT(YEAR FROM d) AS year,
    EXTRACT(QUARTER FROM d) AS quarter,
    EXTRACT(MONTH FROM d) AS month,
    TO_CHAR(d, 'TMMonth') AS month_name,
    EXTRACT(WEEK FROM d) AS week_of_year,
    EXTRACT(DAY FROM d) AS day_of_month,
    TO_CHAR(d, 'TMDay') AS day_name,
    CASE WHEN EXTRACT(ISODOW FROM d) IN (6, 7) THEN TRUE ELSE FALSE END AS is_weekend
FROM generate_series(
    '2023-01-01'::date,
    '2025-12-31'::date,
    interval '1 day'
) AS d;



ALTER TABLE orders
ADD COLUMN date_pk DATE;

UPDATE orders
SET date_pk = DATE(order_date)
WHERE order_date IS NOT NULL;

ALTER TABLE orders
ADD CONSTRAINT fk_orders_date
FOREIGN KEY (date_pk)
REFERENCES dim_date(date_pk);

ALTER TABLE reviews
ADD COLUMN date_pk DATE;

UPDATE reviews
SET date_pk = DATE(review_date)
WHERE review_date IS NOT NULL;


ALTER TABLE support_tickets
ADD COLUMN date_pk DATE;

UPDATE support_tickets
SET date_pk = DATE(created_at)
WHERE created_at IS NOT NULL;





*****************


-- ==========================
--  REVIEWS → dim_date
-- ==========================
ALTER TABLE reviews
ADD CONSTRAINT fk_reviews_date
FOREIGN KEY (date_pk)
REFERENCES dim_date(date_pk);


-- ==========================
--  SUPPORT_TICKETS → dim_date
-- ==========================
ALTER TABLE support_tickets
ADD CONSTRAINT fk_support_tickets_date
FOREIGN KEY (date_pk)
REFERENCES dim_date(date_pk);


-- ==========================
--  MARKETING_SENDS → dim_date (sent_date)
-- ==========================
ALTER TABLE marketing_sends
ADD COLUMN sent_date_pk DATE;

UPDATE marketing_sends
SET sent_date_pk = DATE(sent_date)
WHERE sent_date IS NOT NULL;

ALTER TABLE marketing_sends
ADD CONSTRAINT fk_marketing_sends_sent_date
FOREIGN KEY (sent_date_pk)
REFERENCES dim_date(date_pk);


-- ==========================
--  MARKETING_SENDS → dim_date (open_date)
-- ==========================
ALTER TABLE marketing_sends
ADD COLUMN open_date_pk DATE;

UPDATE marketing_sends
SET open_date_pk = DATE(open_date)
WHERE open_date IS NOT NULL;

ALTER TABLE marketing_sends
ADD CONSTRAINT fk_marketing_sends_open_date
FOREIGN KEY (open_date_pk)
REFERENCES dim_date(date_pk);


-- ==========================
--  CAMPAIGNS → dim_date
-- ==========================
ALTER TABLE campaigns
ADD COLUMN start_date_pk DATE,
ADD COLUMN end_date_pk DATE;

UPDATE campaigns
SET start_date_pk = DATE(start_date)
WHERE start_date IS NOT NULL;

UPDATE campaigns
SET end_date_pk = DATE(end_date)
WHERE end_date IS NOT NULL;

ALTER TABLE campaigns
ADD CONSTRAINT fk_campaigns_start_date
FOREIGN KEY (start_date_pk)
REFERENCES dim_date(date_pk);

ALTER TABLE campaigns
ADD CONSTRAINT fk_campaigns_end_date
FOREIGN KEY (end_date_pk)
REFERENCES dim_date(date_pk);


-- ==========================
--  INVENTORY → dim_date
-- ==========================
ALTER TABLE inventory_adjustments
ADD COLUMN date_pk DATE;

UPDATE inventory_adjustments
SET date_pk = DATE(adjustment_date)
WHERE adjustment_date IS NOT NULL;

ALTER TABLE inventory_adjustments
ADD CONSTRAINT fk_inventory_adjustments_date
FOREIGN KEY (date_pk)
REFERENCES dim_date(date_pk);


DROP TABLE IF EXISTS dim_products CASCADE;

CREATE TABLE dim_products (
    product_pk SERIAL PRIMARY KEY,
    product_id VARCHAR(100) UNIQUE,
    first_seen TIMESTAMP,
    last_seen TIMESTAMP
);

INSERT INTO dim_products (product_id, first_seen, last_seen)
SELECT product_id,
       MIN(review_date),
       MAX(review_date)
FROM reviews
WHERE product_id IS NOT NULL
GROUP BY product_id
ON CONFLICT (product_id) DO NOTHING;

*****

INSERT INTO dim_products (product_id, first_seen, last_seen)
SELECT product_id,
       MIN(snapshot_date),
       MAX(snapshot_date)
FROM competitor_pricing
WHERE product_id IS NOT NULL
GROUP BY product_id
ON CONFLICT (product_id) DO NOTHING;

*****

INSERT INTO dim_products (product_id, first_seen, last_seen)
SELECT product_id,
       MIN(adjustment_date),
       MAX(adjustment_date)
FROM inventory_adjustments
WHERE product_id IS NOT NULL
GROUP BY product_id
ON CONFLICT (product_id) DO NOTHING;

*****


ALTER TABLE reviews
ADD COLUMN product_pk INT;

UPDATE reviews r
SET product_pk = dp.product_pk
FROM dim_products dp
WHERE r.product_id = dp.product_id;

ALTER TABLE reviews
ADD CONSTRAINT fk_reviews_product
FOREIGN KEY (product_pk)
REFERENCES dim_products(product_pk);

ALTER TABLE reviews
ADD COLUMN product_pk INT;

UPDATE reviews r
SET product_pk = dp.product_pk
FROM dim_products dp
WHERE r.product_id = dp.product_id;



ALTER TABLE reviews
ADD CONSTRAINT fk_reviews_product
FOREIGN KEY (product_pk)
REFERENCES dim_products(product_pk);


SELECT DISTINCT product_pk
FROM reviews
WHERE product_pk IS NOT NULL
  AND product_pk NOT IN (SELECT product_pk FROM dim_products);

  ALTER TABLE reviews
ADD CONSTRAINT fk_reviews_product
FOREIGN KEY (product_pk)
REFERENCES dim_products(product_pk);


ALTER TABLE competitor_pricing
ADD COLUMN product_pk INT;

UPDATE competitor_pricing cp
SET product_pk = dp.product_pk
FROM dim_products dp
WHERE cp.product_id = dp.product_id;

ALTER TABLE competitor_pricing
ADD CONSTRAINT fk_competitor_pricing_product
FOREIGN KEY (product_pk)
REFERENCES dim_products(product_pk);



ALTER TABLE inventory_adjustments
ADD COLUMN product_pk INT;

UPDATE inventory_adjustments ia
SET product_pk = dp.product_pk
FROM dim_products dp
WHERE ia.product_id = dp.product_id;

ALTER TABLE inventory_adjustments
ADD CONSTRAINT fk_inventory_product
FOREIGN KEY (product_pk)
REFERENCES dim_products(product_pk);


CREATE INDEX idx_orders_customer_pk
ON orders(customer_pk);

CREATE INDEX idx_reviews_customer_pk
ON reviews(customer_pk);

CREATE INDEX idx_support_tickets_customer_pk
ON support_tickets(customer_pk);

CREATE INDEX idx_marketing_sends_customer_pk
ON marketing_sends(customer_pk);


CREATE INDEX idx_reviews_product_pk
ON reviews(product_pk);

CREATE INDEX idx_inventory_product_id
ON inventory_adjustments(product_id);



CREATE INDEX idx_orders_date_pk
ON orders(date_pk);

CREATE INDEX idx_reviews_date_pk
ON reviews(date_pk);

CREATE INDEX idx_support_tickets_date_pk
ON support_tickets(date_pk);

CREATE INDEX idx_inventory_date_pk
ON inventory_adjustments(date_pk);

CREATE INDEX idx_marketing_sends_date_pk
ON marketing_sends(sent_date);


CREATE OR REPLACE VIEW dim_customers AS
SELECT 
    customer_pk,
    customer_id,
    full_name,
    email,
    country,
    language,
    birth_date,
    registration_date
FROM customers;


DROP TABLE IF EXISTS dim_products CASCADE;

CREATE TABLE dim_products (
    product_pk SERIAL PRIMARY KEY,
    product_id VARCHAR(100) UNIQUE NOT NULL,
    product_name TEXT,
    category TEXT,
    subcategory TEXT,
    brand TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

INSERT INTO dim_products (product_id)
SELECT DISTINCT product_id FROM reviews
WHERE product_id IS NOT NULL
UNION
SELECT DISTINCT product_id FROM competitor_pricing
WHERE product_id IS NOT NULL
UNION
SELECT DISTINCT product_id FROM inventory_adjustments
WHERE product_id IS NOT NULL;


ALTER TABLE reviews
ADD CONSTRAINT fk_reviews_product
FOREIGN KEY (product_id)
REFERENCES products(product_id);

ALTER TABLE competitor_pricing
ADD CONSTRAINT fk_competitor_product
FOREIGN KEY (product_id)
REFERENCES products(product_id);


ALTER TABLE inventory_adjustments
ADD CONSTRAINT fk_inventory_product
FOREIGN KEY (product_id)
REFERENCES products(product_id);


CREATE OR REPLACE VIEW products_view AS
SELECT 
    product_id,
    product_name,
    category,
    subcategory,
    brand
FROM products;


SELECT DISTINCT product_id
FROM reviews
WHERE product_id NOT IN (SELECT product_id FROM products);




CREATE OR REPLACE VIEW vw_sales_by_customer AS
SELECT 
    c.customer_id,
    c.first_name,
    c.last_name,
    c.country,
    SUM(o.total_amount) AS total_sales,
    COUNT(o.order_pk) AS total_orders
FROM orders o
JOIN customers c 
    ON c.customer_pk = o.customer_pk
GROUP BY 
    c.customer_id, c.first_name, c.last_name, c.country;



CREATE OR REPLACE VIEW vw_sales_by_customer AS
SELECT 
    c.customer_id,
    c.full_name,
    c.email,
    c.country,
    SUM(o.total_amount) AS total_sales,
    COUNT(o.order_pk) AS total_orders
FROM orders o
JOIN customers c 
    ON c.customer_pk = o.customer_pk
GROUP BY 
    c.customer_id, c.full_name, c.email, c.country;


CREATE OR REPLACE VIEW vw_sales_by_customer AS
SELECT 
    c.customer_id,
    c.full_name,
    c.email,
    c.country,

    SUM(o.total_amount) AS total_sales,
    COUNT(o.order_pk) AS total_orders,
    AVG(o.total_amount) AS avg_ticket,

    MIN(o.order_date) AS first_order_date,
    MAX(o.order_date) AS last_order_date

FROM orders o
JOIN customers c 
    ON c.customer_pk = o.customer_pk
GROUP BY 
    c.customer_id, c.full_name, c.email, c.country;

	CREATE OR REPLACE VIEW vw_sales_by_country AS
SELECT 
    c.country,
    SUM(o.total_amount) AS total_sales,
    COUNT(o.order_pk) AS total_orders,
    AVG(o.total_amount) AS avg_ticket
FROM orders o
JOIN customers c 
    ON c.customer_pk = o.customer_pk
GROUP BY c.country;


CREATE OR REPLACE VIEW vw_avg_ticket AS
SELECT 
    AVG(total_amount) AS avg_ticket_global
FROM orders;

CREATE OR REPLACE VIEW vw_sales_by_date AS
SELECT
    o.date_pk,
    d.year,
    d.month,
    d.month_name,
    d.week_of_year,
    d.day_of_month,
    SUM(o.total_amount) AS total_sales,
    COUNT(o.order_pk) AS total_orders
FROM orders o
JOIN dim_date d 
    ON o.date_pk = d.date_pk
GROUP BY
    o.date_pk, d.year, d.month, d.month_name, d.week_of_year, d.day_of_month
ORDER BY o.date_pk;


CREATE OR REPLACE VIEW vw_reviews_by_product AS
SELECT 
    p.product_id,
    p.product_name,
    COUNT(r.review_pk) AS total_reviews,
    AVG(r.rating) AS avg_rating
FROM products p
LEFT JOIN reviews r
    ON p.product_id = r.product_id
GROUP BY p.product_id, p.product_name;




CREATE OR REPLACE VIEW vw_competitors_by_product AS
SELECT
    p.product_id,
    p.product_name,
    cp.competitor_name,
    AVG(cp.competitor_price) AS avg_competitor_price,
    AVG(cp.our_price) AS avg_our_price
FROM products p
LEFT JOIN competitor_pricing cp
    ON p.product_id = cp.product_id
GROUP BY
    p.product_id,
    p.product_name,
    cp.competitor_name;


	CREATE OR REPLACE VIEW vw_inventory_by_product AS
SELECT
    p.product_id,
    p.product_name,
    SUM(ia.quantity_change) AS net_stock_change,
    MAX(ia.new_stock) AS latest_stock_level
FROM products p
LEFT JOIN inventory_adjustments ia
    ON p.product_id = ia.product_id
GROUP BY
    p.product_id,
    p.product_name;



	CREATE OR REPLACE VIEW vw_marketing_performance AS
SELECT
    ms.campaign_id,
    COUNT(ms.send_pk) AS emails_sent,
    SUM(CASE WHEN ms.opened THEN 1 ELSE 0 END) AS opens,
    SUM(CASE WHEN ms.clicked THEN 1 ELSE 0 END) AS clicks,
    SUM(CASE WHEN ms.converted THEN 1 ELSE 0 END) AS conversions
FROM marketing_sends ms
GROUP BY ms.campaign_id;






















