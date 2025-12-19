/* =====================================================================
   MODELO DE DATOS – RETO DATA ENGINEER
   Autor: Eddy Oliva Bautista
   Objetivo:
   - Definir estructura base para almacenamiento y análisis
   - Normalizar clientes, órdenes y productos
   - Integrar componentes analíticos: calendario + vistas KPI
   ===================================================================== */


/* =====================================================================
   1) TABLAS NUCLEARES DEL NEGOCIO
   customers + orders
   ===================================================================== */

DROP TABLE IF EXISTS orders CASCADE;
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



/* =====================================================================
   2) TABLAS EXTENDIDAS
   ===================================================================== */

DROP TABLE IF EXISTS reviews CASCADE;
DROP TABLE IF EXISTS competitor_pricing CASCADE;
DROP TABLE IF EXISTS support_tickets CASCADE;
DROP TABLE IF EXISTS marketing_sends CASCADE;
DROP TABLE IF EXISTS campaigns CASCADE;
DROP TABLE IF EXISTS inventory_adjustments CASCADE;

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



/* =====================================================================
   3) DIM PRODUCTOS
   ===================================================================== */

DROP TABLE IF EXISTS products CASCADE;

CREATE TABLE products (
    product_id VARCHAR(100) PRIMARY KEY,
    product_name TEXT,
    category TEXT,
    subcategory TEXT,
    brand TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
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

ALTER TABLE reviews
ADD CONSTRAINT fk_reviews_product
FOREIGN KEY (product_id)
REFERENCES products(product_id);

ALTER TABLE competitor_pricing
ADD CONSTRAINT fk_competitor_pricing_product
FOREIGN KEY (product_id)
REFERENCES products(product_id);

ALTER TABLE inventory_adjustments
ADD CONSTRAINT fk_inventory_adjustments_product
FOREIGN KEY (product_id)
REFERENCES products(product_id);



/* =====================================================================
   4) DIM CALENDARIO
   ===================================================================== */

DROP TABLE IF EXISTS dim_date CASCADE;

CREATE TABLE dim_date (
    date_pk DATE PRIMARY KEY,
    year INT,
    quarter INT,
    month INT,
    month_name TEXT,
    week_of_year INT,
    day_of_month INT,
    day_name TEXT,
    is_weekend BOOLEAN
);

INSERT INTO dim_date (
    date_pk, year, quarter, month, month_name, week_of_year, day_of_month, day_name, is_weekend
)
SELECT
    d::date,
    EXTRACT(YEAR FROM d),
    EXTRACT(QUARTER FROM d),
    EXTRACT(MONTH FROM d),
    TO_CHAR(d, 'TMMonth'),
    EXTRACT(WEEK FROM d),
    EXTRACT(DAY FROM d),
    TO_CHAR(d, 'TMDay'),
    CASE WHEN EXTRACT(ISODOW FROM d) IN (6, 7) THEN TRUE ELSE FALSE END
FROM generate_series(
    '2023-01-01'::date,
    '2025-12-31'::date,
    interval '1 day'
) d;



/* =====================================================================
   5) RELACIÓN FECHAS → ORDERS
   ===================================================================== */

ALTER TABLE orders ADD COLUMN date_pk DATE;
UPDATE orders SET date_pk = DATE(order_date) WHERE order_date IS NOT NULL;
ALTER TABLE orders ADD CONSTRAINT fk_orders_date FOREIGN KEY (date_pk) REFERENCES dim_date(date_pk);



/* =====================================================================
   6) ÍNDICES DE PERFORMANCE
   ===================================================================== */

CREATE INDEX idx_orders_customer_pk ON orders(customer_pk);
CREATE INDEX idx_orders_date_pk ON orders(date_pk);

CREATE INDEX idx_reviews_customer_pk ON reviews(customer_pk);
CREATE INDEX idx_reviews_product_pk ON reviews(product_id);

CREATE INDEX idx_support_tickets_customer_pk ON support_tickets(customer_pk);

CREATE INDEX idx_marketing_sends_customer_pk ON marketing_sends(customer_pk);
CREATE INDEX idx_marketing_sends_date_pk ON marketing_sends(sent_date);

CREATE INDEX idx_inventory_product_id ON inventory_adjustments(product_id);

CREATE INDEX idx_products_id ON products(product_id);



/* =====================================================================
   7) VISTAS ANALÍTICAS – KPI
   ===================================================================== */

/* Ventas por cliente */
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
JOIN customers c ON c.customer_pk = o.customer_pk
GROUP BY
    c.customer_id, c.full_name, c.email, c.country;

/* Ventas por país */
CREATE OR REPLACE VIEW vw_sales_by_country AS
SELECT
    c.country,
    SUM(o.total_amount) AS total_sales,
    COUNT(o.order_pk) AS total_orders,
    AVG(o.total_amount) AS avg_ticket
FROM orders o
JOIN customers c ON c.customer_pk = o.customer_pk
GROUP BY c.country;

/* Ticket promedio global */
CREATE OR REPLACE VIEW vw_avg_ticket AS
SELECT AVG(total_amount) AS avg_ticket_global
FROM orders;

/* Ventas por fecha */
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
JOIN dim_date d ON o.date_pk = d.date_pk
GROUP BY
    o.date_pk, d.year, d.month, d.month_name, d.week_of_year, d.day_of_month
ORDER BY o.date_pk;

/* Reviews por producto */
CREATE OR REPLACE VIEW vw_reviews_by_product AS
SELECT
    p.product_id,
    p.product_name,
    COUNT(r.review_pk) AS total_reviews,
    AVG(r.rating) AS avg_rating
FROM products p
LEFT JOIN reviews r ON p.product_id = r.product_id
GROUP BY p.product_id, p.product_name;

