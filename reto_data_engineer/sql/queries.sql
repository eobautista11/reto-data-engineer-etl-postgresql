/* =====================================================================
   CONSULTAS SQL – RETO DATA ENGINEER
   Autor: Eddy Oliva Bautista
   Objetivo:
   - Validar contenido de tablas
   - Exponer valor analítico
   - Responder KPIs del negocio
   ===================================================================== */


/* =====================================================================
   1) VALIDACIÓN DE CARGA – TABLAS BASE
   ===================================================================== */

SELECT COUNT(*) AS total_customers FROM customers;
SELECT COUNT(*) AS total_orders FROM orders;
SELECT COUNT(*) AS total_reviews FROM reviews;
SELECT COUNT(*) AS total_competitor_pricing FROM competitor_pricing;
SELECT COUNT(*) AS total_support_tickets FROM support_tickets;
SELECT COUNT(*) AS total_marketing_sends FROM marketing_sends;
SELECT COUNT(*) AS total_campaigns FROM campaigns;
SELECT COUNT(*) AS total_inventory_adjustments FROM inventory_adjustments;
SELECT COUNT(*) AS total_products FROM products;
SELECT COUNT(*) AS total_dim_products FROM dim_products;


/* =====================================================================
   2) VALIDACIÓN DE DIMENSIONES
   ===================================================================== */

SELECT * FROM dim_date LIMIT 10;
SELECT COUNT(*) AS total_dim_date FROM dim_date;

SELECT * FROM dim_products ORDER BY product_pk;


/* =====================================================================
   3) TABLAS BASE – INSPECCIÓN DE DATOS
   ===================================================================== */

SELECT * FROM customers LIMIT 20;
SELECT * FROM orders LIMIT 20;
SELECT * FROM products LIMIT 20;


/* =====================================================================
   4) CONSULTAS PRINCIPALES – RETO OFICIAL
   ===================================================================== */

/* Total de ventas por cliente */
SELECT
    c.customer_id,
    c.full_name,
    SUM(o.total_amount) AS total_sales
FROM orders o
JOIN customers c ON c.customer_pk = o.customer_pk
GROUP BY c.customer_id, c.full_name
ORDER BY total_sales DESC;

/* Número de órdenes por país */
SELECT
    c.country,
    COUNT(o.order_pk) AS total_orders
FROM orders o
JOIN customers c ON c.customer_pk = o.customer_pk
GROUP BY c.country
ORDER BY total_orders DESC;

/* Ticket promedio */
SELECT
    AVG(total_amount) AS avg_ticket
FROM orders;


/* =====================================================================
   5) CONSULTAS PRINCIPALES – KPIs EXTENDIDOS
   ===================================================================== */

/* Ticket promedio por país */
SELECT
    c.country,
    AVG(o.total_amount) AS avg_ticket
FROM orders o
JOIN customers c ON c.customer_pk = o.customer_pk
GROUP BY c.country;

/* Ticket promedio por cliente */
SELECT
    c.customer_id,
    AVG(o.total_amount) AS avg_ticket
FROM orders o
JOIN customers c ON c.customer_pk = o.customer_pk
GROUP BY c.customer_id;

/* Total de órdenes por fecha */
SELECT
    date(order_date) AS order_date,
    COUNT(*) AS total_orders
FROM orders
GROUP BY date(order_date)
ORDER BY order_date;


/* =====================================================================
   6) CONSULTAS AVANZADAS – VIEWS
   ===================================================================== */

/* Ventas por cliente */
SELECT * FROM vw_sales_by_customer;

/* Ventas por país */
SELECT * FROM vw_sales_by_country;

/* Ticket promedio global */
SELECT * FROM vw_avg_ticket;

/* Ventas por fecha */
SELECT * FROM vw_sales_by_date ORDER BY date_pk;

/* Reviews por producto */
SELECT * FROM vw_reviews_by_product;

/* Competencia por producto */
SELECT * FROM vw_competitors_by_product;

/* Inventario por producto */
SELECT * FROM vw_inventory_by_product;

/* Marketing – performance */
SELECT * FROM vw_marketing_performance;


/* =====================================================================
   7) CONTROL DE CALIDAD – DATOS INVÁLIDOS
   ===================================================================== */

/* Órdenes sin cliente asociado */
SELECT *
FROM orders
WHERE customer_pk NOT IN (SELECT customer_pk FROM customers);

/* Clientes sin órdenes */
SELECT *
FROM customers
WHERE customer_pk NOT IN (SELECT customer_pk FROM orders);

/* Órdenes con montos negativos */
SELECT *
FROM orders
WHERE total_amount < 0;

/* Correos duplicados */
SELECT email, COUNT(*)
FROM customers
GROUP BY email
HAVING COUNT(*) > 1;

/* Campos nulos críticos */
SELECT *
FROM orders
WHERE order_id IS NULL OR total_amount IS NULL;


/* =====================================================================
   8) DATA PROFILING – ESTADÍSTICAS RÁPIDAS
   ===================================================================== */

SELECT
    MIN(order_date) AS first_order,
    MAX(order_date) AS last_order,
    AVG(total_amount) AS avg_order_value,
    SUM(total_amount) AS total_revenue
FROM orders;

SELECT
    COUNT(*) AS total_customers,
    COUNT(DISTINCT country) AS unique_countries
FROM customers;

