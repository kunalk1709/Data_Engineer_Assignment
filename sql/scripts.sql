# Basic Transformation Using JOINS
SELECT
    o.order_id,
    o.order_date,
    o.customer_id,
    c.customer_name,
    c.customer_segment,
    o.product_id,
    p.product_name,
    p.category,
    o.quantity,
    o.unit_price,
    o.quantity * o.unit_price AS order_amount
FROM tbl_fact_order o
LEFT JOIN tbl_dim_customer c
    ON o.customer_id = c.customer_id
LEFT JOIN tbl_dim_product p
    ON o.product_id = p.product_id
WHERE o.order_date IS NOT NULL;

# Aggregation Query
SELECT
    p.category,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(o.quantity * o.unit_price) AS total_revenue,
    SUM(o.quantity * (o.unit_price - p.cost_price)) AS total_profit
FROM tbl_fact_order o
JOIN tbl_dim_product p
    ON o.product_id = p.product_id
WHERE o.status = 'completed'
GROUP BY p.category;

# Window Function Example – Running Total
SELECT
    o.customer_id,
    o.order_date,
    o.order_id,
    o.quantity * o.unit_price AS order_amount,
    SUM(o.quantity * o.unit_price)
        OVER (
            PARTITION BY o.customer_id
            ORDER BY o.order_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS running_revenue
FROM tbl_fact_order o
WHERE o.status = 'completed';
