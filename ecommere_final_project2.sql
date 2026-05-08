-- ECOMMERCE SALES ANALYSIS PROJECT
-- PostgreSQL SQL Project

-- DATA IMPORT AND TABLE CREATION

CREATE TABLE customers (
    customer_id TEXT PRIMARY KEY,
    customer_unique_id TEXT,
    customer_zip_code_prefix INT,
    customer_city TEXT,
    customer_state TEXT
);

COPY customers
FROM 'D:\Data Analytics Sql Projects\olist_customers_dataset.csv'
DELIMITER ','
CSV HEADER;


CREATE TABLE orders (
    order_id TEXT PRIMARY KEY,
    customer_id TEXT,
    order_status TEXT,
    order_purchase_timestamp TIMESTAMP,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP
);


COPY orders
FROM 'D:\Data Analytics Sql Projects\olist_orders_dataset.csv'
DELIMITER ','
CSV HEADER;


CREATE TABLE order_items (
    order_id TEXT,
    order_item_id INT,
    product_id TEXT,
    seller_id TEXT,
    shipping_limit_date TIMESTAMP,
    price NUMERIC,
    freight_value NUMERIC
);


COPY order_items
FROM 'D:\Data Analytics Sql Projects\olist_order_items_dataset.csv'
DELIMITER ','
CSV HEADER;


CREATE TABLE payments (
    order_id TEXT,
    payment_sequential INT,
    payment_type TEXT,
    payment_installments INT,
    payment_value NUMERIC
);


COPY payments
FROM 'D:\Data Analytics Sql Projects\olist_order_payments_dataset.csv'
DELIMITER ','
CSV HEADER;


CREATE TABLE products (
    product_id TEXT PRIMARY KEY,
    product_category_name TEXT,
    product_name_length INT,
    product_description_length INT,
    product_photos_qty INT,
    product_weight_g INT,
    product_length_cm INT,
    product_height_cm INT,
    product_width_cm INT
);


COPY products
FROM 'D:\Data Analytics Sql Projects\olist_products_dataset.csv'
DELIMITER ','
CSV HEADER;


CREATE TABLE reviews (
    review_id TEXT,
    order_id TEXT,
    review_score INT,
    review_comment_title TEXT,
    review_comment_message TEXT,
    review_creation_date TIMESTAMP,
    review_answer_timestamp TIMESTAMP
);


COPY reviews
FROM 'D:\Data Analytics Sql Projects\olist_order_reviews_dataset.csv'
DELIMITER ','
CSV HEADER;


--BUSINESS ANALYTICS QUERY

SELECT * 
FROM orders
LIMIT 5;


-- Total revenue from delivered orders

SELECT 
	SUM(o2.price + o2.freight_value) AS
	total_revenue
FROM orders o
JOIN order_items o2
ON o.order_id = o2.order_id
WHERE o.order_status = 'delivered';


-- Top customers by revenue

SELECT 
	o.customer_id,
	SUM(o2.price + o2.freight_value) AS
	total_revenue
FROM orders o
JOIN order_items o2
ON o.order_id = o2.order_id
WHERE o.order_status = 'delivered'
GROUP BY o.customer_id
ORDER BY total_revenue DESC
LIMIT 10;


-- Top product categories by revenue

SELECT 
	p.product_category_name,
	SUM(o1.price + o1.freight_value) AS
	total_revenue
FROM order_items o1
JOIN products p
ON o1.product_id = p.product_id
JOIN orders o
ON o1.order_id = o.order_id
WHERE o.order_status = 'delivered'
GROUP BY p.product_category_name
ORDER BY total_revenue DESC
LIMIT 10;


-- Top customer states by revenue

SELECT 
	c.customer_state,
	SUM(o1.price + o1.freight_value) AS
	total_revenue
FROM orders o
JOIN order_items o1
ON o1.order_id = o.order_id
JOIN customers c
ON o.customer_id = c.customer_id
WHERE o.order_status = 'delivered'
GROUP BY c.customer_state
ORDER BY total_revenue DESC
LIMIT 5;


-- Revenue analysis by customer segment

SELECT 
	segment, 
	SUM(c.total_revenue) AS 
	segment_revenue,
	COUNT(c.customer_unique_id) AS segment_cnt
FROM(
SELECT 
	t.customer_unique_id,
	t.total_revenue,
	t.total_orders,
	CASE WHEN t.total_orders = 1
	THEN 'one_time_customer'
	ELSE 'repeat'
	END AS segment
FROM
(SELECT 
	c.customer_unique_id,
	SUM(o1.price + o1.freight_value) AS
	total_revenue,
	COUNT(DISTINCT o.order_id) AS
	total_orders
FROM orders o 
JOIN order_items o1
ON o.order_id = o1.order_id
JOIN customers c
ON o.customer_id = c.customer_id
GROUP BY c.customer_unique_id)T
)c
GROUP BY segment
;


-- Top repeat customers by revenue

SELECT 
	c.customer_unique_id,
	SUM(o1.price + o1.freight_value) AS
	total_revenue,
	COUNT(DISTINCT o.order_id) AS 
	total_orders
FROM customers c
JOIN orders o
ON c.customer_id = o.customer_id
JOIN order_items o1
ON o.order_id = o1.order_id
WHERE o.order_status = 'delivered'
GROUP BY c.customer_unique_id
HAVING COUNT(DISTINCT o.order_id) >= 2
ORDER BY total_revenue DESC
LIMIT 10;


-- Monthly revenue trend analysis

SELECT 
	DATE_TRUNC('MONTH', o.order_purchase_timestamp) AS 
	month,
	SUM(o1.price + o1.freight_value) AS
	total_revenue
FROM orders o
JOIN order_items o1
ON o.order_id = o1.order_id
WHERE o.order_status = 'delivered'
GROUP BY 1
ORDER BY 1
;


-- Delivery performance analysis

SELECT 
	status,
	COUNT(*) AS
	total_orders,
	ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS 
	percentage
FROM
(SELECT 
	order_estimated_delivery_date AS
	estimated_delivery_date,
	order_delivered_customer_date AS
	actual_delivery,
	CASE WHEN order_delivered_customer_date  
	> order_estimated_delivery_date 
	THEN 'delayed'
	ELSE 'on_time'
END AS status
FROM orders
WHERE order_status = 'delivered')t
GROUP BY status
;


-- Order status vs customer review analysis

SELECT 
	status,
	COUNT(*) AS
	total_orders,
	ROUND(AVG(review_score), 2) AS 
	average_review_score	
FROM
(SELECT 
	r.review_score,
	order_estimated_delivery_date AS
	estimated_delivery_date,
	order_delivered_customer_date AS
	actual_delivery,
	CASE WHEN order_delivered_customer_date  
	> order_estimated_delivery_date 
	THEN 'delayed'
	ELSE 'on_time'
END AS status
FROM orders o
JOIN reviews r
ON o.order_id = r.order_id
WHERE order_status = 'delivered')t
GROUP BY status
;


-- Top sellers by revenue and orders

SELECT 
	o1.seller_id,
	SUM(o1.price + o1.freight_value) AS 
	total_revenue,
	COUNT(DISTINCT O.order_id) AS
	total_orders
FROM orders O 
JOIN order_items o1
ON o.order_id = o1.order_id
WHERE o.order_status = 'delivered'
GROUP BY o1.seller_id
ORDER BY total_revenue DESC 
LIMIT 10;


-- Product category review score analysis

SELECT 
	p.product_category_name,
	ROUND(AVG(r.review_score), 2) AS
	average_review_score,
	COUNT(*) AS
	total_count
FROM orders o
JOIN reviews r
ON o.order_id = r.order_id
JOIN order_items o1
ON o.order_id = o1.order_id
JOIN products p
ON p.product_id = o1.product_id
WHERE o.order_status = 'delivered'
GROUP BY p.product_category_name
HAVING COUNT(*) > 100
ORDER BY average_review_score DESC;


-- Payment type performance analysis

SELECT 
	p.payment_type,
	COUNT(DISTINCT o.order_id) AS 
	total_orders, 
	SUM(o1.price + o1.freight_value) AS 
	total_revenue,
	ROUND(AVG(o1.price + o1.freight_value), 2) AS
	average_order_value
FROM orders o
JOIN payments p
ON o.order_id = p.order_id
JOIN order_items o1 
ON o.order_id = o1.order_id
WHERE o.order_status = 'delivered'
GROUP BY p.payment_type
ORDER BY total_orders DESC
;


-- Monthly active customer trend

SELECT 
	DATE_TRUNC('MONTH', o.order_purchase_timestamp) AS 
	month,
	COUNT(DISTINCT c.customer_unique_id) AS 
	unique_customers
FROM orders o 
JOIN customers c
ON o.customer_id = c.customer_id
WHERE o.order_status = 'delivered'
GROUP BY DATE_TRUNC('MONTH', o.order_purchase_timestamp)
ORDER BY month;


-- Monthly revenue summary

SELECT 
	DATE_TRUNC('MONTH', o.order_purchase_timestamp) AS 
	month,
	SUM(o1.price + o1.freight_value) AS
	total_revenue
FROM orders o 
JOIN order_items o1
ON o.order_id = o1.order_id
WHERE o.order_status = 'delivered'
GROUP BY DATE_TRUNC('MONTH', o.order_purchase_timestamp)
ORDER BY MONTH;


-- Month-over-month revenue growth analysis

SELECT *,
	ROUND((total_revenue - prv_mon_rev) * 100.0 / prv_mon_rev, 2) AS
	growth_percentage
FROM
(SELECT
	month, 
	total_revenue,
	LAG(total_revenue) OVER (ORDER BY month) AS
	prv_mon_rev
FROM
(SELECT 
	DATE_TRUNC('MONTH', o.order_purchase_timestamp) AS 
	month,
	SUM(o1.price + o1.freight_value) AS
	total_revenue
FROM orders o 
JOIN order_items o1
ON o.order_id = o1.order_id
WHERE o.order_status = 'delivered'
GROUP BY DATE_TRUNC('MONTH', o.order_purchase_timestamp)
ORDER BY MONTH)t
);


-- Top cities by revenue and orders

SELECT
    c.customer_city,
    SUM(oi.price + oi.freight_value) AS
	total_revenue,
    COUNT(DISTINCT o.order_id) AS
	total_orders
FROM orders o
JOIN customers c
ON o.customer_id = c.customer_id
JOIN order_items oi
ON o.order_id = oi.order_id
WHERE o.order_status = 'delivered'
GROUP BY c.customer_city
ORDER BY total_revenue DESC
LIMIT 10;


-- Best performing product categories

SELECT
    p.product_category_name,
    SUM(oi.price + oi.freight_value) AS 
	total_revenue,
    COUNT(DISTINCT o.order_id) AS 
	total_orders
FROM orders o
JOIN order_items oi
ON o.order_id = oi.order_id
JOIN products p
ON oi.product_id = p.product_id
WHERE o.order_status = 'delivered'
GROUP BY p.product_category_name
ORDER BY total_revenue DESC
LIMIT 10;


-- Average delivery time by state

SELECT 
	c.customer_state,
	ROUND(AVG(
	EXTRACT(
	EPOCH FROM(o.order_delivered_customer_date-
	order_purchase_timestamp))
	/ 86400), 2) AS
	avg_delivery_days,
	COUNT(DISTINCT o.order_id) AS
	total_orders
FROM orders o
JOIN customers c
ON o.customer_id = c.customer_id
WHERE o.order_status = 'delivered'
GROUP BY 1
ORDER BY 2 DESC;


-- Top high-value customers

SELECT
	c.customer_unique_id,
	SUM(o1.price + o1.freight_value) AS 
	total_revenue
FROM customers c
JOIN orders o
ON o.customer_id = c.customer_id
JOIN order_items o1
ON o.order_id = o1.order_id
WHERE o.order_status = 'delivered' 
GROUP BY c.customer_unique_id
ORDER BY total_revenue DESC
LIMIT 10;


-- Sellers with fastest average delivery

SELECT 
	o1.seller_id,
	ROUND(AVG(
	EXTRACT(
	EPOCH FROM(o.order_delivered_customer_date-
	order_purchase_timestamp))
	/ 86400), 2) AS
	avg_delivery_days
FROM orders o
JOIN order_items o1
ON o.order_id = o1.order_id
WHERE o.order_status = 'delivered'
GROUP BY 1
ORDER BY 2 
LIMIT 10;


-- Sellers with fastest average delivery

SELECT 
	DATE_TRUNC('MONTH', order_purchase_timestamp) AS
	month,
	COUNT(order_id) AS total_orders,
	COUNT(CASE
	WHEN order_status = 'canceled' 
THEN 'order_id'
END) AS canceled_orders,
	ROUND(
	COUNT(CASE 
	WHEN order_status = 'canceled'
THEN order_id
END) * 100.0 /
COUNT(order_id), 2) AS 
	cancellation_percentage
FROM orders 
GROUP BY DATE_TRUNC('MONTH', order_purchase_timestamp) 
;


SELECT * FROM customers;
SELECT * FROM orders;
SELECT * FROM order_items;
SELECT * FROM payments;
SELECT * FROM products;
SELECT * FROM reviews;


-- Top rated sellers analysis

SELECT 
	o1.seller_id,
	ROUND(AVG(r.review_score), 2) AS
	average_review_score,
	COUNT(DISTINCT o1.order_id) AS
	total_orders
FROM order_items o1
JOIN reviews r
ON o1.order_id = r.order_id
JOIN orders o
ON o.order_id = o1.order_id
WHERE o.order_status = 'delivered'
GROUP BY o1.seller_id
HAVING COUNT(DISTINCT o1.order_id) >= 50
ORDER  BY average_review_score DESC
LIMIT 10;


-- Product category delivery time analysis

SELECT 
	p.product_category_name,
	ROUND(AVG(
	EXTRACT(
	EPOCH FROM(o.order_delivered_customer_date-
	order_purchase_timestamp))
	/ 86400), 2) AS
	avg_delivery_days,
	COUNT(DISTINCT o.order_id) AS 
	total_orders
FROM orders o
JOIN order_items o1
ON o.order_id = o1.order_id
JOIN PRODUCTS P
ON o1.product_id = p.product_id
WHERE o.order_status = 'delivered'
GROUP BY 1
HAVING COUNT(DISTINCT o.order_id) >= 100
ORDER BY 2 DESC
LIMIT 10;


-- Monthly repeat customer analysis

SELECT 
	month,
	COUNT(DISTINCT customer_unique_id) AS 
	total_customer,
	COUNT(DISTINCT CASE 
	WHEN total_orders > 1
THEN customer_unique_id 
END) AS
repeat_customers,
	ROUND(COUNT(
	DISTINCT CASE 
	WHEN total_orders > 1
THEN customer_unique_id
END) * 100.0 /
COUNT(
	DISTINCT customer_unique_id
),2) AS
repeat_percentage
FROM
(SELECT 
	c.customer_unique_id,
	DATE_TRUNC('MONTH', o.order_purchase_timestamp) AS 
	month,
	COUNT(o.order_id)
	OVER(PARTITION BY c.customer_unique_id) AS 
	total_orders
FROM orders o
join customers c
ON o.customer_id = c.customer_id
where o.order_status = 'delivered')t
GROUP BY month;




