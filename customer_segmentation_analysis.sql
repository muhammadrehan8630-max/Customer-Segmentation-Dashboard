CREATE TABLE online_retail (
    InvoiceNo TEXT,
    StockCode TEXT,
    Description TEXT,
    Quantity INT,
    InvoiceDate TIMESTAMP,
    UnitPrice NUMERIC,
    CustomerID TEXT,
    Country TEXT
);

copy online_retail
FROM 'D:\Downloads\sql_dataset\OnlineRetail.csv'
DELIMITER ','
CSV HEADE
ENCODING 'LATIN1';

SELECT * FROM online_retail;


SELECT
	COUNT(*) FROM online_retail;


SELECT
	COUNT(DISTINCT customerid) 
FROM online_retail;


SELECT
	MIN(invoicedate), MAX(invoicedate)
FROM ONLINE_RETAIL;


SELECT * 
FROM online_retail
WHERE quantity <= 0
	OR invoiceno LIKE 'C%'
	OR customerid IS NULL;


CREATE TABLE clean_retail AS 
SELECT * 
FROM online_retail
WHERE quantity > 0
	AND invoiceno NOT LIKE 'C%'
	AND customerid IS NOT NULL;


SELECT 
	COUNT(*) FROM clean_retail;


SELECT 
	customerid,
	SUM(quantity * unitprice) AS 
	revenue
FROM clean_retail
GROUP BY customerid;


SELECT 
	customerid,
	COUNT(DISTINCT invoiceno) AS 
	total_orders
FROM clean_retail
GROUP BY customerid;


SELECT 
	customerid,
	MAX(invoicedate) AS 
	last_order_date
FROM clean_retail
GROUP BY customerid;


SELECT 
	customerid,
	CURRENT_DATE - MAX(invoicedate :: DATE) AS
	recency_days
FROM clean_retail
GROUP BY customerid;


CREATE TABLE final_segment AS
WITH base AS(
SELECT 
	customerid,
	COUNT(DISTINCT invoiceno) AS 
	frequency,
	SUM(quantity * unitprice) AS 
	monetary,
	CURRENT_DATE - MAX(invoicedate :: DATE) AS
	recency_days
FROM clean_retail
GROUP BY customerid),

rfm AS (SELECT 
	customerid,
	recency_days,
	frequency,
	monetary,

	NTILE(5) OVER(ORDER  BY recency_days DESC) AS 
	r_score,
	NTILE(5) OVER(ORDER  BY frequency) AS
	f_score,
	NTILE(5) OVER(ORDER  BY monetary) AS
	m_score

FROM base),

segments AS(SELECT *,
	CASE 
WHEN r_score <= 2 AND f_score = 1 THEN 'lost'
WHEN r_score <= 2 AND f_score >= 2 THEN 'at risk repeat'
WHEN r_score >= 3 AND f_score = 1 THEN 'new'
WHEN r_score >= 3 AND f_score >= 2 THEN 'active repeat'
ELSE 'others'
END AS segment
FROM rfm)
SELECT *
FROM segments;
	

SELECT
	r_score,
	f_score,
	m_score,
	COUNT(*)
FROM final_segment
WHERE segment = 'others'
GROUP BY r_score, f_score, m_score
ORDER BY COUNT(*) DESC;


SELECT 
	segment, COUNT(*) 
FROM final_segment
GROUP BY segment
ORDER BY COUNT(*) DESC;


SELECT 
	segment,
	SUM(monetary) AS 
	total_revenue
FROM final_segment
GROUP BY segment
ORDER BY total_revenue DESC;


SELECT 
	segment,
	COUNT(*) AS customers
FROM final_segment
GROUP BY segment
ORDER BY customers;


SELECT 
	segment,
	SUM(monetary) AS revenue
FROM final_segment
GROUP BY segment
ORDER BY revenue;


SELECT 
	CASE WHEN frequency = 1
THEN 'one_time'
	ELSE 'repeat_customer'
	END AS type,
	COUNT(*)
FROM final_segment
GROUP BY 	CASE WHEN frequency = 1
THEN 'one_time'
	ELSE 'repeat_customer'
	END;


SELECT 
	segment,
	COUNT(*) AS customers,
	SUM(monetary) AS revenue,
	ROUND(AVG(monetary), 2) AS 
	av_spend
FROM final_segment
GROUP BY segment
ORDER BY revenue DESC;


SELECT *
FROM final_segment
ORDER BY monetary DESC
LIMIT 10;


