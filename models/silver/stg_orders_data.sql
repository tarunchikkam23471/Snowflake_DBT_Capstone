WITH order_items AS (

SELECT *
FROM {{ ref('stg_order_items') }}

),

aggregated_items AS (

SELECT

order_id,

COUNT(product_id) AS total_items,

SUM(quantity) AS total_quantity,

SUM(quantity * unit_price) AS items_revenue,

SUM(quantity * cost_price) AS total_cost,

SUM(item_discount_amount) AS total_item_discount

FROM order_items
GROUP BY order_id

),

orders_base AS (

SELECT DISTINCT

order_id,
customer_id,
employee_id,
campaign_id,
store_id,

order_date,
created_at,
shipping_date,
delivery_date,
estimated_delivery_date,

order_source,
order_status,
payment_method,
shipping_method,

order_discount_amount,
shipping_cost,
tax_amount,
total_amount

FROM order_items

),

final AS (

SELECT
o.*,
a.total_items,
a.total_quantity,
a.items_revenue,
a.total_cost,
a.total_item_discount,


(o.total_amount - 
 (a.total_cost - a.total_item_discount - o.shipping_cost - o.tax_amount)
) AS profit_amount,

CASE
WHEN o.total_amount > 0 THEN
(
(o.total_amount -
 (a.total_cost - a.total_item_discount - o.shipping_cost - o.tax_amount)
) / o.total_amount
) * 100
END AS profit_margin_percentage,


CASE
WHEN EXTRACT(HOUR FROM order_date) BETWEEN 5 AND 11 THEN 'Morning'
WHEN EXTRACT(HOUR FROM order_date) BETWEEN 12 AND 16 THEN 'Afternoon'
WHEN EXTRACT(HOUR FROM order_date) BETWEEN 17 AND 21 THEN 'Evening'
ELSE 'Night'
END AS order_time_of_day,


DATE_TRUNC('week', order_date) AS order_week,
DATE_TRUNC('month', order_date) AS order_month,
DATE_TRUNC('quarter', order_date) AS order_quarter,
DATE_TRUNC('year', order_date) AS order_year,


DATEDIFF(day, order_date, shipping_date) AS processing_days,

DATEDIFF(day, shipping_date, delivery_date) AS shipping_days,

CASE

WHEN delivery_date IS NOT NULL
AND delivery_date <= estimated_delivery_date
THEN 'On Time'

WHEN delivery_date IS NOT NULL
AND delivery_date > estimated_delivery_date
THEN 'Delayed'

WHEN delivery_date IS NULL
AND CURRENT_DATE > estimated_delivery_date
THEN 'Potentially Delayed'

ELSE 'In Transit'

END AS delivery_status

FROM orders_base o
JOIN aggregated_items a
USING(order_id)

)

SELECT *
FROM final