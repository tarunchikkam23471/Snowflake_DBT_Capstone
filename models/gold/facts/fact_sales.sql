WITH source AS (

    SELECT

        orders.*,

        order_items.product_id,
        order_items.quantity,
        order_items.unit_price,
        order_items.item_discount_amount,

        product.cost_price AS prodCostPrice,

        stores.region AS region,

        customers.segment AS AgeSegment

    FROM {{ ref('stg_orders_data') }} orders

    INNER JOIN {{ ref('stg_order_items') }} order_items
        ON orders.order_id = order_items.order_id

    INNER JOIN {{ ref('dim_product') }} product
        ON order_items.product_id = product.ProductID

    INNER JOIN {{ ref('dim_customer') }} customers
        ON orders.customer_id = customers.CustomerID

    LEFT JOIN {{ ref('dim_store') }} stores
        ON orders.store_id = stores.StoreID

),

transformed AS (

    SELECT

        {{ dbt_utils.generate_surrogate_key(['order_id','product_id']) }} AS SalesKey,

        order_id AS OrderID,

        {{ dbt_utils.generate_surrogate_key(['customer_id']) }} AS CustomerKey,

        {{ dbt_utils.generate_surrogate_key(['product_id']) }} AS ProductKey,

        {{ dbt_utils.generate_surrogate_key(['store_id']) }} AS StoreKey,

        {{ dbt_utils.generate_surrogate_key(["TO_DATE(order_date)"]) }} AS DateKey,

        {{ dbt_utils.generate_surrogate_key(['employee_id']) }} AS EmployeeKey,

        {{ dbt_utils.generate_surrogate_key(['campaign_id']) }} AS CampaignKey,

        quantity AS Quantity_Sold,

        unit_price AS Unit_Price,

        quantity * unit_price AS Total_Sales_Amount,

        quantity * prodCostPrice AS Cost_Amount,

        (
            (quantity * unit_price)
            - (quantity * prodCostPrice)
            - item_discount_amount
            - (shipping_cost/total_items)
        ) AS Profit_Amount,

        item_discount_amount AS Discount_Amount,

        shipping_cost AS Shipping_Cost,

        region,

        CASE
            WHEN order_source = 'in-store' THEN 'In-Store'
            ELSE 'Online'
        END AS Sales_Channel,

        AgeSegment AS Customer_Segment_Impact

    FROM source

)

SELECT *
FROM transformed