SELECT DISTINCT
    c.customer_id, 
    c.first_name, 
    c.last_name,
    c.email, 
    o.order_id,

    -- Total order value
    (SELECT SUM(ol_inner.price) 
     FROM order_line ol_inner 
     WHERE ol_inner.order_id = o.order_id) AS total_value,

    -- Number of items
    (SELECT COUNT(*) 
     FROM order_line ol_inner 
     WHERE ol_inner.order_id = o.order_id) AS total_items,

    -- Days since last status update
    CASE 
        WHEN EXISTS (
            SELECT 1 FROM order_history oh_inner 
            WHERE oh_inner.order_id = o.order_id
        ) THEN 
            ROUND(CURRENT_DATE - (
                SELECT MAX(oh_inner.status_date) 
                FROM order_history oh_inner 
                WHERE oh_inner.order_id = o.order_id
            ), 0)
        ELSE NULL
    END AS days_since_last_status

FROM 
    customer c
JOIN 
    cust_order o ON o.customer_id = c.customer_id

-- Filter by country
WHERE c.customer_id IN (
    SELECT ca.customer_id
    FROM customer_address ca
    JOIN address a ON a.address_id = ca.address_id
    JOIN country co ON a.country_id = co.country_id
    WHERE co.country_id = 42
)

ORDER BY 
    c.customer_id, o.order_id

-- Only the first 5 rows are shown
-- FETCH FIRST 5 ROWS ONLY;