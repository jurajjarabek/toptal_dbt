

SELECT 
    sale_id,
    organizer_id,
    reseller_id,
    customer_id,
    event_id,

    type_of_sale,
    sale_date, 
    sale_channel,

    ticket_quantity,
    total_amount,
    commission_rate,
    commission_amount
FROM {{ ref("internal_ticket_sales_transformed") }} internal

UNION ALL

SELECT 
    sale_id,
    organizer_id,
    reseller_id,
    customer_id,
    event_id,

    type_of_sale,
    sale_date, 
    sale_channel,

    ticket_quantity,
    total_amount,
    commission_rate,
    commission_amount
FROM {{ ref("external_ticket_sales_transformed") }} external

ORDER BY sale_date, sale_id

