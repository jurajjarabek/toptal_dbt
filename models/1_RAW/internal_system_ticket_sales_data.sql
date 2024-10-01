

SELECT 
    sale_id,
    organizer_id,
    reseller_id,
    event_id,
    event_name,
    type_of_sale,
    ticket_quantity,
    total_amount,
    sale_date,
    sale_channel,
    customer_id,
    customer_name
FROM {{ source('ticket_platform_data', 'sales_extract') }} 
-- loading from source using source macro