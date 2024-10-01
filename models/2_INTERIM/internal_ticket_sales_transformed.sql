
WITH commision_rates AS (
    SELECT
        organizer_id,
        reseller_id,
        commission_rate
    FROM {{ ref("internal_partnership_aggreements_interim") }}
)
SELECT
    -- keeping IDs only
    Concat('INT_',sale_id) as sale_id,
    sales.organizer_id AS organizer_id,
    sales.reseller_id AS reseller_id,
    customer_id,
    event_id,
    -- relevant fields in FAC
    type_of_sale,
    sale_date, 
    sale_channel,
    -- metrics
    ticket_quantity,
    total_amount,
    commission_rate,
    total_amount * COALESCE(commission_rate,0) as commission_amount
FROM {{ ref("internal_ticket_sales_interim") }} sales
LEFT JOIN commision_rates rates 
ON sales.organizer_id = rates.organizer_id and sales.reseller_id = rates.reseller_id

-- TODO join the conversion rate