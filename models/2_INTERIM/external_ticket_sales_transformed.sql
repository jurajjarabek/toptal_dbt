
WITH events_and_organizers AS (
    SELECT
        event_id,
        event_organizer_id as organizer_id,
        event_name -- we use this field for join
    FROM {{ ref("dim_events_interim") }}
),
customers AS (
    SELECT
        customer_id,
        customer_name -- we use this field for join
    FROM {{ ref("dim_customers_interim") }}
),
commision_rates AS (
    SELECT
        organizer_id,
        reseller_id,
        commission_rate
    FROM {{ ref("internal_partnership_aggreements_interim") }}
)
SELECT
    -- keeping IDs only
    {{ dbt_utils.generate_surrogate_key(['Transaction_ID', 'sales.reseller_id','file_sale_date']) }} AS sale_id,
    COALESCE(events.organizer_id,'-1') AS organizer_id,
    sales.reseller_id AS reseller_id,
    COALESCE(customers.customer_id,'-1') as customer_id,
    COALESCE(events.event_id,'-1') as event_id,
    -- relevant fields in FAC
    'reseller' as type_of_sale,
    PARSE_DATE('%m%d%Y', file_sale_date) as sale_date, 
    Sales_channel as sale_channel,
    -- metrics
    Number_of_purchased_tickets as ticket_quantity,
    Total_amount as total_amount,
    COALESCE(commission_rate,0.1) as commission_rate, -- In case commision rate missing it is 10 % by default
    total_amount * COALESCE(commission_rate,0.1) as commission_amount
FROM {{ ref("external_ticket_sales_interim") }} sales
LEFT JOIN events_and_organizers events
ON sales.event_name = events.event_name
LEFT JOIN customers customers
ON sales.Customer_name = customers.customer_name
LEFT JOIN commision_rates rates 
ON events.organizer_id = rates.organizer_id and sales.reseller_id = rates.reseller_id