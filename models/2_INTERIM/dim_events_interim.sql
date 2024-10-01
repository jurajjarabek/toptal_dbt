/*********************************************************
** DESCRIPTION: This model combines EVENTs from 
** both external resellers sales data and internal sales data
**********************************************************/

WITH events_from_internal_sales AS (
    SELECT
        event_id,
        event_name,
        organizer_id,
        ticket_price
    FROM {{ ref("internal_ticket_sales_interim") }}
    QUALIFY ROW_NUMBER() OVER(PARTITION BY event_id ORDER BY sale_date) = 1
),
events_from_external_sales AS (
    SELECT
        Event_name AS event_name,
        ticket_price
    FROM {{ ref("external_ticket_sales_interim") }}
    QUALIFY ROW_NUMBER() OVER(PARTITION BY Event_name ORDER BY Created_Date) = 1
),
combined_events AS (
    SELECT
        row_number() OVER() AS row_number,
        CASE WHEN inter.event_id IS NOT NULL THEN inter.event_id 
        ELSE NULL END AS event_id,
        organizer_id as event_organizer_id,
        inter.event_name AS event_name_inter,
        exter.event_name AS event_name_exter,
        inter.ticket_price AS ticket_price_inter,
        exter.ticket_price AS ticket_price_exter
    FROM events_from_internal_sales inter
    FULL OUTER JOIN events_from_external_sales exter
    ON inter.event_name = exter.event_name
)
SELECT
    -- in case event_id is missing we then need to autogenerate some key
    COALESCE(event_id,
    {{ dbt_utils.generate_surrogate_key(['row_number', 'event_name_inter','event_name_exter']) }}) AS event_id,
    event_organizer_id,
    COALESCE(event_name_inter,event_name_exter,'N/A') as event_name,
    COALESCE(ticket_price_inter,ticket_price_exter,0) as ticket_price
FROM combined_events
ORDER BY event_id