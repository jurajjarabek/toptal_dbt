/*********************************************************
** DESCRIPTION: Interim Internal Sales FAC
**********************************************************/

SELECT
    sale_id,
    concat('O',organizer_id) AS organizer_id,
    concat('R',reseller_id) as reseller_id,
    concat('C',event_id) as customer_id,
    customer_name, -- to be removed
    concat('E',event_id) as event_id,
    event_name, -- to be removed
    type_of_sale,
    ticket_quantity,
    total_amount / ticket_quantity AS ticket_price, -- calculate ticket price
    total_amount,
    EXTRACT(DATE FROM sale_date) as sale_date, -- convert to date
    sale_channel
FROM {{ ref("internal_system_ticket_sales_data") }}