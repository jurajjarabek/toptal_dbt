/*********************************************************
** DESCRIPTION: Final EVENTS DIM table
**********************************************************/

SELECT
    event_id,
    event_name,
    ticket_price as event_ticket_price,
    event_organizer_id
FROM {{ ref('dim_events_interim') }} 
ORDER BY event_id