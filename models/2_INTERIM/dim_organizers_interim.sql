/*********************************************************
** DESCRIPTION: This model combines ORGANIZERS from 
** internal sales data and partnership aggreements
**********************************************************/

WITH organizers_from_internal_sales AS (
    SELECT DISTINCT
        organizer_id
    FROM {{ ref("internal_ticket_sales_interim") }}
    where organizer_id IS NOT NULL 
),
organizers_from_internal_partnerships_table AS (
    SELECT  
        organizer_id,
        organizer_name,
        organizer_company_sufix,
        organizer_address,
    FROM {{ ref("internal_partnership_aggreements_interim") }}
    QUALIFY ROW_NUMBER() OVER(PARTITION BY organizer_id ORDER BY organizer_id) = 1
)
SELECT
    COALESCE(partn.organizer_id, inter.organizer_id) AS organizer_id,
    COALESCE(organizer_name,'N/A') AS organizer_name,
    COALESCE(organizer_company_sufix,'N/A') AS organizer_company_sufix,
    COALESCE(organizer_address,'N/A') AS organizer_address
FROM organizers_from_internal_partnerships_table partn
FULL OUTER JOIN organizers_from_internal_sales inter
ON partn.organizer_id = inter.organizer_id
ORDER BY organizer_id