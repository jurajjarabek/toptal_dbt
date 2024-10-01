/*********************************************************
** DESCRIPTION: This model combines RESELERS from 
** external resellers sales data, internal sales data, and partnership aggreements
**********************************************************/

WITH resellers_from_external_sales AS (
    SELECT
        reseller_id,
        Office_location as reseller_location
    FROM {{ ref("external_ticket_sales_interim") }}
    QUALIFY ROW_NUMBER() OVER(PARTITION BY reseller_id ORDER BY Created_Date) = 1
),
resellers_from_internal_sales AS (
    SELECT DISTINCT
        reseller_id
    FROM {{ ref("internal_ticket_sales_interim") }}
    where reseller_id IS NOT NULL -- we have nulls for DIRECT sales (Not reseller sales)
),
resellers_from_internal_partnerships_table AS (
    SELECT  
        reseller_id,
        reseller_name,
        reseller_location
    FROM {{ ref("internal_partnership_aggreements_interim") }}
    QUALIFY ROW_NUMBER() OVER(PARTITION BY reseller_id ORDER BY reseller_id) = 1
)
SELECT
    COALESCE(partn.reseller_id, inter.reseller_id, exter.reseller_id) AS reseller_id,
    COALESCE(reseller_name,'N/A') AS reseller_name,
    COALESCE(partn.reseller_location,exter.reseller_location,'N/A') AS reseller_location
FROM resellers_from_internal_partnerships_table partn
FULL OUTER JOIN resellers_from_external_sales exter
ON partn.reseller_id = exter.reseller_id
FULL OUTER JOIN resellers_from_internal_sales inter
ON partn.reseller_id = inter.reseller_id