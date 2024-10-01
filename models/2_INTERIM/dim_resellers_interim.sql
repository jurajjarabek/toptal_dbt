/*********************************************************
** DESCRIPTION: This model combines EVENTs from 
** both external resellers sales data and internal sales data
**********************************************************/

WITH resellers_from_external_sales AS (
    SELECT
        reseller_id,
        Office_location as reseller_location
    FROM {{ ref("external_ticket_sales_interim") }}
    QUALIFY ROW_NUMBER() OVER(PARTITION BY reseller_id ORDER BY Created_Date) = 1
),
resellers_from_internal_partnerships_table AS (
    SELECT  
        reseller_id,
        reseller_name,
        reseller_location
    FROM {{ ref("internal_partnership_aggreements_data") }}
    QUALIFY ROW_NUMBER() OVER(PARTITION BY reseller_id ORDER BY reseller_id) = 1
)
SELECT
    row_number() OVER() AS row_number,
    COALESCE(inter.reseller_id, exter.reseller_id) AS reseller_id,
    COALESCE(reseller_name,'N/A') AS reseller_name,
    COALESCE(inter.reseller_location,exter.reseller_location,'N/A') AS reseller_location
FROM resellers_from_internal_partnerships_table inter
FULL OUTER JOIN resellers_from_external_sales exter
ON inter.reseller_id = exter.reseller_id
