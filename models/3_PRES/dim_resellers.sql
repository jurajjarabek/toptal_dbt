/*********************************************************
** DESCRIPTION: Final RESELLERS DIM table
**********************************************************/

SELECT
    reseller_id,
    reseller_name,
    reseller_location
FROM {{ ref('dim_resellers_interim') }} 
ORDER BY reseller_id