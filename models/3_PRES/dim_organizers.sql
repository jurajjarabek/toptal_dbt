/*********************************************************
** DESCRIPTION: Final ORGANIZERS DIM table
**********************************************************/

SELECT
    organizer_id,
    organizer_name,
    organizer_company_sufix,
    organizer_address
FROM {{ ref('dim_organizers_interim') }} 
ORDER BY organizer_id