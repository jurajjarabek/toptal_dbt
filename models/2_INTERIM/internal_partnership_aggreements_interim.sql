SELECT 
    concat('O',organizer_id) AS organizer_id,
    organizer_name,
    organizer_company_sufix,
    organizer_address,
    concat('R',reseller_id) as reseller_id,
    reseller_name,
    reseller_location,
    commission_rate
FROM {{ ref('internal_partnership_aggreements_data') }} 