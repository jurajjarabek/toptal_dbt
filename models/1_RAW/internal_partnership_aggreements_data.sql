SELECT 
    organizer_id,
    organizer_name,
    organizer_company_sufix,
    organizer_address,
    reseller_id,
    reseller_name,
    reseller_location,
    commission_rate
FROM {{ source('cloud_storage', 'internal_partnership_aggreements_data_EXT') }} 
-- loading from source using source macro