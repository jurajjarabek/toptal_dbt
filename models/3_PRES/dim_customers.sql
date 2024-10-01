/*********************************************************
** DESCRIPTION: Final Customers DIM table
**********************************************************/

SELECT
    customer_id,
    customer_name,
    SPLIT(customer_name, ' ')[OFFSET(0)] AS customer_first_name,
    TRIM(REGEXP_EXTRACT(customer_name, r'^[^ ]+ (.+)$')) AS customer_last_name
    -- => everything after the first space as the last name, regardless of how many words are in the last name.
FROM {{ ref('dim_customers_interim') }} 
ORDER BY customer_id