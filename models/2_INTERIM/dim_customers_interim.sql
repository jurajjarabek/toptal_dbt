/*********************************************************
** DESCRIPTION: This model combines customers from 
** both external resellers sales data and internal sales data
**********************************************************/

WITH customer_from_internal_sales AS (
    SELECT
        customer_id,
        customer_name   
    FROM {{ ref("internal_ticket_sales_interim") }}
    QUALIFY ROW_NUMBER() OVER(PARTITION BY customer_id ORDER BY sale_date) = 1
),
customer_from_external_sales AS (
    SELECT
        Customer_name AS customer_name   
    FROM {{ ref("external_ticket_sales_interim") }}
    QUALIFY ROW_NUMBER() OVER(PARTITION BY Customer_name ORDER BY Created_Date) = 1
),
combined_customers AS (
    SELECT
        row_number() OVER() AS row_number,
        CASE WHEN inter.customer_id IS NOT NULL THEN inter.customer_id 
        ELSE NULL END AS customer_id,
        inter.customer_name AS customer_name_inter,
        exter.customer_name AS customer_name_exter
    FROM customer_from_internal_sales inter
    FULL OUTER JOIN customer_from_external_sales exter
    ON inter.customer_name = exter.customer_name
)
SELECT
    -- in case customer_id is missing we then need to autogenerate some key
    -- This macro concatenates the fields and applies a cryptographic hash (using MD5 by default) to produce a unique ID
    COALESCE(customer_id,
    {{ dbt_utils.generate_surrogate_key(['row_number', 'customer_name_inter','customer_name_exter']) }}) AS customer_id,
    COALESCE(customer_name_inter ,customer_name_exter) AS customer_name
FROM combined_customers
ORDER BY customer_id