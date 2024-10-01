/*********************************************************
** DESCRIPTION: Interim External Sales FAC
**********************************************************/

{{ config(
    materialized = "table"
) }}

WITH src_data_cte as (
    SELECT 
        -- Meta fields
        REGEXP_EXTRACT(file_full_uri, r'/([^/]+)$') AS file_name,  -- Extracts the file name after the last '/'
        *
    FROM {{ ref('external_resellers_ticket_sales_data') }} 
)

SELECT
    file_full_uri,
    file_name,
    -- Extract the Name (first part before the first underscore)
    REGEXP_EXTRACT(file_name, r'^([^_]+)') AS file_description,
    -- Extract the Date (second part, after the first underscore)
    REGEXP_EXTRACT(file_name, r'_([0-9]{8})_') AS file_sale_date,
    -- Extract the ID (third part, before the .csv)
    concat('R',REGEXP_EXTRACT(file_name, r'_([0-9]+)\.csv$')) as reseller_id,

    -- CSV files fields
    Transaction_ID,
    Event_name,
    Number_of_purchased_tickets,
    Total_amount,
    Total_amount / Number_of_purchased_tickets AS ticket_price, -- calculate ticket price
    Sales_channel,
    Customer_first_name,
    Customer_last_name,
    concat(Customer_first_name, ' ',Customer_last_name) as Customer_name,
    Office_location,
    Created_Date

FROM src_data_cte
