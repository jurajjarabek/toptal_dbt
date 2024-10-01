{{ config(
    materialized = "table",
    schema='1_RAW'
) }}


SELECT
    -- full URI of the file in Google Cloud Storage, including the filename
    _FILE_NAME as file_full_uri,
    -- CSV files fields
    Transaction_ID,
    Event_name,
    Number_of_purchased_tickets,
    Total_amount,
    Sales_channel,
    Customer_first_name,
    Customer_last_name,
    Office_location,
    Created_Date

-- loading from source using source macro
FROM {{ source('cloud_storage', 'external_resellers_ticket_sales_data_EXT') }}
