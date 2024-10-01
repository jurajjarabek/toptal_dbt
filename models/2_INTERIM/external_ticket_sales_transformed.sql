SELECT

    file_sale_date,
    reseller_id,

    Transaction_ID, -- make unique
    Event_name, -- join dim_event to get ID and then delete
    Number_of_purchased_tickets,
    Total_amount,
    ticket_price, -- to be deleted
    Sales_channel,
    --Customer_first_name,
    --Customer_last_name,
    Customer_name, -- join dim_customers to get ID
    --Office_location,
    -- Created_Date -- no needed

FROM src_data_cte
