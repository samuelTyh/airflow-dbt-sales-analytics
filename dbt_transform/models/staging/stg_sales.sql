with source as (
    select * from raw.sales
),

cleaned as (
    select
        "SaleID"::integer as sale_id,
        nullif("ProductID", '')::integer as product_id,
        "ProductName" as product_name,
        "Brand" as brand,
        "Category" as category,
        "RetailerID"::integer as retailer_id,
        "RetailerName" as retailer_name,
        "Channel" as channel,
        coalesce(nullif("Location", ''), 'Unknown') as location,
        case 
            when "Quantity" ~ '^-?\d+$' then "Quantity"::integer
            else null
        end as quantity,
        case
            when "Price" ~ '^\d+$' then "Price"::decimal
            when "Price" ~ '^\d+USD$' then replace("Price", 'USD', '')::decimal
            else null
        end as price,
        case
            when "Date" ~ '^\d{4}-\d{2}-\d{2}$' then "Date"::date
            when "Date" ~ '^\d{4}/\d{2}/\d{2}$' then to_date("Date", 'YYYY/MM/DD')
            else null
        end as date,
        batch_id,
        source_file,
        inserted_at
    from source
),

final as (
    select
        sale_id,
        product_id,
        product_name,
        brand,
        category,
        retailer_id,
        retailer_name,
        channel,
        location,
        case when quantity <= 0 then null else quantity end as quantity,
        price,
        date,
        batch_id,
        source_file,
        inserted_at,
        current_timestamp as transformed_at
    from cleaned
    where 
        sale_id is not null
        and product_id is not null
        and retailer_id is not null
        and date is not null
        and quantity is not null
        and price is not null
)

select * from final