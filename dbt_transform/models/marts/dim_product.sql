with stg_sales as (
    select * from {{ ref('stg_sales') }}
),

final as (
    select distinct
        product_id,
        first_value(product_name) over (
            partition by product_id order by transformed_at desc
        ) as product_name,
        first_value(brand) over (
            partition by product_id order by transformed_at desc
        ) as brand,
        first_value(category) over (
            partition by product_id order by transformed_at desc
        ) as category,
        current_timestamp as updated_at
    from stg_sales
    where product_id is not null
)

select * from final