with stg_sales as (
    select * from {{ ref('stg_sales') }}
),

final as (
    select
        row_number() over (order by location) as location_id,
        location,
        current_timestamp as updated_at
    from stg_sales
    group by location
)

select * from final