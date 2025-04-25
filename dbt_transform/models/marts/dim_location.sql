{{
  config(
    materialized = 'incremental',
    unique_key = 'location_id'
  )
}}

with stg_sales as (
    select * from {{ ref('stg_sales') }}
),

locations as (
    select distinct
        location
    from stg_sales
    where location is not null
),

-- Generate a hash-based identifier that remains stable across runs
hashed_locations as (
    select
        {{ dbt_utils.generate_surrogate_key(['location']) }} as location_id,
        location
    from locations
)

{% if is_incremental() %}
, existing_locations as (
    select
        location_id,
        location
    from {{ this }}
)

, final as (
    -- Include existing locations
    select
        location_id,
        location,
        current_timestamp as updated_at
    from existing_locations
    
    union all
    
    -- Add only new locations not already in the table
    select
        l.location_id,
        l.location,
        current_timestamp as updated_at
    from hashed_locations l
    left join existing_locations e 
        on l.location = e.location
    where e.location_id is null
)
{% else %}
, final as (
    select
        location_id,
        location,
        current_timestamp as updated_at
    from hashed_locations
)
{% endif %}

select * from final