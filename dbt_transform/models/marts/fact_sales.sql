{{
  config(
    unique_key = 'sale_id',
    indexes = [
      {'columns': ['sale_id'], 'unique': True},
      {'columns': ['product_id']},
      {'columns': ['retailer_id']},
      {'columns': ['location_id']},
      {'columns': ['channel_id']},
      {'columns': ['date_id']}
    ]
  )
}}

with stg_sales as (
    select * from {{ ref('stg_sales') }}
),

dim_product as (
    select * from {{ ref('dim_product') }}
),

dim_location as (
    select * from {{ ref('dim_location') }}
),

-- Create dimension tables for retailer
dim_retailer as (
    select distinct
        retailer_id,
        retailer_name
    from stg_sales
),

-- Create dimension tables for channel
dim_channel as (
    select
        channel,
        {{ dbt_utils.generate_surrogate_key(['channel']) }} as channel_id
    from stg_sales
    group by channel
),

-- Final fact sales table
final as (
    select
        s.sale_id,
        s.product_id,
        s.retailer_id,
        l.location_id,
        c.channel_id,
        s.date as date_id,
        s.quantity,
        s.price / nullif(s.quantity, 0)::numeric(10, 2) as unit_price, -- Prevent divide by zero
        s.price::numeric(12, 2) as total_amount,
        s.transformed_at
    from stg_sales s
    inner join dim_location l on l.location = s.location
    inner join dim_channel c on c.channel = s.channel
    {% if is_incremental() %}
    where s.transformed_at > (select max(transformed_at) from {{ this }})
    {% endif %}
)

select * from final