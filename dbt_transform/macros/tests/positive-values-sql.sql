{% macro test_positive_values(model, column_name) %}

with validation as (
    select
        {{ column_name }} as column_value
    from {{ model }}
),

validation_errors as (
    select
        column_value
    from validation
    where column_value <= 0
)

select count(*)
from validation_errors

{% endmacro %}