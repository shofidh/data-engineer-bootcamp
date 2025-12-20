{{ config(materialized='table') }}

with base as (
    select * from {{ ref('int_customer_payment') }}
)

select
    customer_id,
    upper(first_name || ' ' || last_name) as nama_lengkap,
    total_amount,
    total_transactions,
    case 
        when total_amount > 100 then 'HIGH'
        when total_amount between 50 and 100 then 'MEDIUM'
        else 'LOW'
    end as kategori_customer
from base