{{ config(materialized='table') }}

with pay as (
    select * from {{ ref('stg_payment') }}
),
cust as (
    select * from customer
)

select
    c.customer_id,
    c.first_name,
    c.last_name,
    sum(p.amount) as total_amount,
    count(p.payment_id) as total_transactions
from cust c
left join pay p using(customer_id)
group by 1,2,3