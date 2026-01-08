{{ config(materialized='view') }}

select
    payment_id,
    customer_id,
    staff_id,
    rental_id,
    amount,
    amount * 1.0 as amount_usd,
    payment_date::date as payment_date
from payment