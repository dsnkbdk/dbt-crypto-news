with

dates as (
    select distinct to_date(published_date) as date_day
    from {{ ref('int_crypto_news__cryptonews') }}
)

select
    date_day,
    year(date_day)  as year,
    month(date_day) as month,
    day(date_day)   as day,
    to_char(date_day, 'YYYY-MM') as year_month
from dates
