{{
    config(
        materialized='incremental',
        unique_key = 'news_id',
        incremental_strategy = 'merge'
    )
}}


with

dim_source as (
  select * from {{ ref('dim_source') }}
),

dim_cryptocurrency as (
  select * from {{ ref('dim_cryptocurrency') }}
),

int_cryptonews as (
    select * from {{ ref('int_crypto_news__cryptonews') }}
)


select
    ic.news_id,
    ic.published_date,

    class,
    polarity,
    subjectivity,

    ds.source_id,
    dc.cryptocurrency_id,

    title_text as news,
    url

from int_cryptonews ic
left join dim_source  ds on ds.source  = ic.source
left join dim_cryptocurrency dc on dc.cryptocurrency = ic.cryptocurrency

{% if is_incremental() %}
    where ic.news_id not in (select news_id from {{ this }})
{% endif %}
