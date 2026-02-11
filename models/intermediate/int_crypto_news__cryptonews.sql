with

base as (
    select * from {{ ref('stg_crypto_news__cryptonews') }}
),

dup_key as (
    select
        *,
        
        md5(
            lower(trim(coalesce(title, ''))) || ' ' ||
            lower(trim(coalesce(text, '')))
        ) as news_key,
        
        (coalesce(title, '') || ' ' || coalesce(text, '')) as title_text
    
    from base
),

dedup as (
    select *
    from dup_key
    qualify row_number() over (partition by news_key order by published_date asc) = 1
),

final as (
    select
        news_key as news_id,
        published_date,
        
        class,
        polarity,
        subjectivity,
        
        source,
        cryptocurrency,
        
        title_text,
        url
    
    from dedup
)

select * from final
