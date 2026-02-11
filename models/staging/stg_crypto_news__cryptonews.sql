with

source as (
    select * from {{ source('crypto_news', 'cryptonews') }}
),

clean as (
    select
        date::timestamp as published_date,
        try_parse_json(replace(sentiment, '''', '"')) as sentiment_json,
        source::string  as source,
        subject::string as cryptocurrency,
        title::string   as title,
        text::string    as text,
        url::string     as url

    from source
),

final as (
    select
        published_date,
        sentiment_json:"class"::string       as class,
        sentiment_json:"polarity"::float     as polarity,
        sentiment_json:"subjectivity"::float as subjectivity,
        source,
        cryptocurrency,
        title,
        text,
        url
    
    from clean
)

select * from final
