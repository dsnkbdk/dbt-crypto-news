select
    md5(cryptocurrency) as cryptocurrency_id,
    cryptocurrency

from {{ ref('int_crypto_news__cryptonews') }}

group by cryptocurrency_id, cryptocurrency