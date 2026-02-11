select
    md5(source) as source_id,
    source

from {{ ref('int_crypto_news__cryptonews') }}

group by source_id, source
