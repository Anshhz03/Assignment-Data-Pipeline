create or replace view coingecko_view as
select
    id as crypto_id,
    symbol as crypto_symbol,
    name as crypto_name,
    current_price as crypto_price,
    market_cap as crypto_market_cap
from coingecko
order by market_cap desc;

select* from coingecko_view