create or replace table coingecko_raw (data variant);

copy into coingecko_raw
from @my_s3_stage/coingecko_data.json
file_format = (type = 'json');
//loading data

create or replace table coingecko_stage (
    id string,
    symbol string,
    name string,
    current_price float,
    market_cap float
);

insert into coingecko_stage
select
    value:id::string as id,
    value:symbol::string as symbol,
    value:name::string as name,
    value:current_price::float as current_price,
    value:market_cap::float as market_cap
from coingecko_raw,
lateral flatten(input => data);
//flattedn and parsing

copy into @my_s3_stage/coingecko_data.csv
from (
    select id, symbol, name, current_price, market_cap
    from coingecko_stage
)
file_format = (type = 'csv' field_optionally_enclosed_by='"')
header = true;