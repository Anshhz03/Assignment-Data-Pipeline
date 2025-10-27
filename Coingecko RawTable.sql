create or replace table coingecko_raw (
    id string,
    symbol string,
    name string,
    current_price float,
    market_cap float
);


copy into coingecko_raw
from @my_s3_stage/coingecko_data.csv
file_format = (type = 'csv' field_optionally_enclosed_by = '"' skip_header = 1);

select * from coingecko_raw;