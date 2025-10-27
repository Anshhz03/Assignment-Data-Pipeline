
create or replace table alphavantage_raw (
    date date,
    open float,
    high float,
    low float,
    close float,
    volume float
);


copy into alphavantage_raw
from @my_s3_stage/alphavantage_data.csv
file_format = (type = 'csv' field_optionally_enclosed_by = '"' skip_header = 1);

select * from alphavantage_raw;

