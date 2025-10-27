--config file 
create or replace stage my_s3_stage
url = 's3://cg-test-bucket-00/'
storage_integration = my_s3_integration; //stage creation

create or replace table coingecko_data (
    id string,
    symbol string,
    name string,
    current_price float,
    market_cap float
);

create or replace table alphavantage_data (
    date date,
    open float,
    high float,
    low float,
    close float,
    volume float
);  //creating main tables for the coingecko and alphavantage



create or replace table coingecko_stage like coingecko;
create or replace table alphavantage_stage like alphavantage;  //creation of staging tables 

create or replace table coingecko (
    id string,
    symbol string,
    name string,
    current_price float,
    market_cap float
);

create or replace table alphavantage (
    date date,
    open float,
    high float,
    low float,
    close float,
    volume float
);



copy into coingecko_stage
from @my_s3_stage/coingecko_data.csv
file_format = (
    type = 'csv'
    field_optionally_enclosed_by = '"'
    skip_header = 1
);

copy into alphavantage_stage
from @my_s3_stage/alphavantage_data.csv
file_format = (
    type = 'csv'
    field_optionally_enclosed_by = '"'
    skip_header = 1
); //data loadingfrom s3

create or replace view market_comparison as
select distinct
    a.date as trade_date,
    a.open as stock_open,
    a.close as stock_close,
    round(((a.close - a.open) / a.open) * 100, 2) as stock_pct_change,
    c.id as crypto_id,
    c.name as crypto_name,
    c.current_price as crypto_price,
    c.market_cap as crypto_market_cap,
    round(
        (
            (c.current_price - lag(c.current_price) over (partition by c.id order by a.date))
            / nullif(lag(c.current_price) over (partition by c.id order by a.date), 0)
        ) * 100, 2
    ) as crypto_pct_change,
    case
        when c.market_cap > 500000000000 then 'mega cap'
        when c.market_cap > 100000000000 then 'large cap'
        else 'mid/small cap'
    end as crypto_size_category,
    round(
        abs(((a.close - a.open) / a.open) * 100) +
        abs(coalesce(
            (
                (c.current_price - lag(c.current_price) over (partition by c.id order by a.date))
                / nullif(lag(c.current_price) over (partition by c.id order by a.date), 0)
            ) * 100, 0)
        ), 2
    ) as risk_index
from alphavantage a
join coingecko c on 1 = 1  
where a.date >= current_date - 7
order by a.date desc;