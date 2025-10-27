create or replace view alphavantage_view as
select
    date as trade_date,
    open as stock_open,
    high as stock_high,
    low as stock_low,
    close as stock_close,
    volume as stock_volume
from alphavantage
order by date desc;