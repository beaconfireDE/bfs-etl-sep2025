use warehouse DE_0928_WH;

use database AIRFLOW0928;
use schema DEV;

create or replace table copy_company_profile_team1
clone US_STOCK_DAILY.DCCM.Company_Profile;

create or replace table copy_stock_hist_team1
clone US_STOCK_DAILY.DCCM.Stock_History;

create or replace table copy_stock_team1
clone US_STOCK_DAILY.DCCM.Symbols;

set t_prof = 'copy_company_profile_team1';
set t_hist = 'copy_stock_hist_team1';
set t_stock = 'copy_stock_team1';

-- Checking the company profiles
select * from identifier($t_prof)
limit 50;

-- Checking the stock history
select * from identifier($t_hist)
limit 50;

-- Checking min/max dates
select min(date) as mindate, max(date) as maxdate
from identifier($t_hist);

-- Checking company name
select count(*) as tot_rows, 
    count(distinct companyname) as companies,
    count(distinct website) as websites,
    count(distinct symbol) as symbols
from identifier($t_prof);

select * from identifier($t_prof)
where companyname is null;

-- Checking the stock symbols
select * from identifier($t_stock)
limit 50;

select count(distinct symbol) as tot_symbols,
       count(*) as tot_rows
from identifier($t_stock);

select count(*) as tot_rows
from identifier($t_hist);


--------- Dim table 1: DimDate
create or replace table dim_date_team1 (
datekey number(8, 0) primary key,
date date not null unique, 
year number(4, 0),
quarter number(1, 0),
month number(2, 0),
month_name varchar,
day number(2, 0),
day_of_week number(1, 0),
dow_name varchar,
week_of_year number,
is_weekend boolean
);

set min_date = (select min(date) from identifier($t_hist));
set max_date = current_date(); -- Using current date as the maximum in case we have future in-flows

insert into dim_date_team1 
(date, datekey, year, quarter, month, month_name, day, day_of_week, dow_name, week_of_year, is_weekend)
with recursive more_dates as (
    select $min_date as d
    union all
    select dateadd('day', 1, d)
    from more_dates
    where d < $max_date
)
select 
    d,
    cast(to_char(d, 'YYYYMMDD') as number),
    year(d),
    quarter(d),
    month(d),
    to_char(d, 'MMMM'),
    day(d),
    dayofweekiso(d),
    to_char(d, 'DY'),
    weekofyear(d),
    iff(dayofweekiso(d) >= 6, True, False)
from more_dates;



--------- Dim table 2: Company
create or replace table dim_company_team1 (
company_id number(8, 0) primary key identity start 1 increment 1,
companyname varchar(512) not null,
website varchar(64),
description varchar(2048),
CEO varchar(64),
sector varchar(64),
industry varchar(64)
);

insert into dim_company_team1
(companyname, website, description, CEO, sector, industry)

with ordered_profiles as (
select coalesce(companyname, 'unnamed company') as companyname, website, description, CEO, sector, industry,
    row_number() over (partition by companyname order by id) as rn
from identifier($t_prof)
)

select companyname, website, description, CEO, sector, industry
from ordered_profiles
where rn = 1;


--------- Dim table 3: Stock symbols
create or replace table dim_symbol_team1 (
symbol varchar(16) primary key,
exchange varchar(64),
name varchar(256),
company_id number(8, 0),
beta number(18,8),
volavg number(38, 0),
mktcap number(38, 0),
lastdiv number(18,8),
range varchar(64),
price number(18,8),
dcf number(18,8),
dcfdiff number(18,8),

constraint fk_dim_symbol_company 
    foreign key (company_id) references dim_company_team1 (company_id)
);

insert into dim_symbol_team1
select s.symbol, s.exchange, s.name,
    c.company_id, com.beta, com.volavg, com.mktcap, 
    com.lastdiv, com.range, com.price, com.dcf, com.dcfdiff
from identifier($t_stock) s
join identifier($t_prof) com
on s.symbol = com.symbol
join dim_company_team1 c
on coalesce(com.companyname, 'unnamed company') = c.companyname;



--------- Fact table: Daily Stock Prices
create or replace table fact_daily_price_team1 (
price_key number(38, 0) identity,
symbol varchar(16) not null,
datekey number(8, 0) not null,
open number(18,8),
high number(18,8),
low number(18,8),
close number(18,8),
adjclose number(18,8),
volume number(38,8),

constraint fk_daily_date foreign key (datekey) references dim_date_team1 (datekey),

constraint fk_daily_symbol foreign key (symbol) references dim_symbol_team1 (symbol)
);

-- drop table fact_daily_price_team1;

insert into fact_daily_price_team1 (symbol, datekey, open, high, low, close, adjclose, volume)
with rownum as (
    select symbol, date, open, high, low, close, adjclose, volume,
        row_number() over (partition by symbol, date order by symbol) as rn
    from identifier($t_hist)
),
unique_price as (
select symbol, date, open, high, low, close, adjclose, volume from rownum
where rn = 1
)
select h.symbol, d.datekey, h.open, h.high, h.low, h.close, h.adjclose, h.volume
from unique_price h
join dim_date_team1 d
on h.date = d.date;

select * from 
(select symbol, date, count(*) as counts
from identifier($t_hist)
group by symbol, date) t
where t.counts > 1;

select * from identifier($t_hist) h
join (
select * from 
(select symbol, date, count(*) as counts
from identifier($t_hist)
group by symbol, date) t
where t.counts > 1) new
on h.symbol = new.symbol and h.date = new.date;


--- Stock history table has duplicates!
with rownum as (
    select symbol, date, open, high, low, close, adjclose, volume,
        row_number() over (partition by symbol, date order by symbol) as rn
    from identifier($t_hist)
),
unique_price as (
select symbol, date, open, high, low, close, adjclose, volume from rownum
where rn = 1
)
select h.symbol, d.datekey, h.open, h.high, h.low, h.close, h.adjclose, h.volume
from unique_price h
join dim_date_team1 d
on h.date = d.date;


------ Sanity tests
select count(*) as tot_rows
from fact_daily_price_team1;

-- No duplicates
select count(*) as num_duplicates
from
(select symbol, datekey, count(*) as cnt
from fact_daily_price_team1
group by symbol, datekey
having count(*) > 1);

-- missing date
select count(*) as num_missing_dates
from
(select f.symbol, f.datekey
from fact_daily_price_team1 f
left join dim_date_team1 d on d.datekey = f.datekey
where d.datekey is null);

-- missing symbol
select count(*) as num_missing_symbols
from
(select f.symbol, f.datekey
from fact_daily_price_team1 f
left join dim_symbol_team1 s on s.symbol = f.symbol
where s.symbol is null);

