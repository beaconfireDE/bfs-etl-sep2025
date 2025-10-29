use warehouse DE_0928_WH;

use database AIRFLOW0928;
use schema DEV;

-- Create table
create or replace file format assignment0928.as_prod.team1_prj1_format
  TYPE = CSV
  field_delimiter = ','
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
  NULL_IF = ('', 'NULL');

-- desc stage S3_STAGE_TRANS_ORDER;
-- show stages;

create or replace table prestage_orders_team1 (
  order_id        NUMBER,
  customer_id     NUMBER,
  order_date      DATE,
  product_id      NUMBER,
  product_name    STRING,
  qty             NUMBER,
  price           NUMBER(10,2),
  currency        STRING,
  sales_channel   STRING,
  region          STRING
);

COPY INTO prestage_orders_team1
FROM @S3_STAGE_TRANS_ORDER
PATTERN = '.*team1_.*\.csv'
FILE_FORMAT = assignment0928.as_prod.team1_prj1_format
ON_ERROR = 'CONTINUE';

select * from prestage_orders_team1
limit 10;

-- desc table prestage_orders_team1;

-- drop table prestage_orders_team1;
