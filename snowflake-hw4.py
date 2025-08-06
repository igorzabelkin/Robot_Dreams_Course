CREATE STORAGE INTEGRATION zabelkini_s3_int
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = 'S3'
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::554739427960:role/izabelkin-snowflake-role'
STORAGE_ALLOWED_LOCATIONS = ('*');

desc integration zabelkini_s3_int;

CREATE DATABASE IF NOT EXISTS izabelkin_nyc_taxy_db;
USE DATABASE izabelkin_nyc_taxy_db;

CREATE SCHEMA IF NOT EXISTS raw;
USE SCHEMA raw;

CREATE or REPLACE FILE FORMAT zabelkini_csv_format 
TYPE = 'CSV'
COMPRESSION = 'AUTO'
FIELD_DELIMITER = ','
RECORD_DELIMITER = '\n'
SKIP_HEADER = 1
TRIM_SPACE = TRUE,
FIELD_OPTIONALLY_ENCLOSED_BY='"'
ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;

CREATE OR REPLACE FILE FORMAT zabelkini_parquet_format
TYPE = 'PARQUET'
  COMPRESSION = AUTO 
  USE_LOGICAL_TYPE = TRUE
  TRIM_SPACE = TRUE;

  
create or replace stage zabelkini_s3_stage
storage_integration = zabelkini_s3_int
url = 's3://robot-dreams-source-data/'
file_format = zabelkini_parquet_format; 

create or replace table TAXI_ZONE_LOOKUP (
LOCATION varchar,
BOROUGH varchar,
ZONE varchar,
SERIVCE_ZONE varchar
);

COPY INTO TAXI_ZONE_LOOKUP
FROM @zabelkini_s3_stage/home-work-1/nyc_taxi/taxi_zone_lookup.csv
FILE_FORMAT = (FORMAT_NAME = zabelkini_csv_format);

select * 
from TAXI_ZONE_LOOKUP

LIST @zabelkini_s3_stage/home-work-1-unified/nyc_taxi/yellow/;

SELECT *
FROM @zabelkini_s3_stage/home-work-1-unified/nyc_taxi/yellow/2023/part-00019-tid-6448896742457724398-99878215-3dd1-4d8f-a20b-53d676948a36-296-1-c000.snappy.parquet
LIMIT 1;

CREATE OR REPLACE TABLE yellow_raw (
  DOLocationID INT,
  PULocationID INT,
  RatecodeID INT,
  VendorID INT,
  airport_fee DOUBLE,
  congestion_surcharge DOUBLE,
  extra DOUBLE,
  fare_amount DOUBLE,
  improvement_surcharge DOUBLE,
  mta_tax DOUBLE,
  passenger_count INT,
  payment_type INT,
  store_and_fwd_flag STRING,
  tip_amount DOUBLE,
  tolls_amount DOUBLE,
  total_amount DOUBLE,
  tpep_dropoff_datetime TIMESTAMP_NTZ,
  tpep_pickup_datetime TIMESTAMP_NTZ,
  trip_distance DOUBLE
);

LIST @zabelkini_s3_stage/home-work-1-unified/nyc_taxi/green/;

SELECT *
FROM @zabelkini_s3_stage/home-work-1-unified/nyc_taxi/green/2022/part-00003-tid-6462612789380156450-8a76215d-ae78-4593-a64c-b28a999e1025-1562-1-c000.snappy.parquet
LIMIT 1;


CREATE OR REPLACE TABLE green_raw (
  DOLocationID INT,
  PULocationID INT,
  RatecodeID INT,
  VendorID INT,
  congestion_surcharge DOUBLE,
  extra DOUBLE,
  fare_amount DOUBLE,
  improvement_surcharge DOUBLE,
  lpep_dropoff_datetime TIMESTAMP_NTZ,
  lpep_pickup_datetime TIMESTAMP_NTZ,
  mta_tax DOUBLE,
  passenger_count INT,
  payment_type INT,
  store_and_fwd_flag STRING,
  tip_amount DOUBLE,
  tolls_amount DOUBLE,
  total_amount DOUBLE,
  trip_distance DOUBLE
);

COPY INTO yellow_raw
FROM @zabelkini_s3_stage/home-work-1-unified/nyc_taxi/yellow/
FILE_FORMAT = (TYPE = 'PARQUET')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR = 'CONTINUE';

COPY INTO green_raw
FROM @zabelkini_s3_stage/home-work-1-unified/nyc_taxi/green/
FILE_FORMAT = (TYPE = 'PARQUET')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR = 'CONTINUE';

Select top 10 * from yellow_enriched;

CREATE TABLE yellow_enriched (
  DOLocationID INT,
  PULocationID INT,
  RatecodeID INT,
  VendorID INT,
  airport_fee DOUBLE,
  congestion_surcharge DOUBLE,
  extra DOUBLE,
  fare_amount DOUBLE,
  improvement_surcharge DOUBLE,
  mta_tax DOUBLE,
  passenger_count INT,
  payment_type INT,
  store_and_fwd_flag STRING,
  tip_amount DOUBLE,
  tolls_amount DOUBLE,
  total_amount DOUBLE,
  tpep_dropoff_datetime TIMESTAMP_NTZ,
  tpep_pickup_datetime TIMESTAMP_NTZ,
  trip_distance DOUBLE,
  pickup_zone STRING,
  dropoff_zone STRING
);

CREATE TABLE green_enriched (
  DOLocationID INT,
  PULocationID INT,
  RatecodeID INT,
  VendorID INT,
  airport_fee DOUBLE,
  congestion_surcharge DOUBLE,
  extra DOUBLE,
  fare_amount DOUBLE,
  improvement_surcharge DOUBLE,
  mta_tax DOUBLE,
  passenger_count INT,
  payment_type INT,
  store_and_fwd_flag STRING,
  tip_amount DOUBLE,
  tolls_amount DOUBLE,
  total_amount DOUBLE,
  tpep_dropoff_datetime TIMESTAMP_NTZ,
  tpep_pickup_datetime TIMESTAMP_NTZ,
  trip_distance DOUBLE,
  pickup_zone STRING,
  dropoff_zone STRING
);


INSERT INTO green_enriched
SELECT
    y.*,
    pu.ZONE AS pickup_zone,
    do.ZONE AS dropoff_zone
FROM green_raw y
LEFT JOIN taxi_zone_lookup pu ON y.PULocationID = pu.LOCATION
LEFT JOIN taxi_zone_lookup do ON y.DOLocationID = do.LOCATION;

DESC TABLE green_raw;

INSERT INTO green_enriched (
  DOLOCATIONID,
  PULOCATIONID,
  RATECODEID,
  VENDORID,
  AIRPORT_FEE,
  CONGESTION_SURCHARGE,
  EXTRA,
  FARE_AMOUNT,
  IMPROVEMENT_SURCHARGE,
  MTA_TAX,
  PASSENGER_COUNT,
  PAYMENT_TYPE,
  STORE_AND_FWD_FLAG,
  TIP_AMOUNT,
  TOLLS_AMOUNT,
  TOTAL_AMOUNT,
  TPEP_DROPOFF_DATETIME,
  TPEP_PICKUP_DATETIME,
  TRIP_DISTANCE,
  PICKUP_ZONE,
  DROPOFF_ZONE
)
SELECT
  g.DOLOCATIONID,
  g.PULOCATIONID,
  g.RATECODEID,
  g.VENDORID,
  NULL AS AIRPORT_FEE, -- отсутствует в green_raw
  g.CONGESTION_SURCHARGE,
  g.EXTRA,
  g.FARE_AMOUNT,
  g.IMPROVEMENT_SURCHARGE,
  g.MTA_TAX,
  g.PASSENGER_COUNT,
  g.PAYMENT_TYPE,
  g.STORE_AND_FWD_FLAG,
  g.TIP_AMOUNT,
  g.TOLLS_AMOUNT,
  g.TOTAL_AMOUNT,
  g.LPEP_DROPOFF_DATETIME AS TPEP_DROPOFF_DATETIME,
  g.LPEP_PICKUP_DATETIME AS TPEP_PICKUP_DATETIME,
  g.TRIP_DISTANCE,
  pu.ZONE AS PICKUP_ZONE,
  do.ZONE AS DROPOFF_ZONE
FROM green_raw g
LEFT JOIN taxi_zone_lookup pu ON g.PULOCATIONID = pu.LOCATION
LEFT JOIN taxi_zone_lookup do ON g.DOLOCATIONID = do.LOCATION;

Select top 10 * from green_enriched;

CREATE OR REPLACE TABLE yellow_transformed AS
SELECT
    *,
    CASE
        WHEN trip_distance < 2 THEN 'Short'
        WHEN trip_distance BETWEEN 2 AND 10 THEN 'Medium'
        WHEN trip_distance > 10 THEN 'Long'
        ELSE 'Unknown'
    END AS trip_category,
    EXTRACT(HOUR FROM tpep_pickup_datetime) AS pickup_hour
FROM yellow_enriched
WHERE
    trip_distance > 0
    AND total_amount > 0
    AND passenger_count BETWEEN 1 AND 6;

CREATE OR REPLACE TABLE green_transformed AS
SELECT
    *,
    CASE
        WHEN trip_distance < 2 THEN 'Short'
        WHEN trip_distance BETWEEN 2 AND 10 THEN 'Medium'
        WHEN trip_distance > 10 THEN 'Long'
        ELSE 'Unknown'
    END AS trip_category,
    EXTRACT(HOUR FROM tpep_pickup_datetime) AS pickup_hour
FROM green_enriched
WHERE
    trip_distance > 0
    AND total_amount > 0
    AND passenger_count BETWEEN 1 AND 6;


CREATE OR REPLACE TABLE yellow_zone_stats AS
SELECT
    pickup_zone,
    trip_category,
    COUNT(*) AS trip_count,
    ROUND(AVG(trip_distance), 2) AS avg_distance,
    ROUND(AVG(total_amount), 2) AS avg_total_amount
FROM yellow_transformed
GROUP BY pickup_zone, trip_category;

CREATE OR REPLACE TABLE green_zone_stats AS
SELECT
    pickup_zone,
    trip_category,
    COUNT(*) AS trip_count,
    ROUND(AVG(trip_distance), 2) AS avg_distance,
    ROUND(AVG(total_amount), 2) AS avg_total_amount
FROM green_transformed
GROUP BY pickup_zone, trip_category;

SELECT * FROM yellow_zone_stats ORDER BY trip_count DESC LIMIT 10;
SELECT * FROM green_zone_stats ORDER BY avg_total_amount DESC LIMIT 10;

-- 1. Видалимо частину записів з green_enriched

DELETE FROM green_enriched
WHERE pickup_zone = 'JFK Airport';

SELECT COUNT(*) FROM green_enriched;
-- осталось 77921129 записей

-- Подивимось історію запитів до green_enriched
SELECT *
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
WHERE query_text ILIKE '%DELETE FROM green_enriched%'
ORDER BY start_time DESC
LIMIT 1;  

-- 01be30af-0001-6425-0001-600a0005501a

SELECT *
FROM green_enriched AT (STATEMENT => '01be30af-0001-6425-0001-600a0005501a');

-- Вставити назад у green_enriched
INSERT INTO green_enriched
SELECT *
FROM green_enriched AT (STATEMENT => '01be30af-0001-6425-0001-600a0005501a')
MINUS
SELECT *
FROM green_enriched;

SELECT COUNT(*) FROM green_enriched;

CREATE OR REPLACE STREAM yellow_enriched_stream
ON TABLE yellow_enriched;

INSERT INTO yellow_enriched (
  DOLocationID, PULocationID, RatecodeID, VendorID,
  airport_fee, congestion_surcharge, extra, fare_amount,
  improvement_surcharge, mta_tax, passenger_count, payment_type,
  store_and_fwd_flag, tip_amount, tolls_amount, total_amount,
  tpep_dropoff_datetime, tpep_pickup_datetime, trip_distance,
  pickup_zone, dropoff_zone
)
VALUES (
  142, 237, 1, 2,
  0.0, 2.5, 1.0, 10.0,
  1.0, 0.5, 1, 1,
  'N', 2.0, 0.0, 16.0,
  '2023-07-01 10:30:00', '2023-07-01 10:15:00', 2.3,
  'Midtown East', 'Chelsea'
);

-- Проверяем STREAM
SELECT * FROM yellow_enriched_stream;

CREATE OR REPLACE TABLE yellow_changes_log AS
SELECT * FROM yellow_enriched_stream;

SELECT * FROM yellow_changes_log ORDER BY tpep_pickup_datetime DESC;

SHOW WAREHOUSES;

CREATE OR REPLACE TASK task_log_yellow_changes
WAREHOUSE = SNOWFLAKE_LEARNING_WH
SCHEDULE = '1 HOUR'
WHEN
  SYSTEM$STREAM_HAS_DATA('yellow_enriched_stream')
AS
INSERT INTO yellow_changes_log
SELECT * FROM yellow_enriched_stream;


CREATE OR REPLACE TABLE zone_hourly_stats (
  hour_timestamp TIMESTAMP,
  pickup_zone STRING,
  trip_category STRING,
  avg_distance FLOAT,
  avg_total_amount FLOAT,
  trip_count INT
);

CREATE OR REPLACE TASK task_update_zone_stats
WAREHOUSE = SNOWFLAKE_LEARNING_WH
SCHEDULE = '1 HOUR'
AS
INSERT INTO zone_hourly_stats
SELECT
  DATE_TRUNC('HOUR', tpep_pickup_datetime) AS hour_timestamp,
  pickup_zone,
  trip_category,
  ROUND(AVG(trip_distance), 2) AS avg_distance,
  ROUND(AVG(total_amount), 2) AS avg_total_amount,
  COUNT(*) AS trip_count
FROM yellow_enriched
WHERE
  trip_distance > 0 AND total_amount > 0 AND passenger_count BETWEEN 1 AND 6
GROUP BY 1, 2, 3;

ALTER TASK task_log_yellow_changes RESUME;
ALTER TASK task_update_zone_stats RESUME;

CREATE OR REPLACE TABLE all_trips (
  pickup_datetime TIMESTAMP,
  dropoff_datetime TIMESTAMP,
  pickup_zone STRING,
  dropoff_zone STRING,
  trip_distance FLOAT,
  total_amount FLOAT,
  passenger_count INT,
  airport_fee FLOAT,
  vendorid INT,
  payment_type INT,
  ratecodeid INT,
  store_and_fwd_flag STRING,
  fare_amount FLOAT,
  extra FLOAT,
  mta_tax FLOAT,
  tip_amount FLOAT,
  tolls_amount FLOAT,
  improvement_surcharge FLOAT,
  congestion_surcharge FLOAT,
  source STRING
);

CREATE OR REPLACE TABLE insert_log (
  run_time TIMESTAMP,
  source STRING,
  inserted_rows INT
);

CREATE OR REPLACE PROCEDURE insert_unique_trips()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
var yellow_sql = `
  INSERT INTO izabelkin_nyc_taxy_db.raw.all_trips
  SELECT
    tpep_pickup_datetime AS pickup_datetime,
    tpep_dropoff_datetime AS dropoff_datetime,
    pickup_zone,
    dropoff_zone,
    trip_distance,
    total_amount,
    passenger_count,
    airport_fee,
    vendorid,
    payment_type,
    ratecodeid,
    store_and_fwd_flag,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    congestion_surcharge,
    'yellow' AS source
  FROM izabelkin_nyc_taxy_db.raw.yellow_enriched y
  WHERE NOT EXISTS (
    SELECT 1 FROM izabelkin_nyc_taxy_db.raw.all_trips a
    WHERE a.pickup_datetime = y.tpep_pickup_datetime
      AND a.dropoff_datetime = y.tpep_dropoff_datetime
      AND a.pickup_zone = y.pickup_zone
      AND a.dropoff_zone = y.dropoff_zone
  );
`;

var green_sql = `
  INSERT INTO IZABELKIN_NYC_TAXY_DB.RAW.ALL_TRIPS
  SELECT
    tpep_pickup_datetime AS pickup_datetime,
    tpep_dropoff_datetime AS dropoff_datetime,
    pickup_zone,
    dropoff_zone,
    trip_distance,
    total_amount,
    passenger_count,
    airport_fee,
    vendorid,
    payment_type,
    ratecodeid,
    store_and_fwd_flag,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    congestion_surcharge,
    'green' AS source
  FROM IZABELKIN_NYC_TAXY_DB.RAW.GREEN_ENRICHED g
  WHERE NOT EXISTS (
    SELECT 1 FROM IZABELKIN_NYC_TAXY_DB.RAW.ALL_TRIPS a
    WHERE a.pickup_datetime = g.tpep_pickup_datetime
      AND a.dropoff_datetime = g.tpep_dropoff_datetime
      AND a.pickup_zone = g.pickup_zone
      AND a.dropoff_zone = g.dropoff_zone
  );
`;

var yellow_result = snowflake.execute({ sqlText: yellow_sql });
var yellow_count = yellow_result.getRowCount();

var green_result = snowflake.execute({ sqlText: green_sql });
var green_count = green_result.getRowCount();

// логируем
snowflake.execute({
  sqlText: `INSERT INTO insert_log VALUES (CURRENT_TIMESTAMP(), 'yellow', ${yellow_count})`
});
snowflake.execute({
  sqlText: `INSERT INTO insert_log VALUES (CURRENT_TIMESTAMP(), 'green', ${green_count})`
});

return `Inserted: yellow=${yellow_count}, green=${green_count}`;
$$;

CALL insert_unique_trips();

CREATE OR REPLACE TABLE izabelkin_nyc_taxy_db.raw.all_trips (
  pickup_datetime TIMESTAMP,
  dropoff_datetime TIMESTAMP,
  pickup_zone STRING,
  dropoff_zone STRING,
  trip_distance FLOAT,
  total_amount FLOAT,
  passenger_count INT,
  airport_fee FLOAT,
  vendorid INT,
  payment_type INT,
  ratecodeid INT,
  store_and_fwd_flag STRING,
  fare_amount FLOAT,
  extra FLOAT,
  mta_tax FLOAT,
  tip_amount FLOAT,
  tolls_amount FLOAT,
  improvement_surcharge FLOAT,
  congestion_surcharge FLOAT,
  source STRING
);
