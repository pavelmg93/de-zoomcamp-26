/* @bruin
name: staging.trips
type: duckdb.sql

depends:
  - ingestion.trips
  - ingestion.payment_lookup

materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_datetime
  time_granularity: timestamp

columns:
  - name: pickup_datetime
    type: timestamp
    primary_key: true
    checks:
      - name: not_null

custom_checks:
  - name: row_count_greater_than_zero
    query: |
      SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END
      FROM staging.trips
    value: 1
@bruin */

WITH green_trips AS (
    SELECT 
        taxi_type,
        CAST(vendor_id AS INTEGER) AS vendor_id,
        CAST(pu_location_id AS INTEGER) AS pickup_location_id,
        CAST(do_location_id AS INTEGER) AS dropoff_location_id,
        CAST(lpep_pickup_datetime AS TIMESTAMP) AS pickup_datetime, 
        CAST(lpep_dropoff_datetime AS TIMESTAMP) AS dropoff_datetime,
        CAST(fare_amount AS NUMERIC) AS fare_amount,
        CAST(payment_type AS INTEGER) AS payment_type
    FROM ingestion.trips
    WHERE taxi_type = 'green' AND vendor_id IS NOT NULL
      -- Apply time_interval logic directly to the raw column for maximum efficiency
      AND lpep_pickup_datetime >= '{{ start_datetime }}'
      AND lpep_pickup_datetime < '{{ end_datetime }}'
),

yellow_trips AS (
    SELECT 
        taxi_type,
        CAST(vendor_id AS INTEGER) AS vendor_id,
        CAST(pu_location_id AS INTEGER) AS pickup_location_id,
        CAST(do_location_id AS INTEGER) AS dropoff_location_id,
        CAST(tpep_pickup_datetime AS TIMESTAMP) AS pickup_datetime, 
        CAST(tpep_dropoff_datetime AS TIMESTAMP) AS dropoff_datetime,
        CAST(fare_amount AS NUMERIC) AS fare_amount,
        CAST(payment_type AS INTEGER) AS payment_type
    FROM ingestion.trips
    WHERE taxi_type = 'yellow' AND vendor_id IS NOT NULL
      -- Apply time_interval logic directly to the raw column
      AND tpep_pickup_datetime >= '{{ start_datetime }}'
      AND tpep_pickup_datetime < '{{ end_datetime }}'
),

unioned_trips AS (
    SELECT * FROM green_trips
    UNION ALL
    SELECT * FROM yellow_trips
)

SELECT
    t.pickup_datetime,
    t.dropoff_datetime,
    t.pickup_location_id,
    t.dropoff_location_id,
    t.fare_amount,
    t.taxi_type,
    COALESCE(p.payment_type_name, 'unmapped_id') AS payment_type_name
FROM unioned_trips t
LEFT JOIN ingestion.payment_lookup p
    ON t.payment_type = p.payment_type_id
-- Deduplicate using the textbook's elegant QUALIFY syntax
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY t.pickup_datetime, t.dropoff_datetime,
                 t.pickup_location_id, t.dropoff_location_id, t.fare_amount
    ORDER BY t.pickup_datetime
) = 1;