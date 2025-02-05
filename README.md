# datawarehouse-zoomcamp-AG
repo for week 3 homework

### Steps
1. download yellow taxi trips for Jan 2024 - June 2024
2. load data into GCS
3. BQ set up:
   - create an external table using the yellow taxi trip data
   - create a (regular/materialized) table in BQ (do not partition or cluster the table)

Steps:

Create external table

```sql
CREATE OR REPLACE EXTERNAL TABLE `kestra-project-449307.yellow_taxi_data_2024.external_yellow_taxi`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://ag-de-zoomcamp-bucket/yellow_taxi_2024/*.parquet']
);
```

Create regular table

```sql
CREATE OR REPLACE TABLE `kestra-project-449307.yellow_taxi_2024.yellow_taxi_regular`
AS
SELECT * FROM `kestra-project-449307.yellow_taxi_2024.external_yellow_taxi`;

```

Create materialized view

```sql
CREATE OR REPLACE MATERIALIZED VIEW `kestra-project-449307.yellow_taxi_2024.yellow_taxi_mat`
AS
SELECT * FROM `kestra-project-449307.yellow_taxi_2024.yellow_taxi_regular`;
```

**Question 1:**

Question 1: What is count of records for the 2024 Yellow Taxi Data?

- 65,623
- 840,402
- **20,332,093** (answer)
- 85,431,289

**Question 2:**

Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.

What is the **estimated amount** of data that will be read when this query is executed on the External Table and the Table?

- 18.82 MB for the External Table and 47.60 MB for the Materialized Table
- **0 MB for the External Table and 155.12 MB for the Materialized Table** (answer)
- 2.14 GB for the External Table and 0MB for the Materialized Table
- 0 MB for the External Table and 0MB for the Materialized Table

```sql
-- count the distinct number of PULocationIDs for the entire dataset on both the tables

-- external - 0 MB
SELECT COUNT(DISTINCT PULocationID)
FROM `yellow_taxi_2024.external_yellow_taxi`;

-- regular - 155.12 MB
SELECT COUNT(DISTINCT PULocationID)
FROM `yellow_taxi_2024.yellow_taxi_regular`;

-- mat - 155.12 MB
SELECT COUNT(DISTINCT PULocationID)
FROM `yellow_taxi_2024.yellow_taxi_mat`;
```

**Question 3:**

Write a query to retrieve the PULocationID from the table (not the external table) in BigQuery. Now write a query to retrieve the PULocationID and DOLocationID on the same table. Why are the estimated number of Bytes different?

- **BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.** (answer)
- BigQuery duplicates data across multiple storage partitions, so selecting two columns instead of one requires scanning the table twice, doubling the estimated bytes processed.
- BigQuery automatically caches the first queried column, so adding a second column increases processing time but does not affect the estimated bytes scanned.
- When selecting multiple columns, BigQuery performs an implicit join operation between them, increasing the estimated bytes processed

```sql
-- 155.12 MB
SELECT PULocationID
FROM `yellow_taxi_2024.yellow_taxi_regular`;

-- 310.24 MB
SELECT PULocationID, DOLocationID
FROM `yellow_taxi_2024.yellow_taxi_regular`;
```

**Question 4:**

How many records have a fare_amount of 0?

- 128,210
- 546,578
- 20,188,016
- **8,333** (answer)

```sql
SELECT COUNT (fare_amount)
FROM `yellow_taxi_2024.yellow_taxi_regular`
WHERE fare_amount = 0;
```

**Question 5:**

What is the best strategy to make an optimized table in Big Query if your query will always filter based on tpep_dropoff_datetime and order the results by VendorID (Create a new table with this strategy)

- **Partition by tpep_dropoff_datetime and Cluster on VendorID** (answer)
- Cluster on by tpep_dropoff_datetime and Cluster on VendorID
- Cluster on tpep_dropoff_datetime Partition by VendorID
- Partition by tpep_dropoff_datetime and Partition by VendorID

```sql
CREATE OR REPLACE TABLE `kestra-project-449307.yellow_taxi_2024.yellow_taxi_partitioned_clustered`
PARTITION BY DATE (tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT * FROM `yellow_taxi_2024.external_yellow_taxi`
```

**Question 6:**

Write a query to retrieve the distinct VendorIDs between tpep_dropoff_datetime 2024-03-01 and 2024-03-15 (inclusive)

Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 5 and note the estimated bytes processed. What are these values?

Choose the answer which most closely matches.

- 12.47 MB for non-partitioned table and 326.42 MB for the partitioned table
- **310.24 MB for non-partitioned table and 26.84 MB for the partitioned table** (answer)
- 5.87 MB for non-partitioned table and 0 MB for the partitioned table
- 310.31 MB for non-partitioned table and 285.64 MB for the partitioned table

```sql
-- for materialized view - 310.24 MB (est)

SELECT COUNT (DISTINCT VendorID)
FROM `yellow_taxi_2024.yellow_taxi_mat`
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' and '2024-03-15';

-- for partitioned clustered - 26.84 MB (est)

SELECT COUNT (DISTINCT VendorID)
FROM `yellow_taxi_2024.yellow_taxi_partitioned_clustered`
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' and '2024-03-15';
```

**Question 7:**

Where is the data stored in the External Table you created?

- Big Query
- Container Registry
- **GCP Bucket** (answer)
- Big Table

**Question 8:**

It is best practice in Big Query to always cluster your data:

- True
- **False** (answer)

**(Bonus: Not worth points) Question 9:**

No Points: Write a `SELECT count(*)` query FROM the materialized table you created. How many bytes does it estimate will be read? Why?

0 MB, because in BigQuery, materialized views are precomputed views that periodically cache the results of a query for increased performance and efficiency.  Since the data is already precomputed, the **query does not process additional data** from the source tables, which means BigQuery can estimate **0 bytes** for the query
