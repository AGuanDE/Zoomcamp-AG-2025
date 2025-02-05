# datawarehouse-zoomcamp-AG
repo for week 3 homework

### Steps
1. download yellow taxi trips for Jan 2024 - June 2024
2. load data into GCS (can use kestra) ** use PARQUET option files when creating an external table **
3. BQ set up:
   - create an external table using the yellow taxi trip data
   - create a (regular/materialized) table in BQ (do not partition or cluster the table)
