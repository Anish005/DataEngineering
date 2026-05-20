# GoodCabs ETL Pipeline: Thought Process & Action Log

This document outlines the architectural decisions and reasoning behind each step of the Databricks LakeFlow Spark Declarative Pipeline built for GoodCabs. 



## 1. Architectural Foundation: Declarative vs. Imperative
*   **Action:** Transition from Imperative PySpark to LakeFlow Spark Declarative Pipelines (SDP).
*   **Thought Process:** In a traditional imperative approach, developers explicitly detail *how* to execute tasks (e.g., manually checking for matching IDs to update records), which previously required ~135 lines of code . By switching to SDP, we focus on *what* to do, reducing the codebase to ~50 lines . The framework automatically orchestrates dependencies and incremental processing .

## 2. Infrastructure & Catalog Setup
*   **Action:** Configure Databricks Free Edition with Unity Catalog and connect to Amazon S3.
*   **Thought Process:** S3 serves as our scalable landing zone . We set up a dedicated `transportation` catalog in Unity Catalog with schemas for `bronze`, `silver`, and `gold` layers . 

## 3. Bronze Layer Processing (Raw Ingestion)
*   **Action:** Ingest the `city` dimension table as a `Materialized View` .
*   **Thought Process:** The `city` table contains static mapping data. Materialized views cache data to disk, making batch processing highly efficient .
*   **Action:** Ingest `trips` fact table as a `Streaming Table` utilizing Databricks Autoloader.
*   **Thought Process:** The trips data receives continuous daily updates. Autoloader processes only net-new files dropped into the S3 bucket [8, 10]. We enabled Schema Evolution (`rescue` mode) so corrupt records are routed into a `_rescued_data` column rather than failing the stream.

## 4. Silver Layer Processing (Cleansing & CDC)
*   **Action:** Programmatically generate a `calendar` dimension table .
*   **Thought Process:** We used PySpark and SQL configuration parameters (`start_date` and `end_date`) to generate dates dynamically, providing a reusable time dimension .
*   **Action:** Implement Data Quality Expectations on the `trips` staging table .
*   **Thought Process:** We validated that driver/passenger ratings are between 1 and 10 and that the business year is > 2020. By applying the `expect` mode, the pipeline successfully logs invalid records but continues processing .
*   **Action:** Apply an Auto CDC (Change Data Capture) flow using SCD Type 1 .
*   **Thought Process:** Auto CDC updates are handled using the trip `id` as a primary key . SCD Type 1 automatically overwrites updated records rather than creating historical duplicates . 

## 5. Gold Layer Processing (Business Ready)
*   **Action:** Create a denormalized `gold_fact_trips` view using SQL .
*   **Thought Process:** We joined the `trips` silver table with the `city` and `calendar` dimension tables into one master view .
*   **Action:** Generate region-specific views (e.g., `fact_trips_vadodara`) .
*   **Thought Process:** Filtering the main fact table into city-specific views gives regional managers targeted access to their local jurisdiction .

## 6. Pipeline Automation & Data Governance
*   **Action:** Switch pipeline mode to `Continuous` .
*   **Thought Process:** A continuous pipeline actively listens to the S3 bucket and automatically detects and processes new rows as soon as files land.
*   **Action:** Implement Role-Based Access Control (RBAC) in Unity Catalog.
*   **Thought Process:** We created specific user groups (e.g., "Vadodara Team") and granted them access strictly to their specific Gold view . 

