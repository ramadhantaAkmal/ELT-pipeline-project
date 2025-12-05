# ELT Pipeline and Data Warehouse Modeling using Airflow, Postgres, dbt and BigQuery

## Overview

This project is to demonstrate ELT data pipeline project that utilized Dbt and BigQuery for data warehouse creatioon, Airflow for pipeline orchestrator, Postgres for source database and python as the main language.

## Technology Used
- **Python**
- **Docker**
- **Apache Airflow**
- **Postgres**
- **BigQuery**
- **dbt**

## Abstract

This project will demonstrate an ELT pipeline where data is first generated using Python and loaded into a local PostgreSQL database. It is then transferred to BigQuery via the BigQuery hook. The data is subsequently transformed into a data warehouse structure using dbt. The entire pipeline is orchestrated with Airflow.
The end result is transformed data stored in a data warehouse built on BigQuery and dbt, following a medallion architecture with three layers:
- Bronze/Staging layer
- Silver/Refined layer
- Gold/Business (Star Schema) layer

## Setup Instructions

### Prerequisites/Requirements
- You have docker installed on your device.
- You have a google cloud service account key
- You have enough hardware resource (more than 8GB RAM, and Intel i5/ Ryzen 5 or above, because if not it's going to be slow)
- (Optional) You need to have dbeaver if you want to check the postgres database

### Running the Pipeline
  1. Place your service account key on `keys` directory (create the directory first if not exist).
  2. Then run this command on your terminal:
     ```bash
     docker compose build --no-cache
     docker compose up airflow-init -d
     docker compose up -d
     ```
  3. Open your browser and put the link http://localhost:8082/, an airflow website will pop up
  4. Run the product and customer loader DAG first, after all task finished run the order loader dag
  5. Run the daily_ingest_to_bigquery DAG and wait until finished
  6. Run the dwh_silver_layer_dag and wait until finished, then repeat the same process with the dwh_gold_layer_dag 
  7. Check your google cloud console then open up BigQuery

  8. Your Data Warehouse are ready

