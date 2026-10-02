# Databricks Data Engineer Certification — Learning Path

A hands-on portfolio of Databricks data engineering notebooks built while studying for the **Databricks Data Engineer Associate** certification. Each notebook demonstrates a real-world data pipeline stage using college football API data, with detailed educational comments explaining the concepts behind every step.

## 📋 Project Overview

This repository follows a complete data engineering workflow on Databricks:

1. **Ingest** raw JSON data from an external API into Unity Catalog volumes
2. **Load** JSON files from volumes into managed Delta tables
3. *(Future)* Transform and model data in the medallion architecture (bronze → silver → gold)

## 📓 Notebooks

| # | Notebook | Description | Key Certification Topics |
| --- | --- | --- | --- |
| 01 | `01_cfbd_landing_ingest.py` | Fetches college football data from the CollegeFootballData.com API and writes raw JSON responses (untouched) into Unity Catalog volumes. Supports one-shot bulk loads and incremental weekly loads. | Unity Catalog volumes, API ingestion, secret management, incremental vs. batch loading |
| 02 | `02_ingest_json_to_delta_table.py` | Reads JSON files from a Unity Catalog volume using Spark, inspects the inferred schema, and writes to a managed Delta table. Includes a bonus section on Auto Loader for continuous ingestion. | Schema inference, Delta Lake, managed vs. external tables, write modes, Auto Loader (`cloudFiles`), Structured Streaming |

## 🔑 Certification Topics Covered

- **Unity Catalog**: Volumes, path format (`/Volumes/catalog/schema/volume/path`), governance
- **Delta Lake**: ACID transactions, time travel, schema enforcement, MERGE operations
- **Managed vs. External Tables**: When to use each, what happens on `DROP TABLE`
- **Schema Inference**: How Spark infers types from JSON, the `multiLine` option, explicit schemas
- **Write Modes**: `overwrite`, `append`, `ignore`, `errorifexists`
- **Auto Loader**: `cloudFiles` format, `schemaLocation`, `schemaEvolutionMode`, `checkpointLocation`
- **Secret Management**: Using Databricks secret scopes (never hardcode API keys)
- **Incremental Loading**: Weekly file layout vs. one-shot bulk loads

## 🏈 Data Source

Data is fetched from the [CollegeFootballData.com API](https://api.collegefootballdata.com). The API provides game scores, team stats, player stats, and calendar data for NCAA college football.

> ⚠️ CFBD's terms allow storing this data for personal use but not redistributing it raw. The raw JSON files in Unity Catalog volumes are not included in this repository.

## 🚀 How to Run

These notebooks are designed to run in a Databricks workspace with:
- **Serverless compute** (or any Databricks Runtime 15.4+)
- A Unity Catalog-enabled workspace
- A CFBD API key stored in a Databricks secret scope

To reproduce:
1. Create a secret scope named `cfbd` with a key `api_key` containing your CFBD API token
2. Run `01_cfbd_landing_ingest` with mode `both` to populate the volumes
3. Run `02_ingest_json_to_delta_table` to load JSON from volumes into Delta tables

## 📂 Repository Structure

```
.
├── 01_cfbd_landing_ingest.py      # API → Unity Catalog volumes (raw JSON)
├── 02_ingest_json_to_delta_table.py # Unity Catalog volumes → Delta tables
├── README.md                       # This file
└── .gitignore
```

## 📜 License

This notebook code is shared for educational purposes. The data itself is subject to CollegeFootballData.com's terms of service.

---

*Built as part of a Databricks Data Engineer certification study journey.*
