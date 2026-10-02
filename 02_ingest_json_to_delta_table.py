# Databricks notebook source
# DBTITLE 1,Overview
# MAGIC %md
# MAGIC # Ingesting JSON Files from a Unity Catalog Volume into Delta Tables
# MAGIC
# MAGIC This notebook demonstrates a fundamental Databricks data engineering workflow: reading JSON files stored in a Unity Catalog volume and loading them into managed Delta tables. This is a key skill tested in the Databricks certification exams.
# MAGIC
# MAGIC **What you'll learn:**
# MAGIC * Listing files in a Unity Catalog volume
# MAGIC * Reading JSON with Spark and understanding schema inference
# MAGIC * Inspecting schemas and sample data
# MAGIC * Writing to Delta tables (managed vs. external)
# MAGIC * Verifying loaded data with SQL
# MAGIC * How Auto Loader extends this for continuous ingestion
# MAGIC
# MAGIC **Our data:** College football API data stored as JSON files in the `cfdb.raw.raw_api_json` volume, organized into subdirectories: `calendar`, `games`, `games_players`, and `games_teams`.

# COMMAND ----------

# DBTITLE 1,List Files in Volume
# ──────────────────────────────────────────────────────────────────────────
# STEP 1: List Files in the Unity Catalog Volume
# ──────────────────────────────────────────────────────────────────────────
# Unity Catalog (UC) volumes are governed cloud storage locations that provide
# a secure, path-based abstraction over object storage (S3 on AWS, ADLS on
# Azure, GCS on GCP). Unlike DBFS mounts, volumes are managed through Unity
# Catalog with fine-grained access control.
#
# Volume path format: /Volumes/<catalog>/<schema>/<volume_name>/<path>
# Our volume: /Volumes/cfdb/raw/raw_api_json/
#
# dbutils.fs.ls() lists the contents of a path inside a volume. It returns
# FileInfo objects with .path, .name, .size, and .isDir attributes.
# This is a control operation (listing files), NOT data processing, so
# driver-side execution is appropriate here.

volume_path = "/Volumes/cfdb/raw/raw_api_json/"

print("=" * 70)
print(f"Listing files in: {volume_path}")
print("=" * 70)

# List top-level contents of the volume
top_level = dbutils.fs.ls(volume_path)
print(f"\nFound {len(top_level)} items at the top level:\n")

for item in top_level:
    print(f"  \U0001F4C1 {item.name}")
    # If it's a directory, list its contents too (one level deep)
    sub_items = dbutils.fs.ls(item.path)
    for sub in sub_items:
        size_kb = sub.size / 1024
        print(f"      \U0001F4C4 {sub.name} ({size_kb:.1f} KB)")

print("\nNote: Each subdirectory contains JSON files for a different data type.")
print("This is a common pattern when organizing raw API data by endpoint.")

# COMMAND ----------

# DBTITLE 1,Read JSON with Schema Inference
# ──────────────────────────────────────────────────────────────────────────
# STEP 2: Read JSON Files with Schema Inference
# ──────────────────────────────────────────────────────────────────────────
# spark.read.json() reads JSON files and automatically infers the schema by
# sampling the data. This is called "schema inference."
#
# Key concepts for the certification exam:
#
# 1. SCHEMA INFERENCE: Spark scans a sample of JSON records to determine
#    column names and data types (string, long, double, boolean, array, etc.).
#    You can also provide an explicit schema with .schema() for performance.
#
# 2. MULTILINE OPTION: By default, Spark treats each line as a separate JSON
#    record (JSON Lines / NDJSON format). If your JSON file contains a single
#    JSON object or array spread across multiple lines (pretty-printed), set:
#        spark.read.option("multiLine", "true").json(path)
#    Our files use JSON Lines format, so the default (multiline=false) works.
#
# 3. WHY SCHEMA MATTERS: An explicit schema improves performance (no inference
#    overhead), ensures data quality (rejects malformed records), and enables
#    type-safe operations downstream.
#
# 4. READING MULTIPLE FILES: spark.read.json() reads ALL .json files in the
#    given path recursively. To read a specific subdirectory, append the path.
#
# We'll read the "games" subdirectory, which contains college football game
# data with a rich schema (strings, longs, doubles, booleans, arrays).

volume_path = "/Volumes/cfdb/raw/raw_api_json/"

# Read JSON files from the games subdirectory
# Spark recursively reads all .json files in the specified path
games_df = spark.read.json(f"{volume_path}games/")

# ── Other useful options ──────────────────────────────────────────────────
# .option("primitivesAsString", "true")  → keep all primitives as strings
# .option("columnNameOfCorruptRecord", "_corrupt_record") → capture bad JSON
# .option("mode", "PERMISSIVE")          → DROPMALFORMED, FAILFAST alternatives
#
# ── Reading ALL subdirectories at once ────────────────────────────────────
# You can read all JSON files recursively with:
#   spark.read.json(f"{volume_path}*/*.json")
# But files with different schemas will be schema-merged, producing a union
# of all columns. Most cells will be null for rows from a different file type.
# Best practice: read each subdirectory separately and load into its own table.

print(f"Games DataFrame created successfully.")
print(f"Row count: {games_df.count():,}")
print(f"Column count: {len(games_df.columns)}")

# COMMAND ----------

# DBTITLE 1,Inspect Schema and Sample Data
# ──────────────────────────────────────────────────────────────────────────
# STEP 3: Inspect the Inferred Schema and Sample Data
# ──────────────────────────────────────────────────────────────────────────
# printSchema() displays the schema in a tree format showing:
#   - Column names (including nested fields within structs)
#   - Data types (string, long, double, boolean, array, struct)
#   - Nullability (true = column can contain null values)
#
# Spark maps JSON values to Spark data types as follows:
#   JSON string  → string
#   JSON number  → long (integer) or double (decimal)
#   JSON boolean → boolean
#   JSON array   → array<element_type>
#   JSON object  → struct<field_name: field_type, ...>
#   JSON null    → null (column is marked nullable)
#
# Note: You may notice that date-like strings (e.g., "2026-08-29T07:00:00Z")
# are inferred as STRING, not TIMESTAMP. Spark's JSON reader does not parse
# timestamps automatically during schema inference. To get timestamps, you
# would either cast the column after reading or provide an explicit schema.

print("=" * 70)
print("INFERRED SCHEMA FOR GAMES DATA")
print("=" * 70)
games_df.printSchema()

print("\n" + "=" * 70)
print("SAMPLE DATA (first 5 rows)")
print("=" * 70)
# display() renders a rich interactive table in Databricks notebooks.
# It's preferred over show() for exploration because it provides:
#   - Sortable, filterable columns
#   - Pagination
#   - Column statistics and profiling
#   - Built-in charting capabilities
display(games_df.limit(5))

# COMMAND ----------

# DBTITLE 1,Write to Delta Table
# ──────────────────────────────────────────────────────────────────────────
# STEP 4: Write the Data to a Delta Table
# ──────────────────────────────────────────────────────────────────────────
# Delta Lake is the default storage format in Databricks. It provides:
#   - ACID transactions (atomic, consistent, isolated, durable)
#   - Time travel (query previous versions: SELECT * FROM table VERSION AS OF 1)
#   - Schema enforcement and schema evolution
#   - UPSERT/MERGE operations (MERGE INTO ... WHEN MATCHED ...)
#   - Efficient incremental reads (change data feed)
#   - Optimized file layout with Z-order and liquid clustering
#
# ── Managed vs. External Tables (common exam topic) ─────────────────────────
#
# MANAGED TABLE (what we create below):
#   - Databricks manages BOTH the metadata AND the underlying data files
#   - DROP TABLE deletes both metadata and the data files
#   - Data is stored in the catalog's default storage location
#   - Created with: df.write.saveAsTable("catalog.schema.table_name")
#   - Simplest to create and manage
#
# EXTERNAL TABLE:
#   - You control the storage location with .option("path", "...")
#   - Databricks manages only the metadata, NOT the data files
#   - DROP TABLE deletes only the metadata; data files remain on disk
#   - Created with: df.write.option("path", "/location/").saveAsTable("...")
#   - Use when data must persist even if the table is dropped, or when
#     sharing data with non-Spark workloads
#
# ── Write Modes (common exam topic) ────────────────────────────────────────
#
# mode("overwrite")     — replaces the entire table if it already exists
#                         Use for: full refreshes, idempotent re-runs
# mode("append")        — adds new rows to existing data
#                         Use for: incremental loads, daily appends
# mode("ignore")        — does nothing if the table already exists
# mode("errorifexists") — (DEFAULT) throws an error if the table exists

# Define the fully qualified table name: catalog.schema.table
# Note: we use "cfdb" (not "dfdb") — verify your catalog name with
# SHOW CATALOGS if unsure.
table_name = "cfdb.raw.raw_api_json_table"

print(f"Writing data to Delta table: {table_name}")

games_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(table_name)

print(f"\n✅ Successfully wrote data to {table_name}")

# ── Alternative: Create an EXTERNAL table ──────────────────────────────────
# external_path = "/Volumes/cfdb/raw/raw_api_json_delta/games/"
# games_df.write \
#     .format("delta") \
#     .mode("overwrite") \
#     .option("path", external_path) \
#     .saveAsTable("cfdb.raw.games_external")
#
# ── Alternative: Save as Delta files WITHOUT a table ──────────────────────
# games_df.write \
#     .format("delta") \
#     .mode("overwrite") \
#     .save("/Volumes/cfdb/raw/raw_api_json_delta/games/")

# COMMAND ----------

# DBTITLE 1,Verify the Table
# MAGIC %sql
# MAGIC -- ──────────────────────────────────────────────────────────────────────────
# MAGIC -- STEP 5: Verify the Table
# MAGIC -- ──────────────────────────────────────────────────────────────────────────
# MAGIC -- After writing data to a table, always verify the load was successful:
# MAGIC --   1. Row count matches expectations:  SELECT COUNT(*) FROM <table>
# MAGIC --   2. Schema is correct:               DESCRIBE TABLE <table>
# MAGIC --   3. Sample rows look correct:        SELECT * FROM <table> LIMIT 10
# MAGIC --   4. No unexpected nulls in key cols: SELECT SUM(CASE WHEN id IS NULL THEN 1 ELSE 0 END) ...
# MAGIC --   5. Data quality:                     SELECT DISTINCT season FROM <table>
# MAGIC --
# MAGIC -- This query retrieves 10 sample rows to visually confirm the data loaded
# MAGIC -- correctly. In Databricks SQL cells, the last statement's results are
# MAGIC -- displayed as an interactive table.
# MAGIC
# MAGIC SELECT * FROM cfdb.raw.raw_api_json_table LIMIT 10

# COMMAND ----------

# DBTITLE 1,Auto Loader Bonus
# MAGIC %md
# MAGIC ## Bonus: Auto Loader for Continuous Ingestion
# MAGIC
# MAGIC The batch approach above reads all files at once — perfect for a one-time load. But what if new JSON files keep arriving in the volume? That's where **Auto Loader** shines.
# MAGIC
# MAGIC ### What is Auto Loader?
# MAGIC Auto Loader is a Structured Streaming source (`cloudFiles`) that incrementally and efficiently processes new files as they arrive in cloud storage. It's the recommended way to ingest streaming data from cloud storage into Delta Lake.
# MAGIC
# MAGIC ### Key differences from the batch approach
# MAGIC
# MAGIC | Aspect | Batch (`spark.read.json`) | Auto Loader (`cloudFiles`) |
# MAGIC | --- | --- | --- |
# MAGIC | **Trigger** | Manual or scheduled one-time run | Continuous or micro-batch streaming |
# MAGIC | **New files** | Must re-read entire directory | Automatically detects and processes new files |
# MAGIC | **Schema handling** | Infers once; schema changes break the job | `schemaLocation` + `schemaEvolutionMode` handle changes |
# MAGIC | **Exactly-once** | Not guaranteed on re-runs | Checkpointing ensures exactly-once semantics |
# MAGIC | **Cost** | Full scan each run | Only processes new files (directory listing or file notification) |
# MAGIC
# MAGIC ### Auto Loader code example
# MAGIC ```python
# MAGIC # Auto Loader reads new JSON files incrementally from the volume
# MAGIC (spark.readStream
# MAGIC     .format("cloudFiles")
# MAGIC     .option("cloudFiles.format", "json")
# MAGIC     .option("cloudFiles.schemaLocation", "/Volumes/cfdb/raw/schema/games_schema")
# MAGIC     .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
# MAGIC     .load("/Volumes/cfdb/raw/raw_api_json/games/")
# MAGIC     .writeStream
# MAGIC     .format("delta")
# MAGIC     .option("checkpointLocation", "/Volumes/cfdb/raw/checkpoints/games")
# MAGIC     .option("mergeSchema", "true")
# MAGIC     .toTable("cfdb.raw.games_streaming")
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ### Certification tips
# MAGIC * Auto Loader uses the `cloudFiles` format — not `json` — in `spark.readStream.format("cloudFiles")`
# MAGIC * `cloudFiles.format` specifies the file format (json, csv, parquet, etc.)
# MAGIC * `schemaLocation` stores the inferred schema for evolution tracking
# MAGIC * `schemaEvolutionMode` options: `addNewColumns`, `addNewColumnsAndNestedFields`, `none`, `rescue`
# MAGIC * `checkpointLocation` is **required** for exactly-once processing and recovery
# MAGIC * File notification mode (uses cloud queue) vs. directory listing mode (polls) — Auto Loader auto-selects
# MAGIC * Auto Loader is tested on both the Databricks Data Engineer Associate and Professional exams