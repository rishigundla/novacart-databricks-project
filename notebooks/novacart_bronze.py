# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Incremental

# COMMAND ----------

# MAGIC %md
# MAGIC # Step 1: Import and Setup
# MAGIC
# MAGIC This cell imports the PySpark and Delta helpers used in the notebook, switches to the correct catalog, and makes sure the Bronze schema exists before we start loading data.

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *
from datetime import datetime
import uuid

# COMMAND ----------

spark.sql("USE CATALOG dbw_data_prj")

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC # Step 2: Bronze Control Table
# MAGIC This table stores the watermark for each source table.
# MAGIC
# MAGIC It helps the pipeline remember:
# MAGIC
# MAGIC - the latest timestamp already processed
# MAGIC - the latest primary key processed at that timestamp
# MAGIC - how many rows were written in the latest run
# MAGIC
# MAGIC This is what makes the Bronze load incremental and rerun-safe.

# COMMAND ----------

spark.sql(
        """
          CREATE TABLE IF NOT EXISTS dbw_data_prj.bronze.ingestion_control
          (
              layer_name STRING,
              table_name STRING,
              current_timestamp TIMESTAMP,
              currnet_pk INT,
              last_successful_timestamp TIMESTAMP,
              last_successful_pk INT,
              last_run_id STRING,
              rows_affected BIGINT,
              run_status STRING,
              updated_at TIMESTAMP
          )
          USING DELTA
        """
        )

# COMMAND ----------

# MAGIC %md
# MAGIC # Step 3: Source Table Configuration
# MAGIC This cell defines which source tables will be loaded into Bronze and which columns should be used as:
# MAGIC
# MAGIC - primary key
# MAGIC - timestamp / watermark column
# MAGIC
# MAGIC It also creates a unique `bronze_run_id` for the current pipeline run.

# COMMAND ----------

table_config = {
    "orders": {"current_timestamp": "updated_at", "current_pk": "order_id"},
    "products": {"current_timestamp": "updated_at", "current_pk": "product_id"},
    "payments": {"current_timestamp": "processed_at", "current_pk": "payment_id"}
}

bronze_run_id = uuid.uuid4()
print("Current Bronze Run ID: ",bronze_run_id)

# COMMAND ----------

# MAGIC %md
# MAGIC # Step 4: Helper Functions
# MAGIC This cell contains reusable functions:
# MAGIC
# MAGIC - `get_last_successful_watermark()` reads the last processed watermark from the control table
# MAGIC - `upsert_bronze_control()` updates the control table after a successful Bronze load
# MAGIC
# MAGIC These functions keep the main load logic cleaner and easier to understand.
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

def get_last_successful_watermark(table_name:str):
    ctrl = (
        spark.table("dbw_data_prj.bronze.ingestion_control")
        .filter(
            (col("layer_name") == "bronze")
            & (col("table_name") == table_name)
            & (col("run_status") == "success")
        )
        .orderBy(col("updated_at").desc())
        .limit(1)
    )
    rows = ctrl.collect()
    if not rows:
        return None
    else:
        return rows[0]["last_successful_timestamp"], rows[0]["last_successful_pk"]

# COMMAND ----------

def upsert_bronze_control(table_name, current_timestamp, currnet_pk, last_successful_timestamp, last_successful_pk, last_run_id, rows_affected):
    ctrl_df = spark.createDataFrame(
        [
            ("bronze", table_name, current_timestamp, currnet_pk, last_successful_timestamp, last_successful_pk, last_run_id, rows_affected, "success", datetime.utcnow())
        ],
        schema = """
            layer_name STRING,
            table_name STRING,
            current_timestamp TIMESTAMP,
            currnet_pk INT,
            last_successful_timestamp TIMESTAMP,
            last_successful_pk INT,
            last_run_id STRING,
            rows_affected BIGINT,
            run_status STRING,
            updated_at TIMESTAMP
            """
    )
    deltaTable = DeltaTable.forName(spark, "dbw_data_prj.bronze.ingestion_control")
    (
        deltaTable.alias("t")
        .merge(ctrl_df.alias("s"), "t.table_name = s.table_name AND t.layer_name = s.layer_name")
        .whenMatchedUpdate(
            set = {
                "current_timestamp": col("s.current_timestamp"),
                "currnet_pk": col("s.currnet_pk"),
                "last_successful_timestamp": col("s.last_successful_timestamp"),
                "last_successful_pk": col("s.last_successful_pk"),
                "last_run_id": col("s.last_run_id"),
                "rows_affected": col("s.rows_affected"),
                "run_status": col("s.run_status"),
                "updated_at": col("s.updated_at")
            }
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

# COMMAND ----------

# MAGIC %md
# MAGIC # Step 5 - Bronze incremental load loop
# MAGIC This is the main Bronze logic.
# MAGIC
# MAGIC For each table, the notebook:
# MAGIC
# MAGIC - reads the last watermark
# MAGIC - reads the source SQL table
# MAGIC - filters only new / changed rows
# MAGIC - adds Bronze audit columns
# MAGIC - appends the rows into the Bronze Delta table
# MAGIC - updates the control table
# MAGIC
# MAGIC This is the core incremental loading logic.

# COMMAND ----------

for table_name, cfg in table_config.items():
    current_timestamp_col = cfg["current_timestamp"]
    current_pk_col = cfg["current_pk"]
    source_table = f"`novacart-catalog`.dbo.{table_name}"
    target_table = f"dbw_data_prj.bronze.{table_name}_raw"

    last_successful = get_last_successful_watermark(table_name)
    if last_successful is not None:
        last_successful_timestamp, last_successful_pk = last_successful
        # Normalize microseconds to milliseconds for timestamp comparison
        if last_successful_timestamp is not None:
            last_successful_timestamp = last_successful_timestamp.replace(
                microsecond=(last_successful_timestamp.microsecond // 1000) * 1000
            )
    else:
        last_successful_timestamp, last_successful_pk = None, None

    print(f"Processing.....{table_name}")
    print(f"last_successful_timestamp:{last_successful_timestamp}")
    print(f"last_successful_pk:{last_successful_pk}")

    source_df = (
        spark.read.table(source_table)
        .withColumn("current_timestamp", date_trunc("MILLISECOND", col(current_timestamp_col).cast("timestamp")))
        .withColumn("current_pk", col(current_pk_col))
    )

    if last_successful_timestamp is None:
        rows_to_load = source_df
    else:
        if last_successful_pk is None:
            rows_to_load = source_df.filter(
                col("current_timestamp") > lit(last_successful_timestamp)
            )
        else:
            rows_to_load = source_df.filter(
                (col("current_timestamp") > lit(last_successful_timestamp)) |
                ((col("current_timestamp") == lit(last_successful_timestamp)) & (col("current_pk") > lit(last_successful_pk)))
            )

    rows_to_load = (
        rows_to_load.withColumn("bronze_ingested_at", current_timestamp())
        .withColumn("bronze_run_id", lit(str(bronze_run_id)))
        .withColumn("bronze_source_table", lit(source_table))
    )

    row_count = rows_to_load.count()
    print(f"{table_name} rows to load: {row_count}")

    if row_count == 0:
        print(f"No new rows for {table_name}")
        upsert_bronze_control(
            table_name,
            None,
            None,
            last_successful_timestamp,
            last_successful_pk,
            bronze_run_id,
            0
        )
        continue

    rows_to_load.write.format("delta").mode("append").saveAsTable(target_table)

    # Find the new watermark after loading
    watermark_row = (
        rows_to_load
        .select("current_timestamp", "current_pk")
        .orderBy(col("current_timestamp").desc(), col("current_pk").cast("long").desc())
        .limit(1)
        .collect()
    )
    if watermark_row:
        max_timestamp = watermark_row[0]["current_timestamp"]
        max_pk = watermark_row[0]["current_pk"]
    else:
        max_timestamp = None
        max_pk = None

    upsert_bronze_control(
        table_name,
        max_timestamp,
        max_pk,
        max_timestamp,
        max_pk,
        bronze_run_id,
        row_count
    )
    print(f"Finished {table_name} with {row_count} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC # Step 6: Quick validation
# MAGIC This final cell prints the Bronze row counts and displays the control table so you can verify that the incremental logic is working correctly.

# COMMAND ----------

print("Orders Bronze Count: ", spark.sql("select count(*) from dbw_data_prj.bronze.orders_raw").collect()[0][0])
print("Products Bronze Count: ", spark.sql("select count(*) from dbw_data_prj.bronze.products_raw").collect()[0][0])
print("Payments Bronze Count: ", spark.sql("select count(*) from dbw_data_prj.bronze.payments_raw").collect()[0][0])

display(spark.sql("select * from dbw_data_prj.bronze.ingestion_control").orderBy("table_name"))