# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Incremental

# COMMAND ----------

# MAGIC %md
# MAGIC # Step 1 - Imports and Setup
# MAGIC
# MAGIC This cell imports Spark, Window, and Delta helpers, switches to the right catalog, makes sure the Silver schema exists, and creates a
# MAGIC `silver_run_id` for the current run.

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *
from datetime import datetime
from pyspark.sql.window import Window
import uuid

# COMMAND ----------

spark.sql("USE CATALOG dbw_data_prj")

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS silver")

# COMMAND ----------

silver_run_id = uuid.uuid4()
print("Current Silver Run ID: ",silver_run_id)

# COMMAND ----------

# MAGIC %md
# MAGIC # Step 2 - Silver Control Table
# MAGIC
# MAGIC This table stores the latest Silver processing state for each entity.
# MAGIC
# MAGIC It helps us track:
# MAGIC
# MAGIC - the latest Bronze run already processed by Silver
# MAGIC - the latest Bronze ingestion timestamp already processed
# MAGIC - how many rows were merged in the latest Silver run

# COMMAND ----------

spark.sql(
        """
          CREATE TABLE IF NOT EXISTS dbw_data_prj.silver.processing_control
          (
              layer_name STRING,
              entity_name STRING,
              last_processed_bronze_run_id STRING,
              last_processed_bronze_ingested_at TIMESTAMP,
              rows_affected BIGINT,
              run_status STRING,
              silver_run_id STRING,
              updated_at TIMESTAMP
          )
          USING DELTA
        """
        )

# COMMAND ----------

# MAGIC %md
# MAGIC # Step 3 - Helper Functions
# MAGIC This cell contains reusable logic for Silver:
# MAGIC
# MAGIC - `upsert_to_silver()` merges cleaned / transformed rows into the Silver target table
# MAGIC - `get_last_processed_bronze_ingested_at()` reads the Silver watermark
# MAGIC - `upsert_silver_control()` updates the Silver control table
# MAGIC - `get_incremental_bronze()` reads only new Bronze rows that Silver has not processed yet

# COMMAND ----------

def upsert_to_silver(df_source, target_table, join_key):
    if spark.catalog.tableExists(target_table):
        dt = DeltaTable.forName(spark, target_table)
        (
            dt.alias("t")
            .merge(df_source.alias("s"), f"t.{join_key} = s.{join_key}")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        df_source.write.format("delta").mode("overwrite").saveAsTable(target_table)

# COMMAND ----------

def get_last_processed_bronze_ingested_at(entity_name: str):
    ctrl = (
        spark.table("dbw_data_prj.silver.processing_control")
        .filter(
            (col("layer_name") == "silver")
            & (col("entity_name") == entity_name)
            & (col("run_status") == "success")
        )
        .orderBy(col("updated_at").desc())
    )
    rows = ctrl.limit(1).collect()
    if not rows:
        return None, None
    else:
        return rows[0]["last_processed_bronze_ingested_at"], rows[0]["last_processed_bronze_run_id"]

# COMMAND ----------

def upsert_silver_control(entity_name, last_processed_bronze_run_id, last_processed_bronze_ingested_at, rows_affected):
    ctrl_df = spark.createDataFrame(
        [
            ("silver", entity_name, last_processed_bronze_run_id, last_processed_bronze_ingested_at, rows_affected, "success", str(silver_run_id), datetime.utcnow())
        ],
        schema="""
              layer_name STRING,
              entity_name STRING,
              last_processed_bronze_run_id STRING,
              last_processed_bronze_ingested_at TIMESTAMP,
              rows_affected BIGINT,
              run_status STRING,
              silver_run_id STRING,
              updated_at TIMESTAMP
            """
    )
    deltaTable = DeltaTable.forName(spark, "dbw_data_prj.silver.processing_control")
    (
        deltaTable.alias("t")
        .merge(
            ctrl_df.alias("s"),
            "t.layer_name = s.layer_name AND t.entity_name = s.entity_name"
        )
        .whenMatchedUpdate(
            set={
                "last_processed_bronze_run_id": "s.last_processed_bronze_run_id",
                "last_processed_bronze_ingested_at": "s.last_processed_bronze_ingested_at",
                "rows_affected": "s.rows_affected",
                "run_status": "s.run_status",
                "silver_run_id": "s.silver_run_id",
                "updated_at": "s.updated_at"
            }
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

# COMMAND ----------

def get_incremental_bronze(bronze_table, entity_name):
    last_ingested_at, last_run_id = get_last_processed_bronze_ingested_at(entity_name)
    bronze_df = spark.read.table(bronze_table)

    if last_ingested_at is None:
        return bronze_df, last_ingested_at, last_run_id

    return bronze_df.filter(col("bronze_ingested_at") > lit(last_ingested_at)), last_ingested_at, last_run_id

# COMMAND ----------

# MAGIC %md
# MAGIC # Step 4 - Orders Incremental Processing
# MAGIC
# MAGIC This cell processes orders from Bronze to Silver.
# MAGIC
# MAGIC It does the following:
# MAGIC
# MAGIC - reads only new Bronze order rows
# MAGIC - cleans values like `order_status` and `order_amount`
# MAGIC - keeps only the latest version per `order_id`
# MAGIC - validates business rules
# MAGIC - sends bad rows to quarantine
# MAGIC - merges good rows into `orders_transformed`
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Cell 15
# Step 4 - Orders incremental processing
# Read only the Bronze order rows that Silver has not processed yet.
orders_inc, last_orders_ingested_at, last_orders_run_id = get_incremental_bronze("dbw_data_prj.bronze.orders_raw","orders")

# Count the incremental order rows entering Silver in this run.
orders_inc_count = orders_inc.count()
print(f"orders rows_to_process_in_silver = {orders_inc_count}")

# Only run Silver order cleaning and validation when there are new Bronze order rows.
if orders_inc_count > 0:
    # Create a window that keeps the latest order record for each order_id.
    order_window = Window.partitionBy("order_id").orderBy(
        col("updated_at").cast("timestamp").desc(),
        col("bronze_ingested_at").desc()
    )

    # Start the Silver order-cleaning pipeline. This block standardizes and deduplicates raw order records.
    orders_cleaned = (
        orders_inc
        # Standardize order_status to uppercase so values such as shipped and SHIPPED become consistent.
        .withColumn("order_status", upper(trim(col("order_status"))))
        .withColumn("order_status", when(col("order_status") == "", lit(None)).otherwise(col("order_status")))
        # Remove formatting characters from order_amount so it can be cast to a numeric type.
        .withColumn("order_amount", regexp_replace(col("order_amount"), r"[$, ]", ""))
        .withColumn("order_amount", when(trim(col("order_amount")).isin("N/A", "NULL", " ?? ", ""), None).otherwise(col("order_amount")))
        .withColumn("order_amount", col("order_amount").cast("double"))
        .withColumn("created_at", to_timestamp("created_at"))
        .withColumn("updated_at", to_timestamp("updated_at"))
        # Assign a row number inside each business key so we can keep only the latest version of that record.
        .withColumn("row_rank", row_number().over(order_window))
        # Keep only the latest record for each business key.
        .filter(col("row_rank") == 1)
        .drop("row_rank")
        .withColumn("silver_run_id", lit(str(silver_run_id)))
    )

    # Merge the cleaned or validated Silver dataset into its Delta target table.
    upsert_to_silver(orders_cleaned, "dbw_data_prj.silver.orders_cleaned", "order_id")

    # Apply Silver data-quality rules to the cleaned order records.
    orders_validated = (
        orders_cleaned
        .withColumn(
            "to_be_verified_by_orders_team", when(col("customer_id").isNull(), "verify_customer_id")
            .when(col("product_id").isNull(), "verify_product_id")
            .when(col("order_status").isNull() | (trim(col("order_status")) == ""), "verify_order_status")
            .when(col("order_amount").isNull() | (col("order_amount") <= 0), "verify_order_amount")
            .otherwise("No Issues")
        )
        .withColumn("check_order_amount", when(col("order_amount").isNull() | (col("order_amount") <= 0), lit(True)).otherwise(lit(False)))
        .withColumn("order_date", to_date("created_at"))
        .withColumn("order_year", year("created_at"))
        .withColumn("order_month", month("created_at"))
        .withColumn("order_day", dayofmonth("created_at"))
        .withColumn("order_dow", date_format("created_at", "E"))
    )

    # Keep only valid order rows for the transformed Silver table.
    orders_good = orders_validated.filter(col("to_be_verified_by_orders_team") == "No Issues")
    # Send invalid order rows to the quarantine dataset for manual review.
    orders_bad = (
        orders_validated
        .filter(col("to_be_verified_by_orders_team") != "No Issues")
        .withColumn("quarantine_ts", current_timestamp())
    )

    # Merge the cleaned or validated Silver dataset into its Delta target table.
    upsert_to_silver(
        orders_good,
        "dbw_data_prj.silver.orders_transformed",
        "order_id"
    )

    # Append bad order rows to the quarantine table instead of losing them.
    orders_bad.write.format("delta").mode("append").saveAsTable("dbw_data_prj.silver.orders_quarantine")

    mx_ingested = orders_inc.agg(max("bronze_ingested_at").alias("mx")).collect()[0]["mx"]
    mx_run = (
        orders_inc.filter(col("bronze_ingested_at") == lit(mx_ingested))
        .agg(max("bronze_run_id").alias("mx"))
        .collect()[0]["mx"]
    )

    upsert_silver_control("orders", mx_run, mx_ingested, orders_good.count())

else:
    print("No new orders Bronze rows for Silver.")

    upsert_silver_control(
        "orders",
        None,
        last_orders_ingested_at,
        last_orders_run_id,
        orders_inc_count
    )

# COMMAND ----------

# MAGIC %md
# MAGIC # Step 5 - Products incremental processing
# MAGIC
# MAGIC This cell processes products from Bronze to Silver.
# MAGIC
# MAGIC It handles:
# MAGIC
# MAGIC - product name cleanup
# MAGIC - category standardization
# MAGIC - price cleanup and numeric conversion
# MAGIC - latest-record selection per `product_id`
# MAGIC - data quality validation
# MAGIC - quarantine for bad rows
# MAGIC - merge into Silver current-state tables

# COMMAND ----------

# DBTITLE 1,Cell 17
# Step 5 - Products incremental processing
# Read only the Bronze product rows that Silver has not processed yet.
products_inc, last_products_ingested_at, last_payments_run_id = get_incremental_bronze("dbw_data_prj.bronze.products_raw", "products")

# Count the incremental product rows entering Silver in this run.
products_inc_count = products_inc.count()
print(f"products rows_to_process_in_silver = {products_inc_count}")

if products_inc_count > 0:
    # Create a window that keeps the latest product record for each product_id.
    product_window = Window.partitionBy("product_id").orderBy(
        col("updated_at").cast("timestamp").desc(),
        col("bronze_ingested_at").desc()
    )

    # Start the Silver product-cleaning pipeline. This block standardizes and deduplicates raw product records.
    products_cleaned = (
        products_inc
        # Standardize product_name by trimming spaces and converting text to uppercase.
        .withColumn("product_name", upper(trim(col("product_name"))))
        .withColumn("product_name", regexp_replace(col("product_name"), r"^PROD[_-]+(?=\d)", "PROD "))
        .withColumn("product_name", when(col("product_name") == "", lit(None)).otherwise(col("product_name")))
        .withColumn(
            "category",
            when(upper(trim(col("category"))).contains("ELECTRNICS"), "ELECTRONICS")
            .when(upper(trim(col("category"))) == "LIFESTYLE", "LIFESTYLE")
            .otherwise(upper(trim(col("category"))))
        )
        # Start cleaning the product price field before converting it to numeric.
        .withColumn("price", trim(col("price")))
        .withColumn("price", regexp_replace(col("price"), r"\$", ""))
        .withColumn("price", regexp_replace(col("price"), ",", ""))
        .withColumn("price", regexp_replace(col("price"), r"\s+", ""))
        .withColumn("price", expr("try_cast(price as double)"))
        .withColumn("updated_at", to_timestamp("updated_at"))
        # Assign a row number inside each business key so we can keep only the latest version of that record.
        .withColumn("row_rank", row_number().over(product_window))
        # Keep only the latest record for each business key.
        .filter(col("row_rank") == 1)
        .drop("row_rank")
        .withColumn("silver_run_id", lit(str(silver_run_id)))
    )

    # Merge the cleaned or validated Silver dataset into its Delta target table.
    upsert_to_silver(products_cleaned, "dbw_data_prj.silver.products_cleaned", "product_id")

    # Apply Silver data-quality rules to the cleaned product records.
    products_validated = (
        products_cleaned
        .withColumn(
            "to_be_verified_by_products_team",
            when(col("product_name").isNull(), "verify_product_name")
            .when(col("category").isNull(), "verify_category")
            .when(col("price").isNull() | (col("price") <= 0), "verify_price")
            .otherwise("No Issues")
        )
        .withColumn(
            "check_product_price",
            when(col("price").isNull() | (col("price") <= 0), lit("invalid_price")).otherwise(lit("valid_price"))
        )
    )

    # Keep only valid product rows for the transformed Silver table.
    products_good = products_validated.filter(
        (col("to_be_verified_by_products_team") == "No Issues") &
        (col("check_product_price") == "valid_price")
    )

    if "price_raw" in products_good.columns:
        products_good = products_good.drop("price_raw")

    # Send invalid product rows to the quarantine dataset for manual review.
    products_bad = products_validated.filter(
        (col("to_be_verified_by_products_team") != "No Issues") |
        (col("check_product_price") == "invalid_price")
    ).withColumn("quarantine_ts", current_timestamp())

    # Merge the cleaned or validated Silver dataset into its Delta target table.
    upsert_to_silver(products_good, "dbw_data_prj.silver.products_transformed", "product_id")
    # Append bad product rows to the quarantine table instead of losing them.
    products_bad.write.format("delta").mode("append").saveAsTable("dbw_data_prj.silver.products_quarantine")

    mx_ingested = products_inc.agg(max("bronze_ingested_at").alias("mx")).collect()[0]["mx"]
    mx_run = products_inc.filter(col("bronze_ingested_at") == lit(mx_ingested)).agg(max("bronze_run_id").alias("mx")).collect()[0]["mx"]
    upsert_silver_control("products", mx_run, mx_ingested, products_good.count())
else:
    print("No new products Bronze rows for Silver.")
    upsert_silver_control(
        "products",
        None,
        last_products_ingested_at,
        last_payments_run_id,
        products_inc_count
    )

# COMMAND ----------

# MAGIC %md
# MAGIC # Step 6 - Payments Incremental Processing
# MAGIC
# MAGIC This cell processes payments from Bronze to Silver.
# MAGIC
# MAGIC It cleans:
# MAGIC
# MAGIC - payment_status
# MAGIC - paid_amount
# MAGIC - processed_at
# MAGIC
# MAGIC Then it validates records, quarantines bad rows, and merges valid rows into the Silver transformed payments table.

# COMMAND ----------

# Step 6 - Payments incremental processing
# Read only the Bronze payment rows that Silver has not processed yet.
payments_inc, last_payments_ingested_at, last_payments_run_id = get_incremental_bronze("dbw_data_prj.bronze.payments_raw", "payments")
print("Payments last processed Bronze ingested_at =", last_payments_ingested_at)

# Count the incremental payment rows entering Silver in this run.
payments_inc_count = payments_inc.count()
print(f"payments rows_to_process_in_silver = {payments_inc_count}")

if payments_inc_count > 0:
    # Create a window that keeps the latest payment record for each payment_id.
    payment_window = Window.partitionBy("payment_id").orderBy(
        col("processed_at").cast("timestamp").desc(),
        col("bronze_ingested_at").desc()
    )

    # Start the Silver payment-cleaning pipeline. This block standardizes and deduplicates raw payment records.
    payments_cleaned = (
        payments_inc
        .withColumn("payment_status", upper(trim(col("payment_status"))))
        .withColumn("payment_status", when(col("payment_status") == "", lit(None)).otherwise(col("payment_status")))
        # Start cleaning the payment amount field before converting it to numeric.
        .withColumn("paid_amount", trim(col("paid_amount")))
        .withColumn("paid_amount", regexp_replace(col("paid_amount"), r"\$", ""))
        .withColumn("paid_amount", regexp_replace(col("paid_amount"), ",", ""))
        .withColumn("paid_amount", regexp_replace(col("paid_amount"), r"\s+", ""))
        .withColumn("paid_amount", expr("try_cast(paid_amount as double)"))
        .withColumn("processed_at", to_timestamp("processed_at"))
        # Assign a row number inside each business key so we can keep only the latest version of that record.
        .withColumn("row_rank", row_number().over(payment_window))
        # Keep only the latest record for each business key.
        .filter(col("row_rank") == 1)
        .drop("row_rank")
        .withColumn("silver_run_id", lit(str(silver_run_id)))
    )

    # Merge the cleaned or validated Silver dataset into its Delta target table.
    upsert_to_silver(payments_cleaned, "dbw_data_prj.silver.payments_cleaned", "payment_id")

    # Apply Silver data-quality rules to the cleaned payment records.
    payments_validated = (
        payments_cleaned
        .withColumn(
            "to_be_verified_by_payments_team",
            when(col("order_id").isNull(), "verify_order_id")
            .when(col("payment_status").isNull(), "verify_payment_status")
            .when(col("paid_amount").isNull() | (col("paid_amount") <= 0), "verify_paid_amount")
            .otherwise("No Issues")
        )
        .withColumn(
            "check_paid_amount",
            when(col("paid_amount").isNull() | (col("paid_amount") <= 0), lit(True)).otherwise(lit(False))
        )
    )

    # Keep only valid payment rows for the transformed Silver table.
    payments_good = payments_validated.filter(col("to_be_verified_by_payments_team") == "No Issues")
    # Send invalid payment rows to the quarantine dataset for manual review.
    payments_bad = payments_validated.filter(col("to_be_verified_by_payments_team") != "No Issues").withColumn("quarantine_ts", current_timestamp())

    # Merge the cleaned or validated Silver dataset into its Delta target table.
    upsert_to_silver(payments_good, "dbw_data_prj.silver.payments_transformed", "payment_id")
    # Append bad payment rows to the quarantine table instead of losing them.
    payments_bad.write.format("delta").mode("append").saveAsTable("dbw_data_prj.silver.payments_quarantine")

    mx_ingested = payments_inc.agg(max("bronze_ingested_at").alias("mx")).collect()[0]["mx"]
    mx_run = payments_inc.filter(col("bronze_ingested_at") == lit(mx_ingested)).agg(max("bronze_run_id").alias("mx")).collect()[0]["mx"]
    upsert_silver_control("payments", mx_run, mx_ingested, payments_good.count())
else:
    print("No new payments Bronze rows for Silver.")
    upsert_silver_control(
        "payments",
        None,
        last_payments_ingested_at,
        last_payments_run_id,
        payments_inc_count
    )

# COMMAND ----------

# MAGIC %md
# MAGIC # Step 7 - Quick Validation
# MAGIC
# MAGIC This final cell prints Silver transformed row counts and shows the Silver control table so you can confirm the incremental processing behavior.

# COMMAND ----------

display(spark.sql("SELECT COUNT(*) AS products_transformed_count FROM dbw_data_prj.silver.products_transformed"))
display(spark.sql("SELECT COUNT(*) AS orders_transformed_count FROM dbw_data_prj.silver.orders_transformed"))
display(spark.sql("SELECT COUNT(*) AS payments_transformed_count FROM dbw_data_prj.silver.payments_transformed"))

display(spark.table("dbw_data_prj.silver.processing_control").orderBy("entity_name"))

# COMMAND ----------

# MAGIC %md
# MAGIC