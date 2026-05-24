# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Incremental

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Step 1 - Import and Setup
# MAGIC
# MAGIC This cell imports the required helpers, switches to the right catalog, makes sure the Gold schema exists, and creates:
# MAGIC
# MAGIC - a gold_run_id
# MAGIC - a run date string
# MAGIC - a run timestamp string
# MAGIC
# MAGIC These are used for tracking and snapshot publishing.

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

spark.sql("CREATE SCHEMA IF NOT EXISTS gold")

# COMMAND ----------

gold_run_id = uuid.uuid4()
print("Current Silver Run ID: ",gold_run_id)

# COMMAND ----------

run_ts_str = datetime.utcnow().strftime("%Y-%m-%d %H : %M :%S")
run_date_str = datetime.utcnow().strftime("%Y-%m-%d")
print("Run Timestamp Folder:", run_ts_str)

# COMMAND ----------

# MAGIC %md
# MAGIC # Step 2 - Gold Control Table
# MAGIC This table stores the latest Gold processing state.
# MAGIC
# MAGIC It tells Gold:
# MAGIC
# MAGIC - which Silver data was processed last time
# MAGIC - how many Gold rows were merged in the last run

# COMMAND ----------

spark.sql(
        """
          CREATE TABLE IF NOT EXISTS dbw_data_prj.gold.processing_control
          (
              layer_name STRING,
              entity_name STRING,
              last_processed_silver_run_id STRING,
              last_processed_silver_ingested_at TIMESTAMP,
              rows_affected BIGINT,
              run_status STRING,
              gold_run_id STRING,
              updated_at TIMESTAMP
          )
          USING DELTA
        """
        )

# COMMAND ----------

# MAGIC %md
# MAGIC # Step 3 - Helper Functions
# MAGIC This cell defines reusable Gold functions:
# MAGIC
# MAGIC - `upsert_to_gold()` merges data into Gold current-state tables
# MAGIC - `get_last_processed_silver_ingested_at()` reads the Gold watermark from the control table
# MAGIC - `upsert_gold_control()` updates Gold control after a successful run

# COMMAND ----------

def upsert_to_gold(df_source, target_table, join_key):
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
        df_source.write.format("delta").saveAsTable(target_table)

# COMMAND ----------

def get_last_processed_silver_ingested_at(entity_name: str):
    ctrl = (
        spark.table("dbw_data_prj.gold.processing_control")
        .filter(
            (col("layer_name") == "gold")
            & (col("entity_name") == entity_name)
            & (col("run_status") == "success")
        )
        .orderBy(col("updated_at").desc())
    )
    rows = ctrl.limit(1).collect()
    if not rows:
        return None
    else:
        return rows[0]["last_processed_silver_ingested_at"]

# COMMAND ----------

def upsert_gold_control(entity_name, last_processed_silver_run_id, last_processed_silver_ingested_at, rows_affected):
    ctrl_df = spark.createDataFrame(
        [
            ("gold", entity_name, last_processed_silver_run_id, last_processed_silver_ingested_at, rows_affected, "success", str(gold_run_id), datetime.utcnow())
        ],
        schema="""
              layer_name STRING,
              entity_name STRING,
              last_processed_silver_run_id STRING,
              last_processed_silver_ingested_at TIMESTAMP,
              rows_affected BIGINT,
              run_status STRING,
              gold_run_id STRING,
              updated_at TIMESTAMP
            """
    )
    deltaTable = DeltaTable.forName(spark, "dbw_data_prj.gold.processing_control")
    (
        deltaTable.alias("t")
        .merge(
            ctrl_df.alias("s"),
            "t.layer_name = s.layer_name AND t.entity_name = s.entity_name"
        )
        .whenMatchedUpdate(
            set={
                "last_processed_silver_run_id": "s.last_processed_silver_run_id",
                "last_processed_silver_ingested_at": "s.last_processed_silver_ingested_at",
                "rows_affected": "s.rows_affected",
                "run_status": "s.run_status",
                "gold_run_id": "s.gold_run_id",
                "updated_at": "s.updated_at"
            }
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

# COMMAND ----------

# MAGIC %md
# MAGIC # Step 4 - Read changed Silver Rows Only
# MAGIC
# MAGIC This cell reads the full Silver current-state tables but filters out only the rows that changed since the last Gold run.
# MAGIC
# MAGIC This is the starting point for Gold incremental processing.
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

last_gold_ts = get_last_processed_silver_ingested_at("orders")

print("Last Processed Silver Timestamp for Gold = ", last_gold_ts)

silver_orders_current = spark.read.table("dbw_data_prj.silver.orders_transformed")
silver_products_current = spark.read.table("dbw_data_prj.silver.products_transformed")
silver_payments_current = spark.read.table("dbw_data_prj.silver.payments_transformed")

if last_gold_ts is None:
    changed_orders = silver_orders_current
    changed_products = silver_products_current
    changed_payments = silver_payments_current
else:
    changed_orders = silver_orders_current.filter(col("updated_at") > last_gold_ts)
    changed_products = silver_products_current.filter(col("updated_at") > last_gold_ts)
    changed_payments = silver_payments_current.filter(col("processed_at") > last_gold_ts)

changed_orders_count = changed_orders.count()
changed_products_count = changed_products.count()
changed_payments_count = changed_payments.count()

print(f"Number of changed orders = {changed_orders_count}")
print(f"Number of changed products = {changed_products_count}")
print(f"Number of changed payments = {changed_payments_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Step 5 - Find Impacted Order IDs
# MAGIC Gold is built at order grain, so if anything changes in orders, products, or payments, we identify which `order_id` values are impacted.
# MAGIC
# MAGIC Only those order IDs are rebuilt in Gold.
# MAGIC
# MAGIC

# COMMAND ----------

impacted_from_orders = changed_orders.select("order_id").distinct()
impacted_from_payments = changed_payments.select("order_id").distinct()
impacted_from_products = (
    changed_products.alias("p")
    .join(silver_orders_current.alias("o"),
          col("p.product_id") == col("o.product_id"),
          "inner")
    .select(col("o.order_id")).distinct()
)

impacted_order_ids = (
    impacted_from_orders
    .union(impacted_from_payments)
    .union(impacted_from_products)
    .distinct()
)

print("impacted_order_ids = ", impacted_order_ids.count())
display(impacted_order_ids.orderBy("order_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC # Step 6 - Build Gold Delta for Impacted Orders
# MAGIC
# MAGIC This cell joins the impacted orders with the current Silver products and payments tables, derives business columns, and builds the Gold delta that will be merged into the Gold current-state table.

# COMMAND ----------

impacted_orders = (
    silver_orders_current.alias("o")
    .join(impacted_order_ids.alias("i"), "order_id", "inner")
)

gold_delta = (
    impacted_orders.alias("o")
    .join(
        silver_products_current.alias("p"),
        col("o.product_id") == col("p.product_id"),
        "inner"
    )
    .join(
        silver_payments_current.alias("py"),
        col("o.order_id") == col("py.order_id"),
        "inner"
    )
    .select(
        col("o.order_id"),
        col("o.customer_id"),
        col("p.product_id"),
        col("p.product_name"),
        col("p.category"),
        col("p.price").alias("product_price"),
        col("o.order_status"),
        col("o.order_amount"),
        col("py.payment_id"),
        col("py.payment_status"),
        col("py.paid_amount"),
        col("o.order_date"),
        col("o.order_month"),
        col("o.order_year"),
        greatest(
            col("o.updated_at").cast("timestamp"),
            col("p.updated_at").cast("timestamp"),
            col("p.updated_at").cast("timestamp")
        ).alias("gold_update_ts")
    )
    .dropDuplicates(["order_id"])
    .withColumn(
        "payment_completion_ratio",
        when(
            col("o.order_amount") > 0,
            col("py.paid_amount") / col("o.order_amount")
        ).otherwise(lit(0.0))
    )
    .withColumn(
        "payment_state",
        when(col("o.order_amount") == 0, "Invalid_order_amount")
        .when(col("payment_completion_ratio") == 0, "Unpaid")
        .when(col("payment_completion_ratio") == 1, "Paid")
        .when(col("payment_completion_ratio") < 1, "Partially_paid")
        .when(col("payment_completion_ratio") > 1, "Overpaid")
    )
    .withColumn("gold_updated_date", to_date(col("gold_update_ts")))
    .withColumn("gold_run_id", lit(str(gold_run_id)))
)

print("gold_delta_rows =", gold_delta.count())
display(gold_delta)

# COMMAND ----------

# MAGIC %md
# MAGIC # Step 7 - Merge Gold Current State Table
# MAGIC
# MAGIC If Gold delta contains rows, this cell merges them into `gold.orders_information`
# MAGIC
# MAGIC If there are no impacted rows, nothing is merged.

# COMMAND ----------

if gold_delta.count() > 0:
    upsert_to_gold(
        gold_delta,
        "dbw_data_prj.gold.orders_information",
        "order_id"
    )
else:
    print("No new rows to insert in gold table")

# COMMAND ----------

# MAGIC %md
# MAGIC # Step 8- Maintain Gold SCD Type 2 History
# MAGIC
# MAGIC This cell updates the SCD2 history table.
# MAGIC
# MAGIC If a current Gold row changes, the old version is closed ( is_current = false ) and a new current version is inserted.

# COMMAND ----------

if not spark.catalog.tableExists("dbw_data_prj.gold.orders_information_scd2"):
    spark.sql("""
        CREATE TABLE dbw_data_prj.gold.orders_information_scd2
        USING DELTA
        AS
        SELECT *,
            CAST(NULL AS TIMESTAMP) AS valid_from_ts,
            CAST(NULL AS TIMESTAMP) AS valid_to_ts,
            TRUE AS is_current
        FROM dbw_data_prj.gold.orders_information
        WHERE 1 = 0
    """)

if gold_delta.count() > 0:
    gold_delta.createOrReplaceTempView("gold_delta_view")
    spark.sql("""
        MERGE INTO dbw_data_prj.gold.orders_information_scd2 t
        USING gold_delta_view s
        ON t.order_id = s.order_id AND t.is_current = true
        WHEN MATCHED AND (
            NOT(t.order_status <=> s.order_status) OR
            NOT(t.order_amount <=> s.order_amount) OR
            NOT(t.paid_amount <=> s.paid_amount) OR
            NOT(t.payment_id <=> s.payment_id) OR
            NOT(t.category <=> s.category) OR
            NOT(t.product_name <=> s.product_name) OR
            NOT(t.product_price <=> s.product_price)
        )
        THEN UPDATE SET
            t.valid_to_ts = s.gold_update_ts,
            t.is_current = false
    """)

    spark.sql("""
        INSERT INTO dbw_data_prj.gold.orders_information_scd2
        SELECT s.*,
            s.gold_update_ts AS valid_from_ts,
            CAST(NULL AS TIMESTAMP) AS valid_to_ts,
            TRUE AS is_current
        FROM gold_delta_view s
        LEFT JOIN dbw_data_prj.gold.orders_information_scd2 t
        ON s.order_id = t.order_id AND t.is_current = true
        WHERE t.order_id IS NULL OR (
            NOT(t.order_status <=> s.order_status) OR
            NOT(t.order_amount <=> s.order_amount) OR
            NOT(t.paid_amount <=> s.paid_amount) OR
            NOT(t.payment_id <=> s.payment_id) OR
            NOT(t.category <=> s.category) OR
            NOT(t.product_name <=> s.product_name) OR
            NOT(t.product_price <=> s.product_price)
        )
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC # Step 9 - Update category-level Gold Aggregation
# MAGIC
# MAGIC This cell recalculates category-level business metrics only for categories impacted in the current run, then merges them into the category performance Gold table.

# COMMAND ----------

if gold_delta.count() > 0:
    impacted_categories = (
        gold_delta
        .select("category")
        .filter(col("category").isNotNull())
        .distinct()
    )

    orders_info = spark.read.table("dbw_data_prj.gold.orders_information")
    orders_info = orders_info.join(impacted_categories, "category", "inner")

    category_perf_delta = (
        orders_info
        .groupBy("category")
        .agg(
            countDistinct("order_id").alias("total_orders"),
            sum(
                when(col("order_amount") > 0, col("order_amount"))
                .otherwise(lit(0.0))
            ).alias("Gross_Merchandise_Value"),
            sum(
                when(col("paid_amount") > 0, col("paid_amount"))
                .otherwise(lit(0.0))
            ).alias("Total_Paid_Amount"),
            avg(col("payment_completion_ratio")).alias("Average_Payment_Completion_Ratio"),
            (sum(when(col("payment_status") == "FAILED", 1).otherwise(lit(0))) / count("*")).alias("Payment_Failure_Rate")
        )
    )

    upsert_to_gold(category_perf_delta, "dbw_data_prj.gold.category_performance", "category")

# COMMAND ----------

# MAGIC %md
# MAGIC # Step 10 - Publish Gold Snapshots to Volume
# MAGIC
# MAGIC This cell writes two kinds of Gold outputs to a Databricks Volume:
# MAGIC
# MAGIC - latest snapshot - overwritten every successful run
# MAGIC - timestamped historical snapshot - a new folder for each successful run
# MAGIC
# MAGIC This is useful for audit and rollback

# COMMAND ----------

spark.sql("CREATE VOLUME IF NOT EXISTS dbw_data_prj.gold.gold_snapshot_vol")

# COMMAND ----------

latest_orders_path = "/Volumes/dbw_data_prj/gold/gold_snapshot_vol/gold_latest/orders_information"
latest_category_path = "/Volumes/dbw_data_prj/gold/gold_snapshot_vol/gold_latest/category_performance"

historical_orders_path = f"/Volumes/dbw_data_prj/gold/gold_snapshot_vol/gold_snapshots/orders_information/run_date={run_date_str}/run_ts={run_ts_str}"
historical_category_path = f"/Volumes/dbw_data_prj/gold/gold_snapshot_vol/gold_snapshots/category_performance/run_date={run_date_str}/run_ts={run_ts_str}"

spark.read.table("dbw_data_prj.gold.orders_information").write.mode("overwrite").format("parquet").save(latest_orders_path)
spark.read.table("dbw_data_prj.gold.category_performance").write.mode("overwrite").format("parquet").save(latest_category_path)

spark.read.table("dbw_data_prj.gold.orders_information").write.mode("overwrite").format("parquet").save(historical_orders_path)
spark.read.table("dbw_data_prj.gold.category_performance").write.mode("overwrite").format("parquet").save(historical_category_path)

print("Latest Orders Path :", latest_orders_path)
print("Latest Category Path :", latest_category_path)
print("Historical Orders Path :", historical_orders_path)
print("Historical Category Path :", historical_category_path)

# COMMAND ----------

# MAGIC %md
# MAGIC # Step 11 - Update Gold Control Table
# MAGIC
# MAGIC This final cell updates the Gold control table using the latest Silver processing metadata and displays the control table for validation.

# COMMAND ----------

latest_silver_ts = silver_orders_current.agg(max("bronze_ingested_at").alias("mx")).collect()[0]["mx"]

latest_silver_run_id = (
    silver_orders_current
    .filter(col("bronze_ingested_at") == latest_silver_ts)
    .agg(max("silver_run_id").alias("mx"))
    .collect()[0]["mx"]
) if latest_silver_ts is not None else None

upsert_gold_control("orders", latest_silver_run_id, latest_silver_ts, gold_delta.count())
display(spark.table("dbw_data_prj.gold.processing_control"))