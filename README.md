# NovaCart Data Engineering Project: An Incre mental Order to Insight Pipeline on Databrick s

This project implements a fully incrementa l **End to End Data Engineering Pipeline** on  the **Databricks Lakehouse Platform** for ** NovaCart**, an online retailer whose orders,  products, and payments flow from an **Azure S QL Database** into a unified, governed analyt ical layer. It leverages **Databricks Lakehou se Federation**, the **Medallion Architecture **, **Delta Lake**, **Unity Catalog**, **SCD  Type 2**, and **Databricks Workflows** to del iver reliable, watermark driven, and business  ready Gold tables along with audit ready sna pshots on a Unity Catalog Volume.

---

## � �� Project Goals & Business Solution

### Bus iness Overview

* **NovaCart (Online Retailer ):** NovaCart processes a continuous stream o f customer orders, product updates, and payme nt transactions every day. The transactional  data is captured in an Azure SQL Database (OL TP) and used by operations and finance teams  for daily reconciliation, payment investigati on, and category performance reviews.
* **Ana lytical Gap:** The OLTP source is not built f or heavy analytical scans or for tracking his torical changes. Querying the source database  directly puts pressure on the transactional  system, misses historical changes such as pri ce corrections and payment status updates, an d offers no isolation between data quality is sues and downstream consumers.

### Managemen t Expectation

The management requires a **si ngle source of truth** for NovaCart order ana lytics that captures every change to orders,  products, and payments, isolates bad records,  tracks history through SCD2, and publishes g overned Gold outputs to a Unity Catalog Volum e without touching the source database.

###  Data Engineering Solution

The Data Engineeri ng team builds a fully **incremental pipeline ** that uses **Databricks Lakehouse Federatio n** to read directly from the Azure SQL Datab ase, lands raw rows in the **Bronze layer**,  cleans and validates them in the **Silver lay er** while quarantining bad rows, and then ** joins, enriches, and historizes** the order g rain in the **Gold layer**. The Gold layer ma intains an SCD2 history table, category level  aggregations, and timestamped snapshots on a  Databricks Volume. The full chain is orchest rated by **Databricks Jobs and Workflows**.

 ---

## 🌟 Project Highlights

* **Lakehous e Federation Ingestion:** Reads the three sou rce tables directly from Azure SQL Database t hrough a Unity Catalog foreign catalog, with  no external file staging.
* **Watermark Drive n Bronze Layer:** A dual key watermark on `(t imestamp, primary_key)` and an `ingestion_con trol` Delta table make every Bronze run incre mental and rerun safe.
* **Silver Layer Stand ardization and Quarantine:** Row level dedupl ication using `row_number()` windows, busines s rule validation, and a dedicated quarantine  table for rows that fail data quality checks .
* **Gold Layer with SCD Type 2 History:** I mpacted order detection across Orders, Produc ts, and Payments drives targeted Gold rebuild s. Historical changes are captured in `orders _information_scd2` so analysts can see what a  record looked like at any point in time.
* * *Category Aggregations:** A `category_perform ance` table maintains Gross Merchandise Value , total orders, total paid amount, average pa yment completion ratio, and payment failure r ate. Only impacted categories are refreshed o n each run.
* **Snapshot Publishing:** Every  Gold run writes a latest snapshot and a times tamped historical snapshot to a Unity Catalog  Volume for audit and rollback.
* **Sequentia l Orchestration:** Databricks Jobs and Workfl ows execute the three notebooks in order, eac h driven by its own Delta control table so re runs and partial failures are safe.

---

##  🧭 Technical Architecture & Data Flow

This  solution implements a watermark driven Medal lion flow on top of Unity Catalog. Each layer  owns its own control table and has one clear  responsibility, which keeps the pipeline eas y to reason about and easy to operate. The pi peline terminates at the Gold layer with mana ged Delta tables and snapshot outputs on a Un ity Catalog Volume.

| Layer | Processing Log ic | Output Tables |
| :--- | :--- | :--- |
|  **Source** | Three Azure SQL tables exposed  to Databricks through **Lakehouse Federation* * under the foreign catalog `novacart-catalog `. | `orders`, `products`, `payments` (foreig n tables). |
| **01_Bronze** | **Incremental  Loading:** Reads only new or changed rows usi ng a dual key watermark of `(timestamp, prima ry_key)`. Adds `bronze_ingested_at`, `bronze_ run_id`, and `bronze_source_table` audit colu mns. | `bronze.orders_raw`, `bronze.products_ raw`, `bronze.payments_raw`, `bronze.ingestio n_control`. |
| **02_Silver** | **Cleaning, S tandardization, and Validation:** Trims and u ppercases categorical fields, parses amounts  and timestamps, deduplicates using `row_numbe r()` windows, validates business rules, and r outes good and bad rows to separate tables. |  `silver.orders_cleaned`, `silver.orders_tran sformed`, `silver.orders_quarantine`, equival ent tables for products and payments, plus `s ilver.processing_control`. |
| **03_Gold** |  **Joining, Enrichment, and History Tracking:* * Identifies impacted orders from changes in  any of the three Silver entities, builds an e nriched Gold delta, MERGEs into the current s tate table, applies SCD2 history, and refresh es category aggregations only for impacted ca tegories. | `gold.orders_information`, `gold. orders_information_scd2`, `gold.category_perf ormance`, `gold.processing_control`. |
| **Vo lume Snapshots** | **Audit and Rollback Outpu ts:** Writes the latest and a timestamped his torical snapshot of `orders_information` and  `category_performance` to a Unity Catalog Vol ume on every successful Gold run. | `/Volumes /dbw_data_prj/gold/gold_snapshot_vol/gold_lat est/...` and `/Volumes/.../gold_snapshots/... `. |
| **Orchestration** | **Databricks Jobs  and Workflows:** Run the three notebooks sequ entially. Each layer reads its own control ta ble so the workflow is restartable. | Workflo w runs and per layer control row updates. |

 ![NovaCart Architecture](./images/novacart_ar chitecture.svg)

---

## ⚙️ Orchestration : Databricks Jobs & Workflows

The pipeline i s automated using a Databricks Workflow job t hat wires the three layers together as a depe ndency chain. Each layer reads its own Delta  control table, so reruns and partial failures  are safe.

* **Trigger Mechanism:** The work flow runs on a recurring schedule and can als o be triggered on demand. Each run picks up o nly the rows that arrived in the source since  the last successful watermark.
* **Execution  Order:** Bronze runs first to land new raw r ows from Azure SQL through Lakehouse Federati on. Silver runs next to clean, deduplicate, v alidate, and quarantine. Gold runs last to id entify impacted orders, MERGE the current sta te, write SCD2 history, refresh category aggr egations, and publish Volume snapshots.
* **R estart Safety:** The Bronze `ingestion_contro l`, Silver `processing_control`, and Gold `pr ocessing_control` Delta tables capture the la st successful watermark for each entity. A fa iled task can be retried independently withou t reprocessing data that has already been mer ged.

![Job Run](./images/novacart_job_run.pn g)

---

## 🧰 Tech Stack

| Category | Too ls / Tech Used |
| :--- | :--- |
| **Data Pla tform** | **Azure Databricks** |
| **Source S ystem** | **Azure SQL Database** (OLTP) |
| * *Ingestion** | **Databricks Lakehouse Federat ion** via Unity Catalog Foreign Catalog |
| * *Storage** | **Delta Lake**, Unity Catalog Ma naged Tables, Unity Catalog **Volumes** for s napshots |
| **Architecture** | Medallion (Br onze, Silver, Gold) with watermark control ta bles |
| **Languages** | **PySpark**, **SQL**  |
| **History Tracking** | Delta MERGE with  **SCD Type 2** |
| **Orchestration** | **Data bricks Jobs and Workflows** |
| **Version Con trol** | **GitHub** |

---

## 🧱 Data Mode l (Gold Layer)

The Gold layer is built at th e order grain. Each row in `orders_informatio n` represents the current state of one custom er order along with its joined product attrib utes, payment status, and derived business co lumns such as `payment_completion_ratio` and  `payment_state`.

### Catalog & Schemas
* **C atalog:** `dbw_data_prj`
* **Schemas:** `dbw_ data_prj.bronze`, `dbw_data_prj.silver`, `dbw _data_prj.gold`

| Table Type | Table Name |  Purpose |
| :--- | :--- | :--- |
| **Current  State Fact** | `gold.orders_information` | Or der grain table built by joining cleaned orde rs, products, and payments. Holds `payment_co mpletion_ratio` and `payment_state` (Paid, Un paid, Partially_paid, Overpaid, Invalid_order _amount). |
| **SCD2 History** | `gold.orders _information_scd2` | Slowly changing dimensio n Type 2 history for tracked Gold attributes.  Adds `valid_from_ts`, `valid_to_ts`, and `is _current`. Closes the old version and inserts  a new current version whenever any tracked c olumn changes. |
| **Aggregation** | `gold.ca tegory_performance` | Category level KPIs: to tal orders, Gross Merchandise Value, total pa id amount, average payment completion ratio,  and payment failure rate. Refreshed only for  categories impacted in the current run. |
| * *Control Table** | `gold.processing_control`  | Tracks the latest Silver run already consum ed by Gold and the number of Gold rows merged  in the latest run. |
| **Snapshot Volume** |  `gold.gold_snapshot_vol` | Stores latest and  timestamped historical Parquet snapshots of  `orders_information` and `category_performanc e`. |

---

## 💾 Data Folder Structure

� �� **`/datasets`**

The `datasets/` folder ho lds the SQL scripts used to set up and exerci se the Azure SQL source. These scripts are no t consumed by Databricks at runtime, but they  make the source environment reproducible.

|  File | Purpose |
| :--- | :--- |
| `sql-tabl es-creation.sql` | Creates the source tables  (`orders`, `products`, `payments`) in Azure S QL Database. |
| `initial-load-for-mess-data. sql` | Loads the initial messy backfill data,  including the deliberate quality issues that  Silver is designed to clean and quarantine.  |
| `IncrementalLoad-1.sql` | Simulates the f irst incremental batch of new and updated row s in the source. |
| `IncrementalLoad-2.sql`  | Simulates a second incremental batch, used  to validate watermark behavior and SCD2 histo ry tracking. |

📁 **`/notebooks`**

The `n otebooks/` folder holds the three Databricks  notebooks that implement the Medallion flow.
 
| File | Purpose |
| :--- | :--- |
| `novaca rt_bronze.py` | Bronze incremental load and B ronze control table maintenance. |
| `novacar t_silver.py` | Silver cleaning, deduplication , validation, quarantine, and control updates . |
| `novacart_gold.py` | Gold MERGE, SCD2 h istory, category aggregations, Volume snapsho t publishing, and control updates. |

---

##  📓 Project Notebooks & Workflow Steps

The  pipeline is implemented in three modular PyS park notebooks. The Databricks Workflow runs  them in the order shown below.

| Step | Note book | Description |
| :--- | :--- | :--- |
|  **01** | `novacart_bronze.py` | Reads Azure  SQL source tables (`orders`, `products`, `pay ments`) through Lakehouse Federation, applies  a dual key watermark of `(timestamp, primary _key)`, appends new or changed rows into `ord ers_raw`, `products_raw`, and `payments_raw`,  and updates `bronze.ingestion_control`. |
|  **02** | `novacart_silver.py` | For each enti ty, reads only Bronze rows newer than the las t Silver watermark, standardizes text and num eric fields, deduplicates with `row_number()`  windows, validates business rules, MERGEs va lid rows into the `_transformed` tables, appe nds invalid rows to the `_quarantine` tables,  and updates `silver.processing_control`. |
|  **03** | `novacart_gold.py` | Detects impact ed orders across Silver orders, products, and  payments. Joins them with the current Silver  products and payments, derives `payment_comp letion_ratio` and `payment_state`, MERGEs int o `gold.orders_information`, maintains `order s_information_scd2`, refreshes `category_perf ormance` for impacted categories only, and pu blishes latest and timestamped snapshots to t he Gold Volume. |

All notebooks are included  in the [`notebooks/`](./notebooks) directory  for reference.

---

## 📚 Key Learnings

 * Designing and implementing a **watermark dr iven Medallion pipeline** with three independ ent control tables (Bronze, Silver, and Gold)  so that each layer is independently restarta ble and idempotent.
* Using **Databricks Lake house Federation** to ingest from Azure SQL D atabase without external staging files or thi rd party connectors.
* Applying **row level d eduplication** with `row_number()` windows or dered by source timestamp and Bronze ingestio n timestamp to keep only the latest version o f each business key.
* Separating valid recor ds and quarantined records in the Silver laye r so that data quality issues are isolated fr om the downstream Gold layer.
* Building a Go ld layer that combines **incremental MERGE**,  **SCD Type 2 history**, and **targeted aggre gation refresh** in a single notebook driven  by impacted order detection.
* Publishing **l atest and timestamped historical snapshots**  to a Unity Catalog Volume for audit and rollb ack support.
* Orchestrating the full pipelin e with **Databricks Jobs and Workflows**, wit h each layer using its own Delta control tabl e for safe and incremental restarts.

---

##  👨‍💻 Author

**Rishikesh Gundla**  
� ��� Senior BI Engineer | 📍 India  
🔗 [L inkedIn](https://www.linkedin.com/in/rishikes hgundla/)
 