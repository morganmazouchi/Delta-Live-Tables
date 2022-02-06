-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC # Delta Live Tables quickstart (SQL)
-- MAGIC ### A simple way to build and manage data pipelines for fresh, high quality data!
-- MAGIC 
-- MAGIC A notebook that provides an example Delta Live Tables pipeline to:
-- MAGIC 
-- MAGIC Read raw JSON clickstream data into a table.
-- MAGIC Read records from the raw data table and use a Delta Live Tables query and expectations to create a new table with cleaned and prepared data.
-- MAGIC Perform an analysis on the prepared data with a Delta Live Tables query.
-- MAGIC 
-- MAGIC <img width="1000" src="https://databricks.com/wp-content/uploads/2021/09/DLT_graphic_tiers.jpg">
-- MAGIC <img width="1000" src="https://databricks.com/wp-content/uploads/2021/05/Bronze-Silver-Gold-Tables.png">                
-- MAGIC ### Setup/Requirements:
-- MAGIC 
-- MAGIC Note: Prior to run this notebook as a pipeline, run notebook 01-Retail_Data_Generator, and use storage path printed in result of Cmd 6 in that notebook and use the path in this notebook to access to the generated retail data.

-- COMMAND ----------

-- DBTITLE 1,Ingest Retail Customer Raw data- Bronze Table - DLT
-- Create the bronze information table containing the raw JSON data taken from the storage path printed in Cmd5 in 01-Retail_Data_Generator notebook

CREATE LIVE TABLE retail_client_raw
COMMENT "The raw customer dataset, ingested from existing storage path"
AS SELECT * FROM json.`/home/morganmazouchi/dlt_demo/landingWithProblematicRecords`;

-- COMMAND ----------

-- DBTITLE 1,Clean and prepare Retail Customer data - Silver Table
CREATE LIVE TABLE retail_client_clean(
  CONSTRAINT valid_id EXPECT (id IS NOT NULL) ON VIOLATION FAIL UPDATE
)
COMMENT "Retail customers data cleaned and prepared for analysis."
AS SELECT
  id AS customer_id,
  name AS customer_name,
  email,
  address,
  operation,
  operation_date 
FROM live.retail_client_raw

-- COMMAND ----------

-- DBTITLE 1,Ingest Sales Order Raw data- Bronze Table - DLT
CREATE INCREMENTAL LIVE TABLE sales_orders_raw
COMMENT "The raw sales orders, ingested from /databricks-datasets."
TBLPROPERTIES ("myCompanyPipeline.quality" = "bronze")
AS
SELECT clicked_items, CAST(customer_id/10000 AS Int) customer_id, customer_name, number_of_line_items, order_datetime
, order_number, ordered_products, promo_info
FROM cloud_files("/databricks-datasets/retail-org/sales_orders/", "json", map("cloudFiles.inferColumnTypes", "true"))

-- COMMAND ----------

-- DBTITLE 1,Clean and prepare Sales Order data - Silver Table
CREATE INCREMENTAL LIVE TABLE sales_orders_clean(
  CONSTRAINT valid_number_of_line_items EXPECT (number_of_line_items <=3) ON VIOLATION DROP ROW
)
PARTITIONED BY (order_date)
COMMENT "The cleaned sales orders with valid order_number(s) and partitioned by order_datetime."
TBLPROPERTIES ("myCompanyPipeline.quality" = "silver")
AS
SELECT f.customer_id, f.customer_name, f.number_of_line_items, 
  TIMESTAMP(from_unixtime((cast(f.order_datetime as long)))) as order_datetime, 
  DATE(from_unixtime((cast(f.order_datetime as long)))) as order_date, 
  f.order_number, f.ordered_products
  FROM STREAM(LIVE.sales_orders_raw) f

-- COMMAND ----------

-- DBTITLE 1,Combine Retail Customers data and Sale data - Gold Table
CREATE LIVE TABLE client_orders_gold(
  CONSTRAINT valid_address EXPECT (address IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT "Retail client data with valid address joined with Sale data and partitioned by operation_date."
TBLPROPERTIES ("quality" = "gold")
AS
SELECT f.address, f.email, f.customer_id, f.customer_name, f.operation, f.operation_date,
    c.number_of_line_items, c.order_datetime, c.order_number, c.ordered_products
FROM LIVE.sales_orders_clean c
LEFT JOIN LIVE.retail_client_clean f
ON c.customer_id = f.customer_id

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## Our pipeline is now ready!
-- MAGIC 
-- MAGIC Open the [DLT pipeline](https://e2-demo-field-eng.cloud.databricks.com/?o=1444828305810485&owned-by-me=true&name-order=ascend#joblist/pipelines/e493795b-2e9c-4e40-bf6d-c752a923b71c) and click on start to visualize your lineage and consume the data incrementally!
-- MAGIC 
-- MAGIC <div style="float:right">
-- MAGIC   <img width="500px" src="https://raw.githubusercontent.com/morganmazouchi/Delta-Live-Tables/main/Images/dlt_quickstart_pipeline.png"/>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Checking your data quality metrics with Delta Live Table
-- MAGIC Delta Live Tables tracks all your data quality metrics. You can leverage the expecations directly as SQL table with Databricks SQL to track your expectation metrics and send alerts as required. This let you build the following dashboards:
-- MAGIC 
-- MAGIC <img width="1000" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/retail-dlt-data-quality-dashboard.png">
-- MAGIC 
-- MAGIC <a href="https://e2-demo-field-eng.cloud.databricks.com/sql/dashboards/6f73dd1b-17b1-49d0-9a11-b3772a2c3357-dlt---retail-data-quality-stats?o=1444828305810485" target="_blank">Data Quality Dashboard</a>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Building our first business dashboard with Databricks SQL
-- MAGIC 
-- MAGIC Let's switch to Databricks SQL to build a new dashboard based on all the data we ingested.
-- MAGIC 
-- MAGIC <img width="1000" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/retail-dashboard.png"/>
-- MAGIC 
-- MAGIC <a href="https://e2-demo-field-eng.cloud.databricks.com/sql/dashboards/ab66e6c6-c2c5-4434-b784-ea5b02fe5eeb-sales-report?o=1444828305810485" target="_blank">Business Dashboard</a>
