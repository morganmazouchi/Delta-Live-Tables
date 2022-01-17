-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC # Implement CDC In DLT Pipeline: Change Data Capture
-- MAGIC ## Use-case: Synchronize your SQL Database with your Lakehouse
-- MAGIC 
-- MAGIC Delta Lake is an <a href="https://delta.io/" target="_blank">open-source</a> storage layer with Transactional capabilities and increased Performances. 
-- MAGIC 
-- MAGIC Delta lake is designed to support CDC workload by providing support for UPDATE / DELETE and MERGE operation.
-- MAGIC 
-- MAGIC In addition, Delta table can support CDC to capture internal changes and propagate the changes downstream.
-- MAGIC 
-- MAGIC <!-- do not remove -->
-- MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fcdc_cdf%2Fcdc_notebook&dt=DELTA">
-- MAGIC <!-- [metadata={"description":"Process CDC from external system and save them as a Delta Table. BRONZE/SILVER.<br/><i>Usage: demo CDC flow.</i>",
-- MAGIC  "authors":["quentin.ambard@databricks.com"],
-- MAGIC  "db_resources":{},
-- MAGIC   "search_tags":{"vertical": "retail", "step": "Data Engineering", "components": ["autoloader", "copy into", "cdc", "cdf"]},
-- MAGIC                  "canonicalUrl": {"AWS": "", "Azure": "", "GCP": ""}}] -->
-- MAGIC                  
-- MAGIC ### Setup/Requirements:
-- MAGIC 
-- MAGIC Note: Prior to run this notebook as a pipeline, run notebook 00_Retail_Data_CDC_Generator, and use storage path printed in result of Cdm 5 in that notebook and use the path in Cmd 5 in this notebook to access the generated CDC data.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## CDC flow
-- MAGIC 
-- MAGIC Here is the flow we'll implement, consuming CDC data from an external database. Note that the incoming could be any format, including message queue such as Kafka.
-- MAGIC 
-- MAGIC <img src="https://raw.githubusercontent.com/morganmazouchi/Delta-Live-Tables/main/Images/cdc%20flow.png" alt='Make all your data ready for BI and ML'/>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## Incremental data loading using Auto Loader
-- MAGIC 
-- MAGIC <img src="https://raw.githubusercontent.com/morganmazouchi/Delta-Live-Tables/main/Images/cdc%20flow%20focused%20on%20autoloader.png" style='float: right' width='600'/>
-- MAGIC 
-- MAGIC Working with external system can be challenging due to schema update. The external database can have schema update, adding or modifying columns, and our system must be robust against these changes.
-- MAGIC 
-- MAGIC Databricks Autoloader (`cloudFiles`) handles schema inference and evolution out of the box

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ### Loading our data using Databricks Autoloader (cloud_files)
-- MAGIC <div style="float:right">
-- MAGIC   <img width="500px" src="https://raw.githubusercontent.com/morganmazouchi/Delta-Live-Tables/main/Images/DLT_CDC_2layer_pipeline.png"/>
-- MAGIC </div>
-- MAGIC   
-- MAGIC Autoloader allow us to efficiently ingest millions of files from a cloud storage, and support efficient schema inference and evolution at scale. In this notebook we leverage Autoloader to handle streaming (and batch) data.
-- MAGIC 
-- MAGIC Let's use it to [create our pipeline](https://e2-demo-field-eng.cloud.databricks.com/?o=1444828305810485&owned-by-me=true&name-order=ascend#joblist/pipelines/fc6d797b-59bf-46b4-8bb4-05e3769dd6fe) and ingest the raw JSON data being delivered by an external provider. 

-- COMMAND ----------

-- DBTITLE 1,Let's explore our incoming data - Bronze Table - Autoloader & DLT
-- Create the bronze information table containing the raw JSON data taken from the storage path printed in Cmd5 in 00_Retail_Data_CDC_Generator notebook

CREATE INCREMENTAL LIVE TABLE retail_client_cdc_raw (
  CONSTRAINT correct_schema EXPECT (id IS NULL)
)
COMMENT "The customers buying finished products ingested in incremental with Auto Loader"
TBLPROPERTIES ("quality" = "bronze")
AS SELECT * FROM cloud_files("/home/morganmazouchi/dlt_demo/landing", "json", map("cloudFiles.inferColumnTypes", "true"));

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## Materializing the silver table
-- MAGIC 
-- MAGIC <img src="https://raw.githubusercontent.com/morganmazouchi/Delta-Live-Tables/main/Images/cdc%20flow%20materialize%20silver.png" alt='Make all your data ready for BI and ML' style='float: right' width='600'/>
-- MAGIC 
-- MAGIC The silver `retail_clinet_silver` table will contains the most up to date view. It'll be a replicate of the original MYSQL table.
-- MAGIC 
-- MAGIC To propagate the `Apply Changes Into` operations downstream to the `Silver` layer, we must explicitly enable the feature in pipeline by adding and enabling the applyChanges configuration to the DLT pipeline settings.

-- COMMAND ----------

-- DBTITLE 1,Delete unwanted clients records - Silver Table - DLT SQL 
CREATE INCREMENTAL LIVE TABLE retail_client_cdc_silver;

APPLY CHANGES INTO live.retail_client_cdc_silver
FROM stream(live.retail_client_cdc_raw)
  KEYS (Id)
  APPLY AS DELETE WHEN operation = "DELETE"
  SEQUENCE BY operation_date --primary key, auto-incrementing ID of any kind that can be used to identity order of events, or timestamp
  COLUMNS * EXCEPT (operation, operation_date);
