-- Databricks notebook source
-- MAGIC %md # DLT Event Log
-- MAGIC 
-- MAGIC * Query the Delta Live Table Event Log to view expecataions and lineage 

-- COMMAND ----------

-- MAGIC %md ## 0. Setup

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC dbutils.widgets.removeAll()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC dbutils.widgets.text('storage_location', 'dbfs:/pipelines/123', 'Storage Location')
-- MAGIC dbutils.widgets.text('db', 'default', 'Database')

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC display(dbutils.fs.ls(dbutils.widgets.get('storage_location')))

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS ${db};
USE ${db}

-- COMMAND ----------

-- MAGIC %md #Delta Live Table expectation analysis
-- MAGIC Delta live table tracks our data quality through expectations. These expectations are stored as technical tables without the DLT log events. We can create a view to simply analyze this information
-- MAGIC 
-- MAGIC **Make sure you set your DLT storage path in the widget!**
-- MAGIC 
-- MAGIC <!-- do not remove -->
-- MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Fretail%2Fdlt%2Fnotebook_quality_expectations&dt=DATA_PIPELINE">
-- MAGIC <!-- [metadata={"description":"Notebook extracting DLT expectations as delta tables used to build DBSQL data quality Dashboard.",
-- MAGIC  "authors":["quentin.ambard@databricks.com"],
-- MAGIC  "db_resources":{"Dashboards": ["DLT Data Quality Stats"]},
-- MAGIC  "search_tags":{"vertical": "retail", "step": "Data Engineering", "components": ["autoloader", "copy into"]},
-- MAGIC  "canonicalUrl": {"AWS": "", "Azure": "", "GCP": ""}}] -->

-- COMMAND ----------

-- DBTITLE 1,Adding our DLT system table to the metastore
CREATE OR REPLACE VIEW retail_pipeline_logs
AS SELECT * FROM delta.`${storage_location}/system/events`

-- COMMAND ----------

-- MAGIC %md ## 1. Event Log - Raw Sequence of Events by Timestamp

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC SELECT * FROM retail_pipeline_logs
-- MAGIC ORDER BY timestamp

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Event Logs Analysis
-- MAGIC The `details` column contains metadata about each Event sent to the Event Log. There are different fields depending on what type of Event it is. Some examples include:
-- MAGIC 
-- MAGIC | Type of event | behavior |
-- MAGIC | --- | --- |
-- MAGIC | `user_action` | Events occur when taking actions like creating the pipeline |
-- MAGIC | `flow_definition`| FEvents occur when a pipeline is deployed or updated and have lineage, schema, and execution plan information |
-- MAGIC | `output_dataset` and `input_datasets` | output table/view and its upstream table(s)/view(s) |
-- MAGIC | `flow_type` | whether this is a complete or append flow |
-- MAGIC | `explain_text` | the Spark explain plan |
-- MAGIC | `flow_progress`| Events occur when a data flow starts running or finishes processing a batch of data |
-- MAGIC | `metrics` | currently contains `num_output_rows` |
-- MAGIC | `data_quality` (`dropped_records`), (`expectations`: `name`, `dataset`, `passed_records`, `failed_records`)| contains an array of the results of the data quality rules for this particular dataset   * `expectations`|

-- COMMAND ----------

-- MAGIC %md ## DLT Quality - Flow Events

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC SELECT
-- MAGIC   details:flow_definition.output_dataset,
-- MAGIC   details:flow_definition.input_datasets,
-- MAGIC   details:flow_definition.flow_type,
-- MAGIC   details:flow_definition.schema,
-- MAGIC   details:flow_definition
-- MAGIC FROM retail_pipeline_logs
-- MAGIC WHERE details:flow_definition IS NOT NULL
-- MAGIC ORDER BY timestamp

-- COMMAND ----------

-- MAGIC %md ## DLT Quality - Pass/Fail Metrics

-- COMMAND ----------

-- MAGIC %sql 
-- MAGIC SELECT
-- MAGIC   id,
-- MAGIC   expectations.dataset,
-- MAGIC   expectations.name,
-- MAGIC   expectations.failed_records,
-- MAGIC   expectations.passed_records
-- MAGIC FROM(
-- MAGIC   SELECT 
-- MAGIC     id,
-- MAGIC     timestamp,
-- MAGIC     details:flow_progress.metrics,
-- MAGIC     details:flow_progress.data_quality.dropped_records,
-- MAGIC     explode(from_json(details:flow_progress:data_quality:expectations
-- MAGIC              ,schema_of_json("[{'name':'str', 'dataset':'str', 'passed_records':42, 'failed_records':42}]"))) expectations
-- MAGIC   FROM retail_pipeline_logs
-- MAGIC   WHERE details:flow_progress.metrics IS NOT NULL) data_quality
