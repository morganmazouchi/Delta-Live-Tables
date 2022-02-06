# Databricks notebook source
# MAGIC %md # Delta Live Tables - Monitoring  
# MAGIC   
# MAGIC Each DLT Pipeline stands up its own events table in the Storage Location defined on the pipeline. From this table we can see what is happening and the quality of the data passing through it.
# MAGIC 
# MAGIC <img src="https://raw.githubusercontent.com/morganmazouchi/Delta-Live-Tables/main/Images/dlt%20end%20to%20end%20flow.png"/>

# COMMAND ----------

# MAGIC %md ## 01 - CONFIG 

# COMMAND ----------

dbutils.widgets.removeAll()
# -- REMOVE WIDGET old

# COMMAND ----------

dbutils.widgets.text('root_location', '/home/morganmazouchi@databricks.com/dlt_demo/')
dbutils.widgets.text('db_name', 'mj_retail')
dbutils.widgets.text('storage_loc','/landing')
dbutils.widgets.text('data_loc','/dlt_storage')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a "table" definition against all CSV files in the data location
# MAGIC DROP TABLE IF EXISTS $db_name.customers_source; 
# MAGIC CREATE TABLE IF NOT EXISTS $db_name.customers_source 
# MAGIC   (
# MAGIC address string,
# MAGIC email string,
# MAGIC id string,
# MAGIC name string,
# MAGIC operation string,
# MAGIC operation_date string
# MAGIC   )
# MAGIC  USING json
# MAGIC OPTIONS (
# MAGIC     path "$root_location/$db_name/$storage_loc/*.json",
# MAGIC     header "true",
# MAGIC     mode "FAILFAST",
# MAGIC     schema 'address string,email string,id string,name string,operation string,operation_date string'
# MAGIC   )
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC   FROM $db_name.customers_source
# MAGIC  ORDER BY id ASC;

# COMMAND ----------

root_location = dbutils.widgets.get('root_location')
db_name       = dbutils.widgets.get('db_name')
data_loc      = dbutils.widgets.get('data_loc')
storage_loc   = dbutils.widgets.get('storage_loc')
storage_path  = storage_path  = root_location + db_name + data_loc

print('root_location: {}\ndb_name:       {}\ndata_loc:      {}\nstorage_loc:   {}'.format(root_location,db_name,data_loc,storage_loc))
print('storage_path:  {}'.format(storage_path))

# COMMAND ----------

# MAGIC %sql
# MAGIC USE $db_name

# COMMAND ----------

# MAGIC %md ## 02 - SETUP 
# MAGIC 
# MAGIC Prior to running Cmd 10, make sure you use the same path of storage path under LOCATION. This is defined in the DLT pipeline when you created your pipeline. 

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS $db_name.event_log; 
# MAGIC CREATE TABLE IF NOT EXISTS $db_name.event_log
# MAGIC  USING delta
# MAGIC LOCATION '$root_location$db_name$data_loc/system/events'

# COMMAND ----------

# MAGIC %md #Delta Live Table expectation analysis
# MAGIC Delta live table tracks our data quality through expectations. These expectations are stored as technical tables without the DLT log events. We can create a view to simply analyze this information
# MAGIC 
# MAGIC **Make sure you set your DLT storage path in the widget!**
# MAGIC 
# MAGIC <!-- do not remove -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Fretail%2Fdlt%2Fnotebook_quality_expectations&dt=DATA_PIPELINE">
# MAGIC <!-- [metadata={"description":"Notebook extracting DLT expectations as delta tables used to build DBSQL data quality Dashboard.",
# MAGIC  "authors":["quentin.ambard@databricks.com"],
# MAGIC  "db_resources":{"Dashboards": ["DLT Data Quality Stats"]},
# MAGIC  "search_tags":{"vertical": "retail", "step": "Data Engineering", "components": ["autoloader", "copy into"]},
# MAGIC  "canonicalUrl": {"AWS": "", "Azure": "", "GCP": ""}}] -->

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Event Logs Analysis
# MAGIC The `details` column contains metadata about each Event sent to the Event Log. There are different fields depending on what type of Event it is. Some examples include:
# MAGIC 
# MAGIC | Type of event | behavior |
# MAGIC | --- | --- |
# MAGIC | `user_action` | Events occur when taking actions like creating the pipeline |
# MAGIC | `flow_definition`| FEvents occur when a pipeline is deployed or updated and have lineage, schema, and execution plan information |
# MAGIC | `output_dataset` and `input_datasets` | output table/view and its upstream table(s)/view(s) |
# MAGIC | `flow_type` | whether this is a complete or append flow |
# MAGIC | `explain_text` | the Spark explain plan |
# MAGIC | `flow_progress`| Events occur when a data flow starts running or finishes processing a batch of data |
# MAGIC | `metrics` | currently contains `num_output_rows` |
# MAGIC | `data_quality` (`dropped_records`), (`expectations`: `name`, `dataset`, `passed_records`, `failed_records`)| contains an array of the results of the data quality rules for this particular dataset   * `expectations`|

# COMMAND ----------

# DBTITLE 1,Event Log - Raw Sequence of Events by Timestamp
# MAGIC %sql
# MAGIC SELECT 
# MAGIC        id,
# MAGIC        timestamp,
# MAGIC        sequence,
# MAGIC        -- origin,
# MAGIC        event_type,
# MAGIC        message,
# MAGIC        level, 
# MAGIC        -- error ,
# MAGIC        details
# MAGIC   FROM $db_name.event_log
# MAGIC  ORDER BY timestamp ASC
# MAGIC ;  

# COMMAND ----------

# MAGIC %md ## 2 - DLT Lineage 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- List of output datasets by type and the most recent change
# MAGIC SELECT details:flow_definition.output_dataset output_dataset,
# MAGIC        details:flow_definition.flow_type,
# MAGIC        MAX(timestamp)
# MAGIC   FROM $db_name.event_log
# MAGIC  WHERE details:flow_definition.output_dataset IS NOT NULL
# MAGIC  GROUP BY details:flow_definition.output_dataset,
# MAGIC           details:flow_definition.schema,
# MAGIC           details:flow_definition.flow_type
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC ----------------------------------------------------------------------------------------
# MAGIC -- Lineage
# MAGIC ----------------------------------------------------------------------------------------
# MAGIC SELECT max_timestamp,
# MAGIC        details:flow_definition.output_dataset,
# MAGIC        details:flow_definition.input_datasets,
# MAGIC        details:flow_definition.flow_type,
# MAGIC        details:flow_definition.schema,
# MAGIC        details:flow_definition.explain_text,
# MAGIC        details:flow_definition
# MAGIC   FROM $db_name.event_log e
# MAGIC  INNER JOIN (
# MAGIC               SELECT details:flow_definition.output_dataset output_dataset,
# MAGIC                      MAX(timestamp) max_timestamp
# MAGIC                 FROM $db_name.event_log
# MAGIC                WHERE details:flow_definition.output_dataset IS NOT NULL
# MAGIC                GROUP BY details:flow_definition.output_dataset
# MAGIC             ) m
# MAGIC   WHERE e.timestamp = m.max_timestamp
# MAGIC     AND e.details:flow_definition.output_dataset = m.output_dataset
# MAGIC --    AND e.details:flow_definition IS NOT NULL
# MAGIC  ORDER BY e.details:flow_definition.output_dataset
# MAGIC ;

# COMMAND ----------

# MAGIC %md ## 3 - Quality Metrics 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   id,
# MAGIC   expectations.dataset,
# MAGIC   expectations.name,
# MAGIC   expectations.failed_records,
# MAGIC   expectations.passed_records
# MAGIC FROM(
# MAGIC   SELECT 
# MAGIC     id,
# MAGIC     timestamp,
# MAGIC     details:flow_progress.metrics,
# MAGIC     details:flow_progress.data_quality.dropped_records,
# MAGIC     explode(from_json(details:flow_progress:data_quality:expectations
# MAGIC              ,schema_of_json("[{'name':'str', 'dataset':'str', 'passed_records':42, 'failed_records':42}]"))) expectations
# MAGIC   FROM event_log
# MAGIC   WHERE details:flow_progress.metrics IS NOT NULL) data_quality

# COMMAND ----------

# MAGIC %md ## 4. Business Aggregate Checks

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- Get details from Silver merged table
# MAGIC SELECT DISTINCT COUNT(id) RecordCount,
# MAGIC        MAX(id) MaxId
# MAGIC   FROM $db_name.customer_silver
# MAGIC --   ORDER BY bal DESC
# MAGIC  LIMIT 20
# MAGIC ;

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- Get details from Bronze landed table
# MAGIC SELECT COUNT(DISTINCT id) DistinctRecordCount,
# MAGIC        COUNT(1) RecordCount,
# MAGIC        MAX(id) MaxId,
# MAGIC        MAX(operation_date) MostRecentUpdate
# MAGIC   FROM $db_name.customer_bronze
# MAGIC --   ORDER BY bal DESC
# MAGIC  LIMIT 20
# MAGIC ;
