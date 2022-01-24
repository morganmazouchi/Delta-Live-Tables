-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC # Implement CDC In DLT Pipeline: Change Data Capture

-- COMMAND ----------

-- MAGIC %python
-- MAGIC slide_id = '10Dmx43aZXzfK9LJvJjH1Bjgwa3uvS2Pk7gVzxhr3H2Q'
-- MAGIC slide_number = 'id.p9'
-- MAGIC  
-- MAGIC displayHTML(f'''<iframe
-- MAGIC  src="https://docs.google.com/presentation/d/{slide_id}/embed?slide={slide_number}&rm=minimal"
-- MAGIC   frameborder="0"
-- MAGIC   width="75%"
-- MAGIC   height="600"
-- MAGIC ></iframe>
-- MAGIC ''')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Importance of Change Data Capture (CDC)
-- MAGIC 
-- MAGIC Change Data Capture (CDC) is the process that captures the changes in records made to a data storage like Database, Data Warehouse, etc. These changes usually refer to operations like data deletion, addition and updating.
-- MAGIC 
-- MAGIC A straightforward way of Data Replication is to take a Database Dump that will export a Database and import it to a LakeHouse/DataWarehouse/Lake, but this is not a scalable approach. 
-- MAGIC 
-- MAGIC Change Data Capture, only capture the changes made to the Database and apply those changes to the target Database. CDC reduces the overhead, and supports real-time analytics. It enables incremental loading while eliminates the need for bulk load updating.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### CDC Approaches 
-- MAGIC 
-- MAGIC 1- **Develop in-house CDC process:** 
-- MAGIC 
-- MAGIC ***Complex Task:*** CDC Data Replication is not a one-time easy to do the project. Mainly due to the differences between Database Providers, Varying Record Formats, and even the inconvenience of accessing Log Records, CDC becomes a challenging task.
-- MAGIC 
-- MAGIC ***Regular Maintainance:*** Writing a script that can implement the CDC process is only the first step. When your Database and Log patterns change, you also need to maintain a customized solution that can map these changes regularly. This implies a lot of time and resources will be used up in maintaining your in-house CDC process. 
-- MAGIC 
-- MAGIC ***Overburdening:*** Developers in companies usually already face the burden of public queries. The added work of building your own CDC solution will affect your existing revenue-generating projects as the developers’ time will be divided now.
-- MAGIC 
-- MAGIC 2- **Using CDC tools** such as Debezium, Hevo Data, IBM Infosphere, Qlik Replicate, Talend, Oracle GoldenGate, StreamSets. In this demo repo we are using CDC data coming from Debezium. 
-- MAGIC Since Debezium is reading database logs:
-- MAGIC We are no longer dependant on developers updating a certain column 
-- MAGIC 
-- MAGIC — Debezium takes care of capturing every changed row. It records the history of data changes in Kafka logs, from where your application consumes them. 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ###How does CDC tools like Debezium output looks like?
-- MAGIC 
-- MAGIC A json message describing the changed data
-- MAGIC Some interesting fields:
-- MAGIC 
-- MAGIC - operation: an operation code (DELETE, APPEND, UPDATE, CREATE)
-- MAGIC - operation_date: the date and timestamp for the record came for each operation action
-- MAGIC 
-- MAGIC Some other fields that you may see in Debezium output (not included in this demo):
-- MAGIC - before: the row before the change
-- MAGIC - after: the row after the change

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### How to synchronize your SQL Database with your Lakehouse
-- MAGIC 
-- MAGIC While in the past it was hard to reliablly perform UPSERTS and DELETES on data lake tables, Delta Lake, along with its other benefits, enables it. Delta Lake is an <a href="https://delta.io/" target="_blank">open-source</a> storage layer with Transactional capabilities and increased Performances. Delta lake is designed to support CDC workload by providing support for UPDATE / DELETE and MERGE operation.
-- MAGIC 
-- MAGIC In addition, Delta table can support CDC to capture internal changes and propagate the changes downstream.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### CDC Strategy Overview with Debezium, autoloader and DLT pipeline
-- MAGIC 
-- MAGIC 
-- MAGIC - Debezium reads database logs, produces json messages that includes the changes, and streams the records with changes description to Kafka
-- MAGIC - Kafka streams the messages  which holds INSERT, UPDATE and DELETE operations, and stores them in cloud object storage (S3 folder, ADLS, etc).
-- MAGIC - Using Autoloader we incrementally load the messages from cloud object storage, and stores them in Bronze table as it stores the raw messages 
-- MAGIC - Next we can perform APPLY CHANGES INTO on the cleaned Bronze layer table to propagate the most updated data downstream to the Silver Table
