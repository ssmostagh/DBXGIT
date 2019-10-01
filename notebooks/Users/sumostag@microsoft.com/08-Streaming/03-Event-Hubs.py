# Databricks notebook source
# MAGIC %md 
# MAGIC # Event Hubs and Spark Structured Streaming
# MAGIC 
# MAGIC In this lesson, you will see how you can perform near-realtime processing of messages using Spark Structured Streaming by sending a batch of messages to Event Hubs. Then, you will write a Structured Streaming query that lets you view the data as it comes in, and perform analytics against the streaming data using Spark SQL.
# MAGIC 
# MAGIC If you are running in an Azure Databricks environment that is already pre-configured with the libraries you need, you can skip to the next cell. To use this notebook in your own Databricks environment, you will need to create libraries, using the [Create Library](https://docs.azuredatabricks.net/user-guide/libraries.html) interface in Azure Databricks. Follow the steps below to attach the `azure-eventhubs-spark` library to your cluster:
# MAGIC 
# MAGIC 1. In the left-hand navigation menu of your Databricks workspace, select **Workspace**, select the down chevron next to **Shared**, and then select **Create** and **Library**.
# MAGIC 
# MAGIC   ![Create Databricks Library](https://databricksdemostore.blob.core.windows.net/images/08/03/databricks-create-library.png 'Create Databricks Library')
# MAGIC 
# MAGIC 2. On the New Library screen, do the following:
# MAGIC 
# MAGIC   - **Source**: Select Maven Coordinate.
# MAGIC   - **Coordinate**: Enter "azure-eventhubs-spark", and then select **com.microsoft.azure:azure-eventhubs-spark_2.11:2.3.5**.
# MAGIC   - Select **Create Library**.
# MAGIC   
# MAGIC   ![Databricks new Maven library](https://databricksdemostore.blob.core.windows.net/images/08/03/databricks-new-maven-library.png 'Databricks new Maven library')
# MAGIC 
# MAGIC 3. On the library page that is displayed, check the **Attach** checkbox next to the name of your cluster to run the library on that cluster.
# MAGIC 
# MAGIC   ![Databricks attach library](https://databricksdemostore.blob.core.windows.net/images/08/03/databricks-attach-library.png 'Databricks attach library')
# MAGIC 
# MAGIC Once complete, return to this notebook to continue with the lesson.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Getting Started
# MAGIC 
# MAGIC Run the following cell to configure your module.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Remember to attach your notebook to a cluster before running any cells in your notebook. In the notebook's toolbar, select the drop down arrow next to Detached, then select your cluster under Attach to.
# MAGIC 
# MAGIC ![Attached to cluster](https://databricksdemostore.blob.core.windows.net/images/03/03/databricks-cluster-attach.png)

# COMMAND ----------

# MAGIC %run "./includes/Module-Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC In order to reach Event Hubs, you will need to insert the connection string-primary key you acquired at the end of the Getting Started notebook in this module. You acquired this from the Azure Portal, and copied it into Notepad.exe or another text editor.
# MAGIC 
# MAGIC > Read this article to learn [how to acquire the connection string for an Event Hub](https://docs.microsoft.com/en-us/azure/event-hubs/event-hubs-create) in your own Azure Subscription.

# COMMAND ----------

event_hub_connection_string = #{your-event-hubs-connection-string-primary-key}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sending Events to Event Hubs
# MAGIC 
# MAGIC First we need to import some support modules that will help us in creating a DataFrame that has the schema expected by Event Hubs.

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, StringType, Row
import json

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC In the following, you create schema definition that represents the structure expected by Event Hubs. Then it adds five rows to that DataFrame and saves the DataFrame to the configured Event Hubs instance. This is in effect sending messages to the Event Hubs instance.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> You can run this cell as many times as you like to add more messages:

# COMMAND ----------

# Set up the Event Hub config dictionary with default settings
writeConnectionString = event_hub_connection_string
checkpointLocation = "///checkpoint.txt"

ehWriteConf = {
  'eventhubs.connectionString' : writeConnectionString
}

event_hubs_schema = StructType([
  StructField("body",StringType(), False),
  StructField("partitionId",StringType(), True),
  StructField("partitionKey",StringType(), True),
])

newRows = [
  Row("This is new message 1!", None, None),
  Row("This is new message 2!", None, None),
  Row("This is new message 3!", None, None),
  Row("This is new message 4!", None, None),
  Row("This is new message 5!", None, None)
]
parallelizeRows = spark.sparkContext.parallelize(newRows)
new_messages = spark.createDataFrame(parallelizeRows, event_hubs_schema)

# Write body data from a DataFrame to EventHubs. Events are distributed across partitions using round-robin model.
ds = new_messages \
  .select("body") \
  .write \
  .format("eventhubs") \
  .options(**ehWriteConf) \
  .option("checkpointLocation", checkpointLocation) \
  .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading Events from Event Hubs
# MAGIC 
# MAGIC Next, let's examine how you can read the messages you have sent. In the cell below you will setup a Structured Streaming query, which will be represented by a DataFrame. This cell effectively starts a process to listen for new messages, but when it first runs it will read from the beginning of the Event Hubs events (this is enabled by setting the offset attribute of the startingPosition configuration to -1). 
# MAGIC 
# MAGIC See this [document](https://github.com/Azure/azure-event-hubs-spark/blob/master/docs/PySpark/structured-streaming-pyspark.md) for more details on the configuration options.

# COMMAND ----------

# Source with default settings
connectionString = event_hub_connection_string
ehConf = {
  'eventhubs.connectionString' : connectionString,
  'eventhubs.startingPosition' : json.dumps({"offset":"-1", "seqNo":-1,"enqueuedTime": None,"isInclusive": True})
}

streaming_df = spark \
  .readStream \
  .format("eventhubs") \
  .options(**ehConf) \
  .load()

# COMMAND ----------

# MAGIC %md
# MAGIC Even though the streaming query is running, we don't see anything in the output. 
# MAGIC 
# MAGIC To see the streaming output, you simply pass the streaming DataFrame to the display method as the following cell shows. This cell will continue to run until you hit cancel. 

# COMMAND ----------

display(streaming_df.withColumn("body", streaming_df["body"].cast("string")))

# COMMAND ----------

# MAGIC %md
# MAGIC You can register the streaming DataFrame as a temporary view, which will enable you to query the streaming using SQL! 
# MAGIC 
# MAGIC Run the following two cells to view the output. Feel free to scroll back up to the top of this notebook to send more messages and to see the count of messages in the SQL output increase (be patient, it can take a few seconds to update).

# COMMAND ----------

streaming_df.withColumn("body", streaming_df["body"].cast("string")).createOrReplaceTempView("eventhubEvents")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Count(body) FROM eventhubEvents

# COMMAND ----------

# MAGIC %md
# MAGIC ## Challenge
# MAGIC Now that you can query using SQL, try authoring an new query that looks only at the enqueuedTime and by using the built-in notebook visualizations produce a bar chart with the count of events that were enqueued at the same time (so the horizontal axis is enqueuedTime and the vertical axis is some form of count).

# COMMAND ----------

# MAGIC %md
# MAGIC ### Answers to Challenge

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 1, enqueuedTime FROM eventhubEvents

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC Start the next lesson, [Databricks Delta Streaming]($./04-Streaming-with-Databricks-Delta)