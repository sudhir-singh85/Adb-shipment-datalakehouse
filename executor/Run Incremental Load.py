# Databricks notebook source
dbutils.notebook.run("/Workspace/Users/<useraccount>/notebooks/ShipmentTracking/load/shipmenttracking_load_bronze_tables",300)

# COMMAND ----------

dbutils.notebook.run("/Workspace/Users/<useraccount>/notebooks/ShipmentTracking/load/shipmenttracking_load_silver_tables",300)

# COMMAND ----------

dbutils.notebook.run("/Workspace/Users/<useraccount>/notebooks/ShipmentTracking/load/shipmenttracking_load_gold_dimensions",300)

# COMMAND ----------

dbutils.notebook.run("/Workspace/Users/<useraccount>/notebooks/ShipmentTracking/load/shipmenttracking_load_gold_facts",300)

# COMMAND ----------

dbutils.notebook.exit("Success")
