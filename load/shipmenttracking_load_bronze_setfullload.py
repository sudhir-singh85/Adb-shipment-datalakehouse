# Databricks notebook source
# MAGIC %sql
# MAGIC TRUNCATE TABLE bronze.orders

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE bronze.shipmentdetails

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE bronze.invoices

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE bronze.transactionlineitems

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO bronze.watermark as t 
# MAGIC USING (select TableName, try_cast('2024-04-01' as Date)as MaxDate
# MAGIC from bronze.watermark)AS s 
# MAGIC ON t.TableName = s.TableName
# MAGIC WHEN MATCHED THEN UPDATE
# MAGIC SET t.WatermarkValue = s.MaxDate
# MAGIC

# COMMAND ----------

dbutils.notebook.exit("Success")
