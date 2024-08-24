# Databricks notebook source
# MAGIC %sql
# MAGIC TRUNCATE TABLE silver.invoices

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE silver.orders

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE silver.shipmentdetails

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE silver.transactionlineitems

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE silver.temp_carriers

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE silver.temp_customers

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE silver.temp_entities

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE silver.temp_persons

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO silver.watermark as t 
# MAGIC USING (select TableName, try_cast('2024-04-01' as Date)as MaxDate
# MAGIC from silver.watermark)AS s 
# MAGIC ON t.TableName = s.TableName
# MAGIC WHEN MATCHED THEN UPDATE
# MAGIC SET t.WatermarkValue = s.MaxDate

# COMMAND ----------

dbutils.notebook.exit("Success")
