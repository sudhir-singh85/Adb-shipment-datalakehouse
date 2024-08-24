# Databricks notebook source
# MAGIC %sql
# MAGIC TRUNCATE TABLE gold.dim_address

# COMMAND ----------

# MAGIC %sql
# MAGIC truncate table gold.dim_customer

# COMMAND ----------

# MAGIC %sql
# MAGIC truncate table gold.dim_person

# COMMAND ----------

# MAGIC %sql
# MAGIC truncate table gold.fact_invoices

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold.watermark as t 
# MAGIC USING (select TableName, try_cast('2024-04-01' as Date)as MaxDate
# MAGIC from gold.watermark)AS s 
# MAGIC ON t.TableName = s.TableName
# MAGIC WHEN MATCHED THEN UPDATE
# MAGIC SET t.WatermarkValue = s.MaxDate

# COMMAND ----------

dbutils.notebook.exit("Success")
