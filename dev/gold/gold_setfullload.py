# Databricks notebook source
 %sql
 TRUNCATE TABLE gold.dim_address

# COMMAND ----------

 %sql
 truncate table gold.dim_customer

# COMMAND ----------

 %sql
 truncate table gold.dim_person

# COMMAND ----------

 %sql
 truncate table gold.fact_invoices

# COMMAND ----------

 %sql
 MERGE INTO gold.watermark as t 
 USING (select TableName, try_cast('2024-04-01' as Date)as MaxDate
 from gold.watermark)AS s 
 ON t.TableName = s.TableName
 WHEN MATCHED THEN UPDATE
 SET t.WatermarkValue = s.MaxDate

# COMMAND ----------

dbutils.notebook.exit("Success")