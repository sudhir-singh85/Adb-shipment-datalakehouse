# Databricks notebook source
 %sql
 TRUNCATE TABLE silver.invoices

# COMMAND ----------

 %sql
 TRUNCATE TABLE silver.orders

# COMMAND ----------

 %sql
 TRUNCATE TABLE silver.shipmentdetails

# COMMAND ----------

 %sql
 TRUNCATE TABLE silver.transactionlineitems

# COMMAND ----------

 %sql
 TRUNCATE TABLE silver.temp_carriers

# COMMAND ----------

 %sql
 TRUNCATE TABLE silver.temp_customers

# COMMAND ----------

 %sql
 TRUNCATE TABLE silver.temp_entities

# COMMAND ----------

 %sql
 TRUNCATE TABLE silver.temp_persons

# COMMAND ----------

 %sql
 MERGE INTO silver.watermark as t 
 USING (select TableName, try_cast('2024-04-01' as Date)as MaxDate
 from silver.watermark)AS s 
 ON t.TableName = s.TableName
 WHEN MATCHED THEN UPDATE
 SET t.WatermarkValue = s.MaxDate

# COMMAND ----------

dbutils.notebook.exit("Success")