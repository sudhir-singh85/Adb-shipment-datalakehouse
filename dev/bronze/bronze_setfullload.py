# Databricks notebook source
%sql
TRUNCATE TABLE bronze.orders

# COMMAND ----------

%sql
TRUNCATE TABLE bronze.shipmentdetails

# COMMAND ----------

%sql
MAGIC TRUNCATE TABLE bronze.invoices

# COMMAND ----------

%sql
TRUNCATE TABLE bronze.transactionlineitems

# COMMAND ----------

%sql
MERGE INTO bronze.watermark as t 
USING (select TableName, try_cast('2024-04-01' as Date)as MaxDate
from bronze.watermark)AS s 
ON t.TableName = s.TableName
WHEN MATCHED THEN UPDATE
SET t.WatermarkValue = s.MaxDate
# MAGIC

# COMMAND ----------

dbutils.notebook.exit("Success")