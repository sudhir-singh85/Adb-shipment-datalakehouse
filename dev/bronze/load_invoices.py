# Databricks notebook source
%run "../configuration/configuration_notebook"
# MAGIC

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType

# COMMAND ----------

dataset_schema = StructType([
    StructField("InvNo", StringType(), nullable=True),
	StructField("InvDate", TimestampType(), nullable=True),
	StructField("Ord", StringType(), nullable=True),
    StructField("Amount", DoubleType(), nullable=True),
	StructField("TaxRate", DoubleType(), nullable=True),
	StructField("TaxAmount", DoubleType(), nullable=True),
	StructField("PaymentDays", IntegerType(), nullable=True),
    StructField("GeneratedBy", StringType(), nullable=True),
    ])

# COMMAND ----------

read_df = spark.read.option("header",True)\
    .schema(dataset_schema)\
    .json(f"{raw_folder_path}/invoices/*.json")

# COMMAND ----------

read_df.createOrReplaceTempView ("v_invoices")

# COMMAND ----------

%sql
MERGE INTO bronze.invoices AS t
USING (SELECT InvNo,
InvDate,
ord,
Amount,
TaxRate,
TaxAmount,
PaymentDays,
GeneratedBy
 FROM v_invoices
  WHERE try_cast(InvDate as Date)>(Select COALESCE (WatermarkValue,'2024-01-01') from bronze.watermark where TableName = 'bronze.invoices')
  ) AS s
ON t.InvNo = s.InvNo
WHEN NOT MATCHED THEN INSERT *
# MAGIC

# COMMAND ----------

%sql
MERGE INTO bronze.watermark as t 
USING (select 4 as watermarkid, 'bronze.invoices' as TableName, Max(try_cast(InvDate as Date))as MaxDate
from bronze.invoices)AS s 
ON t.watermarkid = s.watermarkid
WHEN MATCHED THEN UPDATE
SET t.WatermarkValue = s.MaxDate
WHEN NOT MATCHED THEN INSERT (watermarkid,TableName,WatermarkValue) values(S.watermarkid,S.TableName,S.MaxDate)
# MAGIC

# COMMAND ----------

dbutils.notebook.exit("Success")