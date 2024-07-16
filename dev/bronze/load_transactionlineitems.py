# Databricks notebook source
%run "../configuration/configuration_notebook"
# MAGIC

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType

# COMMAND ----------

dataset_schema = StructType([
	StructField("TransactionLineItemID", IntegerType(), nullable = True),
    StructField("TrNo", StringType(), nullable=True),
	StructField("TrDate", TimestampType(), nullable=True),
	StructField("TrAmount", DoubleType(), nullable=True),
	StructField("Cheque", StringType(), nullable=True),
	StructField("RefNo", StringType(), nullable=True),
	StructField("PaymentType", StringType(), nullable=True),
	StructField("Party", StringType(), nullable=True),
	StructField("BankName", StringType(), nullable=True),
	StructField("Details", StringType(), nullable=True),
	StructField("GeneratedBy", StringType(), nullable=True)
    ])

# COMMAND ----------

read_df = spark.read.option("header",True)\
    .schema(dataset_schema)\
    .csv(f"{raw_folder_path}/transactionlineitems/*.csv")

# COMMAND ----------

read_df.createOrReplaceTempView ("v_transactionlineitems")

# COMMAND ----------

%sql
MERGE INTO bronze.transactionlineitems AS t
USING (SELECT TransactionLineItemID,
TrNo,
TrDate,
TrAmount,
Cheque,
RefNo,
PaymentType,
Party,
BankName,
Details,
GeneratedBy
 FROM v_transactionlineitems
  WHERE try_cast(TrDate as Date)>(Select COALESCE (WatermarkValue,'2024-01-01') from bronze.watermark where TableName = 'bronze.transactionlineitems')
  ) AS s
ON t.TransactionLineItemID = s.TransactionLineItemID
WHEN NOT MATCHED THEN INSERT *
# MAGIC

# COMMAND ----------

%sql
MERGE INTO bronze.watermark as t 
USING (select 5 as watermarkid, 'bronze.transactionlineitems' as TableName, Max(try_cast(TrDate as Date))as MaxDate
from bronze.transactionlineitems)AS s 
ON t.watermarkid = s.watermarkid
WHEN MATCHED THEN UPDATE
SET t.WatermarkValue = s.MaxDate
WHEN NOT MATCHED THEN INSERT (watermarkid,TableName,WatermarkValue) values(S.watermarkid,S.TableName,S.MaxDate)
# MAGIC

# COMMAND ----------

dbutils.notebook.exit("Success")