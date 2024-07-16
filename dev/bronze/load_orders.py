# Databricks notebook source
%run "../configuration/configuration_notebook"
# MAGIC

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType

# COMMAND ----------

dataset_schema = StructType([
    StructField("ord", StringType(), nullable=True),
    StructField("Odate", TimestampType(), nullable=True),
    StructField("wt", DoubleType(), nullable=True),
    StructField("OrderValue", DoubleType(), nullable=True),
    StructField("ProductCategory", StringType(), nullable=True),
    StructField("ProductName", StringType(), nullable=True),
	StructField("ProductSize", IntegerType(), nullable=True),
	StructField("ProductValue", DoubleType(), nullable=True),
	StructField("ProductWeight", DoubleType(), nullable=True),
	StructField("NoOfItems", IntegerType(), nullable=True),
    StructField("SenderName", StringType(), nullable=True),
	StructField("SenderAddress", StringType(), nullable=True),
	StructField("SenderCity", StringType(), nullable=True),
	StructField("SenderState", StringType(), nullable=True),
	StructField("SenderPhoneNo", StringType(), nullable=True),
	StructField("ReceiverName", StringType(), nullable=True),
	StructField("ReceiverAddress", StringType(), nullable=True),
	StructField("ReceiverCity", StringType(), nullable=True),
	StructField("ReceiverState", StringType(), nullable=True),
	StructField("ReceiverPhoneNo", StringType(), nullable=True),
	StructField("EstimatedCost", DoubleType(), nullable=True),
	StructField("BillToSender", StringType(), nullable=True),
	StructField("BillToReceiver", StringType(), nullable=True),
	StructField("BillToOther", StringType(), nullable=True),
	StructField("BillerName", StringType(), nullable=True),
	StructField("BillerAddress", StringType(), nullable=True),
	StructField("BillerCity", StringType(), nullable=True),
	StructField("BillerState", StringType(), nullable=True),
	StructField("BillerPhoneNo", StringType(), nullable=True),
    ])

# COMMAND ----------

read_df = spark.read.option("header",True)\
    .schema(dataset_schema)\
    .csv(f"{raw_folder_path}/Orders/*.csv")

# COMMAND ----------

read_df.createOrReplaceTempView ("v_orders")

# COMMAND ----------

%sql
MERGE INTO bronze.orders AS t
USING (SELECT ord,
ODate,
wt,
OrderValue,
ProductCategory,
ProductName,
ProductSize,
ProductValue,
ProductWeight,
NoOfItems,
SenderName,
SenderAddress,
SenderCity,
SenderState,
SenderPhoneNo,
ReceiverName,
ReceiverAddress,
ReceiverCity,
ReceiverState,
ReceiverPhoneNo,
EstimatedCost,
Substring(BillToSender,1,1)BillToSender,
Substring(BillToReceiver,1,1)BillToReceiver,
Substring(BillToOther,1,1)BillToOther,
BillerName,
BillerAddress,
BillerCity,
BillerState,
BillerPhoneNo
 FROM v_orders
 WHERE try_cast(ODate as Date)>(Select COALESCE (WatermarkValue,'2024-01-01') from bronze.watermark where TableName = 'bronze.orders')
  ) AS s
ON t.Ord = s.Ord
WHEN NOT MATCHED THEN INSERT *
# MAGIC

# COMMAND ----------

%sql
MERGE INTO bronze.watermark as t 
USING (select 1 as watermarkid, 'bronze.orders' as TableName, Max(try_cast(ODate as Date))as MaxDate
from bronze.orders)AS s 
ON t.watermarkid = s.watermarkid
WHEN MATCHED THEN UPDATE
SET t.WatermarkValue = s.MaxDate
WHEN NOT MATCHED THEN INSERT (watermarkid,TableName,WatermarkValue) values(S.watermarkid,S.TableName,S.MaxDate)
# MAGIC

# COMMAND ----------

dbutils.notebook.exit("Success")