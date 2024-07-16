# Databricks notebook source
%run "../configuration/configuration_notebook"
# MAGIC

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType

# COMMAND ----------

dataset_schema = StructType([
    StructField("ShipmentNo", StringType(), nullable=True),
    StructField("ord", StringType(), nullable=True),
    StructField("ShipmentDate", TimestampType(), nullable=True),
    StructField("ShipmentCost", DoubleType(), nullable=True),
    StructField("VehicleNo", StringType(), nullable=True),
    StructField("CarrierName", StringType(), nullable=True),
    StructField("CarrierPhoneNo", StringType(), nullable=True),
    StructField("CarrierCity", StringType(), nullable=True),
    StructField("CarrierState", StringType(), nullable=True),
    
    ])

# COMMAND ----------

read_df = spark.read.option("header",True)\
    .schema(dataset_schema)\
    .csv(f"{raw_folder_path}/shipmentdetails/*.csv")

# COMMAND ----------

read_df.createOrReplaceTempView ("v_shipmentdetails")

# COMMAND ----------

%sql
MERGE INTO bronze.ShipmentDetails AS t
USING (SELECT ShipmentNo,
ord,
ShipmentDate,
ShipmentCost,
VehicleNo,
CarrierName,
CarrierPhoneNo,
CarrierCity,
CarrierState
 FROM v_shipmentdetails
 WHERE try_cast(ShipmentDate as Date)>(Select COALESCE (WatermarkValue,'2024-01-01') from bronze.watermark where TableName = 'bronze.shipmentdetails')
  ) AS s
ON t.ShipmentNo = s.ShipmentNo
WHEN NOT MATCHED THEN INSERT *
# MAGIC

# COMMAND ----------

%sql
MERGE INTO bronze.watermark as t 
USING (select 2 as watermarkid, 'bronze.shipmentdetails' as TableName, Max(try_cast(ShipmentDate as Date))as MaxDate
from bronze.shipmentdetails)AS s 
ON t.watermarkid = s.watermarkid
WHEN MATCHED THEN UPDATE
SET t.WatermarkValue = s.MaxDate
WHEN NOT MATCHED THEN INSERT (watermarkid,TableName,WatermarkValue) values(S.watermarkid,S.TableName,S.MaxDate)
# MAGIC

# COMMAND ----------

dbutils.notebook.exit("Success")