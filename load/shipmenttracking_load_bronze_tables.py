# Databricks notebook source
# MAGIC %run "../set up/config_shipmenttracking"

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

# MAGIC %sql
# MAGIC MERGE INTO bronze.orders AS t
# MAGIC USING (SELECT ord,
# MAGIC ODate,
# MAGIC wt,
# MAGIC OrderValue,
# MAGIC ProductCategory,
# MAGIC ProductName,
# MAGIC ProductSize,
# MAGIC ProductValue,
# MAGIC ProductWeight,
# MAGIC NoOfItems,
# MAGIC SenderName,
# MAGIC SenderAddress,
# MAGIC SenderCity,
# MAGIC SenderState,
# MAGIC SenderPhoneNo,
# MAGIC ReceiverName,
# MAGIC ReceiverAddress,
# MAGIC ReceiverCity,
# MAGIC ReceiverState,
# MAGIC ReceiverPhoneNo,
# MAGIC EstimatedCost,
# MAGIC Substring(BillToSender,1,1)BillToSender,
# MAGIC Substring(BillToReceiver,1,1)BillToReceiver,
# MAGIC Substring(BillToOther,1,1)BillToOther,
# MAGIC BillerName,
# MAGIC BillerAddress,
# MAGIC BillerCity,
# MAGIC BillerState,
# MAGIC BillerPhoneNo
# MAGIC  FROM v_orders
# MAGIC  WHERE try_cast(ODate as Date)>(Select COALESCE (WatermarkValue,'2024-01-01') from bronze.watermark where TableName = 'bronze.orders')
# MAGIC   ) AS s
# MAGIC ON t.Ord = s.Ord
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO bronze.watermark as t 
# MAGIC USING (select 1 as watermarkid, 'bronze.orders' as TableName, Max(try_cast(ODate as Date))as MaxDate
# MAGIC from bronze.orders)AS s 
# MAGIC ON t.watermarkid = s.watermarkid
# MAGIC WHEN MATCHED THEN UPDATE
# MAGIC SET t.WatermarkValue = s.MaxDate
# MAGIC WHEN NOT MATCHED THEN INSERT (watermarkid,TableName,WatermarkValue) values(S.watermarkid,S.TableName,S.MaxDate)

# COMMAND ----------

dataset_schema_shipmentdetails = StructType([
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

read_df_shipmentdetails = spark.read.option("header",True)\
    .schema(dataset_schema_shipmentdetails)\
    .csv(f"{raw_folder_path}/shipmentdetails/*.csv")

# COMMAND ----------

read_df_shipmentdetails.createOrReplaceTempView ("v_shipmentdetails")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO bronze.ShipmentDetails AS t
# MAGIC USING (SELECT ShipmentNo,
# MAGIC ord,
# MAGIC ShipmentDate,
# MAGIC ShipmentCost,
# MAGIC VehicleNo,
# MAGIC CarrierName,
# MAGIC CarrierPhoneNo,
# MAGIC CarrierCity,
# MAGIC CarrierState
# MAGIC  FROM v_shipmentdetails
# MAGIC  WHERE try_cast(ShipmentDate as Date)>(Select COALESCE (WatermarkValue,'2024-01-01') from bronze.watermark where TableName = 'bronze.shipmentdetails')
# MAGIC   ) AS s
# MAGIC ON t.ShipmentNo = s.ShipmentNo
# MAGIC WHEN NOT MATCHED THEN INSERT *
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO bronze.watermark as t 
# MAGIC USING (select 2 as watermarkid, 'bronze.shipmentdetails' as TableName, Max(try_cast(ShipmentDate as Date))as MaxDate
# MAGIC from bronze.shipmentdetails)AS s 
# MAGIC ON t.watermarkid = s.watermarkid
# MAGIC WHEN MATCHED THEN UPDATE
# MAGIC SET t.WatermarkValue = s.MaxDate
# MAGIC WHEN NOT MATCHED THEN INSERT (watermarkid,TableName,WatermarkValue) values(S.watermarkid,S.TableName,S.MaxDate)

# COMMAND ----------

dataset_schema_invoices = StructType([
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

read_df_invoices = spark.read.option("header",True)\
    .schema(dataset_schema_invoices)\
    .json(f"{raw_folder_path}/invoices/*.json")

# COMMAND ----------

read_df_invoices.createOrReplaceTempView ("v_invoices")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO bronze.invoices AS t
# MAGIC USING (SELECT InvNo,
# MAGIC InvDate,
# MAGIC ord,
# MAGIC Amount,
# MAGIC TaxRate,
# MAGIC TaxAmount,
# MAGIC PaymentDays,
# MAGIC GeneratedBy
# MAGIC  FROM v_invoices
# MAGIC   WHERE try_cast(InvDate as Date)>(Select COALESCE (WatermarkValue,'2024-01-01') from bronze.watermark where TableName = 'bronze.invoices')
# MAGIC   ) AS s
# MAGIC ON t.InvNo = s.InvNo
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO bronze.watermark as t 
# MAGIC USING (select 4 as watermarkid, 'bronze.invoices' as TableName, Max(try_cast(InvDate as Date))as MaxDate
# MAGIC from bronze.invoices)AS s 
# MAGIC ON t.watermarkid = s.watermarkid
# MAGIC WHEN MATCHED THEN UPDATE
# MAGIC SET t.WatermarkValue = s.MaxDate
# MAGIC WHEN NOT MATCHED THEN INSERT (watermarkid,TableName,WatermarkValue) values(S.watermarkid,S.TableName,S.MaxDate)

# COMMAND ----------

dataset_schema_tli = StructType([
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

read_df_tli = spark.read.option("header",True)\
    .schema(dataset_schema_tli)\
    .csv(f"{raw_folder_path}/transactionlineitems/*.csv")

# COMMAND ----------

read_df_tli.createOrReplaceTempView ("v_transactionlineitems")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO bronze.transactionlineitems AS t
# MAGIC USING (SELECT TransactionLineItemID,
# MAGIC TrNo,
# MAGIC TrDate,
# MAGIC TrAmount,
# MAGIC Cheque,
# MAGIC RefNo,
# MAGIC PaymentType,
# MAGIC Party,
# MAGIC BankName,
# MAGIC Details,
# MAGIC GeneratedBy
# MAGIC  FROM v_transactionlineitems
# MAGIC   WHERE try_cast(TrDate as Date)>(Select COALESCE (WatermarkValue,'2024-01-01') from bronze.watermark where TableName = 'bronze.transactionlineitems')
# MAGIC   ) AS s
# MAGIC ON t.TransactionLineItemID = s.TransactionLineItemID
# MAGIC WHEN NOT MATCHED THEN INSERT *
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO bronze.watermark as t 
# MAGIC USING (select 5 as watermarkid, 'bronze.transactionlineitems' as TableName, Max(try_cast(TrDate as Date))as MaxDate
# MAGIC from bronze.transactionlineitems)AS s 
# MAGIC ON t.watermarkid = s.watermarkid
# MAGIC WHEN MATCHED THEN UPDATE
# MAGIC SET t.WatermarkValue = s.MaxDate
# MAGIC WHEN NOT MATCHED THEN INSERT (watermarkid,TableName,WatermarkValue) values(S.watermarkid,S.TableName,S.MaxDate)

# COMMAND ----------

dbutils.notebook.exit("Success")
