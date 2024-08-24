# Databricks notebook source
# MAGIC %run "../setup/config_shipmenttracking"

# COMMAND ----------

silver_folder_path

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS silver
# MAGIC LOCATION '/mnt/<storageaccountcontainer>/dev/silver'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver.watermark(watermarkid int,
# MAGIC TableName varchar(100),
# MAGIC WatermarkValue Date)

# COMMAND ----------

# MAGIC %sql
# MAGIC Delete from silver.watermark

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into silver.watermark(watermarkid,TableName,WatermarkValue)
# MAGIC Values(1,'silver.Invoices','2024-04-01'),
# MAGIC (2,'silver.TransactionLineItems','2024-04-01'),
# MAGIC (3,'silver.Orders','2024-04-01'),
# MAGIC (4,'silver.temp_Customers','2024-04-01'),
# MAGIC (5,'silver.temp_Persons','2024-04-01'),
# MAGIC (6,'silver.ShipmentDetails','2024-04-01'),
# MAGIC (7,'silver.temp_Entities','2024-04-01'),
# MAGIC (8,'silver.temp_Carriers','2024-04-01')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver.Orders(
# MAGIC ID long GENERATED ALWAYS AS IDENTITY,
# MAGIC OrderNo varchar(50),
# MAGIC OrderDate TIMESTAMP,
# MAGIC OrderWeight numeric(18,2),
# MAGIC OrderValue numeric(18,2),
# MAGIC ProductCategory varchar(255) ,
# MAGIC ProductName varchar(255) ,
# MAGIC ProductSize int ,
# MAGIC ProductValue numeric(18, 2) ,
# MAGIC ProductWeight numeric(18, 2) ,
# MAGIC NoOfItems int ,
# MAGIC SenderName varchar(255) ,
# MAGIC SenderAddress varchar(255) ,
# MAGIC SenderCity varchar(255) ,
# MAGIC SenderState varchar(255) ,
# MAGIC SenderPhoneNo varchar(100) ,
# MAGIC ReceiverName varchar(255) ,
# MAGIC ReceiverAddress varchar(255) ,
# MAGIC ReceiverCity varchar(255) ,
# MAGIC ReceiverState varchar(255) ,
# MAGIC ReceiverPhoneNo varchar(100) ,
# MAGIC EstimatedCost numeric(18, 2) ,
# MAGIC BillToSender varchar(5) ,
# MAGIC BillToReceiver varchar(5) ,
# MAGIC BillToOther varchar(5) ,
# MAGIC BillerName varchar(255) ,
# MAGIC BillerAddress varchar(255) ,
# MAGIC BillerCity varchar(255) ,
# MAGIC BillerState varchar(255) ,
# MAGIC BillerPhoneNo varchar(100) ,
# MAGIC Ingestion_Date TIMESTAMP
# MAGIC ) Using DELTA
# MAGIC Location '/mnt/<storageaccountcontainer>/dev/silver/Orders'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver.Invoices(
# MAGIC 	ID long GENERATED ALWAYS AS IDENTITY,
# MAGIC 	InvoiceNo varchar(50),
# MAGIC 	InvoiceDate TIMESTAMP,
# MAGIC 	OrderNo varchar(50),
# MAGIC 	InvoiceAmount numeric(18, 2),
# MAGIC 	TaxRate numeric(18, 2),
# MAGIC 	TaxAmount numeric(18, 2),
# MAGIC 	PaymentDays int,
# MAGIC 	GeneratedBy varchar(50),
# MAGIC 	Ingestion_Date TIMESTAMP
# MAGIC ) Using DELTA
# MAGIC Location '/mnt/<storageaccountcontainer>/dev/silver/Invoices'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver.TransactionLineItems(
# MAGIC 	ID long GENERATED ALWAYS AS IDENTITY,
# MAGIC 	TransactionLineItemID Int,
# MAGIC 	TransactionNo varchar(50),
# MAGIC 	TransactionDate varchar(50),
# MAGIC 	TransactionAmount numeric(18,2),
# MAGIC 	ChequeNo varchar(50),
# MAGIC 	ReferenceNo varchar(50),
# MAGIC 	PaymentType varchar(50),
# MAGIC 	Party varchar(100),
# MAGIC 	BankName varchar(50),
# MAGIC 	Details varchar(255),
# MAGIC 	GeneratedBy varchar(50),
# MAGIC 	Ingestion_Date TIMESTAMP
# MAGIC ) Using DELTA
# MAGIC Location '/mnt/<storageaccountcontainer>/dev/silver/TransactionLineItems'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver.ShipmentDetails(
# MAGIC ID long GENERATED ALWAYS AS IDENTITY,
# MAGIC ShipmentNo varchar(50),
# MAGIC OrderNo varchar(50),
# MAGIC ShipmentDate TIMESTAMP,
# MAGIC ShipmentCost numeric(18,2),
# MAGIC VehicleNo varchar(50),
# MAGIC CarrierName varchar(255),
# MAGIC CarrierPhoneNo varchar(50),
# MAGIC CarrierCity varchar(50),
# MAGIC CarrierState varchar(50),
# MAGIC Ingestion_Date TimeStamp
# MAGIC ) Using DELTA
# MAGIC Location '/mnt/<storageaccountcontainer>/dev/silver/ShipmentDetails'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver.temp_Customers(
# MAGIC   ID long GENERATED ALWAYS AS IDENTITY,
# MAGIC   CustomerName varchar(255),
# MAGIC   PhoneNumber varchar(50),
# MAGIC   AddressLine1 varchar(255),
# MAGIC   AddressLine2 varchar(255),
# MAGIC   City varchar(100),
# MAGIC   State varchar(100),
# MAGIC   DataRealisationDate TIMESTAMP,
# MAGIC   Ingestion_Date TIMESTAMP
# MAGIC )Using DELTA
# MAGIC Location '/mnt/<storageaccountcontainer>/dev/silver/temp_Customers'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver.temp_Persons(
# MAGIC   ID long GENERATED ALWAYS AS IDENTITY,
# MAGIC   PersonName varchar(255),
# MAGIC   DataRealisationDate TIMESTAMP,
# MAGIC   Ingestion_Date TIMESTAMP
# MAGIC )Using DELTA
# MAGIC Location '/mnt/<storageaccountcontainer>/dev/silver/temp_Persons'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver.temp_Entities(
# MAGIC   ID long GENERATED ALWAYS AS IDENTITY,
# MAGIC   EntityName varchar(255),
# MAGIC   PhoneNumber varchar(50),
# MAGIC   AddressLine1 varchar(255),
# MAGIC   AddressLine2 varchar(255),
# MAGIC   City varchar(50),
# MAGIC   State varchar(50),
# MAGIC   DataRealisationDate TIMESTAMP,
# MAGIC   Ingestion_Date TIMESTAMP
# MAGIC )Using DELTA
# MAGIC Location '/mnt/<storageaccountcontainer>/dev/silver/temp_Entities'
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver.temp_Carriers(
# MAGIC   ID long GENERATED ALWAYS AS IDENTITY,
# MAGIC   CarrierName varchar(255),
# MAGIC   PhoneNumber varchar(50),
# MAGIC   City varchar(50),
# MAGIC   State varchar(50),
# MAGIC   VehicleNo varchar(50),
# MAGIC   DataRealisationDate TIMESTAMP,
# MAGIC   Ingestion_Date TIMESTAMP
# MAGIC )Using DELTA
# MAGIC Location '/mnt/<storageaccountcontainer>/dev/silver/temp_Carriers'
# MAGIC

# COMMAND ----------


