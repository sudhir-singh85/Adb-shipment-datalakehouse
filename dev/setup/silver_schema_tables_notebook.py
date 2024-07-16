# Databricks notebook source
 %run "../configuration/configuration_notebook"

# COMMAND ----------

silver_folder_path

# COMMAND ----------

 %sql
 CREATE DATABASE IF NOT EXISTS silver
 LOCATION '/mnt/storageaccountname/dev/silver'

# COMMAND ----------

 %sql
 CREATE TABLE IF NOT EXISTS silver.watermark(watermarkid int,
 TableName varchar(100),
 WatermarkValue Date)

# COMMAND ----------

 %sql
 Delete from silver.watermark

# COMMAND ----------

 %sql
 insert into silver.watermark(watermarkid,TableName,WatermarkValue)
 Values(1,'silver.Invoices','2024-04-01'),
 (2,'silver.TransactionLineItems','2024-04-01'),
 (3,'silver.Orders','2024-04-01'),
 (4,'silver.temp_Customers','2024-04-01'),
 (5,'silver.temp_Persons','2024-04-01'),
 (6,'silver.ShipmentDetails','2024-04-01'),
 (7,'silver.temp_Entities','2024-04-01'),
 (8,'silver.temp_Carriers','2024-04-01')

# COMMAND ----------

 %sql
 CREATE TABLE IF NOT EXISTS silver.Orders(
 ID long GENERATED ALWAYS AS IDENTITY,
 OrderNo varchar(50),
 OrderDate TIMESTAMP,
 OrderWeight numeric(18,2),
 OrderValue numeric(18,2),
 ProductCategory varchar(255) ,
 ProductName varchar(255) ,
 ProductSize int ,
 ProductValue numeric(18, 2) ,
 ProductWeight numeric(18, 2) ,
 NoOfItems int ,
 SenderName varchar(255) ,
 SenderAddress varchar(255) ,
 SenderCity varchar(255) ,
 SenderState varchar(255) ,
 SenderPhoneNo varchar(100) ,
 ReceiverName varchar(255) ,
 ReceiverAddress varchar(255) ,
 ReceiverCity varchar(255) ,
 ReceiverState varchar(255) ,
 ReceiverPhoneNo varchar(100) ,
 EstimatedCost numeric(18, 2) ,
 BillToSender varchar(5) ,
 BillToReceiver varchar(5) ,
 BillToOther varchar(5) ,
 BillerName varchar(255) ,
 BillerAddress varchar(255) ,
 BillerCity varchar(255) ,
 BillerState varchar(255) ,
 BillerPhoneNo varchar(100) ,
 Ingestion_Date TIMESTAMP
 ) Using DELTA
 Location '/mnt/storageaccountname/dev/silver/Orders'

# COMMAND ----------

 %sql
 CREATE TABLE IF NOT EXISTS silver.Invoices(
 	ID long GENERATED ALWAYS AS IDENTITY,
 	InvoiceNo varchar(50),
 	InvoiceDate TIMESTAMP,
 	OrderNo varchar(50),
 	InvoiceAmount numeric(18, 2),
 	TaxRate numeric(18, 2),
 	TaxAmount numeric(18, 2),
 	PaymentDays int,
 	GeneratedBy varchar(50),
 	Ingestion_Date TIMESTAMP
 ) Using DELTA
 Location '/mnt/storageaccountname/dev/silver/Invoices'

# COMMAND ----------

 %sql
 CREATE TABLE IF NOT EXISTS silver.TransactionLineItems(
 	ID long GENERATED ALWAYS AS IDENTITY,
 	TransactionLineItemID Int,
 	TransactionNo varchar(50),
 	TransactionDate varchar(50),
 	TransactionAmount numeric(18,2),
 	ChequeNo varchar(50),
 	ReferenceNo varchar(50),
 	PaymentType varchar(50),
 	Party varchar(100),
 	BankName varchar(50),
 	Details varchar(255),
 	GeneratedBy varchar(50),
 	Ingestion_Date TIMESTAMP
 ) Using DELTA
 Location '/mnt/storageaccountname/dev/silver/TransactionLineItems'

# COMMAND ----------

 %sql
 CREATE TABLE IF NOT EXISTS silver.ShipmentDetails(
 ID long GENERATED ALWAYS AS IDENTITY,
 ShipmentNo varchar(50),
 OrderNo varchar(50),
 ShipmentDate TIMESTAMP,
 ShipmentCost numeric(18,2),
 VehicleNo varchar(50),
 CarrierName varchar(255),
 CarrierPhoneNo varchar(50),
 CarrierCity varchar(50),
 CarrierState varchar(50),
 Ingestion_Date TimeStamp
 ) Using DELTA
 Location '/mnt/storageaccountname/dev/silver/ShipmentDetails'

# COMMAND ----------

 %sql
 CREATE TABLE IF NOT EXISTS silver.temp_Customers(
   ID long GENERATED ALWAYS AS IDENTITY,
   CustomerName varchar(255),
   PhoneNumber varchar(50),
   AddressLine1 varchar(255),
   AddressLine2 varchar(255),
   City varchar(100),
   State varchar(100),
   DataRealisationDate TIMESTAMP,
   Ingestion_Date TIMESTAMP
 )Using DELTA
 Location '/mnt/storageaccountname/dev/silver/temp_Customers'

# COMMAND ----------

 %sql
 CREATE TABLE IF NOT EXISTS silver.temp_Persons(
   ID long GENERATED ALWAYS AS IDENTITY,
   PersonName varchar(255),
   DataRealisationDate TIMESTAMP,
   Ingestion_Date TIMESTAMP
 )Using DELTA
 Location '/mnt/storageaccountname/dev/silver/temp_Persons'

# COMMAND ----------

 %sql
 CREATE TABLE IF NOT EXISTS silver.temp_Entities(
   ID long GENERATED ALWAYS AS IDENTITY,
   EntityName varchar(255),
   PhoneNumber varchar(50),
   AddressLine1 varchar(255),
   AddressLine2 varchar(255),
   City varchar(50),
   State varchar(50),
   DataRealisationDate TIMESTAMP,
   Ingestion_Date TIMESTAMP
 )Using DELTA
 Location '/mnt/storageaccountname/dev/silver/temp_Entities'


# COMMAND ----------

 %sql
 CREATE TABLE IF NOT EXISTS silver.temp_Carriers(
   ID long GENERATED ALWAYS AS IDENTITY,
   CarrierName varchar(255),
   PhoneNumber varchar(50),
   City varchar(50),
   State varchar(50),
   VehicleNo varchar(50),
   DataRealisationDate TIMESTAMP,
   Ingestion_Date TIMESTAMP
 )Using DELTA
 Location '/mnt/storageaccountname/dev/silver/temp_Carriers'


# COMMAND ----------

