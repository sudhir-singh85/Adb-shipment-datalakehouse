# Databricks notebook source
 %run "../configuration/configuration_notebook"

# COMMAND ----------

bronze_folder_path

# COMMAND ----------

 %sql
 CREATE DATABASE IF NOT EXISTS bronze
 LOCATION '/mnt/storageaccountname/dev/bronze'

# COMMAND ----------

 %sql
 CREATE TABLE IF NOT EXISTS bronze.watermark(watermarkid int,
 TableName varchar(100),
 WatermarkValue Date)

# COMMAND ----------

 %sql
 CREATE TABLE IF NOT EXISTS bronze.Orders(
 Ord varchar(50),
 ODate varchar(255),
 wt numeric(18,2),
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
 BillerPhoneNo varchar(100) 
 ) Using DELTA
 Location '/mnt/storageaccountname/dev/bronze/Orders'

# COMMAND ----------

 %sql
 CREATE TABLE IF NOT EXISTS bronze.invoices(
 	InvNo varchar(50),
 	InvDate varchar(50),
 	ord varchar(50),
 	Amount numeric(18, 2),
 	TaxRate numeric(18, 2),
 	TaxAmount numeric(18, 2),
 	PaymentDays int,
 	GeneratedBy varchar(50)
 ) Using DELTA
 Location '/mnt/storageaccountname/dev/bronze/invoices'

# COMMAND ----------

 %sql
 CREATE TABLE IF NOT EXISTS bronze.transactionlineitems(
 	TransactionLineItemID Int,
 	TrNo varchar(50),
 	TrDate varchar(50),
 	TrAmount numeric(18,2),
 	Cheque varchar(50),
 	RefNo varchar(50),
 	PaymentType varchar(50),
 	Party varchar(100),
 	BankName varchar(50),
 	Details varchar(255),
 	GeneratedBy varchar(50)
 	
 ) Using DELTA
 Location '/mnt/storageaccountname/dev/bronze/transactionlineitems'


# COMMAND ----------

 %sql
 CREATE TABLE IF NOT EXISTS bronze.ShipmentDetails(
 ShipmentNo varchar(50),
 ord varchar(50),
 ShipmentDate varchar(255),
 ShipmentCost numeric(18,2),
 VehicleNo varchar(50),
 CarrierName varchar(255),
 CarrierPhoneNo varchar(50),
 CarrierCity varchar(50),
 CarrierState varchar(50)
 ) Using DELTA
 Location '/mnt/storageaccountname/dev/bronze/shipmentdetails'