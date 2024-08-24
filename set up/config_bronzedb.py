# Databricks notebook source
# MAGIC %run "../setup/config_shipmenttracking"

# COMMAND ----------

bronze_folder_path

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS bronze
# MAGIC LOCATION '/mnt/<storageaccountcontainer>/dev/bronze'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS bronze.watermark(watermarkid int,
# MAGIC TableName varchar(100),
# MAGIC WatermarkValue Date)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS bronze.Orders(
# MAGIC Ord varchar(50),
# MAGIC ODate varchar(255),
# MAGIC wt numeric(18,2),
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
# MAGIC BillerPhoneNo varchar(100) 
# MAGIC ) Using DELTA
# MAGIC Location '/mnt/<storageaccountcontainer>/dev/bronze/Orders'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS bronze.invoices(
# MAGIC 	InvNo varchar(50),
# MAGIC 	InvDate varchar(50),
# MAGIC 	ord varchar(50),
# MAGIC 	Amount numeric(18, 2),
# MAGIC 	TaxRate numeric(18, 2),
# MAGIC 	TaxAmount numeric(18, 2),
# MAGIC 	PaymentDays int,
# MAGIC 	GeneratedBy varchar(50)
# MAGIC ) Using DELTA
# MAGIC Location '/mnt/<storageaccountcontainer>/dev/bronze/invoices'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS bronze.transactionlineitems(
# MAGIC 	TransactionLineItemID Int,
# MAGIC 	TrNo varchar(50),
# MAGIC 	TrDate varchar(50),
# MAGIC 	TrAmount numeric(18,2),
# MAGIC 	Cheque varchar(50),
# MAGIC 	RefNo varchar(50),
# MAGIC 	PaymentType varchar(50),
# MAGIC 	Party varchar(100),
# MAGIC 	BankName varchar(50),
# MAGIC 	Details varchar(255),
# MAGIC 	GeneratedBy varchar(50)
# MAGIC 	
# MAGIC ) Using DELTA
# MAGIC Location '/mnt/<storageaccountcontainer>/dev/bronze/transactionlineitems'
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS bronze.ShipmentDetails(
# MAGIC ShipmentNo varchar(50),
# MAGIC ord varchar(50),
# MAGIC ShipmentDate varchar(255),
# MAGIC ShipmentCost numeric(18,2),
# MAGIC VehicleNo varchar(50),
# MAGIC CarrierName varchar(255),
# MAGIC CarrierPhoneNo varchar(50),
# MAGIC CarrierCity varchar(50),
# MAGIC CarrierState varchar(50)
# MAGIC ) Using DELTA
# MAGIC Location '/mnt/<storageaccountcontainer>/dev/bronze/shipmentdetails'
