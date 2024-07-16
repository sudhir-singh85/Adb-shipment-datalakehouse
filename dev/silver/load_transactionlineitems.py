# Databricks notebook source
 %sql
 Insert Into silver.TransactionLineItems(TransactionLineItemID, TransactionNo, TransactionDate, TransactionAmount, ChequeNo, ReferenceNo, PaymentType, Party, BankName, Details, GeneratedBy, Ingestion_Date)
 Select   TransactionLineItemID, TrNo, TrDate, TrAmount, Cheque, RefNo, PaymentType, Party, BankName, Details, GeneratedBy, getdate()   
 From bronze.TransactionLineItems
 Where TrDate >(Select WaterMarkValue from silver.Watermark where TableName = 'silver.TransactionLineItems')

# COMMAND ----------

 %sql
 Update silver.Watermark 
 Set WaterMarkValue = (Select Max(TransactionDate) from silver.TransactionLineItems)
 Where TableName = 'silver.TransactionLineItems'

# COMMAND ----------

dbutils.notebook.exit("Success")