# Databricks notebook source
 %sql
 Insert Into silver.temp_persons(PersonName, DataRealisationDate, Ingestion_Date)
 Select Distinct GeneratedBy,InvoiceDate, getdate()  
 From silver.Invoices
 Where InvoiceDate >(Select WaterMarkValue from silver.Watermark where TableName = 'silver.Invoices')

 union

 Select Distinct GeneratedBy,TransactionDate, getdate()  
 From silver.TransactionLineItems
 Where TransactionDate >(Select WaterMarkValue from silver.Watermark where TableName = 'silver.TransactionLineItems')

# COMMAND ----------

 %sql
 Update silver.Watermark 
 Set WaterMarkValue = (Select Max(DataRealisationDate) from silver.temp_persons)
 Where TableName = 'silver.temp_Persons'

# COMMAND ----------

dbutils.notebook.exit("Success")