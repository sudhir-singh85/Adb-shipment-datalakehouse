# Databricks notebook source
 %sql
 Insert Into silver.Invoices(InvoiceNo, InvoiceDate, OrderNo, InvoiceAmount, TaxRate, TaxAmount, PaymentDays, GeneratedBy, Ingestion_Date)
 Select InvNo, InvDate, Ord, Amount, TaxRate, TaxAmount, PaymentDays, GeneratedBy, getdate()
 From bronze.Invoices
 Where InvDate >(Select WaterMarkValue from silver.Watermark where TableName = 'silver.Invoices')

# COMMAND ----------

 %sql
 Update silver.Watermark 
 Set WaterMarkValue = (Select Max(InvoiceDate) from silver.Invoices)
 Where TableName = 'silver.Invoices'

# COMMAND ----------

dbutils.notebook.exit("Success")