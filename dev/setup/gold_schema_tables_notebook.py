# Databricks notebook source
 %run "../configuration/configuration_notebook"

# COMMAND ----------

gold_folder_path

# COMMAND ----------

 %sql
 CREATE DATABASE IF NOT EXISTS gold
 LOCATION '/mnt/storageaccountname/dev/gold'

# COMMAND ----------

 %sql
 CREATE TABLE IF NOT EXISTS gold.watermark(watermarkid int,
 TableName varchar(100),
 WatermarkValue Date)Using DELTA
 Location '/mnt/storageaccountname/dev/gold/watermark'

# COMMAND ----------

 %sql
 insert into gold.watermark(watermarkid,TableName,WatermarkValue)
 Values(1,'gold.dim_Customer','2024-04-01'),
 (2,'gold.dim_Person','2024-04-01'),
 (3,'gold.dim_date','2024-04-01'),
 (4,'gold.fact_Invoices','2024-04-01'),
 (5,'gold.dim_Address','2024-04-01')


# COMMAND ----------

 %sql
 CREATE TABLE IF NOT EXISTS gold.dim_Customer(
   CustomerID long GENERATED ALWAYS AS IDENTITY,
   CustomerName varchar(255),
   PhoneNumber varchar(20),
   AddressID int,
   Ingestion_Date TIMESTAMP
 )USING DELTA
 LOCATION '/mnt/storageaccountname/dev/gold/dim_Customer'

# COMMAND ----------

 %sql
 CREATE TABLE IF NOT EXISTS gold.dim_Person(
   PersonID long GENERATED ALWAYS AS IDENTITY,
   PersonName varchar(255),
   Ingestion_Date TIMESTAMP
 )USING DELTA
 LOCATION '/mnt/storageaccountname/dev/gold/dim_Person'

# COMMAND ----------

 %sql
 CREATE TABLE IF NOT EXISTS gold.dim_Date(
   DateKey int NOT NULL,
   Date date not null,
   Year int,
   Month int,
   Day int,
   Ingestion_Date TIMESTAMP
 )USING DELTA
 LOCATION '/mnt/storageaccountname/dev/gold/dim_Date'

# COMMAND ----------

 %sql
 CREATE TABLE IF NOT EXISTS gold.dim_Address(
   AddressID long GENERATED ALWAYS AS IDENTITY,
   AddressLine1 varchar(255),
   AddressLine2 varchar(255),
   City varchar(50),
   State varchar(50),
   Ingestion_Date TIMESTAMP
 )USING DELTA
 LOCATION '/mnt/storageaccountname/dev/gold/dim_Address'

# COMMAND ----------

 %sql
 CREATE TABLE IF NOT EXISTS gold.fact_Invoices(
   InvoiceId long GENERATED ALWAYS AS IDENTITY,
   InvoiceNo varchar(50),
   InvoiceDateKey INT,
   OrderNo varchar(50),
   InvoiceAmount numeric(18,2),
   TaxAmount numeric(18,2),
   TaxRate numeric(18,2),
   IsPaid boolean,
   CustomerId int,
   CreatedById int,
   Ingestion_Date timestamp
 ) using DELTA
 LOCATION '/mnt/storageaccountname/dev/gold/fact_Invoices'

# COMMAND ----------

