# Databricks notebook source
# MAGIC %run "../setup/config_shipmenttracking"

# COMMAND ----------

gold_folder_path

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS gold
# MAGIC LOCATION '/mnt/<storageaccountcontainer>/dev/gold'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold.watermark(watermarkid int,
# MAGIC TableName varchar(100),
# MAGIC WatermarkValue Date)Using DELTA
# MAGIC Location '/mnt/<storageaccountcontainer>/dev/gold/watermark'

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into gold.watermark(watermarkid,TableName,WatermarkValue)
# MAGIC Values(1,'gold.dim_Customer','2024-04-01'),
# MAGIC (2,'gold.dim_Person','2024-04-01'),
# MAGIC (3,'gold.dim_date','2024-04-01'),
# MAGIC (4,'gold.fact_Invoices','2024-04-01'),
# MAGIC (5,'gold.dim_Address','2024-04-01')
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold.dim_Customer(
# MAGIC   CustomerID long GENERATED ALWAYS AS IDENTITY,
# MAGIC   CustomerName varchar(255),
# MAGIC   PhoneNumber varchar(20),
# MAGIC   AddressID int,
# MAGIC   Ingestion_Date TIMESTAMP
# MAGIC )USING DELTA
# MAGIC LOCATION '/mnt/<storageaccountcontainer>/dev/gold/dim_Customer'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold.dim_Person(
# MAGIC   PersonID long GENERATED ALWAYS AS IDENTITY,
# MAGIC   PersonName varchar(255),
# MAGIC   Ingestion_Date TIMESTAMP
# MAGIC )USING DELTA
# MAGIC LOCATION '/mnt/<storageaccountcontainer>/dev/gold/dim_Person'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold.dim_Date(
# MAGIC   DateKey int NOT NULL,
# MAGIC   Date date not null,
# MAGIC   Year int,
# MAGIC   Month int,
# MAGIC   Day int,
# MAGIC   Ingestion_Date TIMESTAMP
# MAGIC )USING DELTA
# MAGIC LOCATION '/mnt/<storageaccountcontainer>/dev/gold/dim_Date'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold.dim_Address(
# MAGIC   AddressID long GENERATED ALWAYS AS IDENTITY,
# MAGIC   AddressLine1 varchar(255),
# MAGIC   AddressLine2 varchar(255),
# MAGIC   City varchar(50),
# MAGIC   State varchar(50),
# MAGIC   Ingestion_Date TIMESTAMP
# MAGIC )USING DELTA
# MAGIC LOCATION '/mnt/<storageaccountcontainer>/dev/gold/dim_Address'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold.fact_Invoices(
# MAGIC   InvoiceId long GENERATED ALWAYS AS IDENTITY,
# MAGIC   InvoiceNo varchar(50),
# MAGIC   InvoiceDateKey INT,
# MAGIC   OrderNo varchar(50),
# MAGIC   InvoiceAmount numeric(18,2),
# MAGIC   TaxAmount numeric(18,2),
# MAGIC   TaxRate numeric(18,2),
# MAGIC   IsPaid boolean,
# MAGIC   CustomerId int,
# MAGIC   CreatedById int,
# MAGIC   Ingestion_Date timestamp
# MAGIC ) using DELTA
# MAGIC LOCATION '/mnt/<storageaccountcontainer>/dev/gold/fact_Invoices'

# COMMAND ----------


