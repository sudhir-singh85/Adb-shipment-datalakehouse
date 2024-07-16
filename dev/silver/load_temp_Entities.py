# Databricks notebook source
 %sql
 Insert Into silver.temp_Entities(EntityName, PhoneNumber, AddressLine1, AddressLine2, City, State, DataRealisationDate, Ingestion_Date)
 Select Distinct SenderName,SenderPhoneNo,SenderAddress,'',SenderCity,SenderState,OrderDate, getdate()  
 From silver.Orders
 Where OrderDate >(Select WaterMarkValue from silver.Watermark where TableName = 'silver.temp_Entities')

 union

 Select Distinct ReceiverName,ReceiverPhoneNo,ReceiverAddress,'',ReceiverCity,ReceiverState,OrderDate, getdate()  
 From silver.Orders
 Where OrderDate >(Select WaterMarkValue from silver.Watermark where TableName = 'silver.temp_Entities')

# COMMAND ----------

 %sql
 Update silver.Watermark 
 Set WaterMarkValue = (Select Max(DataRealisationDate) from silver.temp_Entities)
 Where TableName = 'silver.temp_Entities'

# COMMAND ----------

dbutils.notebook.exit("Success")