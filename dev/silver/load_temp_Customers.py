# Databricks notebook source
 %sql
 Insert Into silver.temp_Customers(CustomerName, PhoneNumber, AddressLine1, AddressLine2, City, State, DataRealisationDate, Ingestion_Date)
 Select Distinct BillerName,BillerPhoneNo,BillerAddress,'',BillerCity,BillerState,OrderDate, getdate()  
 From silver.Orders
 Where OrderDate >(Select WaterMarkValue from silver.Watermark where TableName = 'silver.Orders')
 and BIllToOther = 'Y'
 Union
 Select Distinct SenderName,SenderPhoneNo,SenderAddress,'',SenderCity,SenderState,OrderDate, getdate()  
 From silver.Orders
 Where OrderDate >(Select WaterMarkValue from silver.Watermark where TableName = 'silver.Orders')
 and BillToSender = 'Y'
 Union
 Select Distinct ReceiverName,ReceiverPhoneNo,ReceiverAddress,'',ReceiverCity,ReceiverState,OrderDate, getdate()  
 From silver.Orders
 Where OrderDate >(Select WaterMarkValue from silver.Watermark where TableName = 'silver.Orders')
 and BillToReceiver = 'Y'

# COMMAND ----------

 %sql
 Update silver.Watermark 
 Set WaterMarkValue = (Select Max(DataRealisationDate) from silver.temp_Customers)
 Where TableName = 'silver.temp_Customers'

# COMMAND ----------

dbutils.notebook.exit("Success")