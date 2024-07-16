# Databricks notebook source
 %sql
 Insert Into silver.Orders( OrderNo, OrderDate, OrderWeight, OrderValue, ProductCategory, ProductName, ProductSize, ProductValue, ProductWeight, NoOfItems, SenderName, SenderAddress, SenderCity, SenderState, SenderPhoneNo, ReceiverName, ReceiverAddress, ReceiverCity, ReceiverState, ReceiverPhoneNo, EstimatedCost, BillToSender, BillToReceiver, BillToOther, BillerName, BillerAddress, BillerCity, BillerState, BillerPhoneNo, Ingestion_Date)
 Select   ord, Odate, wt, OrderValue, ProductCategory, ProductName, ProductSize, ProductValue, ProductWeight, NoOfItems, SenderName, SenderAddress, SenderCity, SenderState, SenderPhoneNo, ReceiverName, ReceiverAddress, ReceiverCity, ReceiverState, ReceiverPhoneNo, EstimatedCost, BillToSender, BillToReceiver, BillToOther, BillerName, BillerAddress, BillerCity, BillerState, BillerPhoneNo, getdate()
 From bronze.Orders
 Where Odate >(Select WaterMarkValue from silver.Watermark where TableName = 'silver.Orders')

# COMMAND ----------

 %sql
 Update silver.Watermark 
 Set WaterMarkValue = (Select Max(OrderDate) from silver.Orders)
 Where TableName = 'silver.Orders'

# COMMAND ----------

dbutils.notebook.exit("Success")