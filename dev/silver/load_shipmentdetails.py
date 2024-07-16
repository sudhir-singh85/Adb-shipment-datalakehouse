# Databricks notebook source
 %sql
 Insert Into silver.ShipmentDetails( ShipmentNo, OrderNo, ShipmentDate, ShipmentCost, VehicleNo, CarrierName, CarrierPhoneNo, CarrierCity, CarrierState, Ingestion_Date)
 Select   ShipmentNo, ord, ShipmentDate, ShipmentCost, VehicleNo, CarrierName, CarrierPhoneNo, CarrierCity, CarrierState, getdate() 
 From bronze.ShipmentDetails
 Where ShipmentDate >(Select WaterMarkValue from silver.Watermark where TableName = 'silver.ShipmentDetails')

# COMMAND ----------

 %sql
 Update silver.Watermark 
 Set WaterMarkValue = (Select Max(ShipmentDate) from silver.ShipmentDetails)
 Where TableName = 'silver.ShipmentDetails'

# COMMAND ----------

dbutils.notebook.exit("Success")