# Databricks notebook source
 %sql
 Insert Into silver.temp_Carriers(CarrierName, PhoneNumber, City, State, VehicleNo, DataRealisationDate, Ingestion_Date)
 Select Distinct CarrierName, CarrierPhoneNo, CarrierCity, CarrierState, VehicleNo, ShipmentDate, getdate()  
 From silver.ShipmentDetails
 Where ShipmentDate >(Select WaterMarkValue from silver.Watermark where TableName = 'silver.temp_Carriers')

# COMMAND ----------

 %sql
 Update silver.Watermark 
 Set WaterMarkValue = (Select Max(DataRealisationDate) from silver.temp_Carriers)
 Where TableName = 'silver.temp_Carriers'

# COMMAND ----------

dbutils.notebook.exit("Success")