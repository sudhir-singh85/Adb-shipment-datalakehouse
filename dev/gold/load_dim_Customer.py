# Databricks notebook source
  %sql
   Merge into gold.Dim_Customer as Target
   using (Select distinct CustomerName,PhoneNumber,AddressID
 		 from silver.temp_customers c
 		 Inner join gold.dim_Address a on C.AddressLine1 = a.AddressLine1 and c.AddressLine2 = a.AddressLine2
 					and c.City = a.City and c.State = a.State
 		 ) as Source
 On Target.CustomerName = Source.CustomerName and Target.PhoneNumber = Source.PhoneNumber and Target.AddressID = Source.AddressID

 When not Matched then Insert(CustomerName,PhoneNumber,AddressID)
 Values(Source.CustomerName, Source.PhoneNumber, Source.AddressID)

# COMMAND ----------

dbutils.notebook.exit("Success")