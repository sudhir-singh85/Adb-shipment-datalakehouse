# Databricks notebook source
  %sql
  Merge into gold.Dim_Address as Target
   using (Select AddressLine1, AddressLine2, City, State
 		 from silver.temp_customers
 		 Union
 		 Select AddressLine1, AddressLine2, City, State
 		 from silver.temp_entities
 		 Union
 		 Select 'NA', 'NA', City, State
 		 from silver.temp_carriers) as Source
 On Target.AddressLine1 = Source.AddressLine1 and Target.AddressLine2 = Source.AddressLine2 
 and Target.City = Source.City and Target.State = Source.State

 When not Matched then Insert(AddressLine1
       ,AddressLine2
       ,City
       ,State)
 Values(Source.AddressLine1, Source.AddressLine2, Source.City, Source.State)

# COMMAND ----------

dbutils.notebook.exit("Success")