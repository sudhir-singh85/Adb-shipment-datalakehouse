# Databricks notebook source
  %sql
  Merge into gold.Dim_Person as Target
   using (Select Distinct PersonName
 		 from silver.temp_Persons
 		 ) as Source
 On Target.PersonName = Source.PersonName 
 When not Matched then Insert(PersonName)
 Values(Source.PersonName)

# COMMAND ----------

dbutils.notebook.exit("Success")