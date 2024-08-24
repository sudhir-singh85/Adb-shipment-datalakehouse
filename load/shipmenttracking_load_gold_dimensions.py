# Databricks notebook source
# MAGIC  %sql
# MAGIC  Merge into gold.Dim_Address as Target
# MAGIC   using (Select AddressLine1, AddressLine2, City, State
# MAGIC 		 from silver.temp_customers
# MAGIC 		 Union
# MAGIC 		 Select AddressLine1, AddressLine2, City, State
# MAGIC 		 from silver.temp_entities
# MAGIC 		 Union
# MAGIC 		 Select 'NA', 'NA', City, State
# MAGIC 		 from silver.temp_carriers) as Source
# MAGIC On Target.AddressLine1 = Source.AddressLine1 and Target.AddressLine2 = Source.AddressLine2 
# MAGIC and Target.City = Source.City and Target.State = Source.State
# MAGIC
# MAGIC When not Matched then Insert(AddressLine1
# MAGIC       ,AddressLine2
# MAGIC       ,City
# MAGIC       ,State)
# MAGIC Values(Source.AddressLine1, Source.AddressLine2, Source.City, Source.State)

# COMMAND ----------

# MAGIC  %sql
# MAGIC  Merge into gold.Dim_Person as Target
# MAGIC   using (Select Distinct PersonName
# MAGIC 		 from silver.temp_Persons
# MAGIC 		 ) as Source
# MAGIC On Target.PersonName = Source.PersonName 
# MAGIC When not Matched then Insert(PersonName)
# MAGIC Values(Source.PersonName)

# COMMAND ----------

# MAGIC  %sql
# MAGIC   Merge into gold.Dim_Customer as Target
# MAGIC   using (Select distinct CustomerName,PhoneNumber,AddressID
# MAGIC 		 from silver.temp_customers c
# MAGIC 		 Inner join gold.dim_Address a on C.AddressLine1 = a.AddressLine1 and c.AddressLine2 = a.AddressLine2
# MAGIC 					and c.City = a.City and c.State = a.State
# MAGIC 		 ) as Source
# MAGIC On Target.CustomerName = Source.CustomerName and Target.PhoneNumber = Source.PhoneNumber and Target.AddressID = Source.AddressID
# MAGIC
# MAGIC When not Matched then Insert(CustomerName,PhoneNumber,AddressID)
# MAGIC Values(Source.CustomerName, Source.PhoneNumber, Source.AddressID)

# COMMAND ----------

dbutils.notebook.exit("Success")
