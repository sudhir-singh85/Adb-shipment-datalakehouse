# Databricks notebook source
# MAGIC %sql
# MAGIC Insert Into silver.Orders( OrderNo, OrderDate, OrderWeight, OrderValue, ProductCategory, ProductName, ProductSize, ProductValue, ProductWeight, NoOfItems, SenderName, SenderAddress, SenderCity, SenderState, SenderPhoneNo, ReceiverName, ReceiverAddress, ReceiverCity, ReceiverState, ReceiverPhoneNo, EstimatedCost, BillToSender, BillToReceiver, BillToOther, BillerName, BillerAddress, BillerCity, BillerState, BillerPhoneNo, Ingestion_Date)
# MAGIC Select   ord, Odate, wt, OrderValue, ProductCategory, ProductName, ProductSize, ProductValue, ProductWeight, NoOfItems, SenderName, SenderAddress, SenderCity, SenderState, SenderPhoneNo, ReceiverName, ReceiverAddress, ReceiverCity, ReceiverState, ReceiverPhoneNo, EstimatedCost, BillToSender, BillToReceiver, BillToOther, BillerName, BillerAddress, BillerCity, BillerState, BillerPhoneNo, getdate()
# MAGIC From bronze.Orders
# MAGIC Where Odate >(Select WaterMarkValue from silver.Watermark where TableName = 'silver.Orders')

# COMMAND ----------

# MAGIC %sql
# MAGIC Update silver.Watermark 
# MAGIC Set WaterMarkValue = (Select Max(OrderDate) from silver.Orders)
# MAGIC Where TableName = 'silver.Orders'

# COMMAND ----------

# MAGIC %sql
# MAGIC Insert Into silver.Invoices(InvoiceNo, InvoiceDate, OrderNo, InvoiceAmount, TaxRate, TaxAmount, PaymentDays, GeneratedBy, Ingestion_Date)
# MAGIC Select InvNo, InvDate, Ord, Amount, TaxRate, TaxAmount, PaymentDays, GeneratedBy, getdate()
# MAGIC From bronze.Invoices
# MAGIC Where InvDate >(Select WaterMarkValue from silver.Watermark where TableName = 'silver.Invoices')

# COMMAND ----------

# MAGIC %sql
# MAGIC Update silver.Watermark 
# MAGIC Set WaterMarkValue = (Select Max(InvoiceDate) from silver.Invoices)
# MAGIC Where TableName = 'silver.Invoices'

# COMMAND ----------

# MAGIC %sql
# MAGIC Insert Into silver.TransactionLineItems(TransactionLineItemID, TransactionNo, TransactionDate, TransactionAmount, ChequeNo, ReferenceNo, PaymentType, Party, BankName, Details, GeneratedBy, Ingestion_Date)
# MAGIC Select   TransactionLineItemID, TrNo, TrDate, TrAmount, Cheque, RefNo, PaymentType, Party, BankName, Details, GeneratedBy, getdate()   
# MAGIC From bronze.TransactionLineItems
# MAGIC Where TrDate >(Select WaterMarkValue from silver.Watermark where TableName = 'silver.TransactionLineItems')

# COMMAND ----------

# MAGIC %sql
# MAGIC Update silver.Watermark 
# MAGIC Set WaterMarkValue = (Select Max(TransactionDate) from silver.TransactionLineItems)
# MAGIC Where TableName = 'silver.TransactionLineItems'

# COMMAND ----------

# MAGIC %sql
# MAGIC Insert Into silver.temp_Customers(CustomerName, PhoneNumber, AddressLine1, AddressLine2, City, State, DataRealisationDate, Ingestion_Date)
# MAGIC Select Distinct BillerName,BillerPhoneNo,BillerAddress,'',BillerCity,BillerState,OrderDate, getdate()  
# MAGIC From silver.Orders
# MAGIC Where OrderDate >(Select WaterMarkValue from silver.Watermark where TableName = 'silver.Orders')
# MAGIC and BIllToOther = 'Y'
# MAGIC Union
# MAGIC Select Distinct SenderName,SenderPhoneNo,SenderAddress,'',SenderCity,SenderState,OrderDate, getdate()  
# MAGIC From silver.Orders
# MAGIC Where OrderDate >(Select WaterMarkValue from silver.Watermark where TableName = 'silver.Orders')
# MAGIC and BillToSender = 'Y'
# MAGIC Union
# MAGIC Select Distinct ReceiverName,ReceiverPhoneNo,ReceiverAddress,'',ReceiverCity,ReceiverState,OrderDate, getdate()  
# MAGIC From silver.Orders
# MAGIC Where OrderDate >(Select WaterMarkValue from silver.Watermark where TableName = 'silver.Orders')
# MAGIC and BillToReceiver = 'Y'

# COMMAND ----------

# MAGIC %sql
# MAGIC Update silver.Watermark 
# MAGIC Set WaterMarkValue = (Select Max(DataRealisationDate) from silver.temp_Customers)
# MAGIC Where TableName = 'silver.temp_Customers'

# COMMAND ----------

# MAGIC %sql
# MAGIC Insert Into silver.temp_persons(PersonName, DataRealisationDate, Ingestion_Date)
# MAGIC Select Distinct GeneratedBy,InvoiceDate, getdate()  
# MAGIC From silver.Invoices
# MAGIC Where InvoiceDate >(Select WaterMarkValue from silver.Watermark where TableName = 'silver.Invoices')
# MAGIC
# MAGIC union
# MAGIC
# MAGIC Select Distinct GeneratedBy,TransactionDate, getdate()  
# MAGIC From silver.TransactionLineItems
# MAGIC Where TransactionDate >(Select WaterMarkValue from silver.Watermark where TableName = 'silver.TransactionLineItems')

# COMMAND ----------

# MAGIC %sql
# MAGIC Update silver.Watermark 
# MAGIC Set WaterMarkValue = (Select Max(DataRealisationDate) from silver.temp_persons)
# MAGIC Where TableName = 'silver.temp_Persons'

# COMMAND ----------

# MAGIC %sql
# MAGIC Insert Into silver.ShipmentDetails( ShipmentNo, OrderNo, ShipmentDate, ShipmentCost, VehicleNo, CarrierName, CarrierPhoneNo, CarrierCity, CarrierState, Ingestion_Date)
# MAGIC Select   ShipmentNo, ord, ShipmentDate, ShipmentCost, VehicleNo, CarrierName, CarrierPhoneNo, CarrierCity, CarrierState, getdate() 
# MAGIC From bronze.ShipmentDetails
# MAGIC Where ShipmentDate >(Select WaterMarkValue from silver.Watermark where TableName = 'silver.ShipmentDetails')

# COMMAND ----------

# MAGIC %sql
# MAGIC Update silver.Watermark 
# MAGIC Set WaterMarkValue = (Select Max(ShipmentDate) from silver.ShipmentDetails)
# MAGIC Where TableName = 'silver.ShipmentDetails'

# COMMAND ----------

# MAGIC %sql
# MAGIC Insert Into silver.temp_Entities(EntityName, PhoneNumber, AddressLine1, AddressLine2, City, State, DataRealisationDate, Ingestion_Date)
# MAGIC Select Distinct SenderName,SenderPhoneNo,SenderAddress,'',SenderCity,SenderState,OrderDate, getdate()  
# MAGIC From silver.Orders
# MAGIC Where OrderDate >(Select WaterMarkValue from silver.Watermark where TableName = 'silver.temp_Entities')
# MAGIC
# MAGIC union
# MAGIC
# MAGIC Select Distinct ReceiverName,ReceiverPhoneNo,ReceiverAddress,'',ReceiverCity,ReceiverState,OrderDate, getdate()  
# MAGIC From silver.Orders
# MAGIC Where OrderDate >(Select WaterMarkValue from silver.Watermark where TableName = 'silver.temp_Entities')

# COMMAND ----------

# MAGIC %sql
# MAGIC Update silver.Watermark 
# MAGIC Set WaterMarkValue = (Select Max(DataRealisationDate) from silver.temp_Entities)
# MAGIC Where TableName = 'silver.temp_Entities'

# COMMAND ----------

# MAGIC %sql
# MAGIC Insert Into silver.temp_Carriers(CarrierName, PhoneNumber, City, State, VehicleNo, DataRealisationDate, Ingestion_Date)
# MAGIC Select Distinct CarrierName, CarrierPhoneNo, CarrierCity, CarrierState, VehicleNo, ShipmentDate, getdate()  
# MAGIC From silver.ShipmentDetails
# MAGIC Where ShipmentDate >(Select WaterMarkValue from silver.Watermark where TableName = 'silver.temp_Carriers')

# COMMAND ----------

# MAGIC %sql
# MAGIC Update silver.Watermark 
# MAGIC Set WaterMarkValue = (Select Max(DataRealisationDate) from silver.temp_Carriers)
# MAGIC Where TableName = 'silver.temp_Carriers'

# COMMAND ----------

dbutils.notebook.exit("Success")
