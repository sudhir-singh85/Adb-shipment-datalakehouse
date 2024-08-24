# Databricks notebook source
# MAGIC  %sql
# MAGIC Insert Into gold.fact_Invoices(
# MAGIC InvoiceNo
# MAGIC       ,InvoiceDateKey
# MAGIC       ,OrderNo
# MAGIC       ,InvoiceAmount
# MAGIC       ,TaxAmount
# MAGIC       ,TaxRate
# MAGIC       ,IsPaid
# MAGIC       ,CustomerID
# MAGIC       ,CreatedByID
# MAGIC       ,Ingestion_Date
# MAGIC 	  )
# MAGIC Select Distinct
# MAGIC InvoiceNo
# MAGIC ,
# MAGIC DateKey as InvoiceDateKey,
# MAGIC inv.OrderNo,
# MAGIC InvoiceAmount,
# MAGIC TaxAmount,
# MAGIC TaxRate,
# MAGIC Case When tli.TransactionLineItemID is not null then 1 else 0 end as IsPaid,
# MAGIC cust.CustomerID,
# MAGIC PersonID as CreatedByID,
# MAGIC CURRENT_TIMESTAMP() as Ingestion_Date
# MAGIC From silver.Invoices inv
# MAGIC Inner join gold.dim_date dimdate on dimdate.Date = CAST(InvoiceDate AS DATE)
# MAGIC left join silver.transactionLineitems tli on inv.InvoiceNo = tli.ReferenceNo and PaymentType = 'InvoiceBalancePayment'
# MAGIC left join silver.Orders ord on inv.OrderNo = ord.OrderNo
# MAGIC Left join gold.dim_customer cust on 
# MAGIC Case when BillToSender = 'Y' and BillToReceiver = 'N' and BIllToOther = 'N' then SenderName
# MAGIC when BillToSender = 'N' and BillToReceiver = 'Y' and BIllToOther = 'N' then ReceiverName
# MAGIC when BillToSender = 'N' and BillToReceiver = 'N' and BIllToOther = 'Y' then BillerName else 'na' end = cust.CustomerName
# MAGIC left join gold.dim_Person Person on inv.GeneratedBy = Person.PersonName
# MAGIC Where CAST(InvoiceDate AS DATE) >(select WatermarkValue from gold.watermark where tablename = 'gold.fact_Invoices')
# MAGIC order by InvoiceNo

# COMMAND ----------

# MAGIC %sql
# MAGIC Update gold.Watermark 
# MAGIC Set WaterMarkValue = (Select Max(Date) 
# MAGIC from gold.fact_Invoices
# MAGIC Inner join gold.dim_date dimdate on dimdate.DateKey = InvoiceDateKey)
# MAGIC Where TableName = 'gold.fact_Invoices'

# COMMAND ----------

dbutils.notebook.exit("Success")
