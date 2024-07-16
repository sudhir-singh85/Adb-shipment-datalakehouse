# Databricks notebook source
  %sql
 Insert Into gold.fact_Invoices(
 InvoiceNo
       ,InvoiceDateKey
       ,OrderNo
       ,InvoiceAmount
       ,TaxAmount
       ,TaxRate
       ,IsPaid
       ,CustomerID
       ,CreatedByID
       ,Ingestion_Date
 	  )
 Select Distinct
 InvoiceNo
 ,
 DateKey as InvoiceDateKey,
 inv.OrderNo,
 InvoiceAmount,
 TaxAmount,
 TaxRate,
 Case When tli.TransactionLineItemID is not null then 1 else 0 end as IsPaid,
 cust.CustomerID,
 PersonID as CreatedByID,
 CURRENT_TIMESTAMP() as Ingestion_Date
 From silver.Invoices inv
 Inner join gold.dim_date dimdate on dimdate.Date = CAST(InvoiceDate AS DATE)
 left join silver.transactionLineitems tli on inv.InvoiceNo = tli.ReferenceNo and PaymentType = 'InvoiceBalancePayment'
 left join silver.Orders ord on inv.OrderNo = ord.OrderNo
 Left join gold.dim_customer cust on 
 Case when BillToSender = 'Y' and BillToReceiver = 'N' and BIllToOther = 'N' then SenderName
 when BillToSender = 'N' and BillToReceiver = 'Y' and BIllToOther = 'N' then ReceiverName
 when BillToSender = 'N' and BillToReceiver = 'N' and BIllToOther = 'Y' then BillerName else 'na' end = cust.CustomerName
 left join gold.dim_Person Person on inv.GeneratedBy = Person.PersonName
 Where CAST(InvoiceDate AS DATE) >(select WatermarkValue from gold.watermark where tablename = 'gold.fact_Invoices')
 order by InvoiceNo

# COMMAND ----------

 %sql
 Update gold.Watermark 
 Set WaterMarkValue = (Select Max(Date) 
 from gold.fact_Invoices
 Inner join gold.dim_date dimdate on dimdate.DateKey = InvoiceDateKey)
 Where TableName = 'gold.fact_Invoices'

# COMMAND ----------

dbutils.notebook.exit("Success")