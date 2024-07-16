# Databricks notebook source
dbutils.notebook.run("load_dim_Address",60)

# COMMAND ----------

dbutils.notebook.run("load_dim_Person",60)

# COMMAND ----------

dbutils.notebook.run("load_dim_Customer",60)

# COMMAND ----------

dbutils.notebook.run("load_fact_Invoices",60)