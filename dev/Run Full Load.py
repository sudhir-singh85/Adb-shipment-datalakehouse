# Databricks notebook source
dbutils.notebook.run("/Workspace/Users/sudhir.singh@domainname/dev/bronze/bronze_setfullload",120)

# COMMAND ----------

dbutils.notebook.run("/Workspace/Users/sudhir.singh@domainname/dev/silver/silver_setfullload",120)

# COMMAND ----------

dbutils.notebook.run("/Workspace/Users/sudhir.singh@domainname/dev/gold/gold_setfullload",120)

# COMMAND ----------

dbutils.notebook.run("/Workspace/Users/sudhir.singh@domainname/dev/bronze/load_bronzelayer",120)

# COMMAND ----------

dbutils.notebook.run("/Workspace/Users/sudhir.singh@domainname/dev/silver/load_silverlayer",480)

# COMMAND ----------

dbutils.notebook.run("/Workspace/Users/sudhir.singh@domainname/dev/gold/load_goldlayer",480)

# COMMAND ----------

