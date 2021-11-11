# Databricks notebook source
# MAGIC %md
# MAGIC ## Formål med notebooken
# MAGIC * Vise expectations skrevet i python

# COMMAND ----------


expectList = {"valid_naeringskode": "naeringskode1.kode is not null","valid_orgnr": "organisasjonsnummer IS NOT NULL"}

# COMMAND ----------

import dlt
@dlt.table(name="silver_brreg_underenheter",
       comment="Enhet med datakvalitetssjekker",
       table_properties={"quality":"silver"})
@dlt.expect_all(expectList)
def silver_underenheter():
  qsilver = spark.sql("""
   SELECT 
      *
  FROM live.bronze_brreg_underenheter""")
  return (qsilver)         


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dlt.bronze_brreg_underenheter;
