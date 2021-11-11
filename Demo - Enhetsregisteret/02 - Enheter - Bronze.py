# Databricks notebook source
# MAGIC  %md
# MAGIC  #Formålet med notebooken
# MAGIC  
# MAGIC  Data hentet fra: https://data.brreg.no/enhetsregisteret/oppslag/enheter
# MAGIC  
# MAGIC 
# MAGIC  

# COMMAND ----------

import dlt

def load_enhet_to_bronze(schemaName,file):
  file_path = f"/FileStore/shared_uploads/eivind.berg@skatteetaten.no/{file}"
  dataset_name = file.split('_')[0]
  
  @dlt.table(name=f"bronze_{schemaName}_{dataset_name}")
  def bronze():          
    return (
      spark.read.option("multiline","true").json(file_path)
    )


# COMMAND ----------

fileList = ["enheter_molde.json", "underenheter_molde.json"]
schemaName = "brreg"

for file in fileList:
  load_enhet_to_bronze(schemaName,file)
    

# COMMAND ----------

# MAGIC %sql
# MAGIC /*WITH enheter_omsetning as (
# MAGIC select distinct organisasjonsnummer, cast('2016' as int) as aar, cast((rand()*10000000)*rand() as int) as omsetning from dlt.bronze_brreg_enheter
# MAGIC ),
# MAGIC enheter_omsetning_2017 as (
# MAGIC select organisasjonsnummer, aar+1 as aar, cast(omsetning*(rand()+1.5) as int) as omsetning from enheter_omsetning
# MAGIC ),
# MAGIC enheter_omsetning_2018 as (
# MAGIC select organisasjonsnummer, aar+1 as aar, cast(omsetning*(rand()+1.5) as int) as omsetning from enheter_omsetning_2017
# MAGIC ),
# MAGIC enheter_omsetning_2019 as (
# MAGIC select organisasjonsnummer, aar+1 as aar, cast(omsetning*(rand()+1.5) as int) as omsetning from enheter_omsetning_2018
# MAGIC ),
# MAGIC enheter_omsetning_2020 as (
# MAGIC select organisasjonsnummer, aar+1 as aar, cast(omsetning*(rand()+1.5) as int) as omsetning from enheter_omsetning_2019
# MAGIC ),
# MAGIC enheter_omsetning_2021 as (
# MAGIC select organisasjonsnummer, aar+1 as aar, cast(omsetning*(rand()+1.5) as int) as omsetning from enheter_omsetning_2020
# MAGIC )
# MAGIC select * from enheter_omsetning
# MAGIC union
# MAGIC select * from enheter_omsetning_2017
# MAGIC union
# MAGIC select * from enheter_omsetning_2018
# MAGIC union
# MAGIC select * from enheter_omsetning_2019
# MAGIC union
# MAGIC select * from enheter_omsetning_2020
# MAGIC union
# MAGIC select * from enheter_omsetning_2021
# MAGIC order by organisasjonsnummer asc;
# MAGIC */

# COMMAND ----------

# MAGIC %sql
# MAGIC /*WITH enheter_omsetning as (
# MAGIC select distinct organisasjonsnummer, cast('2015' as int) as aar, cast((rand()*1000000)*rand() as int) as omsetning from dlt.bronze_brreg_enheter
# MAGIC )
# MAGIC select * from enheter_omsetning;*/

# COMMAND ----------

# MAGIC %sql
# MAGIC /*
# MAGIC select * From dlt.bronze_enheter_aar_omsetning
# MAGIC where aar = 2015;
# MAGIC */
