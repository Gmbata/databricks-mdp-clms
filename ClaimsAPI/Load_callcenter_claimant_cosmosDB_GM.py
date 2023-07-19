# Databricks notebook source
# MAGIC %md
# MAGIC #### ![piclogo](files/images/protective_logo.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Claims CosmosDB
# MAGIC This notebook is to load claims data to cosmos DB

# COMMAND ----------

# MAGIC %python
# MAGIC from pyspark.sql.types import StructType,StructField, StringType, IntegerType, LongType,ArrayType #,DateType,TimestampType,BooleanType,DecimalType
# MAGIC from pyspark.sql.functions import input_file_name,col,concat,concat_ws,lit,split,reverse,explode,explode_outer,udf,substring,locate,length,when,md5
# MAGIC from datetime import datetime,timedelta
# MAGIC import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE DATABASE IF NOT EXISTS audit;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS audit.TableBatchControl
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/mart/audit/TableBatchControl'
# MAGIC as 
# MAGIC select 'ClaimsAPI' as SubjectArea,
# MAGIC 'ClaimsAPI' as ObjectName,
# MAGIC 'NA' as DependsOn,
# MAGIC cast('1900-01-01' as timestamp) as LastRefreshedDateTime,
# MAGIC 1 as IsNew;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select max(LastRefreshedDateTime) as LastRefreshedDateTime from audit.TableBatchControl where SubjectArea = 'ClaimsAPI'

# COMMAND ----------

startTime = spark.sql("select current_timestamp()").collect()[0][0]
lastSuccessfulLoadTime = spark.sql(""" select max(LastRefreshedDateTime) as LastRefreshedDateTime from audit.TableBatchControl where SubjectArea = 'ClaimsAPI' """).collect()[0][0]

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from audit.TableBatchControl

# COMMAND ----------

# The below script can be used in case we need to create change capture views
#create_view_script = """CREATE OR REPLACE TEMPORARY VIEW v_claimant
#    AS SELECT * FROM claims.claimant
#where dwhupdateddatetime >= '{}' """.format(lastSuccessfulLoadTime)
#spark.sql(create_view_script)

# COMMAND ----------

# # ---credentials for INT NEW env---
#cosmosEndpoint = dbutils.secrets.get(scope="databricks-secret-scope",key="pic-mdp-int-coresql-cosmosdb-uri")
#cosmosMasterKey = dbutils.secrets.get(scope="databricks-secret-scope",key="pic-mdp-int-coresql-cosmosdb-rw-key")

#cosmosDatabaseName = "claimsdb_int"
#cosmosContainerName = "callcenter_gm"


# COMMAND ----------

# # ---credentials for DEV NEW env---
cosmosEndpoint = dbutils.secrets.get(scope="databricks-secret-scope",key="pic-mdp-dev-coresql-cosmosdb-uri")
cosmosMasterKey = dbutils.secrets.get(scope="databricks-secret-scope",key="pic-mdp-dev-coresql-cosmosdb-rw-key")

cosmosDatabaseName = "claimsdb_dev"
cosmosContainerName = "callcenter_claimant"


# COMMAND ----------

# # ---credentials for QA NEW env---
#cosmosEndpoint = dbutils.secrets.get(scope="databricks-secret-scope",key="pic-mdp-qa-coresql-cosmosdb-uri")
#cosmosMasterKey = dbutils.secrets.get(scope="databricks-secret-scope",key="pic-mdp-qa-coresql-cosmosdb-rw-key")

#cosmosDatabaseName = "claimsdb_qa"
#cosmosContainerName = "callcenter_gm"

# COMMAND ----------

#write config
# writeConfig = {
  # "Endpoint": cosmosUri,
  # "Masterkey": cosmosKey,
  # "Database": cosmosDatabase,
  # "Collection": cosmosCollection,
  # "writingBatchSize":"10000",
  # "Upsert": "true"
# }

# COMMAND ----------

writeConfig = {
  "spark.cosmos.accountEndpoint" : cosmosEndpoint,
  "spark.cosmos.accountKey" : cosmosMasterKey,
  "spark.cosmos.database" : cosmosDatabaseName,
  "spark.cosmos.container" : cosmosContainerName,
  "spark.cosmos.write.strategy" : "ItemOverwrite",
  "spark.cosmos.serialization.inclusionMode" : "NonNull"
}

# COMMAND ----------

sparkDF = spark.read.format("csv")\
               .option("header", "true")\
               .option("sep", "{")\
               .option("inferSchema", "true")\
               .option("quote","")\
               .load("/mnt/rawzone/ClaimsTemp/claimant_callcenter.csv")
sparkDF.createOrReplaceTempView("tmp_claimant_callcenter")

# COMMAND ----------

display(sparkDF)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tmp_claimant_callcenter where claimNumber='IL-00048745'

# COMMAND ----------

sparkDF_claimant_callcenter =  spark.sql("""
  select distinct 
    claimant_callcenter.claimNumber as id,
  claimant_callcenter.claimNumber as partitionKey,
  'Claimant' as docType,
 claimant_callcenter.claimNumber as claimNumber,
CASE WHEN claimant_callcenter.lossDate IS NULL THEN "1900-01-01" ELSE STRING(claimant_callcenter.lossDate) END as lossDate,
 --claimant_callcenter.lossDate,
 claimant_callcenter.insuranceLineID,
claimant_callcenter.insuranceLineCode,
CASE WHEN claimant_callcenter.claimEntryDate  IS NULL THEN "1900-01-01" ELSE STRING(claimant_callcenter.claimEntryDate) END as claimEntryDate,
 tmp_claimant.claimants

FROM
  tmp_claimant_callcenter claimant_callcenter
  inner join (select
      claimant_callcenter.claimnumber,
      collect_list(
        struct(
                  CASE WHEN claimant_callcenter.claimantID IS NULL THEN "null" ELSE  claimant_callcenter.claimantID END as claimantID,
                  CASE WHEN claimant_callcenter.firstName IS NULL THEN "null" ELSE  claimant_callcenter.firstName END as firstName,
                  CASE WHEN claimant_callcenter.lastName IS NULL THEN "null" ELSE  claimant_callcenter.lastName END as lastName,
                  CASE WHEN claimant_callcenter.companyName IS NULL THEN "null" ELSE  claimant_callcenter.companyName END as companyName,
                  CASE WHEN claimant_callcenter.address1 IS NULL THEN "null" ELSE  claimant_callcenter.address1 END as address1,
                  CASE WHEN claimant_callcenter.address2 IS NULL THEN "null" ELSE claimant_callcenter.address2 END as address2,
                  CASE WHEN claimant_callcenter.city IS NULL THEN "null" ELSE claimant_callcenter.city END as city,
                  CASE WHEN claimant_callcenter.state IS NULL THEN "null" ELSE trim(claimant_callcenter.state) END as state,
                  CASE WHEN claimant_callcenter.zipCode IS NULL THEN "null" ELSE trim(claimant_callcenter.zipCode) END as zipcode,
                  CAST(CASE WHEN claimant_callcenter.isInsured = 'Y' THEN 1 WHEN  claimant_callcenter.isInsured = 'N' THEN 0 END AS BOOLEAN) as isInsured
                 )
              ) claimants
     FROM
  tmp_claimant_callcenter claimant_callcenter
    group by
      claimant_callcenter.claimNumber) tmp_claimant on (tmp_claimant.claimNumber = claimant_callcenter.claimNumber)
  """
                   
                    )
sparkDF_claimant_callcenter.createOrReplaceTempView("final_claimant_callcenter")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from final_claimant_callcenter where claimNumber='9756399'

# COMMAND ----------

display(sparkDF_claimant_callcenter)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(distinct id) from final_claimant_callcenter--184978

# COMMAND ----------

# Load callCenter Data
(sparkDF_claimant_callcenter.na.fill("null").write
 .mode("append")
  .format("cosmos.oltp")
  .options(**writeConfig)
  .save())

# COMMAND ----------

#3.54 90000 batch size  -100k ru
#3.81     50000 batch size -100k ru
#5.87     50000 batch size -50k ru
#6.01     50000 batch size -25k ru

# COMMAND ----------

insert_script = """insert into audit.tablebatchcontrol
select 'ClaimsAPI' as SubjectArea, 'ClaimsAPI' as ObjectName, 'NA' as DependsOn, cast('{}' as timestamp), 1 as IsNew from audit.tablebatchcontrol""".format(startTime)

spark.sql(insert_script)

#Include this Table optimize and vaccum in a weekly maintenance jobs 

#spark.sql("OPTIMIZE audit.tablebatchcontrol ZORDER BY LastRefreshedDateTime") 


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from audit.tablebatchcontrol

# COMMAND ----------


