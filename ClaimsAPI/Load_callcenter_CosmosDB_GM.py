# Databricks notebook source
# MAGIC %md 
# MAGIC
# MAGIC #### ![piclogo](files/images/protective_logo.png)
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Claims CosmosDB
# MAGIC This notebook is to load claims data to cosmos DB

# COMMAND ----------

# DBTITLE 1,Define data types and structure
# MAGIC %python
# MAGIC from pyspark.sql.types import StructType,StructField, StringType, IntegerType, LongType,ArrayType #,DateType,TimestampType,BooleanType,DecimalType
# MAGIC from pyspark.sql.functions import input_file_name,col,concat,concat_ws,lit,split,reverse,explode,explode_outer,udf,substring,locate,length,when,md5
# MAGIC from datetime import datetime,timedelta
# MAGIC import pyspark.sql.functions as F

# COMMAND ----------

# DBTITLE 1,Create Database
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

# DBTITLE 1,The below script can be used in case we need to create change capture views
#create_view_script = """CREATE OR REPLACE TEMPORARY VIEW v_claimant
#    AS SELECT * FROM claims.claimant
#where dwhupdateddatetime >= '{}' """.format(lastSuccessfulLoadTime)
#spark.sql(create_view_script)

# COMMAND ----------

# DBTITLE 1,Below script used to load data for INT environment
# # ---credentials for INT NEW env---
#cosmosEndpoint = dbutils.secrets.get(scope="databricks-secret-scope",key="pic-mdp-int-coresql-cosmosdb-uri")
#cosmosMasterKey = dbutils.secrets.get(scope="databricks-secret-scope",key="pic-mdp-int-coresql-cosmosdb-rw-key")

#cosmosDatabaseName = "claimsdb_int"
#cosmosContainerName = "callcenter"

# COMMAND ----------

# DBTITLE 1,Below script used to load data for DEV environment
# # ---credentials for DEV NEW env---
#cosmosEndpoint = dbutils.secrets.get(scope="databricks-secret-scope",key="pic-mdp-dev-coresql-cosmosdb-uri")
#cosmosMasterKey = dbutils.secrets.get(scope="databricks-secret-scope",key="pic-mdp-dev-coresql-cosmosdb-rw-key")

#cosmosDatabaseName = "claimsdb_dev"
#cosmosContainerName = "callcenter_claimant"

# COMMAND ----------

# DBTITLE 1,Below script used to load data for QA environment
# # ---credentials for QA NEW env---
cosmosEndpoint = dbutils.secrets.get(scope="databricks-secret-scope",key="pic-mdp-qa-coresql-cosmosdb-uri")
cosmosMasterKey = dbutils.secrets.get(scope="databricks-secret-scope",key="pic-mdp-qa-coresql-cosmosdb-rw-key")

cosmosDatabaseName = "claimsdb_qa"
cosmosContainerName = "callcenter"

# COMMAND ----------

# old write config
# #writeConfig = {
#  "Endpoint": cosmosUri,
#  "Masterkey": cosmosKey,
#  "Database": cosmosDatabase,
#  "Collection": cosmosCollection,
#  "writingBatchSize":"50000",
#  "Upsert": "true"
# }

# COMMAND ----------

# DBTITLE 1,Azure Cosmos DB account credentials
writeConfig = {
  "spark.cosmos.accountEndpoint" : cosmosEndpoint,
  "spark.cosmos.accountKey" : cosmosMasterKey,
  "spark.cosmos.database" : cosmosDatabaseName,
  "spark.cosmos.container" : cosmosContainerName,
  "spark.cosmos.write.strategy" : "ItemOverwrite",
  "spark.cosmos.serialization.inclusionMode" : "Always"
}


# COMMAND ----------

#schema = "`date` STRING, `delay` INT, `distance` INT, `origin` STRING, `destination` STRING,`destination2` BOOLEAN"

# COMMAND ----------

schema = "`id` STRING,`claimNumber` STRING, `claimNumber_Trunc` INT, `CheckIndexNumber` INT, `claimPaymentID` INT, `approved` BOOLEAN, `billedAmount` DOUBLE, `checkAmount` DOUBLE, `checkDate` TIMESTAMP, `claimantLast4` STRING, `paymentMethod` STRING, `claimPrefix` STRING, `lossDate` TIMESTAMP, `checkNumber` INT, `payeeName` STRING, `ServiceDate` STRING, `claimStatus` STRING, `phoneNumber` STRING, `checkDescription` STRING, `multipleClaims` STRING, `paidAmount` DOUBLE, `sourceSystem` STRING, `voidedPaymentFlag` STRING"

# COMMAND ----------

#The below are the dataframes created using manual data exports(csv), we will create these temporary views using databricks tables once the tables are avaialable.
#sparkDF = spark.read.format("csv")\
 #              .option("header", "true")\
 #              .option("sep", ",")\
  #             .option("inferSchema", "false")\
 #              .schema(schema)\
 #              .load("/mnt/rawzone/ClaimsTemp/call_center.csv")
#sparkDF.createOrReplaceTempView("tmp_callcenter")

# COMMAND ----------

# DBTITLE 1,Read data in csv format and create temp view/table
sparkDF = spark.read.format("csv")\
               .option("header", "true")\
               .option("sep", "{")\
              .option("inferSchema", "false")\
               .option("quote","")\
                .schema(schema)\
               .load("/mnt/rawzone/ClaimsTemp/call_center.csv")
sparkDF.createOrReplaceTempView("tmp_callcenter")

# COMMAND ----------

#The below are the dataframes created using manual data exports(csv), we will create these temporary views using databricks tables once the tables are avaialable.
#sparkDF = spark.read.format("csv")\
#              .option("header", "true")\
#               .option("sep", ",")\
#              .option("inferSchema", "true")\
#              .option("quote","")\
#              .load("/mnt/rawzone/ClaimsTemp/call_center.csv")
#sparkDF.createOrReplaceTempView("tmp_callcenter")

# COMMAND ----------

dataTypeSeries = sparkDF.dtypes
print(dataTypeSeries)

# COMMAND ----------

# DBTITLE 1,Create Temp view
# MAGIC %sql
# MAGIC create or replace temporary view service_date as (select 
# MAGIC id,
# MAGIC claimNumber,
# MAGIC claimNumber_Trunc,
# MAGIC CheckIndexNumber,
# MAGIC claimPaymentID,
# MAGIC approved,
# MAGIC billedAmount,
# MAGIC checkAmount,
# MAGIC checkDate,
# MAGIC trim(claimantLast4) as claimantLast4,
# MAGIC case when paymentMethod is null then '' else paymentMethod END as paymentMethod,
# MAGIC claimPrefix,
# MAGIC lossDate,
# MAGIC checkNumber,
# MAGIC case when payeeName is null then '' else payeeName END as payeeName,
# MAGIC case when ServiceDate='1900-01-01 00:00:00.0000000' then 
# MAGIC to_timestamp(cast(concat(cast(trim(replace(substring(checkDescription,position('/',checkDescription)-2,2),'/',' ')) as INT),'/',
# MAGIC cast(case when position('/',checkDescription,position('/',checkDescription)+1) - position('/',checkDescription) = 3
# MAGIC   then substring(checkDescription,position('/',checkDescription)+1,2)
# MAGIC   when position('/',checkDescription,position('/',checkDescription)+1) - position('/',checkDescription) <> 3
# MAGIC   then substring(checkDescription,position('/',checkDescription)+1,1)
# MAGIC   end as INT),'/',
# MAGIC     case when position(' ',(substring(checkDescription,position('/',checkDescription,position('/',checkDescription)+1)+1,4))) > 0 
# MAGIC   and cast(substring(checkDescription,position('/',checkDescription,position('/',checkDescription)+1)+1,2) as INT) > cast(substring(year(current_date),3,2) as int)
# MAGIC   then substring(lpad(substring(checkDescription,position('/',checkDescription,position('/',checkDescription)+1)+1,6),8,'19'),1,4)
# MAGIC       when position(' ',(substring(checkDescription,position('/',checkDescription,position('/',checkDescription)+1)+1,4))) > 0 
# MAGIC   and cast(substring(checkDescription,position('/',checkDescription,position('/',checkDescription)+1)+1,2) as INT) <= cast(substring(year(current_date),3,2) as int)
# MAGIC   then substring(lpad(substring(checkDescription,position('/',checkDescription,position('/',checkDescription)+1)+1,7),4,'20'),1,3)
# MAGIC   --+1,6),8,'20'),1,4)
# MAGIC   else
# MAGIC   substring(checkDescription,position('/',checkDescription,position('/',checkDescription)+1)+1,4) end) as STRING),'M/d/yyyy')
# MAGIC   ELSE ServiceDate END
# MAGIC   as ServiceDate,
# MAGIC --ServiceDate as serviceDateOriginal,
# MAGIC claimStatus,
# MAGIC case when phoneNumber is null then '' else phoneNumber END as phoneNumber,
# MAGIC checkDescription,
# MAGIC multipleClaims,
# MAGIC paidAmount,
# MAGIC sourceSystem,
# MAGIC voidedPaymentFlag
# MAGIC from tmp_callcenter
# MAGIC where claimNumber is not null)
# MAGIC --where claimNumber='608772' )
# MAGIC --M/d/yyyy

# COMMAND ----------

spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")

# COMMAND ----------

##converts id into a hash
#sparkDF_vehicle = sparkDF_vehicle.withColumn("id",md5(sparkDF_vehicle["id"]))
#sparkDF_vehicle = sparkDF_vehicle.withColumn("claimantId",md5(sparkDF_vehicle["claimantId"]))

# COMMAND ----------

### callcenter Data ###

sparkDF_callcenter =  spark.sql("""SELECT
id,
CAST(callcenter.claimNumber_Trunc as string) as partitionKey,
'Payment' as docType,
callcenter.claimPaymentID as claimPaymentID,
CAST(callcenter.claimNumber as string) as claimNumber,
Cast(callcenter.claimNumber_Trunc as string) as claimNumberTrunc,
CAST(CASE WHEN callcenter.approved='TRUE' THEN 1 ELSE 0 END AS BOOLEAN) as isApproved,
callcenter.billedAmount as billedAmount,
callcenter.checkAmount as checkAmount,
CASE WHEN callcenter.checkDate  IS NULL THEN "1900-01-01" ELSE STRING(callcenter.checkDate) END as checkDate,
callcenter.claimantLast4,
callcenter.paymentMethod as paymentMethod,
callcenter.claimPrefix as claimPrefix,
CASE WHEN callcenter.lossDate IS NULL THEN "1900-01-01" ELSE STRING(callcenter.lossDate) END as lossDate,
CAST(callcenter.checkNumber AS STRING) as checkNumber,
callcenter.payeeName as payeeName,
CASE WHEN callcenter.ServiceDate IS NULL THEN "1900-01-01" ELSE STRING(callcenter.ServiceDate) END as serviceDate,
callcenter.claimStatus as claimStatus,
callcenter.phoneNumber as phoneNumber,
callcenter.checkDescription as checkDescription,
CAST(CASE WHEN callcenter.multipleClaims='TRUE' THEN 1 ELSE 0 END AS BOOLEAN)  as isMultipleClaims,
callcenter.paidAmount as paidAmount,
CASE WHEN callcenter.sourceSystem IS NULL THEN '' ELSE STRING(callcenter.sourceSystem) END as sourceSystem,
CAST(CASE WHEN callcenter.voidedPaymentFlag='TRUE' THEN 1 ELSE 0 END AS BOOLEAN)  as isVoidedPaymentFlag
from service_date callcenter """
             
                    )
sparkDF_callcenter.createOrReplaceTempView("tmp_finaldf")

# COMMAND ----------

#%sql
#select * from service_date callcenter WHERE claimNumber = '9811775'

# COMMAND ----------

# %sql select count(id) from tmp_finaldf--5197855

# COMMAND ----------

##converts id into a hash
sparkDF_callcenter = sparkDF_callcenter.withColumn("id",md5(sparkDF_callcenter["id"]))

# COMMAND ----------

# Check duplicates for Loss Financial's Id's
#if sparkDF_callcenter.count() > sparkDF_callcenter.dropDuplicates(["id"]).count():
 # dbutils.notebook.exit("Data Frame has duplicates")
#else:
#  print("No duplicates")

# COMMAND ----------

display(sparkDF_callcenter)

# COMMAND ----------

sparkDF_callcenter.cache()
sparkDF_callcenter.count()

# COMMAND ----------

# Load callCenter Data
# (sparkDF_callcenter.na.fill("null").write
#   .mode("overwrite")
#   .format("com.microsoft.azure.cosmosdb.spark")
#   .options(**writeConfig)
#   .save())

# COMMAND ----------

# Load callCenter Data
(sparkDF_callcenter.na.fill("null").write
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
