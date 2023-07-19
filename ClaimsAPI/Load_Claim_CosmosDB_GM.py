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

# DBTITLE 1,Below script used to load data for DEV environment
#---credentials for DEV env---
#cosmosEndpoint = dbutils.secrets.get(scope="databricks-secret-scope",key="pic-mdp-dev-coresql-cosmosdb-uri")
#cosmosMasterKey = dbutils.secrets.get(scope="databricks-secret-scope",key="pic-mdp-dev-coresql-cosmosdb-rw-key")

#cosmosDatabaseName = "claimsdb_dev"
#cosmosContainerName = "claim_gm"


# COMMAND ----------

# DBTITLE 1,Below script used to load data for INT environment
# # ---credentials for INT env---
#cosmosEndpoint = dbutils.secrets.get(scope="databricks-secret-scope",key="pic-mdp-int-coresql-cosmosdb-uri")
#cosmosMasterKey = dbutils.secrets.get(scope="databricks-secret-scope",key="pic-mdp-int-coresql-cosmosdb-rw-key")

#cosmosDatabaseName = "claimsdb_int"
#cosmosContainerName = "claim"

# COMMAND ----------

# DBTITLE 1,Below script used to load data for QA environment
#---credentials for QA env---
cosmosEndpoint = dbutils.secrets.get(scope="databricks-secret-scope",key="pic-mdp-qa-coresql-cosmosdb-uri")
cosmosMasterKey = dbutils.secrets.get(scope="databricks-secret-scope",key="pic-mdp-qa-coresql-cosmosdb-rw-key")

cosmosDatabaseName = "claimsdb_qa"
cosmosContainerName = "claim_gm"


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

# ---credentials for dailyfact container---
cosmosEndpoint = dbutils.secrets.get(scope="databricks-secret-scope",key="pic-mdp-qa-coresql-cosmosdb-uri")
cosmosMasterKey = dbutils.secrets.get(scope="databricks-secret-scope",key="pic-mdp-qa-coresql-cosmosdb-rw-key")
cosmosDatabaseName = "dailyfact"

#write config for dailyfact container
writeConfigDailyfact = {
  "spark.cosmos.accountEndpoint" : cosmosEndpoint,
  "spark.cosmos.accountKey" : cosmosMasterKey,
  "spark.cosmos.database" : cosmosDatabaseName,
  "spark.cosmos.container" : cosmosContainerName,
  "spark.cosmos.write.strategy" : "ItemOverwrite",
  "spark.cosmos.serialization.inclusionMode" : "Always"
}


# COMMAND ----------

# DBTITLE 1,Read data in csv format and create temp view/table
sparkDF = spark.read.format("csv")\
               .option("header", "true")\
               .option("sep", "{")\
               .option("inferSchema", "true")\
               .option("quote","")\
               .load("/mnt/rawzone/ClaimsTemp/ClaimsCurly.csv")
sparkDF.createOrReplaceTempView("tmp_Claims")

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from tmp_Claims

# COMMAND ----------

# DBTITLE 1,Read csv file into dataframe  -Added 6/5/2023 -GM
#df=spark.read.csv("/mnt/rawzone/ClaimsTemp/ClaimsCurly.csv", header=True)

# COMMAND ----------

# DBTITLE 1,Convert csv file to parquet format
#df.write.parquet("ClaimsCurly.parquet")

# COMMAND ----------

# DBTITLE 1,Read data in csv format and create temp view/table
sparkDF = spark.read.format("csv")\
               .option("header", "true")\
              .option("sep", ",")\
               .option("inferSchema", "true")\
               .load("/mnt/rawzone/ClaimsTemp/MaxLastUpdateDate.csv")
sparkDF.createOrReplaceTempView("tmp_LastUpdateDate")

# COMMAND ----------

sparkDF = spark.read.format("csv")\
               .option("header", "true")\
                .option("sep", ",")\
               .option("inferSchema", "true")\
               .load("/mnt/rawzone/ClaimsTemp/entityID.csv")
sparkDF.createOrReplaceTempView("tmp_entityID")

# COMMAND ----------

sparkDF = spark.read.format("csv")\
               .option("header", "true")\
                .option("sep", ",")\
               .option("inferSchema", "true")\
               .load("/mnt/rawzone/ClaimsTemp/Vehicles.csv")
sparkDF.createOrReplaceTempView("tmp_Vehicles")

# COMMAND ----------

sparkDF = spark.read.format("csv")\
               .option("header", "true")\
               .option("sep", ",")\
               .option("inferSchema", "true")\
               .load("/mnt/rawzone/ClaimsTemp/Adjuster.csv")
sparkDF.createOrReplaceTempView("tmp_Adjuster")

# COMMAND ----------

##import org.apache.spark.sql.types._
#import org.apache.spark.sql.types.{IntegerType,StringType,StructType,StructField}
#import org.apache.spark.sql.{Row, SparkSession}
###07/9/2021 changed inferschema from true to false. this then picked up decimals point when loading csv files
#.schema(customSchema)\

sparkDF = spark.read.format("csv")\
               .option("header", "true")\
                .option("sep", ",")\
               .option("inferSchema", "true")\
                .load("/mnt/rawzone/ClaimsTemp/Loss_Financials.csv")
sparkDF.createOrReplaceTempView("tmp_Lossfinancials")
#sparkDF.createOrReplaceView("tmp_Lossfinancials")

# COMMAND ----------

sparkDF = spark.read.format("csv")\
               .option("header", "true")\
                .option("sep", ",")\
               .option("inferSchema", "true")\
               .load("/mnt/rawzone/ClaimsTemp/deductible.csv")
sparkDF.createOrReplaceTempView("tmp_deductible")

# COMMAND ----------

##test
#dataTypeSeries = sparkDF.dtypes
#print(dataTypeSeries)
#sparkDF.show()
#sparkDF.printSchema()

# COMMAND ----------

sparkDF = spark.read.format("csv")\
               .option("header", "true")\
                .option("sep", ",")\
               .option("inferSchema", "true")\
               .load("/mnt/rawzone/ClaimsTemp/Financials.csv")
sparkDF.createOrReplaceTempView("tmp_financials")

# COMMAND ----------

sparkDF = spark.read.format("csv")\
               .option("header", "true")\
                .option("sep", ",")\
               .option("inferSchema", "true")\
               .load("/mnt/rawzone/ClaimsTemp/Customer.csv")
sparkDF.createOrReplaceTempView("tmp_Customer")

# COMMAND ----------

sparkDF = spark.read.format("csv")\
               .option("header", "true")\
                .option("sep", ",")\
               .option("inferSchema", "true")\
               .load("/mnt/rawzone/ClaimsTemp/Dailyfacts.csv")
sparkDF.createOrReplaceTempView("tmp_dailyfacts")

# COMMAND ----------

### Dailyfacts Data ###

sparkDF_dailyfacts =  spark.sql("""
SELECT distinct 
  'DailyFact' as partitionKey,
    'DailyFact' as docType,
    dailyfacts.id as id,
    nvl(Customer.customer, 'NA') as customer,
    CASE WHEN dailyfacts.initialCloseDate IS NULL THEN "1900-01-01" ELSE dailyfacts.initialCloseDate END AS initialCloseDate,
    CASE WHEN dailyfacts.lastRecloseDate IS NULL THEN "1900-01-01" ELSE dailyfacts.lastRecloseDate END AS lastRecloseDate,
    CASE WHEN dailyfacts.initialOpenDate IS NULL THEN "1900-01-01" ELSE dailyfacts.initialOpenDate END AS initialOpenDate,
    CAST(CASE WHEN dailyfacts.coverageDailyId IS NULL THEN "0" ELSE dailyfacts.coverageDailyId END AS INT) AS coverageDailyId,
    CASE WHEN dailyfacts.lastReopenDate IS NULL THEN "1900-01-01" ELSE dailyfacts.lastReopenDate END AS lastReopenDate,
    CASE WHEN dailyfacts.firstReopenDate IS NULL THEN "1900-01-01" ELSE dailyfacts.firstReopenDate END AS firstReopenDate,
    CAST(CASE WHEN dailyfacts.isCwp = 'Y' THEN 1 WHEN dailyfacts.isCwp = 'False' THEN 0 END AS BOOLEAN) AS isCwp,
    CASE WHEN dailyfacts.coverageCode IS NULL THEN "null" ELSE dailyfacts.coverageCode END AS coverageCode,
    CAST(CASE WHEN dailyfacts.claimantNumber IS NULL THEN "0" ELSE dailyfacts.claimantNumber END AS INT) AS claimantNumber,
    CASE WHEN dailyfacts.claimNumber IS NULL THEN "N/A" ELSE dailyfacts.claimNumber END AS claimNumber,
    CAST(CASE WHEN dailyfacts.systemId IS NULL THEN "0" ELSE dailyfacts.systemId END AS INT) AS systemId,
    CASE WHEN dailyfacts.feature IS NULL THEN "null" ELSE dailyfacts.feature  END AS feature,
    CASE WHEN dailyfacts.systemDescription IS NULL THEN "null" ELSE dailyfacts.systemDescription END AS systemDescription,
    CASE WHEN dailyfacts.snapshotDate IS NULL THEN "1900-01-01" ELSE dailyfacts.snapshotDate END AS snapshotDate,
    CASE WHEN dailyfacts.featureStatus IS NULL THEN "null" ELSE dailyfacts.featureStatus END AS featureStatus
FROM
  tmp_dailyfacts dailyfacts
  left join tmp_Customer Customer on (dailyfacts.claimNumber = Customer.claimNumber)
  LEFT JOIN audit.TableBatchControl ON SubjectArea = 'ClaimsAPI'

WHERE dailyfacts.snapshotDate > LastRefreshedDateTime

  """)               


# COMMAND ----------

##converts id into a hash
sparkDF_dailyfacts = sparkDF_dailyfacts.withColumn("id",md5(sparkDF_dailyfacts["id"]))

# COMMAND ----------

sparkDF = spark.read.format("csv")\
               .option("header", "true")\
                .option("sep", ",")\
               .option("inferSchema", "true")\
               .load("/mnt/rawzone/ClaimsTemp/ClaimExaminer.csv")
sparkDF.createOrReplaceTempView("tmp_claimexaminer")

# COMMAND ----------

### ClaimExaminer Data ###

sparkDF_claimexaminer =  spark.sql("""
SELECT DISTINCT
  'ClaimExaminer' as partitionKey,
    'ClaimExaminer' as docType,
    claimexaminer.id as id,
    nvl(Customer.customer, 'NA') as customer,
    TRIM(claimexaminer.NewExaminerCode) AS newExaminerCode,
    TRIM(claimexaminer.OldExaminerCode) AS oldExaminerCode,
  CASE WHEN claimexaminer.TransferDateTime IS NULL THEN "1900-01-01 00:00:00" ELSE claimexaminer.TransferDateTime END AS transferDateTime,
  CAST(CASE WHEN claimexaminer.isTransferred = 'Y' THEN 1 WHEN (claimexaminer.isTransferred='False' OR claimexaminer.isTransferred='N') THEN 0 END AS BOOLEAN) AS isTransferred,
  CASE WHEN claimexaminer.InitialOpenDate IS NULL THEN "1900-01-01" ELSE claimexaminer.InitialOpenDate END AS initialOpenDate,
  CASE WHEN claimexaminer.ClaimNumber IS NULL THEN "N/A" ELSE claimexaminer.ClaimNumber END AS claimNumber,
  CAST(CASE WHEN claimexaminer.systemId IS NULL THEN "0" ELSE claimexaminer.systemId END AS INT) AS systemId,
  CASE WHEN claimexaminer.examinerType IS NULL THEN "null" ELSE claimexaminer.examinerType END AS examinerType,
  CASE WHEN claimexaminer.systemDescription IS NULL THEN "null" ELSE claimexaminer.systemDescription END AS systemDescription
  FROM
  tmp_claimexaminer claimexaminer
  left join tmp_Customer Customer on (claimexaminer.claimNumber = Customer.claimNumber)
  LEFT JOIN audit.TableBatchControl ON SubjectArea = 'ClaimsAPI'

WHERE claimexaminer.TransferDateTime > LastRefreshedDateTime
  """)

# COMMAND ----------

##converts id into a hash
sparkDF_claimexaminer = sparkDF_claimexaminer.withColumn("id",md5(sparkDF_claimexaminer["id"]))

# COMMAND ----------

### Vehicle Data ###

sparkDF_vehicle =  spark.sql("""
SELECT distinct 
 -- concat('Vehicle','-',nvl(Customer.customer, 'NA')) as partitionKey,
  'Vehicle' as partitionKey,
  Vehicle.id as id,
  nvl(Customer.customer, 'NA') as customer,
  Vehicle.claimNumber as claimNumber,
  'Vehicle' as docType,
  Vehicle.vehicleType as vehicleType,
  Vehicle.make as make,
  Vehicle.year as year,
  Vehicle.serialID as serialID,
  Vehicle.vin as vin,
  Vehicle.driverName as driverName,
  Vehicle.isInsured as isInsured,
  STRING(Vehicle.claimantId) as claimantId,
  --Vehicle.transactionDate as transactionDate
  --CASE WHEN ***.transactionDate IS NULL THEN '1900-01-01' ELSE ***.transactionDate END as transactionDate 

  ----Note: After New ADF Instance is set up and ClaimsAPI Vehicle pipeline ran, work on this.

FROM
  tmp_Vehicles Vehicle
  left join tmp_Customer Customer on Vehicle.claimNumber = Customer.claimNumber
  --left join tmp_LastUpdateDate tmp_LastUpdateDate on Vehicle.claimNumber = tmp_LastUpdateDate.claimNumber
  LEFT JOIN audit.TableBatchControl ON SubjectArea = 'ClaimsAPI'

  WHERE Vehicle.transactionDate > LastRefreshedDateTime

""" )

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * FROM tmp_Vehicles

# COMMAND ----------

display(sparkDF_vehicle)

# COMMAND ----------

##converts id into a hash
sparkDF_vehicle = sparkDF_vehicle.withColumn("id",md5(sparkDF_vehicle["id"]))
sparkDF_vehicle = sparkDF_vehicle.withColumn("claimantId",md5(sparkDF_vehicle["claimantId"]))

# COMMAND ----------

### Adjuster Data ###

sparkDF_adjuster =  spark.sql("""
SELECT distinct 
 -- concat('Adjuster','-',nvl(Customer.customer, 'NA')) as partitionKey,
  'Adjuster' as partitionKey,
  Adjuster.id  as id,
  nvl(Customer.customer, 'NA') as customer,
  Adjuster.claimNumber as claimNumber,
  'Adjuster' as docType,
  Adjuster.adjusterType as adjusterType,
  Adjuster.employeeLastName as lastName,
  Adjuster.employeeFirstName as firstName,
  Adjuster.phone as phone,
  Adjuster.emailAddress as emailAddress,
  CASE WHEN lastUpdatedDate.LastUpdateDate IS NULL THEN "1900-01-01" ELSE CAST(lastUpdatedDate.LastUpdateDate AS STRING) END as lastUpdatedDate

FROM
  tmp_Adjuster Adjuster
  
  LEFT JOIN tmp_Customer Customer on Adjuster.claimNumber = Customer.claimNumber 
  LEFT JOIN tmp_LastUpdateDate lastUpdatedDate ON Adjuster.claimNumber = lastUpdatedDate.claimNumber 
  LEFT JOIN audit.TableBatchControl ON SubjectArea = 'ClaimsAPI'

  WHERE lastUpdatedDate.LastUpdateDate > LastRefreshedDateTime
  """ )

# COMMAND ----------

display(sparkDF_adjuster)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tmp_Adjuster

# COMMAND ----------

sparkDF_adjuster = sparkDF_adjuster.withColumn("id",md5(sparkDF_adjuster["id"]))

# COMMAND ----------

#display(sparkDF_lossfinancial)
#sparkDF_lossfinancial2.count()


# COMMAND ----------

##original##
#sparkDF_financial =  spark.sql("""SELECT distinct
#  'Financial' as partitionKey,
# -- concat('Financial','-',nvl(Customer.customer, 'NA')) as partitionKey,
#  financials.id  as id,
#  'Financial' as docType,
#  nvl(Customer.customer, 'NA') as customer,
# financials.claimNumber as claimNumber,
 # sum(financials.AppliedSIRAmount) as appliedSirAmount,
 # Cast(sum(financials.salvageAmount) as DECIMAL(18,2)) as salvageAmount,
 # sum(financials.subrogationAmount) as subrogationAmount,
 # sum(financials.lossPaidAmount) as lossPaidAmount,
 # sum(financials.lossIncurredAmount) as lossIncurredAmount,
 # sum(financials.lossReserveAmount) as lossReserveAmount,
 # sum(financials.expenseIncurredAmount) as expenseIncurredAmount,
 # sum(financials.expensePaidAmount) as expensePaidAmount,
 # sum(financials.expensereserveAmount) as expenseReserveAmount,
 # 
 # tmp_deductibles.deductibles,
 # tmp_paymentdetail.paymentDetails
  #
#FROM
 # tmp_financials financials
  #inner join (select
   #           financials.claimNumber,
    #           collect_list(
     #                 struct(
      #                       CASE WHEN financials.coverageCode IS NULL THEN "null" ELSE financials.coverageCode END as coverageCode,
       #                      CASE WHEN financials.amount IS NULL THEN "null" ELSE financials.amount END as amount
        #                    )
         #                  ) deductibles
  #
   #             FROM
    #                 tmp_financials financials
     #                group by financials.claimNumber) tmp_deductibles
   #on (financials.claimNumber=tmp_deductibles.claimNumber)
   #left join (select
    #          lossfinancials.claimNumber,
     #          collect_list(
      #                struct(
       #                      CASE WHEN lossfinancials.lossType IS NULL THEN "null" ELSE lossfinancials.lossType END as lossType,
        #                     CASE WHEN lossfinancials.lossPaidAmount IS NULL THEN "null" ELSE lossfinancials.lossPaidAmount END as lossPaidAmount,
         #                    CASE WHEN lossfinancials.expensePaidAmount IS NULL THEN "null" ELSE lossfinancials.expensePaidAmount END as expensePaidAmount
          #                  )
           #                ) paymentDetails
  #
            #    FROM
   #                  tmp_Lossfinancials lossfinancials
    #                 group by lossfinancials.claimNumber) tmp_paymentdetail
   #on (financials.claimNumber=tmp_paymentdetail.claimNumber)
   #left join tmp_Customer Customer on (Customer.claimNumber = financials.claimNumber )
   # group by financials.id,nvl(Customer.customer, 'NA'), financials.claimNumber,tmp_deductibles.deductibles,tmp_paymentdetail.paymentDetails
   #   
  #"""
  #)

# COMMAND ----------

# DBTITLE 1,Financial data
sparkDF_financial =  spark.sql("""SELECT distinct
  'Financial' as partitionKey,
 -- concat('Financial','-',nvl(Customer.customer, 'NA')) as partitionKey,
  financials.id  as id,
  'Financial' as docType,
  nvl(Customer.customer, 'NA') as customer,
  financials.claimNumber as claimNumber,
  Cast(sum(financials.AppliedSIRAmount) as DECIMAL(18,2)) as appliedSirAmount,
  Cast(sum(financials.salvageAmount) as DECIMAL(18,2)) as salvageAmount,
  Cast(sum(financials.subrogationAmount) as DECIMAL(18,2)) as subrogationAmount,
  Cast(sum(financials.lossPaidAmount) as DECIMAL(18,2)) as lossPaidAmount,
  Cast(sum(financials.lossIncurredAmount) as DECIMAL(18,2)) as lossIncurredAmount,
  Cast(sum(financials.lossReserveAmount) as DECIMAL(18,2)) as lossReserveAmount,
  Cast(sum(financials.expenseIncurredAmount) as DECIMAL(18,2)) as expenseIncurredAmount,
  Cast(sum(financials.expensePaidAmount) as DECIMAL(18,2)) as expensePaidAmount,
  Cast(sum(financials.expensereserveAmount) as DECIMAL(18,2)) as expenseReserveAmount,
  
  tmp_deductibles.deductibles,
  tmp_paymentdetail.paymentDetails,
  tmp_financialClaim.claimSummary
  
FROM
  tmp_financials financials
  inner join (select
              tmp_deductible.claimNumber,
               collect_list(
                      struct(
                             CASE WHEN tmp_deductible.coverageCode IS NULL THEN "null" ELSE tmp_deductible.coverageCode END as coverageCode,
                             Cast(tmp_deductible.amount as DECIMAL(18,2)) as amount
                            )
                           ) deductibles
  
                FROM
                     tmp_deductible tmp_deductible
                     group by tmp_deductible.claimNumber) tmp_deductibles
   on (financials.claimNumber=tmp_deductibles.claimNumber)
   left join (select
              lossfinancials.claimNumber,
               collect_list(
                      struct(
                             CASE WHEN lossfinancials.lossType IS NULL THEN "null" ELSE lossfinancials.lossType END as lossType,
                             Cast(lossfinancials.lossPaidAmount as DECIMAL(18,2))  as lossPaidAmount,
                             Cast(lossfinancials.expensePaidAmount as DECIMAL(18,2)) as expensePaidAmount 
                            )
                           ) paymentDetails
  
                FROM
                     tmp_Lossfinancials lossfinancials
                     group by lossfinancials.claimNumber) tmp_paymentdetail
   on (financials.claimNumber=tmp_paymentdetail.claimNumber)
    left join (select 
   tmp_financialClaims.claimNumber,
    -- to_json(
       struct(
               CASE WHEN tmp_financialClaims.policyNumber IS NULL THEN "null" ELSE tmp_financialClaims.policyNumber END as policyNumber,
               CASE WHEN tmp_financialClaims.policyPrefix IS NULL THEN "null" ELSE tmp_financialClaims.policyPrefix END as policyPrefix,
               CASE WHEN tmp_financialClaims.effectiveDate IS NULL THEN "null" ELSE tmp_financialClaims.effectiveDate END as effectiveDate,
               CASE WHEN tmp_financialClaims.accountNumber IS NULL THEN "null" ELSE tmp_financialClaims.accountNumber END as accountNumber,
               CASE WHEN tmp_financialClaims.agentName IS NULL THEN "null" ELSE tmp_financialClaims.agentName END as agentName,
               CASE WHEN tmp_financialClaims.agentNumber IS NULL THEN "null" ELSE tmp_financialClaims.agentNumber END as agentNumber,
               CASE WHEN tmp_financialClaims.program IS NULL THEN "null" ELSE tmp_financialClaims.program END as program,
               CASE WHEN tmp_financialClaims.productLine IS NULL THEN "null" ELSE tmp_financialClaims.productLine END as productLine,
               CASE WHEN tmp_financialClaims.lastUpdatedDate IS NULL THEN "null" ELSE tmp_financialClaims.lastUpdatedDate END as lastUpdatedDate,
               CASE WHEN tmp_financialClaims.lossDate IS NULL THEN "null" ELSE tmp_financialClaims.lossDate END as lossDate,
               CASE WHEN tmp_financialClaims.claimStatus IS NULL THEN "null" ELSE tmp_financialClaims.claimStatus END as claimStatus,
               CASE WHEN tmp_financialClaims.openDate IS NULL THEN "null" ELSE tmp_financialClaims.openDate END as openDate,
               CASE WHEN tmp_financialClaims.closedDate IS NULL THEN "null" ELSE tmp_financialClaims.closedDate END as closedDate
              ) claimSummary
               from tmp_financials tmp_financialClaims  ) tmp_financialClaim
              -- group by tmp_financialClaims.claimNumber) tmp_financialClaim
    on (financials.claimNumber=tmp_financialClaim.claimNumber)
   left join tmp_Customer Customer on (Customer.claimNumber = financials.claimNumber)
   LEFT JOIN audit.TableBatchControl ON SubjectArea = 'ClaimsAPI'

WHERE financials.lastUpdatedDate > LastRefreshedDateTime
GROUP BY financials.id,nvl(Customer.customer, 'NA'), financials.claimNumber,tmp_deductibles.deductibles,tmp_paymentdetail.paymentDetails,tmp_financialClaim.claimSummary
  """
  )

# COMMAND ----------

display(sparkDF_financial)

# COMMAND ----------

sparkDF_financial = sparkDF_financial.withColumn("id",md5(sparkDF_financial["id"]))

# COMMAND ----------

### Claims Data ###

sparkDF_claims =  spark.sql("""
  select distinct 
  --concat('Claim','-',nvl(Customer.customer, 'NA')) as partitionKey, 
  'Claim' as partitionKey,
  Claims.id as id,
  nvl(Customer.customer, 'NA') as customer,
  Claims.claimNumber,
  'Claim' as docType,
  Claims.carrierClaimNumber,
  --added 8/1/2022
  CASE WHEN Claims.reportedDate IS NULL THEN "1900-01-01" ELSE Claims.reportedDate END as reportedDate,
  CASE WHEN Claims.claimLossTypeID IS NULL THEN "null" ELSE Claims.claimLossTypeID END as lossTypeID,
  CASE WHEN Claims.claimLossTypeWorkCompID IS NULL THEN "null" ELSE  Claims.claimLossTypeWorkCompID END as lossTypeWorkCompID,
  --end of added 8/1/2022
  CASE WHEN Claims.lossDate IS NULL THEN "1900-01-01" ELSE STRING(Claims.lossDate) END as lossDate,
  Claims.lossTime,
  CASE WHEN Claims.openDate IS NULL THEN "1900-01-01" ELSE STRING(Claims.openDate) END as openDate,
  Claims.claimStatus,
  Claims.lossDescription,
  CASE WHEN Claims.lastReopenDate IS NULL THEN "1900-01-01" ELSE STRING(Claims.lastReopenDate) END as lastReopenDate,
  CASE WHEN Claims.lastRecloseDate IS NULL THEN "1900-01-01" ELSE STRING(Claims.lastRecloseDate) END as lastRecloseDate,
  CASE WHEN Claims.closedDate IS NULL THEN "1900-01-01" ELSE STRING(Claims.closedDate) END as closedDate,
  Claims.lossCauseDescription,
  Claims.claimType,
  Claims.roadwayConditions,
  Claims.roadwayType,
  Claims.weatherCondition,
  Claims.jurisdictionState,
  Claims.lossCauseCode,
  Claims.lossCauseCodeExternal,
  CASE WHEN tmp_LastUpdateDate.LastUpdateDate IS NULL THEN "1900-01-01" ELSE STRING(tmp_LastUpdateDate.LastUpdateDate) END as lastUpdatedDate,
  -- Claims.customer,
  Claims.program,
  Claims.division,
  Claims.classcode,
  CASE WHEN Claims.awardDate IS NULL THEN "1900-01-01" ELSE STRING(Claims.awardDate) END as awardDate,
  Claims.terminalLocation,
  Claims.claimReportingFlag,
  Claims.claimReportingFlagDescription,
  Claims.injuryCauseID,
  Claims.injuryCauseDescription,
  Claims.commercialDrivingExperience,
  CASE WHEN Claims.physicalAddressLine1 IS NULL THEN "null" ELSE Claims.physicalAddressLine1 END as physicalAddressLine1,
  CASE WHEN Claims.physicalAddressLine2 IS NULL THEN "null" ELSE Claims.physicalAddressLine2 END as physicalAddressLine2,
  CASE WHEN Claims.physicalCity IS NULL THEN "null" ELSE Claims.physicalCity END as physicalCity,
  CASE WHEN Claims.physicalState IS NULL THEN "null" ELSE Claims.physicalState END as physicalState,
  CASE WHEN TRIM(Claims.physicalZipcode) IS NULL THEN "null" ELSE TRIM(Claims.physicalZipcode) END as physicalZipcode,
  CASE WHEN Claims.physicalCounty IS NULL THEN "null" ELSE Claims.physicalCounty END as physicalCounty,
  CASE WHEN Claims.physicalCountry IS NULL THEN "null" ELSE Claims.physicalCountry END as physicalCountry,
  CASE WHEN Claims.mailingAddressLine1 IS NULL THEN "null" ELSE Claims.mailingAddressLine1 END as mailingAddressLine1,
  CASE WHEN Claims.mailingAddressLine2 IS NULL THEN "null" ELSE Claims.mailingAddressLine2 END as mailingAddressLine2,
  CASE WHEN Claims.mailingCity IS NULL THEN "null" ELSE Claims.mailingCity END as mailingCity,
  CASE WHEN Claims.mailingState IS NULL THEN "null" ELSE Claims.mailingState END as mailingState,
  CASE WHEN TRIM(Claims.mailingZipcode) IS NULL THEN "null" ELSE TRIM(Claims.mailingZipcode) END as mailingZipcode,
  CASE WHEN Claims.mailingCounty IS NULL THEN "null" ELSE Claims.mailingCounty END as mailingCounty,
  CASE WHEN Claims.mailingCountry IS NULL THEN "null" ELSE Claims.mailingCountry END as mailingCountry,
  CASE WHEN TRIM(Claims.policyNumber) IS NULL THEN "null" ELSE TRIM(Claims.policyNumber) END as policyNumber,
  CASE WHEN Claims.policyPrefix IS NULL THEN "null" ELSE Claims.policyPrefix END as policyPrefix ,
  CASE WHEN Claims.policySuffix IS NULL THEN "null" ELSE Claims.policySuffix END as policySuffix,
  CASE WHEN Claims.effectiveDate IS NULL THEN "1900-01-01" ELSE  STRING(Claims.effectiveDate) END as effectiveDate,
  CASE WHEN Claims.expirationDate  IS NULL THEN "1900-01-01" ELSE STRING(Claims.expirationDate) END as expirationDate,
  STRING(Claims.underwritingCode) as underwritingCode,
  STRING(Claims.underwritingCompanyName) as underwritingCompanyName,
  INT(Claims.underwritingCompanyId) as underwritingCompanyId,
  STRING(Claims.productLine2) as productLine2,
  CASE WHEN Claims.jurisdictionCode IS NULL THEN "null" ELSE Claims.jurisdictionCode END as jurisdictionCode,
  CASE WHEN Claims.sicCode IS NULL THEN "null" ELSE Claims.sicCode END as sicCode,
  CASE WHEN Claims.naicsCode IS NULL THEN "null" ELSE Claims.naicsCode END as naicsCode,
  CASE WHEN Claims.productLine IS NULL THEN "null" ELSE Claims.productLine END as productLine,
  CASE WHEN Claims.name IS NULL THEN "null" ELSE Claims.name END AS name,
  CASE WHEN STRING(Claims.clientID) IS NULL THEN "null" ELSE STRING(Claims.clientID) END as clientID,
  --Claims.clientIDCheckForError,
  CASE WHEN tmp_entityID.entityID IS NULL THEN "null" ELSE tmp_entityID.entityID END AS entityID,
  CAST(Claims.accountID as INT) as accountID,
  CASE WHEN Claims.accountName IS NULL THEN "null" ELSE Claims.accountName END AS accountName,
  CASE WHEN Claims.accountNumber IS NULL THEN "null" ELSE Claims.accountNumber END AS accountNumber,
  CASE WHEN STRING(Claims.motorCarrier) IS NULL THEN "null" ELSE STRING(Claims.motorCarrier) END as motorCarrier,
  CASE WHEN TRIM(Claims.agentName) IS NULL THEN "null" ELSE TRIM(Claims.agentName) END AS agentName,
  CASE WHEN STRING(Claims.agentNumber) IS NULL THEN "null" ELSE STRING(Claims.agentNumber) END AS agentNumber,
  Claims.yearsEmployedWithInsured,
  tmp_lossLocation.lossLocations,
  --added 09/26/2022 - nthati
  CASE WHEN Claims.lossTypeCode IS NULL THEN "null" ELSE Claims.lossTypeCode END as lossTypeCode,
  CASE WHEN Claims.lossTypeDescription IS NULL THEN "null" ELSE Claims.lossTypeDescription END as lossTypeDescription,
  CASE WHEN Claims.equipmentTypePowerUnitType IS NULL THEN "null" ELSE Claims.equipmentTypePowerUnitType END as equipmentTypePowerUnitType,
  CASE WHEN Claims.trailerTypeAndConfiguration IS NULL THEN "null" ELSE Claims.trailerTypeAndConfiguration END as trailerTypeAndConfiguration,
  CASE WHEN Claims.accountZipCode IS NULL THEN "null" ELSE Claims.accountZipCode END AS accountZipCode,
  CASE WHEN Claims.policyID IS NULL THEN "null" ELSE Claims.policyID END AS policyID,
  --end added 09/26/2022 - nthati
  --end added 10/20/2022 - nthati
  CASE WHEN Claims.claimInjuryTypeDescription IS NULL THEN "null" ELSE Claims.claimInjuryTypeDescription END AS injuryTypeDescription,
  --end added 10/20/2022 - nthati
  CASE WHEN Claims.systemDescription IS NULL THEN "null" ELSE Claims.systemDescription END AS systemDescription,
  CASE WHEN (Claims.supervisorCode IS NULL OR TRIM(Claims.supervisorCode)='') THEN "0" ELSE TRIM(Claims.supervisorCode) END AS supervisorCode,
  CASE WHEN Claims.sequenceCode IS NULL THEN "0" ELSE Claims.sequenceCode END AS sequenceCode
FROM
  tmp_Claims Claims
  inner join (select
      Claims.claimNumber,
      collect_list(
        struct(
                  CASE WHEN Claims.lossAddressLine1 IS NULL THEN "null" ELSE  Claims.lossAddressLine1 END as addressLine1,
                  CASE WHEN Claims.lossAddressLine2 IS NULL THEN "null" ELSE Claims.lossAddressLine2 END as addressLine2,
                  CASE WHEN Claims.lossCity IS NULL THEN "null" ELSE Claims.lossCity END as city,
                  CASE WHEN Claims.lossState IS NULL THEN "null" ELSE Claims.lossState END as state,
                  CASE WHEN Claims.lossZipcode IS NULL THEN "null" ELSE Claims.lossZipcode END as zipcode,
                  CASE WHEN Claims.lossCounty IS NULL THEN "null" ELSE Claims.lossCounty END as county,
                  CASE WHEN Claims.lossCountry IS NULL THEN "null" ELSE Claims.lossCountry END as country
                 )
              ) lossLocations
     FROM
  tmp_Claims Claims
    group by
      Claims.claimNumber) tmp_lossLocation on (tmp_lossLocation.claimNumber = Claims.claimNumber)
      left join tmp_Customer Customer on (Customer.claimNumber = Claims.claimNumber )
      left join tmp_LastUpdateDate tmp_LastUpdateDate on (tmp_LastUpdateDate.claimNumber = Claims.claimNumber )
      left join tmp_entityID on (tmp_entityID.claimNumber = Claims.claimNumber)
      INNER JOIN audit.TableBatchControl ON SubjectArea = 'ClaimsAPI'

  WHERE tmp_LastUpdateDate.LastUpdateDate  > LastRefreshedDateTime

  """
                   
                    )

# COMMAND ----------

#%sql
#select *  --GET(max) lastUpdatedDate, transferDateTime
#from tmp_Claims
#WHERE lastUpdatedDate >= '05/25/2023'

# COMMAND ----------

display(sparkDF_claims)
#sparkDF_claims.where(sparkDF_claims.claimNumber = '10001555').show(truncate=false)

# COMMAND ----------

sparkDF_claims = sparkDF_claims.withColumn("id",md5(sparkDF_claims["id"]))

# COMMAND ----------

##policy
policy_cols = ["policyNumber",
"policyPrefix",
"policySuffix",
"effectiveDate",
"expirationDate",
"underwritingCode",
"underwritingCompanyName",
"jurisdictionCode",
"sicCode",
"naicsCode",
"productLine",
"policyID",
"agentNumber",
"agentName", 
"underwritingCompanyId", 
"productLine2"]


sparkDF_claims= sparkDF_claims.withColumn('policy',F.to_json(F.struct([x for x in policy_cols])))

schema = StructType([StructField("policyNumber", StringType()),StructField("policyPrefix", StringType()),\
                    StructField("policySuffix",StringType()),StructField("effectiveDate", StringType()),StructField("expirationDate", StringType()),\
                    StructField("underwritingCode", StringType()),StructField("underwritingCompanyName", StringType()),\
                    StructField("jurisdictionCode", StringType()),StructField("sicCode", StringType()),\
                    StructField("naicsCode", StringType()),StructField("productLine", StringType()),\
                    StructField("policyID", StringType()),\
                    StructField("agentNumber", StringType()),StructField("agentName", StringType()),\
                    StructField("underwritingCompanyId", IntegerType()),StructField("productLine2", StringType())])

    
sparkDF_claims = sparkDF_claims.withColumn('policy',F.from_json(F.col('policy'), schema))


address_cols = ['addressLine1','addressLine2','city','state','zipcode','county','country']
##physicalAddress
physicalAddress_cols =['physicalAddressLine1',
   'physicalAddressLine2',
   'physicalCity',
   'physicalState',
   'physicalZipcode',
   'physicalCounty',
    'physicalCountry']

for i,j in zip(physicalAddress_cols,address_cols):
  sparkDF_claims = sparkDF_claims.withColumnRenamed(i,j)

#display(sparkDF_claims)
  
sparkDF_claims= sparkDF_claims.withColumn('physicalAddress',F.to_json(F.struct([x for x in address_cols]))).drop(*address_cols)
schema2 = StructType([StructField("addressLine1", StringType()),StructField("addressLine2", StringType()),\
                    StructField("city",StringType()),StructField("state", StringType()),\
                    StructField("zipcode", StringType()),StructField("county", StringType()),StructField("country", StringType())]
                    )
    
sparkDF_claims = sparkDF_claims.withColumn('physicalAddress',F.from_json(F.col('physicalAddress'), schema2))
#print(sparkDF_claims.printSchema())
#mailingAddresses
mailingAddress_cols =['mailingAddressLine1',
   'mailingAddressLine2',
   'mailingCity',
   'mailingState',
   'mailingZipcode',
   'mailingCounty',
   'mailingCountry']

for i,j in zip(mailingAddress_cols,address_cols):
  sparkDF_claims = sparkDF_claims.withColumnRenamed(i,j)

sparkDF_claims= sparkDF_claims.withColumn('mailingAddress',F.to_json(F.struct([x for x in address_cols]))).drop(*address_cols)


schema3= StructType([StructField("addressLine1", StringType()),StructField("addressLine2", StringType()),\
                    StructField("city",StringType()),StructField("state", StringType()),\
                    StructField("zipcode", StringType()),StructField("county", StringType()),StructField("country", StringType())]
                    )

sparkDF_claims = sparkDF_claims.withColumn('mailingAddress',F.from_json(F.col('mailingAddress'), schema3))

#sparkDF_claims= sparkDF_claims.withColumn('mailingAddress',F.col('physicalAddress'))

#insured
insured_cols = ["name",
"clientID",
"entityID",
"accountID",
"accountName",
"accountNumber",
"motorCarrier",
"physicalAddress",
"mailingAddress",
"accountZipCode",
"agentNumber",
"agentName"]


sparkDF_claims= sparkDF_claims.withColumn('insured',F.to_json(F.struct([x for x in insured_cols])))
schema4 = (StructType([StructField("name", StringType()),StructField("clientID", StringType()),
                    StructField("entityID",StringType()),StructField("accountID", IntegerType()),
                    StructField("accountName", StringType()),StructField("accountNumber", StringType()),
                    StructField("motorCarrier", StringType()),StructField("physicalAddress", schema2),
                      StructField("mailingAddress", schema3), StructField("accountZipCode", StringType()),
                      StructField("agentNumber", StringType()),StructField("agentName", StringType())])
          )

    
sparkDF_claims = sparkDF_claims.withColumn('insured',F.from_json(F.col('insured'), schema4)).drop(*insured_cols)

columns_to_delete = policy_cols + insured_cols

sparkDF_claims = sparkDF_claims.drop(*columns_to_delete)

#sparkDF_claims = sparkDF_claims.join(sparkDF2.join(df4,"claimNumber").select('claimNumber','insured','policy'),"claimNumber")

# COMMAND ----------

display(sparkDF_claims)
#sparkDF_claims.where(sparkDF_claims.claimNumber == 'MF-00003158').show(truncate=False)

# COMMAND ----------

# Check duplicates for Claims Id's
if sparkDF_claims.count() > sparkDF_claims.dropDuplicates(["id"]).count():
  dbutils.notebook.exit("Data Frame has duplicates")
else:
  print("No duplicates")

# COMMAND ----------

# # Check duplicates for Vehicle's Id's
# if sparkDF_vehicle.count() > sparkDF_vehicle.dropDuplicates(["id"]).count():
#   dbutils.notebook.exit("Data Frame has duplicates")
# else:
#   print("No duplicates")

# COMMAND ----------

# Check duplicates for Financials's Id's
if sparkDF_financial.count() > sparkDF_financial.dropDuplicates(["id"]).count():
  dbutils.notebook.exit("Data Frame has duplicates")
else:
  print("No duplicates")

# COMMAND ----------

# Check duplicates for ClaimExaminer Ids
if sparkDF_claimexaminer.count() > sparkDF_claimexaminer.dropDuplicates(["id"]).count():
  dbutils.notebook.exit("Data Frame has duplicates")
else:
  print("No duplicates")

# COMMAND ----------

# Function for the retries
import time
def retry_function(func,num_retries = 3,retry_interval = 5):
  #num_retries is the number of tentatives that will be performed
  #retry_interval value in minutes between retries
  for i in range(0,num_retries):
    try:
      func()
      break
    except Exception as e:
      print(f"{i+1} Tentative - Waiting {retry_interval} minutes  - Error:")
      print(e)
      print("\n")
      time.sleep(60*retry_interval)
      continue

# COMMAND ----------

# Load Claims Data
def load_data_1():
    (sparkDF_claims.write
      .mode("append")
      .format("cosmos.oltp")
      .options(**writeConfig)
      .save())
    
retry_function(load_data_1)

# COMMAND ----------

# Load Vehicle Data
def load_data_2():
  (sparkDF_vehicle.na.fill("null").write
    .mode("append")
    .format("cosmos.oltp")
    .options(**writeConfig)
    .save())

retry_function(load_data_2)

# COMMAND ----------

# Load Adjuster Data
def load_data_3():
  (sparkDF_adjuster.na.fill("null").write
    .mode("append")
    .format("cosmos.oltp")
    .options(**writeConfig)
    .save())

retry_function(load_data_3)

# COMMAND ----------

# # Load Loss Financials Data
# (sparkDF_lossfinancial.na.fill("null").write
#  .mode("overwrite")
#  .format("com.microsoft.azure.cosmosdb.spark")
#  .options(**writeConfig)
#  .save())

# COMMAND ----------

# Load Financials Data
def load_data_4():
  (sparkDF_financial.na.fill("null").write
    .mode("append")
    .format("cosmos.oltp")
    .options(**writeConfig)
    .save())

retry_function(load_data_4)

# COMMAND ----------

# Load ClaimExaminer Data
def load_data_5():
  (sparkDF_claimexaminer.na.fill("null").write
    .mode("append")
    .format("cosmos.oltp")
    .options(**writeConfig)
    .save())
  
retry_function(load_data_5) 

# COMMAND ----------

# Check duplicates for Dailyyfact Ids
if sparkDF_dailyfacts.count() > sparkDF_dailyfacts.dropDuplicates(["id"]).count():
  dbutils.notebook.exit("Data Frame has duplicates")
else:
  print("No duplicates")

# COMMAND ----------

# Load Dailyfact Data
def load_data_6():
  sparkDF_dailyfacts.na.fill("null").write.mode("append").format("cosmos.oltp").options(**writeConfigDailyfact).save()
  
retry_function(load_data_6)

# COMMAND ----------

insert_script = """insert into audit.tablebatchcontrol
select 'ClaimsAPI' as SubjectArea, 'ClaimsAPI' as ObjectName, 'NA' as DependsOn, cast('{}' as timestamp), 1 as IsNew from audit.tablebatchcontrol""".format(startTime)

spark.sql(insert_script)

#Include this Table optimize and vaccum in a weekly maintenance jobs 

#spark.sql("OPTIMIZE audit.tablebatchcontrol ZORDER BY LastRefreshedDateTime") 


# COMMAND ----------

# DBTITLE 1,Incremental load process for Claim (Cmd 71 - 73 )
# MAGIC %sql
# MAGIC
# MAGIC --Create INSERT INTO audit.TableBatchControl
# MAGIC INSERT INTO audit.TableBatchControl 
# MAGIC VALUES('ClaimsAPI', 'ClaimsAPI', 'NA', cast('1900-01-01' as timestamp), 1 )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from audit.tablebatchcontrol

# COMMAND ----------

# Update audit.TableBatchControl to set LastRefreshedDateTime

spark.sql("""
UPDATE audit.TableBatchControl
SET LastRefreshedDateTime = current_date() -- "1900-01-01" 
WHERE SubjectArea = 'ClaimsAPI'
""")

# COMMAND ----------

# ***** Incremental load Steps  ********

# Identify source file for each doctype - (claimsCurly.csv)  - ** Done
# Identify dateField column needed for Incremental        - ** Done
# In source file, identify column (lastUpdatedDate)    - ** Done
# Insert record into audit.TableBatchControl      - ** Done
# Update SELECT statements to return each doctype record  - ** Done
# Update audit.TableBatchControl to set current date and time (i.e - LastRefreshedDateTime)  - ** Done

#Note: 
# Incremental - next load should run where lastupdatedDate > 2023-07-11T00:00:00.000+0000 (current_date())
