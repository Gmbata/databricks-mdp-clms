# Databricks notebook source
# MAGIC %md 
# MAGIC
# MAGIC #### ![piclogo](files/images/protective_logo.png)
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Claimants CosmosDB
# MAGIC This notebook is to load claimants data to cosmos DB

# COMMAND ----------

# MAGIC %python
# MAGIC from pyspark.sql.types import StructType,StructField, StringType, IntegerType, LongType,ArrayType #,DateType,TimestampType,BooleanType,DecimalType
# MAGIC from pyspark.sql.functions import input_file_name,col,concat,concat_ws,lit,split,reverse,explode,explode_outer,udf,substring,locate,length,when,trim,md5
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
# MAGIC 'ClaimaintsAPI' as ObjectName,
# MAGIC 'NA' as DependsOn,
# MAGIC cast('1900-01-01' as timestamp) as LastRefreshedDateTime,
# MAGIC 1 as IsNew;
# MAGIC
# MAGIC

# COMMAND ----------

startTime = spark.sql("select current_timestamp()").collect()[0][0]
lastSuccessfulLoadTime = spark.sql(""" select max(LastRefreshedDateTime) as LastRefreshedDateTime from audit.TableBatchControl where SubjectArea = 'ClaimsAPI' """).collect()[0][0]

# COMMAND ----------

# The below script can be used in case we need to create change capture views
#create_view_script = """CREATE OR REPLACE TEMPORARY VIEW v_claimant
#    AS SELECT * FROM claims.claimant
#where dwhupdateddatetime >= '{}' """.format(lastSuccessfulLoadTime)
#spark.sql(create_view_script)

# COMMAND ----------

# # ---credentials for DEV env---
#cosmosEndpoint = dbutils.secrets.get(scope="databricks-secret-scope",key="pic-mdp-dev-coresql-cosmosdb-uri")
#cosmosMasterKey = dbutils.secrets.get(scope="databricks-secret-scope",key="pic-mdp-dev-coresql-cosmosdb-rw-key")
#cosmosDatabaseName = "claimsdb_dev"
#cosmosContainerName = "claimant"

# COMMAND ----------

# # ---credentials for INT env---
#cosmosEndpoint = dbutils.secrets.get(scope="databricks-secret-scope",key="pic-mdp-int-coresql-cosmosdb-uri")
#cosmosMasterKey = dbutils.secrets.get(scope="databricks-secret-scope",key="pic-mdp-int-coresql-cosmosdb-rw-key")
#cosmosDatabaseName = "claimsdb_int"
#cosmosContainerName = "claimant"

# COMMAND ----------

# ---credentials for QA env---
cosmosEndpoint = dbutils.secrets.get(scope="databricks-secret-scope",key="pic-mdp-qa-coresql-cosmosdb-uri")
cosmosMasterKey = dbutils.secrets.get(scope="databricks-secret-scope",key="pic-mdp-qa-coresql-cosmosdb-rw-key")

cosmosDatabaseName = "claimsdb_qa"
cosmosContainerName = "claimant"

# COMMAND ----------

writeConfig = {
  "spark.cosmos.accountEndpoint" : cosmosEndpoint,
  "spark.cosmos.accountKey" : cosmosMasterKey,
  "spark.cosmos.database" : cosmosDatabaseName,
  "spark.cosmos.container" : cosmosContainerName,
  "spark.cosmos.write.strategy" : "ItemOverwrite",
  "spark.cosmos.serialization.inclusionMode" : "Always"
}


# COMMAND ----------

#The below are the dataframes created using manual data exports(csv), we will create these temporary views using databricks tables once the tables are available.
sparkDF = spark.read.format("csv")\
               .option("header", "true")\
               .option("inferSchema", "true")\
               .load("/mnt/rawzone/ClaimsTemp/Customer.csv")
sparkDF.createOrReplaceTempView("tmp_Customer")

# COMMAND ----------

sparkDF = spark.read.format("csv")\
          .option("header", "True")\
          .option("inferSchema", "false")\
          .option("sep", "{")\
          .option("quote","")\
          .load("/mnt/rawzone/ClaimsTemp/Claimants.csv")
sparkDF.createOrReplaceTempView("tmp_Claimants")             

# COMMAND ----------

sparkDF = spark.read.format("csv")\
               .option("header", "true")\
               .option("inferSchema", "true")\
               .load("/mnt/rawzone/ClaimsTemp/Vanlines.csv")
sparkDF.createOrReplaceTempView("tmp_Vanlines")

# COMMAND ----------

sparkDF = spark.read.format("csv")\
               .option("header", "true")\
               .option("inferSchema", "true")\
               .load("/mnt/rawzone/ClaimsTemp/Coverages.csv")
sparkDF.createOrReplaceTempView("tmp_Coverages")

# COMMAND ----------

#updated results to use | as delimited as some fields had commas.
sparkDF = spark.read.format("csv")\
               .option("header", "true")\
               .option("inferSchema", "true")\
               .option("sep",'|')\
               .load("/mnt/rawzone/ClaimsTemp/ClaimantPayment.csv")
sparkDF.createOrReplaceTempView("tmp_ClaimantPayment")

# COMMAND ----------

sparkDF = spark.read.format("csv")\
               .option("header", "true")\
               .option("inferSchema", "true")\
               .load("/mnt/rawzone/ClaimsTemp/Reserve.csv")
sparkDF.createOrReplaceTempView("tmp_Reserve")

# COMMAND ----------

sparkDF = spark.read.format("csv")\
               .option("header", "true")\
               .option("inferSchema", "true")\
               .load("/mnt/rawzone/ClaimsTemp/Incurred.csv")
sparkDF.createOrReplaceTempView("tmp_Incurred")

# COMMAND ----------

#The below are the dataframes created using manual data exports(csv), we will create these temporary views using databricks tables once the tables are avaialable.
sparkDF = spark.read.format("csv")\
               .option("header", "true")\
               .option("sep", ",")\
               .option("inferSchema", "true")\
               .load("/mnt/rawzone/ClaimsTemp/claimVerification.csv")
sparkDF.createOrReplaceTempView("tmp_claimVerification")

# COMMAND ----------

### Claimant Data ###

sparkDF_claimant =  spark.sql("""
SELECT
  --concat('Claimant','-',nvl(Customer.customer, 'NA')) as partitionKey, 
  'Claimant' as partitionKey,
  nvl(Customer.customer, 'NA') as customer,
  'Claimant' as docType,
  STRING(Claimants.id) as id,
  STRING(Claimants.claimantID) as claimantID,
  Claimants.claimNumber,
  Claimants.claimantType,
  Claimants.typeOfSettlement,
  TRIM(Claimants.firstName) as firstName,
  TRIM(Claimants.middleName) as middleName,
  TRIM(Claimants.lastName) as lastName,
  Claimants.lastNameSuffix,
  TRIM(Claimants.companyName) as companyName,
 -- STRING(Claimants.dateOfBirth) as dateOfBirth,
  Claimants.gender,
  INT(Claimants.ageAtTimeOfLoss) as ageAtTimeOfLoss,
  Claimants.bodyPartDescription,
  INT(Claimants.bodyPartID) as bodyPartID,
  Claimants.bmi,
  CAST(CASE WHEN Claimants.isLitigated = 'Y' THEN 1 WHEN Claimants.isLitigated = 'N' THEN 0 END AS BOOLEAN) AS isLitigated,
  Claimants.natureOfInjuryID, 
  CAST(CASE WHEN Claimants.isTotaled ='Y' THEN 1 WHEN Claimants.isTotaled ='N' THEN 0 END AS BOOLEAN) as isTotaled,
  Claimants.numberOfDependents,
  Claimants.bodyPartCode,
  CASE WHEN Claimants.deathDate IS NULL THEN "1900-01-01" ELSE STRING(Claimants.deathDate) END as deathDate,
 -- CASE WHEN Claimants.reportedDate IS NULL THEN "1900-01-01" ELSE STRING(Claimants.reportedDate) END as reportedDate,
  CAST(CASE WHEN Claimants.isOffWork ='Y' THEN 1 WHEN Claimants.isOffWork ='N' THEN 0 END AS BOOLEAN) AS isOffWork,
  CASE WHEN Claimants.returnToWorkDate IS NULL THEN "1900-01-01" ELSE STRING(Claimants.returnToWorkDate) END as returnToWorkDate,
  CASE WHEN Claimants.insurerReportedDate IS NULL THEN "1900-01-01" ELSE STRING(Claimants.insurerReportedDate) END as insurerReportedDate,
  STRING(Claimants.returnToWorkCode) as returnToWorkCode,
  CAST(CASE WHEN Claimants.isDenied ='Y' THEN 1 WHEN Claimants.isDenied ='N' THEN 0 END AS BOOLEAN) AS isDenied,
  Claimants.deniedReason,
  CASE WHEN Claimants.deniedDate IS NULL THEN "1900-01-01" ELSE STRING(Claimants.deniedDate) END AS deniedDate,
  CAST(CASE WHEN Claimants.isDelayed ='Y' THEN 1 WHEN Claimants.isDelayed ='N' THEN 0 END AS BOOLEAN) AS isDelayed,
  Claimants.delayedReason,
  CASE WHEN Claimants.delayedDate IS NULL THEN "1900-01-01" ELSE STRING(Claimants.delayedDate) END as delayedDate,
  CAST(CASE WHEN Claimants.isAccepted ='Y' THEN 1 WHEN Claimants.isAccepted ='N' THEN 0 END AS BOOLEAN) AS isAccepted,
  CAST(CASE WHEN Claimants.isFullDayPaid ='Y' THEN 1 WHEN Claimants.isFullDayPaid ='N' THEN 0 END AS BOOLEAN) AS isFullDayPaid,
  CAST(CASE WHEN Claimants.isSalaryContinued ='Y' THEN 1 WHEN Claimants.isSalaryContinued ='N' THEN 0 END AS BOOLEAN) AS isSalaryContinued,
  Claimants.howInjuryOccured,
  Claimants.maritalStatus,
  Claimants.natureofInjuryDescription,
  --Claimants.medicalImprovementDate,
  CASE WHEN Claimants.medicalImprovementDate IS NULL THEN "1900-01-01" ELSE STRING(Claimants.medicalImprovementDate) END as medicalImprovementDate,
 -- CASE WHEN Claimants.awardDate IS NULL THEN "1900-01-01" ELSE STRING(Claimants.awardDate) END as awardDate,
 -- Claimants.employerReportedDate,
  CASE WHEN Claimants.employerReportedDate IS NULL THEN "1900-01-01" ELSE STRING(Claimants.employerReportedDate) END as employerReportedDate,
  CASE WHEN Claimants.severityID IS NULL THEN 0 ELSE INT(Claimants.severityID) END as severityID,
  Claimants.severityDescription,
  CAST(CASE WHEN Claimants.isAttorneyInvolved ='Yes' THEN 1 WHEN (Claimants.isAttorneyInvolved ='No' OR Claimants.isAttorneyInvolved ='Unknown') THEN 0 END AS BOOLEAN) AS isAttorneyInvolved,
  Claimants.averageWeeklyWage,
 CASE WHEN Claimants.hireDate IS NULL THEN "1900-01-01" ELSE STRING(Claimants.hireDate) END as hireDate,
 --Claimants.hireDate,
CASE WHEN Claimants.claimantContactDate IS NULL THEN "1900-01-01" ELSE STRING(Claimants.claimantContactDate) END as claimantContactDate,
CASE WHEN Claimants.insuredContactDate IS NULL THEN "1900-01-01" ELSE STRING(Claimants.insuredContactDate) END as insuredContactDate,
CASE WHEN Claimants.claimantInjuryTypeDescription IS NULL THEN "null" ELSE Claimants.claimantInjuryTypeDescription end as claimantInjuryTypeDescription,
CASE WHEN Claimants.ClaimantNumber IS NULL THEN "null" ELSE Claimants.ClaimantNumber end as claimantNumber,
CASE WHEN Claimants.claimantContactTime IS NULL THEN "00:00" ELSE STRING(Claimants.claimantContactTime) end as claimantContactTime,
CASE WHEN Claimants.InsuredContactTime IS NULL THEN "00:00" ELSE STRING(Claimants.InsuredContactTime) end as insuredContactTime,
CASE WHEN Claimants.dateSuitFiled IS NULL THEN "1900-01-01" ELSE STRING(Claimants.dateSuitFiled) end as dateSuitFiled,
CASE WHEN Claimants.fraudDescription IS NULL THEN "null" ELSE Claimants.fraudDescription end as fraudDescription,
CASE WHEN Claimants.salvageFlag IS NULL THEN "null" ELSE Claimants.salvageFlag end as salvageFlag,
CASE WHEN Claimants.claimantZipCode IS NULL THEN "null" ELSE Claimants.claimantZipCode end as claimantZipCode,
CAST(CASE WHEN Claimants.IsInsured = 'Y' THEN 1 ELSE 0 END AS BOOLEAN) AS isInsured,
--CAST(CASE WHEN Claimants.IsInsured IS NULL THEN "null" WHEN Claimants.IsInsured = 'Y' THEN 1 WHEN Claimants.IsInsured = 'N' THEN 0 END AS BOOLEAN) AS isInsured,
  Claimants.employmentStatus,
  round(Claimants.ppdRate,2) as ppdRate,
  round(Claimants.ttdRate,2) as ttdRate,
  Coverages.coverages,
 tmp_vanLineAuthority.vanLineAuthorities
FROM
  tmp_Claimants Claimants
  left join (
    select
      a.ClaimantID as ClaimantID,
      collect_list(
        struct(          
		CASE WHEN b.CoverageCode IS NULL THEN "null" ELSE b.CoverageCode END AS code,
		CASE WHEN b.CoverageDescription IS NULL THEN "null" ELSE b.CoverageDescription END as description,
        CASE WHEN b.annualStatementLine IS NULL THEN '0' ELSE Cast(b.annualStatementLine AS INT) end as annualStatementLine,
        CASE WHEN b.coverageGroup1Code IS NULL THEN "null" ELSE b.coverageGroup1Code end as coverageGroup1Code,
        CASE WHEN b.coverageID IS NULL THEN "0" ELSE b.coverageID end as coverageID,
        CASE WHEN b.injuryCauseDescription IS NULL THEN "null" ELSE b.injuryCauseDescription end as injuryCauseDescription
		)
		) coverages
   
    from
      tmp_Claimants as a left join 
      tmp_coverages as b
      on a.claimantID = b.claimantID
      group by
      a.ClaimantID
  ) Coverages on (Claimants.claimantID = Coverages.ClaimantID)
  
   left join (
    select
      a.ClaimantID,
      collect_list(
        struct(
          CASE WHEN b.vanLineAuthority IS NULL THEN "null" ELSE b.vanLineAuthority END as vanLineAuthority
        )
      ) vanLineAuthorities
    from
      tmp_Claimants as a left join
	  tmp_Vanlines as b
	on a.claimantID = b.claimantID
   --nb where a.ClaimantId = '9635990'
    group by
      a.ClaimantID
  ) tmp_vanLineAuthority on (Claimants.claimantID = tmp_vanLineAuthority.ClaimantID)
  
  left join tmp_Customer Customer on (Customer.ClaimMasterId = Claimants.ClaimMasterId )
-- where Claimants.id ='233044' or Claimants.id ='155510' or Claimants.id ='600579'
  """ )

# COMMAND ----------

sparkDF_claimant = sparkDF_claimant.withColumn("id",md5(sparkDF_claimant["id"]))
sparkDF_claimant = sparkDF_claimant.withColumn("claimantID",md5(sparkDF_claimant["claimantID"]))


# COMMAND ----------

display(sparkDF_claimant)

# COMMAND ----------

### Payment Data ###

sparkDF_pymt =  spark.sql("""
SELECT distinct
  --concat('Payment','-',nvl(Customer.customer, 'NA')) as partitionKey,
  'Payment' as partitionKey,
  nvl(Customer.customer, 'NA') as customer,
  'Payment' as docType,
  ClaimantPayment.id as id,
  ClaimantPayment.claimNumber,
  CAST(ClaimantPayment.checkNumber AS STRING) as checkNumber,
  ClaimantPayment.description,
 -- ClaimantPayment.amount,
  CAST(CASE WHEN ClaimantPayment.transactionAmount IS NULL THEN '0.0' ELSE ClaimantPayment.transactionAmount END AS DOUBLE) as transactionAmount,
  CASE WHEN ClaimantPayment.issueDate IS NULL THEN "1900-01-01" ELSE STRING(ClaimantPayment.issueDate) END as issueDate,
  CASE WHEN ClaimantPayment.coverStartDate IS NULL THEN "1900-01-01" ELSE STRING(ClaimantPayment.coverStartDate) END as coverStartDate,
  CASE WHEN ClaimantPayment.coverEndDate IS NULL THEN "1900-01-01" ELSE STRING(ClaimantPayment.coverEndDate) END as coverEndDate,
  CASE WHEN ClaimantPayment.kindOfLossCode IS NULL THEN '0' ELSE ClaimantPayment.kindOfLossCode END AS kindOfLossCode,
  ClaimantPayment.kindOfLossDescription,
  STRING(ClaimantPayment.claimantID) AS claimantID,
  ClaimantPayment.benefitType,
  CAST(CASE WHEN ClaimantPayment.reducedEarningsAmount IS NULL THEN '0.0' ELSE ClaimantPayment.reducedEarningsAmount END AS DOUBLE) AS reducedEarningsAmount,
  CAST(CASE WHEN ClaimantPayment.isAutoPayment ='Y' THEN 1 WHEN ClaimantPayment.isAutoPayment ='N' OR ClaimantPayment.isAutoPayment IS NULL THEN 0 END AS BOOLEAN) AS isAutoPayment,
  --ClaimantPayment.isAutoPayment,  
  ClaimantPayment.additionalPayeeName,
 -- ClaimantPayment.payeeTypeCode,
  ClaimantPayment.paymentID,
 -- STRING(ClaimantPayment.processedDate) as processedDate,
  STRING(ClaimantPayment.kindOfLossCategoryDescription) as kindOfLossCategoryDescription,
  CASE WHEN trim(ClaimantPayment.name) IS NULL THEN "null" ELSE trim(ClaimantPayment.name) END as name,
  CASE WHEN ClaimantPayment.payeeType IS NULL THEN "null" ELSE ClaimantPayment.payeeType END as payeeType,
  CASE WHEN ClaimantPayment.KindOfLossCategoryCode IS NULL THEN '0' ELSE ClaimantPayment.KindOfLossCategoryCode END AS kindOfLossCategoryCode,  
  CASE WHEN ClaimantPayment.method IS NULL THEN "null" ELSE ClaimantPayment.method END AS method,
  CAST(CASE WHEN ClaimantPayment.BilledAmount IS NULL THEN '0.0' ELSE ClaimantPayment.BilledAmount END AS DOUBLE) AS billedAmount,
  CASE WHEN ClaimantPayment.paymentTypeCode IS NULL THEN '0' ELSE ClaimantPayment.paymentTypeCode END AS paymentTypeCode,
  CASE WHEN ClaimantPayment.PaymentTypeDescription IS NULL THEN "null" ELSE ClaimantPayment.PaymentTypeDescription END AS paymentTypeDescription,
  CAST(CASE WHEN ClaimantPayment.paymentAmount IS NULL THEN '0.0' ELSE ClaimantPayment.paymentAmount END AS DOUBLE) AS paymentAmount,
  CAST(CASE WHEN ClaimantPayment.CheckAmount IS NULL THEN '0.0' ELSE ClaimantPayment.CheckAmount END AS DOUBLE) AS checkAmount,
  CASE WHEN ClaimantPayment.DocumentNumber IS NULL THEN "null" ELSE ClaimantPayment.DocumentNumber END AS documentNumber,
  CASE WHEN ClaimantPayment.InvoiceNumber IS NULL THEN "null" ELSE ClaimantPayment.InvoiceNumber END AS invoiceNumber,
  CASE WHEN ClaimantPayment.ProcessedDate IS NULL THEN "1900-01-01" ELSE STRING(ClaimantPayment.ProcessedDate) END AS processedDate,
  CASE WHEN ClaimantPayment.indexNumber IS NULL THEN "null" ELSE ClaimantPayment.indexNumber END AS indexNumber,
  CASE WHEN ClaimantPayment.TransactionCode IS NULL THEN '0' ELSE ClaimantPayment.TransactionCode END AS transactionCode,
  CASE WHEN ClaimantPayment.TransactionDate IS NULL THEN "1900-01-01" ELSE STRING(ClaimantPayment.TransactionDate) END AS transactionDate,
  CASE WHEN ClaimantPayment.transactionTypeDescription IS NULL THEN "null" ELSE ClaimantPayment.transactionTypeDescription END AS transactionTypeDescription,
  STRING(Reserve.systemDescription) AS systemDescription,
  ClaimantPayment.enteredBy as enteredBy
FROM
  tmp_ClaimantPayment ClaimantPayment
   left join tmp_Customer Customer on (Customer.claimNumber = ClaimantPayment.claimNumber)
   left join tmp_Reserve Reserve on (Reserve.claimNumber = Customer.claimNumber)

  """
  )

# COMMAND ----------

sparkDF_pymt = sparkDF_pymt.withColumn("id",md5(sparkDF_pymt["id"]))
sparkDF_pymt = sparkDF_pymt.withColumn("claimantID",md5(sparkDF_pymt["claimantID"]))


# COMMAND ----------

display(sparkDF_pymt)

# COMMAND ----------

##payee
payee_cols = ["name",
"payeeType"]


sparkDF_pymt= sparkDF_pymt.withColumn('payee',F.to_json(F.struct([x for x in payee_cols]))).drop(*payee_cols)
schema = StructType([StructField("name", StringType()),StructField("payeeType", StringType())])

    
sparkDF_pymt = sparkDF_pymt.withColumn('payee',F.from_json(F.col('payee'), schema))


# COMMAND ----------

display(sparkDF_pymt)

# COMMAND ----------

### Reserve Data ###

sparkDF_reserve =  spark.sql("""
SELECT distinct 
  --concat('Reserve','-',nvl(Customer.customer, 'NA')) as partitionKey, 
  'Reserve' as partitionKey,
  'Reserve' as docType,
  Reserve.id as id,
  CASE WHEN Reserve.coverageDescription IS NULL THEN 'null' ELSE Reserve.coverageDescription END AS coverageDescription,
  Reserve.claimNumber as claimNumber,
  Reserve.reserveReductionDueToPayment as reserveReductionDueToPayment,
  nvl(Customer.customer, 'NA') as customer,
  Reserve.reserveType,
  CAST(CASE WHEN Reserve.amount IS NULL THEN '0.0' ELSE Reserve.amount END AS DOUBLE) as amount,
  CASE WHEN Reserve.transactionCode IS NULL THEN '0' ELSE Reserve.transactionCode END AS transactionCode,
  CASE WHEN Reserve.entryDate IS NULL THEN "1900-01-01" ELSE STRING(Reserve.entryDate) END as entryDate,
  CASE WHEN Reserve.updatedDate IS NULL THEN "1900-01-01" ELSE STRING(Reserve.updatedDate) END as updatedDate,
  CASE WHEN Reserve.transactionTypeDescription IS NULL THEN "null" ELSE STRING(Reserve.transactionTypeDescription) END AS transactionTypeDescription,
  STRING(Reserve.claimantID) as claimantID,
  CASE WHEN Reserve.systemDescription IS NULL THEN "null" ELSE STRING(Reserve.systemDescription) END AS systemDescription
FROM
  tmp_Reserve Reserve
  
  left join tmp_Customer Customer on (Reserve.claimNumber = Customer.claimNumber)"""
                   
                    )

# COMMAND ----------

sparkDF_reserve = sparkDF_reserve.withColumn("id",md5(sparkDF_reserve["id"]))
sparkDF_reserve = sparkDF_reserve.withColumn("claimantID",md5(sparkDF_reserve["claimantID"]))

# COMMAND ----------

#sparkDF_reserve.count()
display(sparkDF_reserve)

# COMMAND ----------

### Incurred Data ###

sparkDF_Incurred =  spark.sql("""
SELECT distinct 
  --concat('Incurred','-',nvl(Customer.customer, 'NA')) as partitionKey, 
  'Incurred' as partitionKey,
  'Incurred' as docType,
  Incurred.id as id,
  Incurred.coverageDescription,
  Incurred.claimNumber as claimNumber,
  nvl(Customer.customer, 'NA') as customer,
  Incurred.reserveType,
  cast(Incurred.amount as DOUBLE) as amount,
  --Incurred.amount,
  CASE WHEN Incurred.entryDate IS NULL THEN "1900-01-01" ELSE STRING(Incurred.entryDate) END as entryDate,
  --CASE WHEN Incurred.updatedDate IS NULL THEN "1900-01-01" ELSE STRING(Incurred.updatedDate) END as updatedDate,
  STRING(Incurred.claimantID) as claimantID
FROM
  tmp_Incurred Incurred
  
  left join tmp_Customer Customer on (Incurred.claimNumber = Customer.claimNumber )"""
                   
                    )

# COMMAND ----------

sparkDF_Incurred = sparkDF_Incurred.withColumn("id",md5(sparkDF_Incurred["id"]))
sparkDF_Incurred = sparkDF_Incurred.withColumn("claimantID",md5(sparkDF_Incurred["claimantID"]))

# COMMAND ----------

display(sparkDF_Incurred)

# COMMAND ----------

### claimVerification Data ###

sparkDF_claimverification =  spark.sql("""
SELECT distinct 
  'ClaimVerification' as partitionKey,
    'ClaimVerification' as docType,
    CAST(claimVerification.id as STRING) id,
    nvl(Customer.customer, 'NA') as customer,
    CAST(claimVerification.claimNumber AS STRING) AS claimNumber,
    CAST(claimVerification.claimantNumber AS STRING) claimantNumber,
    CAST(claimverification.systemId AS INT) AS systemId,
    CAST(claimverification.claimantid AS STRING) claimantID,
    claimverification.verificationTypeId AS verificationTypeId,
    claimverification.comments AS comment,
    CAST(CASE WHEN claimverification.isCleared = 'Y'  THEN 1 WHEN claimverification.isCleared = 'N' THEN 0 END AS BOOLEAN) isCleared,
    CASE WHEN claimverification.clearedDate IS NULL THEN "1900-01-01" ELSE claimverification.clearedDate END AS clearedDate,
    claimverification.clearedBy AS clearedBy,
    CASE WHEN claimverification.updateddate IS NULL THEN "1900-01-01" ELSE claimverification.updateddate END AS updatedDate,
    claimverification.updatedBy AS updatedBy,
    CASE WHEN claimverification.entryDate IS NULL THEN "1900-01-01" ELSE claimverification.entryDate END AS entryDate,
    claimverification.enteredBy AS enteredBy,
    claimVerification.description AS description,
    cast(claimVerification.level AS INT) level,
    claimVerification.systemDescription AS systemDescription
FROM
  tmp_claimVerification claimVerification
  left join tmp_Customer Customer on (claimVerification.claimNumber = Customer.claimNumber)
  """)               

# COMMAND ----------

#sparkDF_claimverification = sparkDF_claimverification.withColumn("id",md5(sparkDF_claimverification["id"]))
sparkDF_claimverification = sparkDF_claimverification.withColumn("claimantID",md5(sparkDF_claimverification["claimantID"]))

# COMMAND ----------

# Check duplicates for Incurred Id's
#if sparkDF_Incurred.count() > sparkDF_Incurred.dropDuplicates(["id"]).count():
 # dbutils.notebook.exit("Data Frame has duplicates")
#else:
#  print("No duplicates")

# COMMAND ----------

# Check duplicates for Reserve Id's
#if sparkDF_reserve.count() > sparkDF_reserve.dropDuplicates(["id"]).count():
#  dbutils.notebook.exit("Data Frame has duplicates")
#else:
#  print("No duplicates")#

# COMMAND ----------

# Check duplicates for Claimants Id's
#if sparkDF_claimant.count() > sparkDF_claimant.dropDuplicates(["id"]).count():
#  dbutils.notebook.exit("Data Frame has duplicates")
#else:
#  print("No duplicates")

# COMMAND ----------

# Check duplicates for Payments Id's
#if sparkDF_pymt.count() > sparkDF_pymt.dropDuplicates(["id"]).count():
#  dbutils.notebook.exit("Data Frame has duplicates")
#else:
#  print("No duplicates")

# COMMAND ----------

# Load Incurred Data
(sparkDF_Incurred.na.fill("null").write
  .mode("append")
  .format("cosmos.oltp")
  .options(**writeConfig)
  .save())

# COMMAND ----------

# Load Claimants Payments Data
(sparkDF_pymt.na.fill("null").write
  .mode("append")
  .format("cosmos.oltp")
  .options(**writeConfig)
  .save())

# COMMAND ----------

 # Load Reserve Data
(sparkDF_reserve.na.fill("null").write
  .mode("append")
  .format("cosmos.oltp")
  .options(**writeConfig)
  .save())

# COMMAND ----------

# Load Claimants Data
(sparkDF_claimant.na.fill("null").write
  .mode("append")
  .format("cosmos.oltp")
  .options(**writeConfig)
  .save())

# COMMAND ----------

# Load claimverification Data
(sparkDF_claimverification.na.fill("null").write
.mode("append")
.format("cosmos.oltp")
.options(**writeConfig)
.save())

# COMMAND ----------

insert_script = """insert into audit.tablebatchcontrol
select 'ClaimsAPI' as SubjectArea, 'ClaimantsAPI' as ObjectName, 'NA' as DependsOn, cast('{}' as timestamp), 1 as IsNew from audit.tablebatchcontrol""".format(startTime)

spark.sql(insert_script)

#Include this Table optimize and vaccum in a weekly maintenance jobs 

#spark.sql("OPTIMIZE audit.tablebatchcontrol ZORDER BY LastRefreshedDateTime") 

