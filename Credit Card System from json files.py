# Import necessary modules
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, BooleanType, DoubleType

# Set up Spark session
spark = SparkSession.builder.appName("CreditCardSystem").getOrCreate()

# Specify file paths
customer_file = "cdw_sapp_custmer.json"
branch_file = "cdw_sapp_branch.json"
creditcard_file = "cdw_sapp_credit.json"

# Read JSON files into PySpark dataframes
customer_df = spark.read.json(customer_file)
branch_df = spark.read.json(branch_file)
creditcard_df = spark.read.json(creditcard_file)

# CDW_SAPP_CUDSTOMER dataframes

from pyspark.sql.functions import concat, initcap, lit, regexp_replace, substring
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from pyspark.sql.functions import col

# Define the schema with comments/descriptions
customer_schema = StructType([
    StructField("SSN", IntegerType(), True, {"comment": "Social Security Number of the customer (National ID)"}),
    StructField("FIRST_NAME", StringType(), True, {"comment": "First Name of the Customer", "type": "varchar(50)"}),
    StructField("MIDDLE_NAME", StringType(), True, {"comment": "Middle Name of the customer", "type": "varchar(50)"}),
    StructField("LAST_NAME", StringType(), True, {"comment": "Last Name of the customer", "type": "varchar(50)"}),
    StructField("CREDIT_CARD_NO", StringType(), True, {"comment": "Credit card number of customer", "type": "varchar(20)"}),
    StructField("FULL_STREET_ADDRESS", StringType(), True, {"comment": "Apartment no and Street name of customer's Residence", "type": "varchar(200)"}),
    StructField("CUST_CITY", StringType(), True, {"comment": "Customer’s Current City", "type": "varchar(50)"}),
    StructField("CUST_STATE", StringType(), True, {"comment": "Customer’s State code", "type": "varchar(2)"}),
    StructField("CUST_COUNTRY", StringType(), True, {"comment": "Customer’s country code", "type": "varchar(2)"}),
    StructField("CUST_ZIP", IntegerType(), True, {"comment": "Zip code of Customer's Country"}),
    StructField("CUST_PHONE", StringType(), True, {"comment": "Contact Number of the customer", "type": "varchar(15)"}),
    StructField("CUST_EMAIL", StringType(), True, {"comment": "Email address of the customer", "type": "varchar(50)"}),
    StructField("LAST_UPDATED", TimestampType(), True, {"comment": "Record inserted / modification date."})
])

# Perform necessary transformations on dataframes according to mapping document

customer_df = customer_df.withColumn("SSN", col("SSN").cast(IntegerType())) \
    .withColumn("FIRST_NAME", col("FIRST_NAME").cast(StringType())) \
    .withColumn("MIDDLE_NAME", col("MIDDLE_NAME").cast(StringType())) \
    .withColumn("LAST_NAME", col("LAST_NAME").cast(StringType())) \
    .withColumn("CREDIT_CARD_NO", col("CREDIT_CARD_NO").cast(StringType())) \
    .withColumn("FULL_STREET_ADDRESS", concat(initcap(col("STREET_NAME")), lit(", "), col("APT_NO")).cast(StringType())) \
    .withColumn("CUST_CITY", col("CUST_CITY").cast(StringType())) \
    .withColumn("CUST_STATE", col("CUST_STATE").cast(StringType())) \
    .withColumn("CUST_COUNTRY", col("CUST_COUNTRY").cast(StringType())) \
    .withColumn("CUST_ZIP", col("CUST_ZIP").cast(IntegerType())) \
    .withColumn("CUST_PHONE", concat(lit("(267)"), substring(col("CUST_PHONE"), 1, 3), lit("-"), substring(col("CUST_PHONE"), 4, 7)).cast(StringType())) \
    .withColumn("CUST_EMAIL", col("CUST_EMAIL").cast(StringType())) \
    .withColumn("LAST_UPDATED", col("LAST_UPDATED").cast(TimestampType())) \
    .select("SSN", "FIRST_NAME", "MIDDLE_NAME", "LAST_NAME", "CREDIT_CARD_NO", "FULL_STREET_ADDRESS", 
            "CUST_CITY", "CUST_STATE", "CUST_COUNTRY", "CUST_ZIP", "CUST_PHONE", "CUST_EMAIL", "LAST_UPDATED") 

# Update the schema to match the new schema
customer_df = spark.createDataFrame(customer_df.rdd, schema=customer_schema)

# Print schema of transformed CDW_SAPP_CUSTOMER dataframe
print("CDW_SAPP_CUSTOMER transformed schema:")
customer_df.printSchema()
# Show transformed CDW_SAPP_CUSTOMER dataframe
customer_df.show()

# CDW_SAPP_BRANCH dataframes

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import coalesce

# Define the new schema with reordered columns
branch_schema = StructType([
    StructField("BRANCH_CODE", IntegerType(), True, {"comment": "Uniquely identifies a branch of the retail store"}),
    StructField("BRANCH_NAME", StringType(), True, {"comment": "Name of the Branch"}),
    StructField("BRANCH_STREET", StringType(), True, {"comment": "Street Address"}),
    StructField("BRANCH_CITY", StringType(), True, {"comment": "City name where the branch is located"}),
    StructField("BRANCH_STATE", StringType(), True, {"comment": "State name where the branch is located"}),
    StructField("BRANCH_ZIP", IntegerType(), True, {"comment": "Zip postal code(default value 999999)"}),
    StructField("BRANCH_PHONE", StringType(), True, {"comment": "Phone number of the branch"}),
    StructField("LAST_UPDATED", TimestampType(), True, {"comment": "Record inserted / modification date."})
])
# Perform necessary transformations on dataframes according to mapping document

branch_df = branch_df.withColumn("BRANCH_CODE", col("BRANCH_CODE").cast(IntegerType())) \
    .withColumn("BRANCH_NAME", col("BRANCH_NAME").cast(StringType())) \
    .withColumn("BRANCH_STREET", col("BRANCH_STREET").cast(StringType())) \
    .withColumn("BRANCH_CITY", col("BRANCH_CITY").cast(StringType())) \
    .withColumn("BRANCH_STATE", col("BRANCH_STATE").cast(StringType())) \
    .withColumn("BRANCH_ZIP", coalesce(col("BRANCH_ZIP"), lit(99999)).cast(IntegerType())) \
    .withColumn("BRANCH_PHONE", regexp_replace(col("BRANCH_PHONE"), "^(\d{3})(\d{3})(\d{4})$", "($1)$2-$3").cast(StringType())) \
    .withColumn("LAST_UPDATED", col("LAST_UPDATED").cast(TimestampType())) \
    .select("BRANCH_CODE", "BRANCH_NAME", "BRANCH_STREET", "BRANCH_CITY", "BRANCH_STATE", "BRANCH_ZIP", "BRANCH_PHONE", "LAST_UPDATED") \
    .orderBy("BRANCH_CODE")

# Print schema of CDW_SAPP_BRANCH dataframes to verify data extraction
print("CDW_SAPP_BRANCH schema:")
branch_df.printSchema()

# Show CDW_SAPP_BRANCH dataframes
branch_df.show()

# CDW_SAPP_CREDITCARD dataframe

from pyspark.sql.functions import concat_ws, col, lpad

# Create a new column called TIMEID by concatenating the Year, Month, and Day columns
#creditcard_df = creditcard_df.withColumn("TIMEID", concat_ws("", col("YEAR"), col("MONTH"), col("DAY")).cast("string"))
creditcard_df = creditcard_df.withColumn("TIMEID", concat_ws("", col("YEAR"), lpad(col("MONTH"), 2, "0"), lpad(col("DAY"), 2, "0")).cast("string"))

# Drop the Year, Month, and Day columns
#creditcard_df = creditcard_df.drop("YEAR", "MONTH", "DAY")
# Select all columns except Year, Month, and Day
creditcard_df = creditcard_df.select([col for col in creditcard_df.columns if col not in {'YEAR', 'MONTH', 'DAY'}])

# Target column order
creditcard_df = creditcard_df.select("CREDIT_CARD_NO", "TIMEID", "CUST_SSN", "BRANCH_CODE", "TRANSACTION_TYPE", "TRANSACTION_VALUE", "TRANSACTION_ID")

# Target Datetype
creditcard_df = creditcard_df.select(
    col("CREDIT_CARD_NO").alias("CUST_CC_NO").cast("string"),
    col("TIMEID").alias("TIMEID").cast("string"),
    col("CUST_SSN").alias("CUST_SSN").cast("int"),
    col("BRANCH_CODE").alias("BRANCH_CODE").cast("int"),
    col("TRANSACTION_TYPE").alias("TRANSACTION_TYPE").cast("string"),
    col("TRANSACTION_VALUE").alias("TRANSACTION_VALUE").cast("double"),
    col("TRANSACTION_ID").alias("TRANSACTION_ID").cast("int")
)

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Define the schema with comments/descriptions
creditcard_schema = StructType([
    StructField("CUST_CC_NO", StringType(), True, {"comment": "Credit card number of customer"}),
    StructField("TIMEID", StringType(), True, {"comment": "Surrogate key of the period(time) table"}),
    StructField("CUST_SSN", IntegerType(), True, {"comment": "Surrogate key of the customer table. Used to uniquely identify a row."}),
    StructField("BRANCH_CODE", IntegerType(), True, {"comment": "Surrogate key of the branch table"}),
    StructField("TRANSACTION_TYPE", StringType(), True, {"comment": "Type of transaction"}),
    StructField("TRANSACTION_VALUE", DoubleType(), True, {"comment": "Monetary value of transaction"}),
    StructField("TRANSACTION_ID", IntegerType(), True, {"comment": "Uniquely identifies a transaction"})
])
# Print schema of transformed CDW_SAPP_CREDITCARD dataframe
print("CDW_SAPP_CREDITCARD transformed schema:")
creditcard_df.printSchema()

# Show CDW_SAPP_CREDITCARD dataframe
creditcard_df.show()

# Save transformed CDW_SAPP_CUSTOMER dataframes to MariaDB 

table = "CDW_SAPP_CUSTOMER"


customer_df.write.format("jdbc").options(driver="com.mysql.cj.jdbc.Driver",
                                 url="jdbc:mysql://localhost:3306/creditcard_capstone",
                                 user="root",
                                 password="password",
                                 dbtable=table).mode("overwrite").save()

# Save transformed CDW_SAPP_BRANCH dataframes to MariaDB 

table = "CDW_SAPP_BRANCH"


branch_df.write.format("jdbc").options(driver="com.mysql.cj.jdbc.Driver",
                                 url="jdbc:mysql://localhost:3306/creditcard_capstone",
                                 user="root",
                                 password="password",
                                 dbtable=table).mode("overwrite").save()

# Save transformed CDW_SAPP_CREDIT_CARD dataframes to MariaDB 

table = "CDW_SAPP_CREDIT_CARD"


creditcard_df.write.format("jdbc").options(driver="com.mysql.cj.jdbc.Driver",
                                 url="jdbc:mysql://localhost:3306/creditcard_capstone",
                                 user="root",
                                 password="password",
                                 dbtable=table).mode("overwrite").save()


spark.stop()