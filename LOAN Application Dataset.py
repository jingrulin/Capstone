import requests
url = "https://raw.githubusercontent.com/platformps/LoanDataset/main/loan_data.json"

response = requests.get(url)

# Find the status code of the above API endpoint. Hint: status code could be 200, 400, 404, 401.
print("Status code:", response.status_code)

loan_data = response.json()
print(loan_data)

# Import necessary modules
import pandas as pd
from pyspark.sql import SparkSession

# Set up Spark session
spark = SparkSession.builder.appName("Loan_Application").getOrCreate()

# Specify file paths
loan_file = "loan_data.json"

# Read JSON files into PySpark dataframes
loan_df = spark.read.json(loan_file)

# Write DataFrame to SQL table
table = "CDW_SAPP_LOAN_APPLICATION"

loan_df.write.format("jdbc").options(driver="com.mysql.cj.jdbc.Driver",
                                 url="jdbc:mysql://localhost:3306/creditcard_capstone",
                                 user="root",
                                 password="password",
                                 dbtable=table).mode("overwrite").save()

spark.stop()