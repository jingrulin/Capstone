# Find and plot the percentage of applications approved for self-employed applicants.
import pandas as pd
import matplotlib.pyplot as plt
import mysql.connector

# Connect to MariaDB
mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="password",
  database="creditcard_capstone"
)

# Query database and fetch filtered data into a pandas DataFrame
mycursor = mydb.cursor()
query = '''
    SELECT *
    FROM cdw_sapp_loan_application
    WHERE Self_Employed = 'YES'
    '''

mycursor.execute(query)
result = mycursor.fetchall()


# Calculate the percentage of applications approved for self-employed applicants
approved_df = self_employed_df[self_employed_df['Application_Status'] == 'Y']
total_applications = self_employed_df.shape[0]
if total_applications == 0:
    approved_percentage = 0
else:
    approved_percentage = approved_df.shape[0] / total_applications * 100

# Create a pie chart of the percentage of applications approved for self-employed applicants
plt.pie([approved_percentage, 100-approved_percentage], labels=['Approved', 'Not Approved'], colors=['green', 'red'], autopct='%1.1f%%')
plt.title('Loan Application Status of Self-Employed Applicants')

# Print the percentage of applications approved for self-employed applicants
print(f"The percentage of applications approved for self-employed applicants is {approved_percentage:.1f}%.")

plt.show()

# Close the database connection
mydb.close()

# Find the percentage of rejection for married male applicants.
import pandas as pd
import matplotlib.pyplot as plt
import mysql.connector

# Connect to MariaDB
mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="password",
  database="creditcard_capstone"
)

# Query database and fetch filtered data into a pandas DataFrame
mycursor = mydb.cursor()
query = '''
    SELECT *
    FROM cdw_sapp_loan_application
    WHERE Gender = 'Male' AND Married = 'YES'
    '''
mycursor.execute(query)
result = mycursor.fetchall()

# Convert result into a pandas DataFrame
df = pd.DataFrame(result, columns=[i[0] for i in mycursor.description])

# Calculate the percentage of rejection among married male applicants
total_applications = len(df)
approved = len(df[df['Application_Status'] == 'Y'])
rejected = len(df[df['Application_Status'] == 'N'])
rejected_percentage = (rejected / total_applications) * 100

# Plot a pie chart of rejection percentage among married male applicants
labels = ['Approved', 'Rejected']
sizes = [approved, rejected]
colors = ['green', 'red']
plt.pie(sizes, labels=labels, colors=colors, autopct='%1.1f%%', startangle=90)
plt.axis('equal')
plt.title(f"Loan Application Status of Married Male Applicants")

# Print the percentage of rejection for married male applicants
print(f"The percentage of rejection for married male applicants is {rejected_percentage:.1f}%.")

plt.show()

# Close the database connection
mydb.close()

# Find and plot the top three months with the largest transaction data.
import mysql.connector
import pandas as pd
import matplotlib.pyplot as plt
import calendar

# Connect to MySQL database
mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="password",
  database="creditcard_capstone"
)

# Define the SQL query to get the total transaction value per month
mycursor = mydb.cursor()

query = '''
    SELECT
        SUBSTRING(TIMEID, 1, 6) AS month,
        SUM(TRANSACTION_VALUE) AS total_transaction_value
    FROM
        cdw_sapp_credit_card
    GROUP BY
        month
    ORDER BY
        total_transaction_value DESC
    LIMIT
        3
'''
mycursor.execute(query)
result = mycursor.fetchall()

# Convert month number to month name
df = pd.DataFrame(result, columns=['month', 'total_transaction_value'])
df['month'] = df['month'].apply(lambda x: calendar.month_name[int(x[-2:])])

# Plot the bar chart
plt.bar(df['month'], df['total_transaction_value'])
plt.title('Top Three Months with the Largest Transaction Data')
plt.xlabel('Month')
plt.ylabel('Total Transaction Value ($)')

# Print the the top three months with the largest transaction data
print(f"The top three months with the largest transaction data are: ")
for index, row in df.iterrows():
    formatted_value = format(round(row['total_transaction_value'], 2), ',')
    print(f"{row['month']}: ${formatted_value}")
    
plt.show()


# Close the database connection
mydb.close()

# Find and plot which branch processed the highest total dollar value of healthcare transactions.
import pandas as pd
import matplotlib.pyplot as plt
import mysql.connector

# Connect to MariaDB
mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="password",
  database="creditcard_capstone"
)

# Query database and fetch filtered data into a pandas DataFrame
mycursor = mydb.cursor()
query = '''
    SELECT BRANCH_CODE, SUM(TRANSACTION_VALUE) as total_healthcare
    FROM cdw_sapp_credit_card
    WHERE TRANSACTION_TYPE = 'Healthcare' 
    GROUP BY BRANCH_CODE
    ORDER BY total_healthcare DESC
    LIMIT 1
'''

mycursor.execute(query)
result = mycursor.fetchall()

# Convert query result to a pandas DataFrame
df = pd.DataFrame(result, columns=['BRANCH_CODE', 'total_healthcare'])

# Plot the bar chart
plt.bar(df['BRANCH_CODE'].astype(str), df['total_healthcare'])
plt.title("Branch with Highest Total Dollar Value of Healthcare Transactions")
plt.ylabel("Healthcare Transactions ($)")
plt.xlabel("Branch Code")

# Print the branch processed the highest total dollar value of healthcare transactions
print("The branch processed the highest total dollar value of healthcare transactions is branch", end=" ")
for index, row in df.iterrows():
    print(f"{int(row['BRANCH_CODE'])} with a total healthcare transaction value of ${round(row['total_healthcare'], 2)}.")

plt.show()

# Close the database connection
mydb.close()