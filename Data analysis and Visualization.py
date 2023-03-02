# Find and plot which transaction type has a high rate of transactions.
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

# Query database
mycursor = mydb.cursor()
query = '''
    SELECT TRANSACTION_TYPE, COUNT(*) AS TRANSACTION_COUNT
    FROM cdw_sapp_credit_card
    GROUP BY TRANSACTION_TYPE
    '''
mycursor.execute(query)
result = mycursor.fetchall()

# Create a DataFrame from the result
df = pd.DataFrame(result, columns=['Transaction Type', 'Transaction Count'])

# Calculate the percentage of transactions for each type
total_transactions = df['Transaction Count'].sum()
df['Transaction Percentage'] = (df['Transaction Count'] / total_transactions) * 100

# Plot a pie chart showing the rate of transactions by transaction type
plt.pie(df['Transaction Count'], labels=df['Transaction Type'], autopct='%1.1f%%')
plt.title('Transaction Type Distribution')

# Print the transaction type with the highest rate of transactions
highest_percentage = df['Transaction Percentage'].max()
highest_type = df.loc[df['Transaction Percentage'] == highest_percentage, 'Transaction Type'].iloc[0]
print(f"The transaction type with the highest rate of transactions is '{highest_type}' with {highest_percentage:.1f}% of transactions.")

# Close the database connection
mydb.close()

# Find and plot which state has a high number of customers.
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

# Query database for customer count by state
mycursor = mydb.cursor()
query = '''
        SELECT CUST_STATE, COUNT(CREDIT_CARD_NO) AS CUSTOMER_COUNT
        FROM cdw_sapp_customer
        GROUP BY CUST_STATE
        ORDER BY CUSTOMER_COUNT DESC
    '''
mycursor.execute(query)
result = mycursor.fetchall()

# Create Pandas dataframe from result
df = pd.DataFrame(result, columns=['State', 'Customer Count'])

# Plot bar chart of customer count by state
plt.figure(figsize=(10, 6))
plt.bar(df['State'], df['Customer Count'])
plt.title('Customer Count by State')
plt.xlabel('State')
plt.ylabel('Customer Count')

# Print the state with the highest number of customers and its customer count
highest_cust_state = df.iloc[0]['State']
highest_cust_count = df.iloc[0]['Customer Count']
print(f"The state with the highest number of customers is {highest_cust_state} with {highest_cust_count} customers.")

plt.show()

# Close database connection
mydb.close()

# Find and plot the sum of all transactions for the top 10 customers, and which customer has the highest transaction amount. hint(use CUST_SSN). 
import mysql.connector
import pandas as pd
import matplotlib.pyplot as plt

# Connect to MariaDB
mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="password",
  database="creditcard_capstone"
)

# Query the database
mycursor = mydb.cursor()
query = '''
    SELECT cc.CUST_SSN, cu.FIRST_NAME, cu.LAST_NAME, SUM(cc.TRANSACTION_VALUE) AS TRANSACTION_SUM
    FROM cdw_sapp_credit_card cc
    INNER JOIN cdw_sapp_customer cu ON cc.CUST_SSN = cu.SSN
    GROUP BY cc.CUST_SSN
    ORDER BY TRANSACTION_SUM DESC
    LIMIT 10
'''
mycursor.execute(query)
result = mycursor.fetchall()

# Convert the result to a pandas dataframe
df = pd.DataFrame(result, columns=["SSN", "First Name", "Last Name", "Transaction Sum"])

# Plot a pie chart to show the sum of transactions for the top 10 customers
fig, ax = plt.subplots()
ax.pie(df["Transaction Sum"], labels=df["First Name"] + " " + df["Last Name"], autopct='%1.1f%%')
ax.set_title("All Transactions of Top 10 Customers Distribution")

# Find the customer with the highest transaction amount
max_customer = df.loc[df["Transaction Sum"].idxmax()]

# Print the customer with the highest transaction amount
print(f"The sum of all transactions for the top 10 customers is ${df['Transaction Sum'].sum():.2f}.") 
print(f"The customer with the highest transaction amount is {max_customer['First Name']} {max_customer['Last Name']} with a transaction sum of ${max_customer['Transaction Sum']:.2f}.")

# Show the plot
plt.show()

# Close the database connection
mydb.close()

