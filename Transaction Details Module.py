# Display transaction by customers by their given living zip code for agiven month and year. Order by day in descending order
import mysql.connector
from prettytable import PrettyTable

# Connect to MariaDB
mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="password",
  database="creditcard_capstone"
)

# Get user input
zip_code = input("Enter zip code (5-digits): ")
month = input("Enter month (MM): ")
year = input("Enter year (YYYY): ")

# Query database
mycursor = mydb.cursor()
query = f'''
        SELECT *
        FROM cdw_sapp_credit_card cc
        INNER JOIN cdw_sapp_customer cu ON cu.CREDIT_CARD_NO = cc.CUST_CC_NO
        WHERE cu.CUST_ZIP = {zip_code} AND cc.TIMEID BETWEEN '{year}{month}01' AND '{year}{month}31'
        ORDER BY cc.TIMEID DESC
    '''
mycursor.execute(query)
result = mycursor.fetchall()

# Print results
if len(result) == 0:
    print("No transactions found")
else:
    column_names = [i[0] for i in mycursor.description]
    table = PrettyTable()
    table.field_names = ["Transaction Date", "Transaction ID", "Transaction Type", "Transaction Value", "Customer First Name", "Customer Last Name"]
    for row in result:
        table.add_row([row[column_names.index('TIMEID')],
                       row[column_names.index('TRANSACTION_ID')], 
                       row[column_names.index('TRANSACTION_TYPE')], 
                       row[column_names.index('TRANSACTION_VALUE')], 
                       row[column_names.index('FIRST_NAME')],
                       row[column_names.index('LAST_NAME')]])
    print(f"Here is the table of transactions made by customers living in zip code of {zip_code} in {month}/{year} in ascending day order:")
    print(table)

# Close database connection
mydb.close()

# For testing, do zip_code = 17201, month = 08, year = 2018

# Display the number and total values of transactions for a given type.
import mysql.connector
from prettytable import PrettyTable

# Connect to MariaDB
mydb1 = mysql.connector.connect(
  host="localhost",
  user="root",
  password="password",
  database="creditcard_capstone"
)

# Get user input
transaction_type = input("Enter a transaction type from bills, education, grocery, healthcare, entertainment, gas, and test: ")

# Query database
mycursor1 = mydb1.cursor()
query = f'''
        SELECT TRANSACTION_ID, TRANSACTION_VALUE 
        FROM cdw_sapp_credit_card 
        WHERE TRANSACTION_TYPE = '{transaction_type}'
    '''
mycursor1.execute(query)
result1 = mycursor1.fetchall()

# Create a table to display the results
table1 = PrettyTable()
table1.field_names = ["Transaction ID", "Transaction Value"]

# Iterate through the results to calculate the number and total value of transactions
total_value = 0
num_transactions = 0
for row in result1:
    transaction_id = row[0]
    transaction_value = row[1]
    total_value += transaction_value
    num_transactions += 1
    
    # Add row to table
    table1.add_row([transaction_id, transaction_value])

# Display the results
print(f"Number of {transaction_type} type of transactions: {num_transactions:,}")
print(f"Total value of {transaction_type} type of transactions: {total_value:,.2f}")
print(f"Here is the table of {transaction_type} type of transactions: \n{table1}")

# Close database connection
mydb.close()

# For testing, do transaction_type = bills

# Display the total number and total values of transactions for branches in a given state.
import mysql.connector
from prettytable import PrettyTable

# Connect to MariaDB
mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="password",
  database="creditcard_capstone"
)

# Get user input
state = input("Enter a state in two letters for your branch, PA for Pennsylvania for example: ")

# Query database
mycursor = mydb.cursor()
query = f'''
        SELECT b.BRANCH_CODE, c.TRANSACTION_ID, c.TRANSACTION_VALUE
        FROM cdw_sapp_credit_card c 
        INNER JOIN cdw_sapp_branch b ON c.BRANCH_CODE = b.BRANCH_CODE 
        WHERE b.BRANCH_STATE = '{state}'
    '''
mycursor.execute(query)
result = mycursor.fetchall()

# Print results in a table
if len(result) == 0:
    print(f"No transactions found for branches in {state}")
else:
    table = PrettyTable()
    table.field_names = ["Branch Code", "Transaction ID", "Transaction Value"]
    # Iterate through the results to calculate the number and total value of transactions
    total_value = 0
    num_transactions = 0
    for row in result:
        branch_code = row[0]
        transaction_id = row[1]
        transaction_value = row[2]
        total_value += transaction_value
        num_transactions += 1
        
        # Add row to table
        table.add_row([branch_code, transaction_id, transaction_value])

    print(f"Total Number of transactions in {state} state branches: {num_transactions:,}")
    print(f"Total value of transactions in {state} state branches in USD: {total_value:,.2f}")
    print(f"Here is the table of transactions in PA state branches: \n{table}")

# Close database connection
mydb.close()

# For testing, state = PA
