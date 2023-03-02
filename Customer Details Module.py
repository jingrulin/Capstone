# Check the existing account details of a customer.
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
firstname = input("Enter customer's first name (no space): ")
lastname = input("Enter customer's last name (no space): ")
email = input("Enter customer's email address (no space): ")

# Query database
mycursor = mydb.cursor()
query = f'''
        SELECT SUBSTRING(SSN, -4) AS SSN, FIRST_NAME, MIDDLE_NAME, LAST_NAME, CREDIT_CARD_NO, FULL_STREET_ADDRESS, CUST_CITY, CUST_STATE, CUST_COUNTRY, CUST_ZIP, CUST_PHONE, CUST_EMAIL, LAST_UPDATED
        FROM cdw_sapp_customer
        WHERE FIRST_NAME = '{firstname}' AND LAST_NAME = '{lastname}' AND CUST_EMAIL = '{email}'
    '''
mycursor.execute(query)
result = mycursor.fetchall()

# Print results in a table only showing last 4 digits of SSN to protect customers' privacy
if len(result) == 0:
    print(f"No transactions found for {firstname} {lastname} in {email}")
else:
    table = PrettyTable()
    table.field_names = ['last 4 digits of SSN', 'FIRST_NAME', 'MIDDLE_NAME', 'LAST_NAME', 'CREDIT_CARD_NO', 'FULL_STREET_ADDRESS', 'CUST_CITY', 'CUST_STATE', 'CUST_COUNTRY', 'CUST_ZIP', 'CUST_PHONE', 'CUST_EMAIL', 'LAST_UPDATED']
    for row in result:
        table.add_row(row)
    print(f"Account details for {firstname} {lastname} with email addresss {email}:")
    print(table)


# Close database connection
mydb.close()

# For testing, firstname = Thurman, lastname = Vera, and email = Tvera@example.com

# Modify the existing account details of a customer.
import mysql.connector
from prettytable import PrettyTable

# Connect to MariaDB
mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="password",
  database="creditcard_capstone"
)

# Get user input for search criteria
firstname = input("Enter customer's first name (no space): ")
lastname = input("Enter customer's last name (no space): ")
email = input("Enter customer's email address (no space): ")

# Query database for search criteria
mycursor = mydb.cursor()
query = f'''
        SELECT SUBSTRING(SSN, -4) AS SSN, FIRST_NAME, MIDDLE_NAME, LAST_NAME, CREDIT_CARD_NO, FULL_STREET_ADDRESS, CUST_CITY, CUST_STATE, CUST_COUNTRY, CUST_ZIP, CUST_PHONE, CUST_EMAIL, LAST_UPDATED        FROM cdw_sapp_customer
        WHERE FIRST_NAME = '{firstname}' AND LAST_NAME = '{lastname}' AND CUST_EMAIL = '{email}'
    '''
mycursor.execute(query)
result = mycursor.fetchall()

# Print search results in a table
if len(result) == 0:
    print(f"No transactions found for {firstname} {lastname} in {email}")
else:
    table = PrettyTable()
    table.field_names = ['SSN', 'FIRST_NAME', 'MIDDLE_NAME', 'LAST_NAME', 'CREDIT_CARD_NO', 'FULL_STREET_ADDRESS', 'CUST_CITY', 'CUST_STATE', 'CUST_COUNTRY', 'CUST_ZIP', 'CUST_PHONE', 'CUST_EMAIL', 'LAST_UPDATED']
    for row in result:
        table.add_row(row)
    print(f"Account details for {firstname} {lastname} with email address {email}:")
    print(table)

    # Get user input for which field to modify and new value
    field_to_modify = input("Which field would you like to modify? Type in the field name shown in the table above, CUST_CITY for example: ")
    new_value = input(f"What is the new value for {field_to_modify}? ")

    # Update database with new value
    update_query = f'''
                    UPDATE cdw_sapp_customer
                    SET {field_to_modify} = '{new_value}'
                    WHERE FIRST_NAME = '{firstname}' AND LAST_NAME = '{lastname}' AND CUST_EMAIL = '{email}'
                    '''
    mycursor.execute(update_query)
    mydb.commit()

    # Query database again to get the updated result
    mycursor.execute(query)
    updated_result = mycursor.fetchall()

    # Print updated result in a table
    if len(updated_result) == 0:
        print(f"No transactions found for {firstname} {lastname} in {email}")
    else:
        table1 = PrettyTable()
        table1.field_names = ['SSN', 'FIRST_NAME', 'MIDDLE_NAME', 'LAST_NAME', 'CREDIT_CARD_NO', 'FULL_STREET_ADDRESS', 'CUST_CITY', 'CUST_STATE', 'CUST_COUNTRY', 'CUST_ZIP', 'CUST_PHONE', 'CUST_EMAIL', 'LAST_UPDATED']
        for row1 in updated_result:
            table1.add_row(row1)
        print(f"Updated account details for {firstname} {lastname} with email address {email}:")
        print(table1)

        print("Updated successful!")

# Close database connection
mydb.close()

# For testing, firstname = Barbra, lastname = Lau, email = BLau@example.com, field_to_modify = CUST_CITY, and new_value = Sion. Original = Zion

# Generate a monthly bill for a credit card number for a given month and year.
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
creditcardno = input("Enter credit card number: ")
month = input("Enter month (MM): ")
year = input("Enter year (YYYY): ")

# Query database for monthly bill
mycursor = mydb.cursor()
query = f'''
        SELECT *
        FROM cdw_sapp_credit_card cc
        INNER JOIN cdw_sapp_customer cu ON cu.CREDIT_CARD_NO = cc.CUST_CC_NO
        WHERE cc.CUST_CC_NO = {creditcardno} AND cc.TIMEID BETWEEN '{year}{month}01' AND '{year}{month}31'
        ORDER BY cc.TIMEID DESC
    '''
mycursor.execute(query)
result = mycursor.fetchall()

# Print monthly bill in a table
if len(result) == 0:
    print(f"No transactions found for credit card number {creditcardno} in {month}/{year}")
else:
    column_names = [i[0] for i in mycursor.description]
    table = PrettyTable()
    table.field_names = ["Transaction Data", "Transaction ID", "Transaction Type", "Transaction Value ($)"]
    total_value = 0
    for row in result:
        table.add_row([row[column_names.index('TIMEID')],
                       row[column_names.index('TRANSACTION_ID')],
                       row[column_names.index('TRANSACTION_TYPE')],
                       row[column_names.index('TRANSACTION_VALUE')]])
        total_value += row[column_names.index('TRANSACTION_VALUE')]
    print(table)
    print(f"Table above is showing all transactions in {month}/{year} for credit card number {creditcardno}.")
    print(f"As a result, its monthly bill is ${total_value:,.2f}.")

    
    
# Close database connection
mydb.close()

# For testing, creditcardno = 4210653317182916 , month = 01, and year = 2018

# Display the transactions made by a customer between two dates. Order by year, month, and day in descending order.

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
firstname = input("Enter customer's first name (no space): ")
lastname = input("Enter customer's last name (no space): ")
email = input("Enter customer's email address (no space): ")
start_date = input("Enter start date (YYYYMMDD): ")
end_date = input("Enter end date (YYYYMMDD): ")

# Query database for transactions
mycursor = mydb.cursor()
query = f'''
        SELECT cc.*, cu.*
        FROM cdw_sapp_credit_card cc
        INNER JOIN cdw_sapp_customer cu ON cu.CREDIT_CARD_NO = cc.CUST_CC_NO
        WHERE cu.FIRST_NAME = '{firstname}' AND cu.LAST_NAME = '{lastname}' AND cu.CUST_EMAIL = '{email}' AND cc.TIMEID BETWEEN '{start_date}' AND '{end_date}'
        ORDER BY cc.TIMEID DESC
    '''
mycursor.execute(query)
result = mycursor.fetchall()

# Print transactions in a table
if len(result) == 0:
    print(f"No transactions found for credit card number {creditcardno} between {start_date} and {end_date}")
else:
    column_names = [i[0] for i in mycursor.description]
    table = PrettyTable()
    table.field_names = ["Transaction Date", "Transaction ID", "Transaction Type", "Transaction Value ($)"]
    for row in result:
        table.add_row([row[column_names.index('TIMEID')],
                       row[column_names.index('TRANSACTION_ID')],
                       row[column_names.index('TRANSACTION_TYPE')],
                       row[column_names.index('TRANSACTION_VALUE')]])
    print(table)
    print(f"Table above is showing all transactions for {firstname} {lastname} with email addresss {email} between {start_date} and {end_date}.")
    
# Close database connection
mydb.close()

# For testing, firstname = Thurman, lastname = Vera, email = TVera@example.com, start_date = 20180101, cend_date = 20180331