Title: Loan Application and Credit Card Data ETL


Description: This is a capstone project in the end of 17 weeks of Per Scholas' Data Engineering Bootcamp powered by TEKSystems to demonstrate knowledge and abilities I have acquired throughout the course. THis project aims to build an end-to-end ETL pipeline to manage and process two datasets: a Credit Card dataset dataset and a Loan Application. The project includes data preprocessing, data cleaning, data transformation, and data loading into a MariaDB database using Apache Spark and Python.


Technologies:

1. Python: The Python programming language will be used for data preprocessing, data cleaning, and data transformation. The Pandas library will be used to manipulate and analyze the datasets. Advanced modules like Matplotlib will be used for data visualization and analysis.

2. MariaDB: The MariaDB database will be used to store the cleaned and transformed data. MariaDB is an open-source relational database management system and a drop-in replacement for MySQL.

3. Apache Spark: Apache Spark is a distributed computing system that will be used for data processing. Spark Core will be used for parallel processing, while Spark SQL will be used for data querying.

4. Python Visualization and Analytics libraries: Python Visualization and Analytics libraries like Seaborn, Plotly, and Bokeh will be used for data visualization and analysis.


Steps:

1. Data acquisition: The Credit Card datasets will be acquired as JSON files and the Loan Application dataset will be acquired from an API endpoint as a JSON file as well. Please download all four files in JSON format:

   - `CDW_SAPP_CUSTOMER.JSON`: This file contains the existing customer details.
   - `CDW_SAPP_CREDITCARD.JSON`: This file contains all credit card transaction information.
   - `CDW_SAPP_BRANCH.JSON`: Each branch’s information and details are recorded in this file.
   - API Endpoint: https://raw.githubusercontent.com/platformps/LoanDataset/main/loan_data.json

   Note: the API endpoint dataset will be loaned to database as a downloaded JSON file due to technical difficulties.

2. Data preprocessing: The Credit Card datasets will be preprocessed according to the Mapping document which is include in my repo.

3. Data cleaning: The datasets will be cleaned by applying various data cleaning techniques like data normalization, standardization, and outlier removal.

4. Data transformation: The cleaned datasets will be transformed by applying data aggregation, joining, and filtering operations.

5. Data loading: The transformed data will be loaded into a MariaDB database called “creditcard_capstone” using the Python MySQL Connector. The following tables will be created in the database:
   - `CDW_SAPP_BRANCH`
   - `CDW_SAPP_CREDIT_CARD`
   - `CDW_SAPP_CUSTOMER`
   - `CDW_SAPP_LOAN_APPLICATION`

6. Data analysis and visualization: The transformed data will be analyzed and visualized using Python Visualization and Analytics libraries with a front-end console-based Python program to see/display data. Basically it will for user inputs to call for the data anlysis.

7. Deployment: The final ETL pipeline will be deployed on local machines and tested.

This capstone project requires technologies to manage an ETL process for a Credit Card dataset and a Loan Application dataset: Python (Pandas, advanced modules e.g., Matplotlib), MariaDB, Apache Spark (Spark Core, Spark SQL), and Python Visualization and Analytics libraries. I will set up environments and perform installations on local machines. 

Installation: In order to install my project, please download everything my repo and use requirements.txt for python libraries that need to be installed.

Examples: This capstone project will be useful in the real world for a bank teller using the front-end console-based Python program to see/display data in the bank and business development.

Contribution: Thank you to Sammi and Ben for all the supports and lectures over the 17 weeks! You are the best!

Credits: Give credit to any contributors, third-party libraries, or resources that you used in your project.

Contact: For any questions or feedback, contact the author at jingrulin123@gmail.com.







