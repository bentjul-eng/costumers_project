# LINKIT Data Engineer assignment - v2.2024.002

## Important: Please read this whole assignment text before starting to develop the solution.

The objective of this test is to evaluate your software development and design skills. Please create my_experience.md file outlining the instructions to execute the code and any issues you faced and assumptions you came into. If certain parts of the test is incomplete you can mention this too in the file. 

Consider performance, maintainability and scaling during the development, using programming best practices and developing your code with seperation of concerns is highly encouraged. You are free to execute this exercise in any cloud environment of your choice or even in your local laptop, but we expect the following:

- Include the instructions to execute the application.
- Please use a mainstream language such as Python, Scala or Java for the coding.
- Apache Spark should be used for querying the data.
- We do not accept Spark SQL queries to generate the results, instead use the spark dataframe API functionalities.
- Adding appropriate unit testing to your code is highly encouraged and a definite plus point.
- You are free to choose file formats and different packages and copy paste snippets of code from the internet, but make sure you are able to explain your choices and code in the final technical interview. 

## **01 - Data Generation:**

Generate and save 3 sets of data outlined below using the python Faker Library (https://faker.readthedocs.io/en/master/index.html). You are free to choose the file format for the tables.

**Table 1 (DS1): CUSTOMER_DATA**

Contains 200 records

- CUSTOMER_NUMBER - Should have the structure: 4 letters + 4 numbers - e.g.: AAAA0001;
- FIRST_NAME;
- LAST_NAME;
- BIRTH_DATE - Considering minimum age of 18;
- SSN;
- CUSTOMER_ADDRESS.STREET;
- CUSTOMER_ADDRESS.HOUSE_NUMBER;
- CUSTOMER_ADDRESS.CITY;
- CUSTOMER_ADDRESS.STATE;
- CUSTOMER_ADDRESS.COUNTRY;
- CUSTOMER_ADDRESS.ZIP_CODE;
- CREDITCARD.NUMBER;
- CREDITCARD.EXPIRATION_DATE;
- CREDITCARD.VERIFICATION_CODE;
- CREDITCARD.PROVIDER;

**Table 2 (DS2): CREDITCARD_TRANSACTION** 

**This table will have the transaction data for each transaction, contains 2000 records**

- TRANSACTION_ID - Random UUID (Universal Unique Identifier);
- CUSTOMER_NUMBER - Random key from CUSTOMER_DATA;
- TRANSACTION_VALUE - Sum of ITEM_VALUE X ITEM_QUANTITY in the PRODUCT_TRANSACTION table for that TRANSACTION_ID;
- TRANSACTION_DATE_TIME - Random DATE TIME in the year of 2022;

**Table 3 (DS3):  PRODUCT_TRANSACTION**

**This table will have all items that is part of one transaction, contains 2000 records**

- TRANSACTION_ID - Consistent with CREDITCARD_TRANSACTION.TRANSACTION_ID, as it will be used for joins;
- ITEM_EAN - Random EAN (European Article Number/International Article Number);
- ITEM_DEPARTMENT - Any random 50 different words;
- ITEM_VALUE - Random float smaller than 100.000;
- ITEM_QUANTITY - Random number between 1 and 200 for a item in a transaction.

## **02 - Querying Data**

- Generate a dataframe that contains the top 100 customers that has performed the biggest transactions in one single transaction, the records should be ordered by transaction value, the dataframe should contain the following fields:
    - CUSTOMER_ID
    - TRANSACTION_ID
    - TRANSACTION_VALUE
    - TRANSACTION_DATE_TIME

- Generate a dataframe that contains total quantity and total value purchased by CREDITCARD.PROVIDER, where sales happened between midnight and noon. The dataframe should have the following fields:
    - CREDITCARD.PROVIDER
    - Total quantity of items
    - Total value of items

## **03 - JSON output**

Generate one JSON file per customer of the top 100 customers by biggest transaction value in one single transaction, each file should contain:
    - All customer details
    - All details of the customers biggest transaction by value
    - All product details of that transaction

## **Bonus questions**

Attempt any one of the below tasks if you like to show off your skills and has some extra time.

- Containerize the application, make sure data generation and data querying can be executed using the container image.
- Create an application architecture diagram to productionize this application in the cloud, highlight the different cloud services you would use and there integrations.
- Publish the creditcard_transactions data to a kafka message queue, ingest the data and save it using spark streaming. Make sure this executes with your container image.

Goodluck!!!
