import pytest
from pyspark.sql import SparkSession, Row
import pyspark.sql.functions as F
import sys
import os

src_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src"))
sys.path.insert(0, src_path) # resolve import issues

from transformations.query_data import top_customers, total_by_provider


# Create a single Spark session for all tests
@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[1]") \
        .appName("unit-tests") \
        .getOrCreate()


# Test for top_customers function
def test_top_customers(spark):
    # Mock data
    data = [
        Row(CUSTOMER_NUMBER="CUST001", TRANSACTION_ID="T1", TRANSACTION_VALUE=500, TRANSACTION_DATE_TIME="2022-05-01 10:00:00"),
        Row(CUSTOMER_NUMBER="CUST002", TRANSACTION_ID="T2", TRANSACTION_VALUE=200, TRANSACTION_DATE_TIME="2022-05-01 11:00:00"),
        Row(CUSTOMER_NUMBER="CUST003", TRANSACTION_ID="T3", TRANSACTION_VALUE=1000, TRANSACTION_DATE_TIME="2022-05-01 12:00:00"),
    ]
    df = spark.createDataFrame(data)

    # Call the function
    result_df = top_customers(df)

    # Collect results
    results = result_df.collect()

    # Assertions
    assert len(results) == 3
    assert results[0]["CUSTOMER_NUMBER"] == "CUST003"  # Highest transaction value should come first
    assert results[1]["TRANSACTION_VALUE"] == 500


#  Test for total_by_provider function
def test_total_by_provider(spark):
    # Mock customers
    customers_data = [
        Row(CUSTOMER_NUMBER="CUST001", CREDITCARD_PROVIDER="VISA"),
        Row(CUSTOMER_NUMBER="CUST002", CREDITCARD_PROVIDER="MASTERCARD"),
    ]
    customers_df = spark.createDataFrame(customers_data)

    # Mock transactions
    transactions_data = [
        Row(TRANSACTION_ID="T1", CUSTOMER_NUMBER="CUST001", TRANSACTION_DATE_TIME="2022-05-01 08:00:00"),
        Row(TRANSACTION_ID="T2", CUSTOMER_NUMBER="CUST002", TRANSACTION_DATE_TIME="2022-05-01 15:00:00"),  # Outside filter
    ]
    transactions_df = spark.createDataFrame(transactions_data)

    # Mock products
    products_data = [
        Row(TRANSACTION_ID="T1", ITEM_VALUE=100.0, ITEM_QUANTITY=2),
        Row(TRANSACTION_ID="T2", ITEM_VALUE=50.0, ITEM_QUANTITY=1),
    ]
    products_df = spark.createDataFrame(products_data)

    # Call the function
    result_df = total_by_provider(customers_df, transactions_df, products_df)

    # Collect results
    results = result_df.collect()

    # Assertions
    assert len(results) == 1  # Only one transaction within 00:00 –12:00 filter
    assert results[0]["CREDITCARD_PROVIDER"] == "VISA"
    assert results[0]["TOTAL_QUANTITY"] == 2
    assert results[0]["TOTAL_VALUE"] == 200.0
