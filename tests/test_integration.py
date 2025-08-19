import pytest
import sys
import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
)
from unittest.mock import patch

src_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src"))
sys.path.insert(0, src_path)

from main import main


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[1]") \
        .appName("integration-tests") \
        .getOrCreate()


def create_mock_bronze_data(spark):
    """Create mock data to simulate the Bronze layer"""

    # Customers schema
    customers_schema = StructType([
        StructField("CUSTOMER_NUMBER", StringType(), True),
        StructField("CREDITCARD_PROVIDER", StringType(), True),
    ])
    customers_data = [
        ("CUST001", "VISA"),
        ("CUST002", "MASTERCARD"),
        ("CUST003", "VISA")
    ]
    customers_df = spark.createDataFrame(customers_data, schema=customers_schema)

    # Transactions schema
    transactions_schema = StructType([
        StructField("TRANSACTION_ID", StringType(), True),
        StructField("CUSTOMER_NUMBER", StringType(), True),
        StructField("TRANSACTION_VALUE", DoubleType(), True),
        StructField("TRANSACTION_DATE_TIME", StringType(), True)
    ])
    transactions_data = [
        ("T001", "CUST001", 1000.0, "2022-05-01 10:00:00"),
        ("T002", "CUST002", 500.0, "2022-05-01 11:00:00"),
        ("T003", "CUST003", 750.0, "2022-06-01 09:00:00")
    ]
    transactions_df = spark.createDataFrame(transactions_data, schema=transactions_schema)

    # Products schema
    products_schema = StructType([
        StructField("TRANSACTION_ID", StringType(), True),
        StructField("ITEM_VALUE", DoubleType(), True),
        StructField("ITEM_QUANTITY", IntegerType(), True)
    ])
    products_data = [
        ("T001", 100.0, 10),
        ("T002", 50.0, 10),
        ("T003", 75.0, 10)
    ]
    products_df = spark.createDataFrame(products_data, schema=products_schema)

    return customers_df, transactions_df, products_df


def create_mock_silver_data(spark):
    """Create mock data to simulate the Silver layer with transformations applied"""

    silver_schema = StructType([
        StructField("TRANSACTION_ID", StringType(), True),
        StructField("CUSTOMER_NUMBER", StringType(), True),
        StructField("TRANSACTION_VALUE", DoubleType(), True),
        StructField("TRANSACTION_DATE_TIME", StringType(), True), 
        StructField("ANO_MES", StringType(), True)
    ])

    silver_data = [
        ("T001", "CUST001", 1000.0, "2022-05-01 10:00:00", "2022-05"),
        ("T002", "CUST002", 500.0, "2022-05-01 11:00:00", "2022-05"),
        ("T003", "CUST003", 750.0, "2022-06-01 09:00:00", "2022-06")
    ]

    return spark.createDataFrame(silver_data, schema=silver_schema)


def create_mock_gold_data(spark):
    """Create mock data to simulate the Gold layer with aggregations"""

    # Top customers schema
    top_customers_schema = StructType([
        StructField("CUSTOMER_NUMBER", StringType(), True),
        StructField("TRANSACTION_VALUE", DoubleType(), True),
        StructField("TRANSACTION_DATE_TIME", StringType(), True)
    ])
    top_customers_data = [
        ("CUST001", 1000.0, "2022-05-01 10:00:00"),
        ("CUST003", 750.0, "2022-06-01 09:00:00"),
        ("CUST002", 500.0, "2022-05-01 11:00:00")
    ]
    top_customers_df = spark.createDataFrame(top_customers_data, schema=top_customers_schema)

    # Totals by provider schema
    totals_by_provider_schema = StructType([
        StructField("CREDITCARD_PROVIDER", StringType(), True),
        StructField("TOTAL_QUANTITY", IntegerType(), True),
        StructField("TOTAL_VALUE", DoubleType(), True)
    ])
    totals_by_provider_data = [
        ("VISA", 20, 1750.0),
        ("MASTERCARD", 10, 500.0)
    ]
    totals_by_provider_df = spark.createDataFrame(totals_by_provider_data, schema=totals_by_provider_schema)

    return top_customers_df, totals_by_provider_df


def test_pipeline_integration_with_mocks(spark):
    """
    Integration test for the pipeline using mocked data.
    Validates all three layers (Bronze, Silver, Gold) and business rules.
    """

    # Create mock data
    mock_bronze_customers, mock_bronze_transactions, mock_bronze_products = create_mock_bronze_data(spark)
    mock_silver_transactions = create_mock_silver_data(spark)
    mock_top_customers, mock_totals_by_provider = create_mock_gold_data(spark)

    # Mock parquet read/write operations
    with patch('pyspark.sql.DataFrameReader.parquet') as mock_read, \
         patch('pyspark.sql.DataFrameWriter.parquet') as mock_write_parquet, \
         patch('pyspark.sql.DataFrameWriter.json') as mock_write_json:


        def mock_parquet_read(path):
            if 'bronze/customers' in path:
                return mock_bronze_customers
            elif 'bronze/transactions' in path:
                return mock_bronze_transactions
            elif 'bronze/products' in path:
                return mock_bronze_products
            elif 'silver/transactions' in path:
                return mock_silver_transactions
            elif 'gold/top_customers' in path:
                return mock_top_customers
            elif 'gold/totals_by_provider' in path:
                return mock_totals_by_provider
            else:
                raise ValueError(f"Unmocked path: {path}")

        mock_read.side_effect = mock_parquet_read
        mock_write_parquet.return_value = None
        mock_write_json.return_value = None

        # Run the pipeline
        main(
            bronze_path="/mock/bronze",
            silver_path="/mock/silver",
            gold_path="/mock/gold",
            spark=spark
        )

        # Validate the results
   # 1. Bronze Layer - raw data
    assert mock_bronze_customers.count() == 3, "Bronze customers should have 3 records"
    assert mock_bronze_transactions.count() == 3, "Bronze transactions should have 3 records"
    assert mock_bronze_products.count() == 3, "Bronze products should have 3 records"
    assert "CUSTOMER_NUMBER" in mock_bronze_customers.columns
    assert "CREDITCARD_PROVIDER" in mock_bronze_customers.columns

    # 2. Silver Layer - transformed data
    assert "ANO_MES" in mock_silver_transactions.columns, "Silver should have ANO_MES partition column"
    assert mock_silver_transactions.count() == 3, "Silver should have 3 transactions"
    assert mock_silver_transactions.filter(F.col("TRANSACTION_VALUE") < 0).count() == 0, \
        "Silver should not have negative values"

    # 3. Gold Layer - aggregated data
    assert mock_top_customers.count() <= 100, "Top customers should be limited to 100 records"
    assert mock_top_customers.count() > 0, "Top customers should have data"
    assert "CREDITCARD_PROVIDER" in mock_totals_by_provider.columns
    assert mock_totals_by_provider.count() == 2, "Should have 2 providers"

    # 4. Business Rules - consistency between layers
    top_customer_max = mock_top_customers.agg(F.max("TRANSACTION_VALUE")).first()[0]
    silver_max = mock_silver_transactions.agg(F.max("TRANSACTION_VALUE")).first()[0]
    assert top_customer_max == silver_max == 1000.0, \
        f"Top customer max ({top_customer_max}) should match Silver max ({silver_max})"

    print("Pipeline integration test passed")
