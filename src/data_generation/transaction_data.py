import uuid
import random
from faker import Faker
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

fake = Faker()

def generate_transaction_data(spark, df_customers, num_records=2000):
    customers = [row["CUSTOMER_NUMBER"] for row in df_customers.collect()]
    
    transactions_data = []
    for _ in range(num_records):
        transaction_id = str(uuid.uuid4())  
        customer_number = random.choice(customers)
        transaction_date_time = fake.date_time_between(
            start_date=datetime(2022, 1, 1), 
            end_date=datetime(2022, 12, 31)
        )   
        
        record = (
            transaction_id,
            customer_number,
            0.0,
            transaction_date_time
        )
        transactions_data.append(record)

    schema = StructType([
        StructField("TRANSACTION_ID", StringType(), False),
        StructField("CUSTOMER_NUMBER", StringType(), False),
        StructField("TRANSACTION_VALUE", DoubleType(), False),
        StructField("TRANSACTION_DATE_TIME", TimestampType(), False)
    ])
    
    return spark.createDataFrame(transactions_data, schema)
