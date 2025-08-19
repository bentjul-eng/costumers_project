from faker import Faker
import random
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

fake = Faker()

def generate_product_transactions(spark, txn_ids):
    departments = [fake.word() for _ in range(50)] 
    
    product_data = []
    for txn_id in txn_ids:
        num_items = random.randint(1, 5)
        for _ in range(num_items):
            product_data.append((
                txn_id,
                fake.ean13(),
                random.choice(departments),
                round(random.uniform(1, 100000), 2),
                random.randint(1, 200)
            ))

    schema = StructType([
        StructField("TRANSACTION_ID", StringType(), False),
        StructField("ITEM_EAN", StringType(), False),
        StructField("ITEM_DEPARTMENT", StringType(), False),
        StructField("ITEM_VALUE", DoubleType(), False),
        StructField("ITEM_QUANTITY", IntegerType(), False)
    ])

    return spark.createDataFrame(product_data, schema)
