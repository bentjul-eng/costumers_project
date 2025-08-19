from faker import Faker
import random
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

fake = Faker()

def generate_customer_data(spark, num_records=200, seed=42):
    Faker.seed(seed)
    random.seed(seed)
    
    data = []
    for _ in range(num_records):
        customer_number = f"{fake.lexify(text='????')}{fake.numerify(text='####')}"
        record = (
            customer_number,
            fake.first_name(),
            fake.last_name(),
            fake.date_of_birth(minimum_age=18).strftime("%Y-%m-%d"),
            fake.ssn(),
            fake.street_name(),
            random.randint(1, 9999),
            fake.city(),
            fake.state(),
            fake.country(),
            fake.postcode(),
            fake.credit_card_number(),
            fake.credit_card_expire(),
            fake.credit_card_security_code(),
            fake.credit_card_provider()
        )
        data.append(record)

    schema = StructType([
        StructField("CUSTOMER_NUMBER", StringType(), False),
        StructField("FIRST_NAME", StringType(), False),
        StructField("LAST_NAME", StringType(), False),
        StructField("BIRTH_DATE", StringType(), False),
        StructField("SSN", StringType(), False),
        StructField("CUSTOMER_ADDRESS_STREET", StringType(), False),
        StructField("CUSTOMER_ADDRESS_HOUSE_NUMBER", IntegerType(), False),
        StructField("CUSTOMER_ADDRESS_CITY", StringType(), False),
        StructField("CUSTOMER_ADDRESS_STATE", StringType(), False),
        StructField("CUSTOMER_ADDRESS_COUNTRY", StringType(), False),
        StructField("CUSTOMER_ADDRESS_ZIP_CODE", StringType(), False),
        StructField("CREDITCARD_NUMBER", StringType(), False),
        StructField("CREDITCARD_EXPIRATION_DATE", StringType(), False),
        StructField("CREDITCARD_VERIFICATION_CODE", StringType(), False),
        StructField("CREDITCARD_PROVIDER", StringType(), False)
    ])

    return spark.createDataFrame(data, schema)
