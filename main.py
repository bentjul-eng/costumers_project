from src.utils.spark_session import get_spark_session
from src.data_generation.customer_data import generate_customer_data
from src.data_generation.transaction_data import generate_transaction_data
from src.data_generation.product_transactions import generate_product_transactions
from src.transformations.transactions_update import update_transaction_values
from src.utils.write_functions import write_parquet, write_parquet_partitioned, write_json
from src.transformations.query_data import top_customers, total_by_provider 


def main(bronze_path="data/bronze", silver_path="data/silver", gold_path="data/gold", spark=None):
    if not spark:
        spark = get_spark_session()

    # PHASE 1: DATA GENERATION
    customer_df = generate_customer_data(spark)
    write_parquet(customer_df, f"{bronze_path}/customers")

    creditcard_df = generate_transaction_data(spark, customer_df)
    write_parquet(creditcard_df, f"{bronze_path}/transactions")

    txn_ids = [row["TRANSACTION_ID"] for row in creditcard_df.collect()]
    df_products = generate_product_transactions(spark, txn_ids)
    write_parquet(df_products, f"{bronze_path}/products")

    # PHASE 2: DATA TRANSFORMATION 
    df_transactions_updated = update_transaction_values(creditcard_df, df_products)
    write_parquet_partitioned(
        df_transactions_updated, f"{silver_path}/transactions", "ANO_MES"
    )


     # PHASE 3: ANALYTICS (GOLD) 

    top_customers_df = top_customers(df_transactions_updated)
    top_customers_df.show(truncate=False)
    write_json(top_customers_df, f"{gold_path}/top_customers")

    totals_by_provider_df = total_by_provider(customer_df, df_transactions_updated, df_products)
    totals_by_provider_df.show(truncate=False)
    write_json(totals_by_provider_df, f"{gold_path}/totals_by_provider")

if __name__ == "__main__":
    main()
