from pyspark.sql import functions as F

def top_customers(transactions_df):
    result = (transactions_df
              .select("CUSTOMER_NUMBER", "TRANSACTION_ID", "TRANSACTION_VALUE", "TRANSACTION_DATE_TIME")
              .orderBy(F.desc("TRANSACTION_VALUE"))
              .limit(100))
    
    return result


def total_by_provider(customers_df, transactions_df, products_df):
    joined_df = (transactions_df
                .join(customers_df, "CUSTOMER_NUMBER", "inner")
                .join(products_df, "TRANSACTION_ID", "inner"))
    
    # Ensure TRANSACTION_DATE_TIME is in timestamp format
    transactions_df = transactions_df.withColumn(
        "TRANSACTION_DATE_TIME",
        F.to_timestamp("TRANSACTION_DATE_TIME")
    )
    
    # Filter transactions between 00:00 and 12:00
    filtered_df = joined_df.filter(
        (F.hour(F.col("TRANSACTION_DATE_TIME")) >= 0) & 
        (F.hour(F.col("TRANSACTION_DATE_TIME")) < 12)
    )

    result = (filtered_df
              .withColumn("TOTAL_ITEM_VALUE", F.col("ITEM_VALUE") * F.col("ITEM_QUANTITY"))
              .groupBy("CREDITCARD_PROVIDER")
              .agg(
                  F.sum("ITEM_QUANTITY").alias("TOTAL_QUANTITY"),
                  F.sum("TOTAL_ITEM_VALUE").alias("TOTAL_VALUE")
              )
             )
    
    return result

