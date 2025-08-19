from pyspark.sql import functions as F

def update_transaction_values(df_transactions, df_products):
    txn_sums = (
        df_products
        .withColumn("TOTAL_ITEM_VALUE", F.col("ITEM_VALUE") * F.col("ITEM_QUANTITY"))
        .groupBy("TRANSACTION_ID")
        .agg(F.sum("TOTAL_ITEM_VALUE").alias("NEW_TRANSACTION_VALUE"))
    )

    df_transactions_updated = (
        df_transactions
        .join(txn_sums, on="TRANSACTION_ID", how="left")
        .withColumn(
            "TRANSACTION_VALUE", 
            F.coalesce(F.col("NEW_TRANSACTION_VALUE"), F.col("TRANSACTION_VALUE"))
        )
        .drop("NEW_TRANSACTION_VALUE")
    )

    # Add year and month (partitions) columns based on the TRANSACTION_DATE_TIME column
    df_transactions_updated = (
        df_transactions_updated
        .withColumn(
            "ANO_MES", 
            F.date_format(F.col("TRANSACTION_DATE_TIME"), "yyyy-MM"))
    )

    return df_transactions_updated
