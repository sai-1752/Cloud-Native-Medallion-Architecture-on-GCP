from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, sum as _sum, countDistinct

# ---------------------------------------------------
# Initialize Spark Session
# ---------------------------------------------------
spark = SparkSession.builder.appName("MedallionArchitectureETL").getOrCreate()

bronze_path = "gs://e-commerce-bronze/raw/ecommerce_transactions.csv"

df = spark.read.option("header", True) \
               .option("inferSchema", True) \
               .csv(bronze_path)

print("=== BRONZE SCHEMA ===")
df.printSchema()

df_clean = df.dropDuplicates().na.drop()

# Convert Transaction_Date to proper date format
df_clean = df_clean.withColumn(
    "transaction_date",
    to_date(col("Transaction_Date"))
)

# Write to SILVER bucket as partitioned Parquet
silver_path = "gs://e-commerce-silver/transactions/"

df_clean.write.mode("overwrite") \
        .partitionBy("transaction_date") \
        .parquet(silver_path)

print("Silver layer written successfully.")


gold_daily_revenue = df_clean.groupBy("transaction_date") \
    .agg(
        _sum("Purchase_Amount").alias("total_revenue"),
        countDistinct("User_Name").alias("unique_customers")
    )

gold_daily_revenue.write.mode("overwrite") \
    .parquet("gs://e-commerce-gold/daily_revenue/")

print("Gold daily revenue table written successfully.")

gold_category_perf = df_clean.groupBy("Product_Category") \
    .agg(
        _sum("Purchase_Amount").alias("total_revenue"),
        countDistinct("User_Name").alias("unique_customers")
    )

gold_category_perf.write.mode("overwrite") \
    .parquet("gs://e-commerce-gold/category_performance/")

print("Gold category performance table written successfully.")

# ---------------------------------------------------
# Stop Spark Session
# ---------------------------------------------------
spark.stop()

print("ETL Pipeline Completed Successfully.")