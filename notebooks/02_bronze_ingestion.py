# Databricks notebook source
from pyspark.sql.functions import current_timestamp, input_file_name

base_path = "/Volumes/workspace/quickkart/quickkart_source"

print("QuickKart Bronze Ingestion Started")

# COMMAND ----------

customers_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(f"{base_path}/customers")
)

display(customers_df.limit(5))

# COMMAND ----------

customers_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.quickkart.bronze_customers")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.quickkart.bronze_customers
# MAGIC LIMIT 5;

# COMMAND ----------

datasets = [
    "products",
    "stores",
    "orders",
    "order_items",
    "inventory",
    "deliveries"
]

for name in datasets:

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(f"{base_path}/{name}")
    )

    table_name = f"workspace.quickkart.bronze_{name}"

    df.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(table_name)

    print(f"Bronze table created: {table_name}")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN workspace.quickkart;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT 'bronze_customers' AS table_name, COUNT(*) AS total_records
# MAGIC FROM workspace.quickkart.bronze_customers
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'bronze_products', COUNT(*)
# MAGIC FROM workspace.quickkart.bronze_products
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'bronze_stores', COUNT(*)
# MAGIC FROM workspace.quickkart.bronze_stores
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'bronze_orders', COUNT(*)
# MAGIC FROM workspace.quickkart.bronze_orders
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'bronze_order_items', COUNT(*)
# MAGIC FROM workspace.quickkart.bronze_order_items
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'bronze_inventory', COUNT(*)
# MAGIC FROM workspace.quickkart.bronze_inventory
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'bronze_deliveries', COUNT(*)
# MAGIC FROM workspace.quickkart.bronze_deliveries;