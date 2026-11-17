# Databricks notebook source
df_orders_silver = spark.table(
    "workspace.quickkart.silver_orders"
)

df_order_items_silver = spark.table(
    "workspace.quickkart.silver_order_items"
)

df_products_silver = spark.table(
    "workspace.quickkart.silver_products"
)

# COMMAND ----------

df_orders_silver.printSchema()
df_order_items_silver.printSchema()
df_products_silver.printSchema()

# COMMAND ----------

spark.sql("""
SHOW TABLES IN workspace.quickkart
""").show(truncate=False)

# COMMAND ----------

spark.sql("""
SHOW TABLES IN workspace.quickkart
""").show(truncate=False)

# COMMAND ----------

df_orders_silver = spark.table(
    "workspace.quickkart.silver_orders"
)

df_order_items_silver = spark.table(
    "workspace.quickkart.silver_order_items"
)

df_products_silver = spark.table(
    "workspace.quickkart.silver_products"
)

# COMMAND ----------

print("Orders:", df_orders_silver.count())
print("Order Items:", df_order_items_silver.count())
print("Products:", df_products_silver.count())

# COMMAND ----------

df_total_order_table = df_orders_silver.join(
    df_order_items_silver,
    on="order_id",
    how="inner"
)

display(df_total_order_table)

# COMMAND ----------

from pyspark.sql.functions import *
df_total_order_table.count()

# COMMAND ----------

df_All_products=df_total_order_table.join(df_products_silver,
on="product_id",
how="inner")

# COMMAND ----------

df_All_products.count()

# COMMAND ----------

df_discount=1-(col("discount_pct")/100)
df_All_products=df_All_products.withColumn("total_revenue",col("quantity")*col("unit_price")*(df_discount))

# COMMAND ----------

display(df_All_products)

# COMMAND ----------

df_All_products = df_total_order_table.join(
    df_products_silver,
    on="product_id",
    how="inner"
).select(
    df_total_order_table["*"],
    df_products_silver["product_name"],
    df_products_silver["category"]
)

# COMMAND ----------

display(df_All_products)

# COMMAND ----------

df_products_revenue = df_All_products.groupBy("category").agg(
    sum("total_revenue").alias("total_category_revenue")
)

# COMMAND ----------

print(df_All_products.columns)

# COMMAND ----------

df_products_revenue = df_All_products.groupBy("category").agg(
    sum("total_revenue").alias("total_category_revenue")
)

# COMMAND ----------

df_products_revenue = df_products_revenue.orderBy(
    col("total_category_revenue").desc()
)

display(df_products_revenue)

# COMMAND ----------

df_stores_silver = spark.table(
    "workspace.quickkart.silver_stores"
)

# COMMAND ----------

df_sales_by_store = df_All_products.join(
    df_stores_silver,
    on="store_id",
    how="inner"
)

# COMMAND ----------

print("Joined records:", df_sales_by_store.count())
display(df_sales_by_store)

# COMMAND ----------

df_store_revenue = df_sales_by_store.groupBy("store_name").agg(
    sum("total_revenue").alias("total_store_revenue")
)


# COMMAND ----------

df_store_revenue = df_store_revenue.orderBy(
    col("total_store_revenue").desc()
)

# COMMAND ----------

display(df_store_revenue)

# COMMAND ----------

df_order_status_summary = df_orders_silver.groupBy(
    "order_status"
).count()

# COMMAND ----------



display(df_order_status_summary)

# COMMAND ----------

total_orders = df_orders_silver.count()

delivered_orders = df_orders_silver.filter(
    col("order_status") == "Delivered"
).count()

delivery_success_rate = (delivered_orders / total_orders) * 100

print("Total Orders:", total_orders)
print("Delivered Orders:", delivered_orders)
print("Delivery Success Rate:",__builtins__.round(delivery_success_rate, 2), "%")

# COMMAND ----------

from pyspark.sql.functions import sum, col

df_top_products = df_All_products.groupBy(
    "product_id", "product_name"
).agg(
    sum("quantity").alias("total_quantity_sold")
)

# COMMAND ----------

df_top_products = df_top_products.orderBy(
    col("total_quantity_sold").desc()
).limit(10).display()

# COMMAND ----------

from pyspark.sql.functions import sum, col, row_number
from pyspark.sql.window import Window

df_top3_category = (
    df_All_products
    .groupBy("category", "product_id", "product_name")
    .agg(sum("quantity").alias("total_quantity_sold"))
    .withColumn(
        "rank",
        row_number().over(
            Window.partitionBy("category").orderBy(col("total_quantity_sold").desc())
        )
    )
    .filter(col("rank") <= 3)
    .orderBy("category", "rank")
)

display(df_top3_category)

# COMMAND ----------

df_inventory_silver = spark.table("workspace.quickkart.silver_inventory")

display(df_inventory_silver)

# COMMAND ----------

df_low_stock = df_inventory_silver.filter(
    col("stock_quantity") < 20
)

display(df_low_stock)

# COMMAND ----------

df_inventory_details = df_low_stock.join(
    df_products_silver,
    on="product_id",
    how="inner"
).join(
    df_stores_silver,
    on="store_id",
    how="inner"
).select(
    "product_id",
    "product_name",
    "category",
    "store_id",
    "store_name",
    "stock_quantity"
)

display(df_inventory_details)

# COMMAND ----------

df_store_inventory = df_inventory_silver.groupBy(
    "store_id"
).agg(
    sum("stock_quantity").alias("total_stock")
).join(
    df_stores_silver,
    on="store_id",
    how="inner"
).select(
    "store_id",
    "store_name",
    "total_stock"
).orderBy(
    col("total_stock").desc()
)

display(df_store_inventory)

# COMMAND ----------

spark.sql("SHOW TABLES IN workspace.quickkart").display()

# COMMAND ----------

df_orders_silver = spark.table("workspace.quickkart.silver_orders")
df_order_items_silver = spark.table("workspace.quickkart.silver_order_items")
df_products_silver = spark.table("workspace.quickkart.silver_products")
df_stores_silver = spark.table("workspace.quickkart.silver_stores")
df_inventory_silver = spark.table("workspace.quickkart.silver_inventory")
df_deliveries_silver = spark.table("workspace.quickkart.silver_deliveries")
df_customers_silver = spark.table("workspace.quickkart.silver_customers")

print("All Silver DataFrames loaded successfully!")

# COMMAND ----------

# Save Gold DataFrames as Delta tables

df_products_revenue.write.format("delta").mode("overwrite").saveAsTable("workspace.quickkart.gold_sales_by_category")

df_store_revenue.write.format("delta").mode("overwrite").saveAsTable("workspace.quickkart.gold_sales_by_store")

df_order_status_summary.write.format("delta").mode("overwrite").saveAsTable("workspace.quickkart.gold_order_status_summary")

df_top_products.write.format("delta").mode("overwrite").saveAsTable("workspace.quickkart.gold_top_products")

df_top3_category.write.format("delta").mode("overwrite").saveAsTable("workspace.quickkart.gold_top3_category")

df_low_stock.write.format("delta").mode("overwrite").saveAsTable("workspace.quickkart.gold_low_stock")

df_store_inventory.write.format("delta").mode("overwrite").saveAsTable("workspace.quickkart.gold_store_inventory")

print("All Gold tables saved successfully!")

# COMMAND ----------

print("Category Revenue:", df_products_revenue.count())
print("Store Revenue:", df_store_revenue.count())
print("Order Status:", df_order_status_summary.count())
print("Top Products:", df_top_products.count())
print("Top 3 by Category:", df_top3_category.count())
print("Low Stock:", df_low_stock.count())
print("Store Inventory:", df_store_inventory.count())

# COMMAND ----------

spark.sql("SHOW TABLES IN workspace.quickkart").display()

# COMMAND ----------

