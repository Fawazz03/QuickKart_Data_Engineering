# Databricks notebook source
base_path = "/Volumes/workspace/quickkart/quickkart_source"

customers_df = spark.read.option("header", True).option("inferSchema", True).csv(f"{base_path}/customers")

products_df = spark.read.option("header", True).option("inferSchema", True).csv(f"{base_path}/products")

stores_df = spark.read.option("header", True).option("inferSchema", True).csv(f"{base_path}/stores")

orders_df = spark.read.option("header", True).option("inferSchema", True).csv(f"{base_path}/orders")

order_items_df = spark.read.option("header", True).option("inferSchema", True).csv(f"{base_path}/order_items")

inventory_df = spark.read.option("header", True).option("inferSchema", True).csv(f"{base_path}/inventory")

deliveries_df = spark.read.option("header", True).option("inferSchema", True).csv(f"{base_path}/deliveries")

print("All source datasets loaded!")

# COMMAND ----------

datasets = {
    "customers": customers_df,
    "products": products_df,
    "stores": stores_df,
    "orders": orders_df,
    "order_items": order_items_df,
    "inventory": inventory_df,
    "deliveries": deliveries_df
}

for name, df in datasets.items():

    print(f"\n========== {name.upper()} ==========")

    df.printSchema()
    

# COMMAND ----------

from pyspark.sql.functions import *

customers_df.select(
    count("customer_id").alias("total_records"),
    countDistinct("customer_id").alias("unique_customer_ids")
).display()

# COMMAND ----------

invalid_customers = orders_df.join(
    customers_df,
    "customer_id",
    "left_anti"
)

print("Orders with invalid customer IDs:", invalid_customers.count())

# COMMAND ----------

invalid_stores = orders_df.join(
    stores_df,
    "store_id",
    "left_anti"
)

print("Orders with invalid store IDs:", invalid_stores.count())

# COMMAND ----------

invalid_products = order_items_df.join(
    products_df,
    "product_id",
    "left_anti"
)

print("Order items with invalid product IDs:", invalid_products.count())

# COMMAND ----------

primary_keys = {
    "customers": (customers_df, "customer_id"),
    "products": (products_df, "product_id"),
    "stores": (stores_df, "store_id"),
    "orders": (orders_df, "order_id"),
    "order_items": (order_items_df, "order_item_id"),
    "inventory": (inventory_df, "inventory_id"),
    "deliveries": (deliveries_df, "delivery_id")
}

for name, (df, key) in primary_keys.items():

    duplicates = df.count() - df.select(key).distinct().count()

    print(name, "duplicate records:", duplicates)

# COMMAND ----------

# Check whether every order has a valid customer
invalid_customers = orders_df.join(
    customers_df,
    "customer_id",
    "left_anti"
)

print("Invalid customer IDs:", invalid_customers.count())


# Check whether every order has a valid store
invalid_stores = orders_df.join(
    stores_df,
    "store_id",
    "left_anti"
)

print("Invalid store IDs:", invalid_stores.count())

# COMMAND ----------

# Order Items → Orders
invalid_orders = order_items_df.join(
    orders_df, "order_id", "left_anti"
)

# Order Items → Products
invalid_products = order_items_df.join(
    products_df, "product_id", "left_anti"
)

# Inventory → Stores
invalid_inventory_stores = inventory_df.join(
    stores_df, "store_id", "left_anti"
)

# Inventory → Products
invalid_inventory_products = inventory_df.join(
    products_df, "product_id", "left_anti"
)

# Deliveries → Orders
invalid_delivery_orders = deliveries_df.join(
    orders_df, "order_id", "left_anti"
)

print("Invalid Order IDs:", invalid_orders.count())
print("Invalid Product IDs:", invalid_products.count())
print("Invalid Inventory Store IDs:", invalid_inventory_stores.count())
print("Invalid Inventory Product IDs:", invalid_inventory_products.count())
print("Invalid Delivery Order IDs:", invalid_delivery_orders.count())