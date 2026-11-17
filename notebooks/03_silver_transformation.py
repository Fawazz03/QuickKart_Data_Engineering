# Databricks notebook source
from pyspark.sql.functions import *

customers_bronze = spark.table(
    "workspace.quickkart.bronze_customers"
)

display(customers_bronze.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Checking for Null values

# COMMAND ----------

null_checks = []

for column_name in customers_bronze.columns:
    check = sum(
        col(column_name).isNull().cast("int")
    ).alias(column_name)

    null_checks.append(check)

customers_bronze.select(null_checks).show()

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Check for duplicates in customer id 
# MAGIC

# COMMAND ----------

total_records = customers_bronze.count()

unique_records = customers_bronze.select(
    "customer_id"
).distinct().count()

duplicate_records = total_records - unique_records

print("Total records:", total_records)
print("Unique customers:", unique_records)
print("Duplicate records:", duplicate_records)

# COMMAND ----------

customers_cleaned = customers_bronze.dropDuplicates(
    ["customer_id"]
)

print("Records before:", customers_bronze.count())
print("Records after:", customers_cleaned.count())

# COMMAND ----------

from pyspark.sql.functions import *

customers_cleaned = customers_cleaned.withColumn(
    "customer_name",
    trim(col("customer_name"))
)

customers_cleaned = customers_cleaned.withColumn(
    "city",
    initcap(trim(col("city")))
)

customers_cleaned = customers_cleaned.withColumn(
    "state",
    initcap(trim(col("state")))
)

display(customers_cleaned.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Invalid customers using NUll 

# COMMAND ----------

invalid_customers = customers_cleaned.filter(
    col("customer_id").isNull() |
    col("customer_name").isNull() |
    col("city").isNull() |
    col("state").isNull()
)

valid_customers = customers_cleaned.filter(
    col("customer_id").isNotNull() &
    col("customer_name").isNotNull() &
    col("city").isNotNull() &
    col("state").isNotNull()
)

print("Invalid customers:", invalid_customers.count())
print("Valid customers:", valid_customers.count())

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Save in delta format

# COMMAND ----------

valid_customers.write\
    .format("delta").mode("overwrite")\
        .saveAsTable(" workspace.quickkart.silver_customers")



# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM workspace.quickkart.silver_customers
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC #### Checking the products table 
# MAGIC

# COMMAND ----------

products_bronze = spark.table(
    "workspace.quickkart.bronze_products"
)

display(products_bronze.limit(10))
products_bronze.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### checking for null in products table

# COMMAND ----------

from pyspark.sql.functions import col, sum

products_bronze.select([
    sum(col(c).isNull().cast("int")).alias(c)
    for c in products_bronze.columns
]).show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC #### Check duplicate product IDs

# COMMAND ----------

total_records = products_bronze.count()

unique_records = products_bronze.select(
    "product_id"
).distinct().count()

print("Total records:", total_records)
print("Unique products:", unique_records)
print("Duplicate records:", total_records - unique_records)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC #### Checking invalid prices 
# MAGIC

# COMMAND ----------

invalid_prices = products_bronze.filter(
    col("unit_price") <= 0
)

print("Invalid price records:", invalid_prices.count())

# COMMAND ----------

products_cleaned = products_bronze.dropDuplicates(
    ["product_id"]
)

print("Records before:", products_bronze.count())
print("Records after:", products_cleaned.count())

# COMMAND ----------

from pyspark.sql.functions import *

products_cleaned = products_cleaned.withColumn(
    "product_name",
    trim(col("product_name"))
)

products_cleaned = products_cleaned.withColumn(
    "category",
    initcap(trim(col("category")))
)

display(products_cleaned.limit(10))

# COMMAND ----------

invalid_products = products_cleaned.filter(
    col("product_id").isNull() |
    col("product_name").isNull() |
    col("category").isNull() |
    col("unit_price").isNull() |
    (col("unit_price") <= 0) |
    col("active_flag").isNull()
)

valid_products = products_cleaned.filter(
    col("product_id").isNotNull() &
    col("product_name").isNotNull() &
    col("category").isNotNull() &
    col("unit_price").isNotNull() &
    (col("unit_price") > 0) &
    col("active_flag").isNotNull()
)

print("Valid products:", valid_products.count())
print("Invalid products:", invalid_products.count())

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Saving the silver products table 
# MAGIC

# COMMAND ----------

valid_products.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.quickkart.silver_products")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT COUNT(*)
# MAGIC FROM workspace.quickkart.silver_products;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Bromze in stores conversion 
# MAGIC

# COMMAND ----------

stores_bronze = spark.table(
    "workspace.quickkart.bronze_stores"
)

display(stores_bronze.limit(10))

# COMMAND ----------

stores_bronze.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Read the data 
# MAGIC

# COMMAND ----------

stores_bronze = spark.table(
    "workspace.quickkart.bronze_stores"
)

display(stores_bronze)

# COMMAND ----------

from pyspark.sql.functions import col, sum

stores_bronze.select([
    sum(col(c).isNull().cast("int")).alias(c)
    for c in stores_bronze.columns
]).show()

# COMMAND ----------

total_records = stores_bronze.count()

unique_records = stores_bronze.select(
    "store_id"
).distinct().count()

print("Total stores:", total_records)
print("Unique stores:", unique_records)
print("Duplicate records:", total_records - unique_records)

# COMMAND ----------

stores_bronze.select("store_type").distinct().show()

# COMMAND ----------


from pyspark.sql.functions import col, trim, initcap

stores_cleaned = stores_bronze.dropDuplicates(
    ["store_id"]
)

stores_cleaned = stores_cleaned.withColumn(
    "store_name", initcap(trim(col("store_name")))
)

stores_cleaned = stores_cleaned.withColumn(
    "city", initcap(trim(col("city")))
)

stores_cleaned = stores_cleaned.withColumn(
    "state", initcap(trim(col("state")))
)

stores_cleaned = stores_cleaned.withColumn(
    "store_type", initcap(trim(col("store_type")))
)

display(stores_cleaned)

# COMMAND ----------

stores_cleaned.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.quickkart.silver_stores")
    

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT COUNT(*)
# MAGIC FROM workspace.quickkart.silver_stores;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC #### Transforming Bronze orders

# COMMAND ----------



# COMMAND ----------

orders_bronze = spark.table(
    "workspace.quickkart.bronze_orders"
)

display(orders_bronze.limit(10))

# COMMAND ----------

orders_bronze.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col, sum

orders_bronze.select([
    sum(col(c).isNull().cast("int")).alias(c)
    for c in orders_bronze.columns
]).show()

# COMMAND ----------

total_records = orders_bronze.count()

unique_records = orders_bronze.select(
    "order_id"
).distinct().count()

print("Total orders:", total_records)
print("Unique orders:", unique_records)
print("Duplicate orders:", total_records - unique_records)

# COMMAND ----------

orders_bronze.select("order_status").distinct().show()

orders_bronze.select("payment_method").distinct().show()

# COMMAND ----------

customers_silver = spark.table(
    "workspace.quickkart.silver_customers"
)

stores_silver = spark.table(
    "workspace.quickkart.silver_stores"
)

invalid_customers = orders_bronze.join(
    customers_silver,
    "customer_id",
    "left_anti"
)

invalid_stores = orders_bronze.join(
    stores_silver,
    "store_id",
    "left_anti"
)

print("Invalid customer references:", invalid_customers.count())
print("Invalid store references:", invalid_stores.count())

# COMMAND ----------

orders_bronze.select("order_status").distinct().show()

orders_bronze.select("payment_method").distinct().show()

# COMMAND ----------

from pyspark.sql.functions import *
df_orders_bronze = spark.table(
    "workspace.quickkart.bronze_orders"
)
df_orders_cleaned = df_orders_bronze.dropDuplicates(
    ["order_id"]
)

df_orders_cleaned = df_orders_cleaned.withColumn(
    "order_status",
    initcap(trim(col("order_status")))
)

# COMMAND ----------

display(df_orders_cleaned.limit(10))

# COMMAND ----------

df_orders_cleaned.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.quickkart.silver_orders")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT COUNT(*)
# MAGIC FROM workspace.quickkart.silver_orders;

# COMMAND ----------

df_order_items_bronze = spark.table(
    "workspace.quickkart.bronze_order_items"
)

display(df_order_items_bronze.limit(10))

# COMMAND ----------

invalid_quantities = df_order_items_bronze.filter(
    col("quantity") <= 0
)

invalid_prices = df_order_items_bronze.filter(
    col("unit_price") <= 0
)

invalid_discounts = df_order_items_bronze.filter(
    (col("discount_pct") < 0) |
    (col("discount_pct") > 100)
)

print("Invalid quantities:", invalid_quantities.count())
print("Invalid prices:", invalid_prices.count())
print("Invalid discounts:", invalid_discounts.count())

# COMMAND ----------

df_orders_silver = spark.table(
    "workspace.quickkart.silver_orders"
)

df_products_silver = spark.table(
    "workspace.quickkart.silver_products"
)

invalid_orders = df_order_items_bronze.join(
    df_orders_silver,
    "order_id",
    "left_anti"
)

invalid_products = df_order_items_bronze.join(
    df_products_silver,
    "product_id",
    "left_anti"
)

print("Invalid order references:", invalid_orders.count())
print("Invalid product references:", invalid_products.count())

# COMMAND ----------

print("Invalid order references:", invalid_orders.count())
print("Invalid product references:", invalid_products.count())

# COMMAND ----------

df_order_items_cleaned = df_order_items_bronze.dropDuplicates(
    ["order_item_id"]
)

# COMMAND ----------

invalid_order_items = df_order_items_cleaned.filter(
    col("order_item_id").isNull() |
    col("order_id").isNull() |
    col("product_id").isNull() |
    col("quantity").isNull() |
    (col("quantity") <= 0) |
    col("unit_price").isNull() |
    (col("unit_price") <= 0) |
    col("discount_pct").isNull() |
    (col("discount_pct") < 0) |
    (col("discount_pct") > 100)
)

valid_order_items = df_order_items_cleaned.filter(
    col("order_item_id").isNotNull() &
    col("order_id").isNotNull() &
    col("product_id").isNotNull() &
    col("quantity").isNotNull() &
    (col("quantity") > 0) &
    col("unit_price").isNotNull() &
    (col("unit_price") > 0) &
    col("discount_pct").isNotNull() &
    (col("discount_pct") >= 0) &
    (col("discount_pct") <= 100)
)

print("Valid order items:", valid_order_items.count())
print("Invalid order items:", invalid_order_items.count())

# COMMAND ----------

df_inventory_bronze = spark.table("workspace.quickkart.bronze_inventory")

display(df_inventory_bronze)
df_inventory_bronze.printSchema()


# COMMAND ----------

from pyspark.sql.functions import col

for column in df_inventory_bronze.columns:
    null_count = df_inventory_bronze.filter(
        col(column).isNull()
    ).count()

    print(column, ":", null_count)

# COMMAND ----------

print("Total records:", df_inventory_bronze.count())

print(
    "Unique inventory IDs:",
    df_inventory_bronze.select("inventory_id").distinct().count()
)

print(
    "Duplicate inventory IDs:",
    df_inventory_bronze.count()
    - df_inventory_bronze.select("inventory_id").distinct().count()
)

# COMMAND ----------

df_stores_silver = spark.table("workspace.quickkart.silver_stores")
df_products_silver = spark.table("workspace.quickkart.silver_products")

invalid_stores = df_inventory_bronze.join(
    df_stores_silver,
    "store_id",
    "left_anti"
)

invalid_products = df_inventory_bronze.join(
    df_products_silver,
    "product_id",
    "left_anti"
)

print("Invalid store references:", invalid_stores.count())
print("Invalid product references:", invalid_products.count())

# COMMAND ----------

invalid_stock = df_inventory_bronze.filter(
    col("stock_quantity") < 0
)

print("Negative stock records:", invalid_stock.count())

# COMMAND ----------

df_inventory_bronze.select(
    "stock_quantity"
).describe().show()

# COMMAND ----------

df_inventory_cleaned = df_inventory_bronze.dropDuplicates(
    ["inventory_id"]
)

df_inventory_cleaned = df_inventory_cleaned.filter(
    col("stock_quantity") >= 0
)

print("Cleaned inventory records:", df_inventory_cleaned.count())

# COMMAND ----------

df_inventory_bronze.select("stock_quantity") \
    .orderBy("stock_quantity") \
    .show(20)

# COMMAND ----------

df_inventory_bronze.filter(
    col("stock_quantity") < 0
).show()

# COMMAND ----------

df_inventory_cleaned = df_inventory_bronze.dropDuplicates(
    ["inventory_id"]
)

df_inventory_cleaned = df_inventory_cleaned.filter(
    col("stock_quantity") >= 0
)

print("Bronze count:", df_inventory_bronze.count())
print("Cleaned count:", df_inventory_cleaned.count())

# COMMAND ----------

df_inventory_cleaned.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.quickkart.silver_inventory")

# COMMAND ----------

df_inventory_silver = spark.table(
    "workspace.quickkart.silver_inventory"
)

print("Silver Inventory count:", df_inventory_silver.count())

# COMMAND ----------

df_deliveries_bronze = spark.table(
    "workspace.quickkart.bronze_deliveries"
)

display(df_deliveries_bronze)

df_deliveries_bronze.printSchema()

# COMMAND ----------

for column in df_deliveries_bronze.columns:
    null_count = df_deliveries_bronze.filter(
        col(column).isNull()
    ).count()

    print(column, ":", null_count)

# COMMAND ----------

print("Total records:", df_deliveries_bronze.count())

print(
    "Unique delivery IDs:",
    df_deliveries_bronze.select("delivery_id").distinct().count()
)

print(
    "Duplicate delivery IDs:",
    df_deliveries_bronze.count()
    - df_deliveries_bronze.select("delivery_id").distinct().count()
)

# COMMAND ----------

df_orders_silver = spark.table(
    "workspace.quickkart.silver_orders"
)

invalid_delivery_orders = df_deliveries_bronze.join(
    df_orders_silver,
    "order_id",
    "left_anti"
)

print(
    "Invalid order references:",
    invalid_delivery_orders.count()
)

# COMMAND ----------

df_deliveries_bronze.select(
    "delivery_status"
).distinct().show()
df_deliveries_bronze.groupBy(
    "delivery_status"
).count().show()


# COMMAND ----------

invalid_timestamps = df_deliveries_bronze.filter(
    (col("picked_timestamp") < col("assigned_timestamp")) |
    (col("delivered_timestamp") < col("picked_timestamp"))
)

print("Invalid timestamp records:", invalid_timestamps.count())

# COMMAND ----------

df_deliveries_cleaned = df_deliveries_bronze.dropDuplicates(
    ["delivery_id"]
)

df_deliveries_cleaned = df_deliveries_cleaned.filter(
    col("delivery_id").isNotNull() &
    col("order_id").isNotNull() &
    col("delivery_partner_id").isNotNull() &
    col("assigned_timestamp").isNotNull() &
    col("delivery_status").isNotNull()
)

print("Bronze records:", df_deliveries_bronze.count())
print("Cleaned records:", df_deliveries_cleaned.count())

# COMMAND ----------

df_deliveries_cleaned.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.quickkart.silver_deliveries")

# COMMAND ----------

print("Valid order items:", valid_order_items.count())
print("Invalid order items:", invalid_order_items.count())

# COMMAND ----------

from pyspark.sql.functions import col

df_order_items_bronze = spark.table(
    "workspace.quickkart.bronze_order_items"
)

print("Bronze count:", df_order_items_bronze.count())

# COMMAND ----------

df_order_items_cleaned = df_order_items_bronze.dropDuplicates(
    ["order_item_id"]
)

valid_order_items = df_order_items_cleaned.filter(
    col("order_item_id").isNotNull() &
    col("order_id").isNotNull() &
    col("product_id").isNotNull() &
    col("quantity").isNotNull() &
    (col("quantity") > 0) &
    col("unit_price").isNotNull() &
    (col("unit_price") > 0) &
    col("discount_pct").isNotNull() &
    (col("discount_pct") >= 0) &
    (col("discount_pct") <= 100)
)

print("Valid order items:", valid_order_items.count())

# COMMAND ----------

df_orders_silver = spark.table("workspace.quickkart.silver_orders")
df_products_silver = spark.table("workspace.quickkart.silver_products")

print(
    "Invalid orders:",
    valid_order_items.join(
        df_orders_silver, "order_id", "left_anti"
    ).count()
)

print(
    "Invalid products:",
    valid_order_items.join(
        df_products_silver, "product_id", "left_anti"
    ).count()
)

# COMMAND ----------

valid_order_items.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.quickkart.silver_order_items")

# COMMAND ----------

df_order_items_silver = spark.table(
    "workspace.quickkart.silver_order_items"
)

print("Silver Order Items:", df_order_items_silver.count())