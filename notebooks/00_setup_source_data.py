# Databricks notebook source
print("Welcome to QuickKart Data Engineering Project!")

# Check Spark is working
print("Spark Version:", spark.version)

# Create a small sample DataFrame
test_df = spark.range(1, 6)

# Display the data
display(test_df)

# COMMAND ----------

display(spark.sql("SHOW CATALOGS"))

# COMMAND ----------

display(
    spark.sql("SELECT current_catalog(), current_schema()")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.quickkart;

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA workspace.quickkart;

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG workspace;

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA quickkart;

# COMMAND ----------

display(
    spark.sql("SELECT current_catalog(), current_schema()")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW VOLUMES;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS quickkart_source;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW VOLUMES;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS workspace.quickkart.quickkart_source;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW VOLUMES IN workspace.quickkart;
# MAGIC

# COMMAND ----------

import random
from datetime import datetime, timedelta

random.seed(42)

# Cities
cities = [
    ("Chennai", "Tamil Nadu"),
    ("Bangalore", "Karnataka"),
    ("Hyderabad", "Telangana"),
    ("Mumbai", "Maharashtra"),
    ("Pune", "Maharashtra")
]

# Product categories
categories = [
    "Dairy", "Bakery", "Snacks",
    "Beverages", "Grocery", "Personal Care"
]

# Other business values
payment_methods = ["UPI", "Card", "Cash"]

order_statuses = ["Delivered", "Pending", "Cancelled"]

base_date = datetime(2026, 1, 1)

print("QuickKart environment initialized!")

# COMMAND ----------

products_data = []

for i in range(1, 101):

    products_data.append((
        2000 + i,
        f"Product_{i}",
        random.choice(categories),
        round(random.uniform(10, 500), 2),
        True
    ))

products_df = spark.createDataFrame(
    products_data,
    ["product_id", "product_name", "category", "unit_price", "active_flag"]
)

display(products_df)

# COMMAND ----------

customers_data = []

for i in range(1, 501):

    city, state = random.choice(cities)

    customers_data.append((
        1000 + i,
        f"Customer_{i}",
        city,
        state,
        (base_date - timedelta(days=random.randint(1, 500))).date()
    ))

customers_df = spark.createDataFrame(
    customers_data,
    ["customer_id", "customer_name", "city", "state", "signup_date"]
)



# COMMAND ----------

stores_data = []

for i in range(1, 11):

    city, state = cities[(i - 1) % len(cities)]

    stores_data.append((
        3000 + i,
        f"QuickKart_Store_{i}",
        city,
        state,
        "Dark Store"
    ))

stores_df = spark.createDataFrame(
    stores_data,
    ["store_id", "store_name", "city", "state", "store_type"]
)

display(stores_df)

# COMMAND ----------

orders_data = []

for i in range(1, 5001):

    order_date = base_date + timedelta(
        days=random.randint(0, 89),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59)
    )

    orders_data.append((
        4000 + i,
        random.randint(1001, 1500),
        random.randint(3001, 3010),
        order_date,
        random.choices(
            order_statuses,
            weights=[85, 10, 5]
        )[0],
        random.choice(payment_methods)
    ))

orders_df = spark.createDataFrame(
    orders_data,
    [
        "order_id",
        "customer_id",
        "store_id",
        "order_timestamp",
        "order_status",
        "payment_method"
    ]
)



# COMMAND ----------

order_items_data = []

item_id = 1

for order in orders_data:

    order_id = order[0]

    for _ in range(random.randint(1, 4)):

        product = random.choice(products_data)

        order_items_data.append((
            item_id,
            order_id,
            product[0],
            random.randint(1, 5),
            product[3],
            round(random.uniform(0, 0.15), 2)
        ))

        item_id += 1

order_items_df = spark.createDataFrame(
    order_items_data,
    [
        "order_item_id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        "discount_pct"
    ]
)

display(order_items_df)

# COMMAND ----------

inventory_data = []

inventory_id = 1

for store in stores_data:

    for product in products_data:

        inventory_data.append((
            inventory_id,
            store[0],
            product[0],
            random.randint(0, 100),
            base_date + timedelta(days=random.randint(0, 89))
        ))

        inventory_id += 1

inventory_df = spark.createDataFrame(
    inventory_data,
    [
        "inventory_id",
        "store_id",
        "product_id",
        "stock_quantity",
        "last_updated"
    ]
)

display(inventory_df)

# COMMAND ----------

deliveries_data = []

delivery_id = 1

for order in orders_data:

    if order[4] == "Delivered":

        assigned = order[3] + timedelta(
            minutes=random.randint(1, 5)
        )

        picked = assigned + timedelta(
            minutes=random.randint(5, 15)
        )

        delivered = picked + timedelta(
            minutes=random.randint(5, 35)
        )

        deliveries_data.append((
            delivery_id,
            order[0],
            5000 + random.randint(1, 50),
            assigned,
            picked,
            delivered,
            "Delivered"
        ))

        delivery_id += 1

deliveries_df = spark.createDataFrame(
    deliveries_data,
    [
        "delivery_id",
        "order_id",
        "delivery_partner_id",
        "assigned_timestamp",
        "picked_timestamp",
        "delivered_timestamp",
        "delivery_status"
    ]
)

display(deliveries_df)

# COMMAND ----------

print("CUSTOMERS:", customers_df.count())
print("PRODUCTS:", products_df.count())
print("STORES:", stores_df.count())
print("ORDERS:", orders_df.count())
print("ORDER ITEMS:", order_items_df.count())
print("INVENTORY:", inventory_df.count())
print("DELIVERIES:", deliveries_df.count())

# COMMAND ----------

base_path = "/Volumes/workspace/quickkart/quickkart_source"

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

# COMMAND ----------

for name, df in datasets.items():

    output_path = f"{base_path}/{name}"

    df.write \
        .mode("overwrite") \
        .option("header", True) \
        .csv(output_path)

    print(f"{name} saved successfully!")

print("All QuickKart CSV datasets saved!")

# COMMAND ----------

display(dbutils.fs.ls(base_path))

# COMMAND ----------

base_path = "/Volumes/workspace/quickkart/quickkart_source"

customers_df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(f"{base_path}/customers")

products_df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(f"{base_path}/products")

stores_df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(f"{base_path}/stores")

orders_df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(f"{base_path}/orders")

order_items_df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(f"{base_path}/order_items")

inventory_df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(f"{base_path}/inventory")

deliveries_df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(f"{base_path}/deliveries")

print("All seven CSV datasets loaded successfully!")

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
    print(name, ":", df.count())

# COMMAND ----------

display(customers_df)

# COMMAND ----------

customers_df.printSchema()

# COMMAND ----------

for name, df in datasets.items():

    print(f"\n========== {name.upper()} ==========")

    df.printSchema()