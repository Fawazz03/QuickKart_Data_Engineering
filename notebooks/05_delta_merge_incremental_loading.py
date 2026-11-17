# Databricks notebook source
from delta.tables import DeltaTable

print("Delta Lake imported successfully!")

# COMMAND ----------

new_customers = [
    (101, "Arun", "Bangalore", "Karnataka"),
    (501, "Vijay", "Hyderabad", "Telangana")
]

columns = [
    "customer_id",
    "customer_name",
    "city",
    "state"
]

df_new_customers = spark.createDataFrame(
    new_customers,
    columns
)

display(df_new_customers)

# COMMAND ----------

delta_target = DeltaTable.forName(
    spark,
    "workspace.quickkart.silver_customers"
)

print("Target Delta table loaded successfully!")

# COMMAND ----------

from pyspark.sql.functions import col

new_customers = [
    (101, "Arun", "Bangalore", "Karnataka", "2025-01-15"),
    (501, "Vijay", "Hyderabad", "Telangana", "2026-11-16")
]

columns = [
    "customer_id",
    "customer_name",
    "city",
    "state",
    "signup_date"
]

df_new_customers = spark.createDataFrame(
    new_customers,
    columns
).withColumn(
    "signup_date",
    col("signup_date").cast("date")
)

display(df_new_customers)

# COMMAND ----------

delta_target.alias("target").merge(
    df_new_customers.alias("source"),
    "target.customer_id = source.customer_id"
).whenMatchedUpdateAll(
).whenNotMatchedInsertAll(
).execute()

print("MERGE completed successfully!")

# COMMAND ----------

df_customers_check = spark.table(
    "workspace.quickkart.silver_customers"
)

display(
    df_customers_check.filter(
        col("customer_id").isin(101, 501)
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ####Phase 5: Incremental Data Loading

# COMMAND ----------

df_existing_orders = spark.table(
    "workspace.quickkart.silver_orders"
)

print("Existing order count:", df_existing_orders.count())

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ####2: Simulate incoming orders

# COMMAND ----------

from pyspark.sql.functions import *
new_orders = [
    (6001, 101, 3001, "2026-11-17 10:30:00", "Delivered", "UPI"),
    (6002, 102, 3002, "2026-11-17 10:45:00", "Pending", "Card"),
    (6003, 103, 3003, "2026-11-17 11:00:00", "Processing", "Cash")
]

columns = [
    "order_id",
    "customer_id",
    "store_id",
    "order_timestamp",
    "order_status",
    "payment_method"
]

df_new_orders = spark.createDataFrame(
    new_orders,
    columns
).withColumn(
    "order_timestamp",
    col("order_timestamp").cast("timestamp")
)

display(df_new_orders)

# COMMAND ----------

df_incremental_orders = df_new_orders.join(
    df_existing_orders,
    on="order_id",
    how="left_anti"
)

print("New orders:", df_incremental_orders.count())

display(df_incremental_orders)

# COMMAND ----------

print("Incoming orders:")
display(df_new_orders.select("order_id"))

print("Existing orders:")
display(df_existing_orders.select("order_id").orderBy(col("order_id").desc()).limit(10))

# COMMAND ----------

print("Incoming schema:")
df_new_orders.printSchema()

print("Existing schema:")
df_existing_orders.printSchema()

# COMMAND ----------

display(
    df_existing_orders.filter(
        col("order_id").isin(6001, 6002, 6003)
    )
)

# COMMAND ----------

new_orders = [
    (10001, 101, 3001, "2026-11-17 10:30:00", "Delivered", "UPI"),
    (10002, 102, 3002, "2026-11-17 10:45:00", "Pending", "Card"),
    (10003, 103, 3003, "2026-11-17 11:00:00", "Processing", "Cash")
]

columns = [
    "order_id",
    "customer_id",
    "store_id",
    "order_timestamp",
    "order_status",
    "payment_method"
]

df_new_orders = spark.createDataFrame(
    new_orders, columns
).withColumn(
    "order_id", col("order_id").cast("int")
).withColumn(
    "customer_id", col("customer_id").cast("int")
).withColumn(
    "store_id", col("store_id").cast("int")
).withColumn(
    "order_timestamp", col("order_timestamp").cast("timestamp")
)

# COMMAND ----------

df_incremental_orders = df_new_orders.join(
    df_existing_orders,
    on="order_id",
    how="left_anti"
)

print("New orders:", df_incremental_orders.count())
display(df_incremental_orders)

# COMMAND ----------

df_incremental_orders.write.format("delta").mode("append").saveAsTable(
    "workspace.quickkart.silver_orders"
)

print("Incremental loading completed!")

# COMMAND ----------

df_orders_updated = spark.table(
    "workspace.quickkart.silver_orders"
)

print("Updated order count:", df_orders_updated.count())

display(
    df_orders_updated.filter(
        col("order_id").isin(10001, 10002, 10003)
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### SCD _ Slowly changing Dimensions

# COMMAND ----------


from pyspark.sql.functions import *
updated_customers = [
    (102, "Rahul", "Chennai", "Tamil Nadu", "2025-02-10")
]

columns = [
    "customer_id",
    "customer_name",
    "city",
    "state",
    "signup_date"
]

df_customer_updates = spark.createDataFrame(
    updated_customers,
    columns
).withColumn(
    "signup_date",
    col("signup_date").cast("date")
)

display(df_customer_updates)

# COMMAND ----------

delta_target = DeltaTable.forName(
    spark,
    "workspace.quickkart.silver_customers"
)

delta_target.alias("target").merge(
    df_customer_updates.alias("source"),
    "target.customer_id = source.customer_id"
).whenMatchedUpdateAll(
).execute()

print("SCD Type 1 update completed!")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ####SCD- 2 

# COMMAND ----------

from pyspark.sql.functions import current_date, lit

df_customer_history = spark.table(
    "workspace.quickkart.silver_customers"
).withColumn(
    "start_date", col("signup_date")
).withColumn(
    "end_date", lit(None).cast("date")
).withColumn(
    "is_current", lit(True)
)

df_customer_history.write.format("delta").mode("overwrite").saveAsTable(
    "workspace.quickkart.scd2_customers"
)

print("SCD Type 2 history table created!")

# COMMAND ----------

updated_customer = [
    (102, "Rahul", "Bangalore", "Karnataka")
]

columns = [
    "customer_id",
    "customer_name",
    "city",
    "state"
]

df_scd2_updates = spark.createDataFrame(
    updated_customer,
    columns
)

display(df_scd2_updates)

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import current_date

delta_scd2 = DeltaTable.forName(
    spark,
    "workspace.quickkart.scd2_customers"
)

delta_scd2.update(
    condition="customer_id = 102 AND is_current = true",
    set={
        "is_current": "false",
        "end_date": "current_date()"
    }
)

print("Old customer record expired!")

# COMMAND ----------

from pyspark.sql.functions import current_date, lit

df_new_version = df_scd2_updates.withColumn(
    "start_date", current_date()
).withColumn(
    "end_date", lit(None).cast("date")
).withColumn(
    "is_current", lit(True)
)

df_new_version.write.format("delta").mode("append").saveAsTable(
    "workspace.quickkart.scd2_customers"
)

print("New customer version inserted!")

# COMMAND ----------

display(
    spark.table("workspace.quickkart.scd2_customers")
    .filter(col("customer_id") == 102)
    .orderBy("start_date")
)

# COMMAND ----------

