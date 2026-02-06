from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("PySparkExample").getOrCreate()

# Create a DataFrame
data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
df = spark.createDataFrame(data, ["Name", "Age"])

# Print the DataFrame
print("Original DataFrame:")
print(df.summary().show())

# Perform a simple transformation
df_filtered = df.filter(df["Age"] > 30)
print("Filtered DataFrame for over 30:")
print(df_filtered.summary().show())

# Sorting the DataFrame
df_sorted = df.orderBy(df["Age"])
print("Sorted DataFrame:")
print(df_sorted.summary().show())

# Grouping and aggregating data
df_grouped = df.groupBy("Name").agg({"Age": "avg"})
print("Grouped DataFrame by Name:")
print(df_grouped.summary().show())

# Stop the SparkSession
spark.stop()