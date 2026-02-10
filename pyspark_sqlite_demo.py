from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    FloatType
)
import os
import sys
import shutil

# --- CONFIGURATION ---
# In Docker, we can rely on standard paths or env vars set by Dockerfile.
# But we keep some fallbacks just in case.

# Ensure we are pointing to localhost for Spark
#os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

# Create a local temp directory for Spark to avoid any permission issues,
# even inside Docker it's cleaner to have a dedicated temp dir.
local_tmp_dir = os.path.abspath("spark_temp")
if os.path.exists(local_tmp_dir):
    try:
        shutil.rmtree(local_tmp_dir)
    except:
        pass
os.makedirs(local_tmp_dir, exist_ok=True)
os.environ["SPARK_LOCAL_DIRS"] = local_tmp_dir

def main():
    print("----------------------------------------------------------------")
    print("Starting PySpark SQLite Demo...")
    print(f"Using Python: {sys.executable}")
    print(f"Using Java Home: {os.environ.get('JAVA_HOME', 'Not Set')}")
    print(f"Using Temp Dir: {local_tmp_dir}")
    print("----------------------------------------------------------------")

    try:
        # 1. Start Spark and ensure we have the SQLite JDBC driver
        # We specify the JDBC driver package directly in the SparkSession config.
        b = SparkSession.builder \
            .appName("SQLite Demo") \
            .config("spark.jars.packages", "org.xerial:sqlite-jdbc:3.47.2.0") \
            .config("spark.driver.extraJavaOptions", f"-Djava.io.tmpdir={local_tmp_dir}")
            
        # Add necessary opens for Java 17+ (which we are using in Docker)
        b = b.config("spark.driver.extraJavaOptions", 
                     f"-Djava.io.tmpdir={local_tmp_dir} "
                     "--add-opens=java.base/java.lang=ALL-UNNAMED "
                     "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED "
                     "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED "
                     "--add-opens=java.base/java.io=ALL-UNNAMED "
                     "--add-opens=java.base/java.net=ALL-UNNAMED "
                     "--add-opens=java.base/java.nio=ALL-UNNAMED "
                     "--add-opens=java.base/java.util=ALL-UNNAMED "
                     "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED "
                     "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED "
                     "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
                     "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED "
                     "--add-opens=java.base/sun.security.action=ALL-UNNAMED "
                     "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED "
                     "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED")

        spark = b.getOrCreate()
            
        print("Spark initialized successfully.")
    except Exception as e:
        print(f"CRITICAL ERROR: Failed to initialize SparkSession. {e}")
        return

    # 2. Define the exact path to our local SQLite database file
    # This will create a file named 'demo.db' in the current directory
    db_file = os.path.abspath("demo.db")
    db_url = f"jdbc:sqlite:{db_file}"
    print(f"Database URL: {db_url}")

    # 3. Create some mock data
    print("\nCreating mock data...")
    data = [
        (1, "Product A", 10.5, "Electronics"),
        (2, "Product B", 20.0, "Electronics"),
        (3, "Product C", 5.75, "Stationery"),
        (4, "Product D", 15.2, "Clothing"),
        (5, "Product E", 8.99, "Stationery")
    ]

    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), True),
        StructField("price", FloatType(), True),
        StructField("category", StringType(), True)
    ])

    df_writer = spark.createDataFrame(data, schema)
    
    print("Mock DataFrame created:")
    df_writer.show()
    
    # 4. Write data to SQLite using JDBC
    # The 'write' method will automatically create the table/database if it doesn't exist
    print("Writing data to SQLite database...")
    
    # Using 'overwrite' mode to make this script idempotent
    # We set transaction isolation level to NONE to avoid warnings with SQLite JDBC
    # Spark default is READ_UNCOMMITTED (1) but SQLite JDBC often defaults to SERIALIZABLE (8)
    try:
        df_writer.write \
            .format("jdbc") \
            .option("url", db_url) \
            .option("dbtable", "products") \
            .option("driver", "org.sqlite.JDBC") \
            .option("isolationLevel", "NONE") \
            .mode("overwrite") \
            .save()
            
        print("Data successfully written to 'products' table in demo.db")
    except Exception as e:
        print(f"Error writing to SQLite: {e}")
        return

    # 5. Read the data back from SQLite into a NEW DataFrame
    print("\nReading data back from SQLite database...")
    
    try:
        df_reader = spark.read \
            .format("jdbc") \
            .option("url", db_url) \
            .option("dbtable", "products") \
            .option("driver", "org.sqlite.JDBC") \
            .option("isolationLevel", "NONE") \
            .load()
    
        print("Data read from SQLite:")
        df_reader.show()
    
        # Show schema of what we read back to confirm types
        print("Schema of data read from SQLite:")
        df_reader.printSchema()
        
        # Perform a simple query using SQL on the read data
        print("Performing SQL query on read data (Filter: price > 10.0)")
        df_reader.createOrReplaceTempView("products_view")
        spark.sql("SELECT * FROM products_view WHERE price > 10.0").show()

    except Exception as e:
        print(f"Error reading from SQLite: {e}")

    spark.stop()
    print("Demo completed.")
    
    # Cleanup temp dir
    try:
        shutil.rmtree(local_tmp_dir)
    except:
        pass

if __name__ == "__main__":
    main()
