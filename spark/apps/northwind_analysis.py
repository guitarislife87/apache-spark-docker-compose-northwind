from datetime import datetime, timedelta
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col, sum as _sum, count, avg, round as _round, date_format

# Database connection properties
db_properties = {
    "user": "spark_user",
    "password": "spark_password",
    "driver": "org.postgresql.Driver"
}

jdbc_url = "jdbc:postgresql://postgres-db:5432/analytics"

# Create Spark session
spark = SparkSession.builder \
    .appName("Northwind Analysis") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.instances", "3") \
    .config("spark.executor.cores", "2") \
    .config("spark.executor.memory", "2g") \
    .config("spark.jars", "/opt/spark/addon-jars/postgresql-42.7.1.jar") \
    .getOrCreate()

print("\\n=== READING DATA FROM POSTGRESQL ===")

# Read Orders table
orders_df = spark.read.jdbc(
    url=jdbc_url,
    table="northwind.orders",
    properties=db_properties
)

print(f"\\nTotal orders: {orders_df.count()}")
orders_df.show()

# Computing Date Dimension
print("\\n=== COMPUTING DATE DIMENSION ===")


# Generate date range from 1970-01-01 to today
start_date = datetime(1970, 1, 1)
end_date = datetime.today()
num_days = (end_date - start_date).days + 1

date_rows = [
    Row(
        date_dimension_id=int((start_date + timedelta(days=i)).strftime('%Y%m%d')),
        date_actual=(start_date + timedelta(days=i)),
        epoch=(start_date + timedelta(days=i)).timestamp(),
        day_name=(start_date + timedelta(days=i)).strftime('%A'),
        day_of_week=(start_date + timedelta(days=i)).isoweekday(),
        day_of_month=(start_date + timedelta(days=i)).day,
        day_of_year=(start_date + timedelta(days=i)).timetuple().tm_yday,
        week_of_month=(start_date + timedelta(days=i)).day // 7 + 1,
        week_of_year=(start_date + timedelta(days=i)).isocalendar()[1],
        month_actual=(start_date + timedelta(days=i)).month,
        month_name=(start_date + timedelta(days=i)).strftime('%B'),
        month_name_abbreviated=(start_date + timedelta(days=i)).strftime('%b'),
        quarter_actual=((start_date + timedelta(days=i)).month - 1) // 3 + 1,
        year_actual=(start_date + timedelta(days=i)).year,
        weekend_indr=(start_date + timedelta(days=i)).weekday() >= 5
    )
    for i in range(num_days)
]

date_dimension = spark.createDataFrame(date_rows)



# Perform analysis: Order Facts
order_facts = orders_df.repartition(12) \
    .withColumn("order_date", date_format(col("order_date"), "yyyyMMdd").cast(IntegerType())) \
    .withColumn("required_date", date_format(col("required_date"), "yyyyMMdd").cast(IntegerType())) \
    .withColumn("shipped_date", date_format(col("shipped_date"), "yyyyMMdd").cast(IntegerType()))


# Write results back to database
print("\\n=== WRITING RESULTS TO DATABASE ===")
date_dimension.write \
    .option("truncate", "true") \
    .jdbc(
        url=jdbc_url,
        table="northwind_dw.date_dimension",
        mode="append",
        properties=db_properties
    )

order_facts.write \
    .option("truncate", "true") \
    .jdbc(
        url=jdbc_url,
        table="northwind_dw.order_facts",
        mode="append",
        properties=db_properties
    )

print("Analysis complete! Results saved to 'order_facts' table.")

spark.stop()
