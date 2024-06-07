from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import avg, col, min, max, floor

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

sc = spark.sparkContext

#Netflix

path = "listings.csv"

df = spark.read.csv(path, header=True, sep=",", inferSchema=True)

tot = df.count()

# Room Type

df.groupBy("room_type") \
    .count() \
    .withColumnRenamed('count', 'cnt_per_type') \
    .withColumn('perc_of_count_total', (F.col('cnt_per_type') / tot) * 100) \
    .orderBy(F.col('cnt_per_type').desc()) \
    .show()

# Activity

df.select(avg(col("price")),
          avg(col("minimum_nights")),
          avg(col("number_of_reviews")))\
    .withColumn("avg_nights_booked", (col("avg(minimum_nights)") * col("avg(number_of_reviews)")) / 2) \
    .withColumn("avg_income", (col("avg_nights_booked") * col("avg(price)"))) \
    .show()

# Short Terms Rentals

df.createOrReplaceTempView("listing")

sqlDF = spark.sql("""SELECT 
    (
        SELECT COUNT(*) FROM listing l WHERE l.minimum_nights < 30
    ) AS short_terms,
    (
        SELECT COUNT(*) FROM listing l WHERE l.minimum_nights > 30
    ) AS long_terms,
    count(*) as total,
    (short_terms / total) * 100 AS perc_short_terms, 
    (long_terms / total) * 100 AS perc_long_terms
FROM
    listing""")

sqlDF.show()

sqlDF = spark.sql("""SELECT
(SELECT SUM(count_listing) FROM (
    SELECT COUNT(host_id) as count_listing, host_id
    FROM listing 
    GROUP BY host_id
    HAVING COUNT(host_id) > 1
)) AS multi_listing,
(SELECT COUNT(*) FROM (
    SELECT COUNT(host_id), host_id
    FROM listing 
    GROUP BY host_id
    HAVING COUNT(host_id) = 1
))  AS single_listing,
COUNT(*) AS total,
    (multi_listing / total) * 100 AS perc_multi_listing, 
    (single_listing / total) * 100 AS perc_single_listing
FROM
listing
""")

sqlDF.show()

sqlDF = spark.sql("""
SELECT 
    host_name,
    SUM(CASE WHEN room_type = 'Private room' THEN 1 ELSE 0 END) AS private_rooms,
    SUM(CASE WHEN room_type = 'Entire home/apt' THEN 1 ELSE 0 END) AS entire_rooms,
    SUM(CASE WHEN room_type = 'Shared room' THEN 1 ELSE 0 END) AS shared_rooms,
    SUM(CASE WHEN room_type = 'Hotel room' THEN 1 ELSE 0 END) AS hotel_rooms,
    COUNT(*) AS total_listing
FROM
    listing
GROUP BY
    host_name
ORDER BY
    total_listing DESC;
""")
sqlDF.show()