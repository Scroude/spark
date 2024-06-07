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

path = "netflix_titles.csv"

df = spark.read.csv(path, header=True, sep=",", inferSchema=True)

tot = df.count()

#Lister les réalisateurs les plus prolifiques, et leur nombre de films respectifs

df.filter(df["type"] == "Movie") \
    .groupBy("director") \
    .count() \
    .withColumnRenamed("count", "count_per_films") \
    .orderBy("count_per_films", ascending=False) \
    .show()

#Afficher en pourcentages les pays dans lesquels les films/séries ont été produits

df.groupBy("country") \
    .count() \
    .withColumnRenamed('count', 'cnt_per_country') \
    .withColumn('perc_of_count_total', (F.col('cnt_per_country') / tot) * 100) \
    .orderBy(F.col('cnt_per_country').desc()) \
    .show()

#Quelle est la durée moyenne des films sur Netflix? Le film le plus long? Le film le plus court?


df.filter(df["type"] == "Movie") \
    .withColumn("duration_int", col("duration").substr(0, 3)) \
    .select(avg(col("duration_int").cast("int")),
            max(col("duration_int").cast("int")),
            min(col("duration_int").cast("int"))) \
    .show()

#Afficher la durée moyenne des films par intervalles de 2 ans. Exemple: 2024-2022: x minutes, 2022-2020: y minutes, etc...

df.filter(df["type"] == "Movie") \
    .withColumn("interval", (floor((col("release_year") - 1) / 2) * 2 + 1).cast("int")) \
    .withColumn("duration_int", col("duration").substr(0, 3)) \
    .groupBy("interval") \
    .agg(avg(col("duration_int").cast("int")).alias("average_duration")) \
    .orderBy("interval") \
    .show()

#Quel est le duo réalisateur-acteur qui a collaboré dans le plus de films?
df.filter(df["type"] == "Movie") \
    .groupBy("director", "cast") \
    .count() \
    .orderBy("count", ascending=False) \
    .show()

