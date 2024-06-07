from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

sc = spark.sparkContext

#Trump insults

path = "trump_insult_tweets_2014_to_2021.csv"

df = spark.read.csv(path, header=True, sep=",")

tot = df.count()

#Afficher les 7 comptes que Donald Trump insulte le plus

df.groupBy("target") \
  .count() \
  .withColumnRenamed('count', 'cnt_per_target') \
  .withColumn('perc_of_count_total', (F.col('cnt_per_target') / tot) * 100 ) \
  .orderBy(F.col('cnt_per_target').desc()) \
  .show(n=7)

#Afficher les insultes que Donald Trump utilise le plus (en nombres et en pourcentages)

df.groupBy("insult") \
  .count() \
  .withColumnRenamed('count', 'cnt_per_insult') \
  .withColumn('perc_of_count_total', (F.col('cnt_per_insult') / tot) * 100 ) \
  .orderBy(F.col('cnt_per_insult').desc()) \
  .show()

#Quelle est l'insulte que Donald Trump utilise le plus pour Joe Biden?

df.filter(df['target'] == "joe-biden") \
    .groupBy("insult") \
    .count() \
    .withColumnRenamed('count', 'cnt_per_insult') \
    .orderBy(F.col('cnt_per_insult').desc()) \
    .show(n=1)

#Combien de fois a-t-il tweeté le mot "Mexico"? Le mot "China"? Le mot "coronavirus"?

df.createOrReplaceTempView("tweets")

sqlDF = spark.sql("SELECT COUNT(*) as nbMexico FROM tweets t WHERE t.tweet like '%Mexico%'")
sqlDF.show()
sqlDF = spark.sql("SELECT COUNT(*) as nbChina FROM tweets t WHERE t.tweet like '%China%'")
sqlDF.show()
sqlDF = spark.sql("SELECT COUNT(*) as nbCoronavirus FROM tweets t WHERE t.tweet like '%coronavirus%'")
sqlDF.show()

#Classer le nombre de tweets par période de 6 mois
sqlDF = spark.sql("SELECT YEAR(tweets.date) AS annee, FLOOR((MONTH(tweets.date) - 1) / 6) + 1 AS periode, COUNT(*) AS nombre_tweets FROM tweets GROUP BY annee, periode ORDER BY annee, periode;")
sqlDF.show()

