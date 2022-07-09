from pyspark.sql import SparkSession
from pyspark.sql.functions import col

import sys
sys.path.append('../')


spark = SparkSession \
    .builder \
    .appName("Process CSV data") \
    .getOrCreate()

df = spark.read.csv('../csv_data/', header=True, inferSchema=True).drop('_c0')

df.createOrReplaceTempView('stocks')


spark.sql('SELECT Date, Stock, UpChange, MaxUpRank = RANK() OVER (PARTITION BY Date ORDER BY UpChange DESC) '
          'FROM (SELECT T1.Stock AS Stock, T1.Date AS Date, ROUND((T1.High - T2.Open)/T2.Open, 2) AS UpChange, '
          'ROUND((T1.Low - T2.Open)/T2.Open, 2) AS DownChange FROM stocks AS T1 INNER JOIN stocks AS T2 ON '
          'T1.Date=T2.Date)').show()

# df.printSchema()


