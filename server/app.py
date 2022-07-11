from pyspark.sql import SparkSession
from flask import Flask
import json
from
import sys
sys.path.append('../')


spark = SparkSession \
    .builder \
    .appName("Process CSV data") \
    .getOrCreate()

df = spark.read.csv('../csv_data/', header=True, inferSchema=True).drop('_c0')

app = Flask(__name__)


@app.route("/get_max_moved_stocks_per_day")
def get_max_moved_stocks_per_day():
    res_df = Queries.get_max_moved_stocks_per_day(spark, df)
    res_df.show()


if __name__ == "__main__":
    app.run(debug=True, port=5000)

