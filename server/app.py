import sys
sys.path.append('../')
sys.path.insert(0, '../utils')

from pyspark.sql import SparkSession
from flask import Flask, render_template
import json
import pandas
from utils.queries import Queries


spark = SparkSession \
    .builder \
    .appName("Process CSV data") \
    .getOrCreate()

df = spark.read.csv('../csv_data/', header=True, inferSchema=True).drop('_c0')
df.createOrReplaceTempView('stocks')
app = Flask(__name__)

@app.route("/")
def home():
    q1_1 = '<a href="http://127.0.0.1:5000/get_max_moved_stocks_per_day/positive">' + 'Query: 1.1 ==>  get_max_moved_stocks_per_day_positive_direction' + '</a><br>'
    q1_2 = '<a href="http://127.0.0.1:5000/get_max_moved_stocks_per_day/negative">' + 'Query: 1.2 ==>  get_max_moved_stocks_per_day_negative_direction' + '</a><br>'
    q2 = '<a href="http://127.0.0.1:5000/get_most_traded_stock_per_day">' + 'Query: 2 ==>  get_most_traded_stock_per_day' + '</a><br>'
    q3 = '<a href="http://127.0.0.1:5000/get_max_gap_per_stock_per_day">' + 'Query: 3 ==>  get_max_gap_per_stock_per_day' + '</a><br>'
    q4 = '<a href="http://127.0.0.1:5000/get_most_moved_stock">' + 'Query: 4 ==>  get_most_moved_stock' + '</a><br>'
    q5 = '<a href="http://127.0.0.1:5000/get_stdv_per_stock">' + 'Query: 5 ==>  get_stdv_per_stock' + '</a><br>'
    q6 = '<a href="http://127.0.0.1:5000/get_mean_median_per_stock">' + 'Query: 6 ==>  get_mean_median_per_stock' + '</a><br>'
    q7 = '<a href="http://127.0.0.1:5000/get_avg_volume_per_stock">' + 'Query: 7 ==>  get_avg_volume_per_stock' + '</a><br>'
    q8 = '<a href="http://127.0.0.1:5000/get_stock_with_max_avg_volume">' + 'Query: 8 ==>  get_stock_with_max_avg_volume' + '</a><br>'
    q9 = '<a href="http://127.0.0.1:5000/get_peak_low_prices_per_stock">' + 'Query: 9 ==>  get_peak_low_prices_per_stock' + '</a><br>'
    return q1_1 + q1_2 + q2 + q3 + q4 + q5 + q6 + q7 + q8 + q9


@app.route("/get_max_moved_stocks_per_day/<direction>")
def get_max_moved_stocks_per_day(direction):
    try:
        spark.catalog.dropTempView("query")
    except Exception as e:
        print('Temp View not created!')
        
    res_df = Queries.get_max_moved_stocks_per_day(spark, df, direction)
    res_df.show()
    pandas_df = res_df.toPandas()
    return render_template('table.html', tables=[pandas_df.to_html(classes='data')], titles=pandas_df.columns.values)


@app.route("/get_most_traded_stock_per_day")
def get_most_traded_stock_per_day():
    res_df = Queries.get_most_traded_stock_per_day(spark, df)
    res_df.show()
    pandas_df = res_df.toPandas()
    return render_template('table.html', tables=[pandas_df.to_html(classes='data')], titles=pandas_df.columns.values)


@app.route("/get_max_gap_per_stock_per_day")
def get_max_gap_per_stock_per_day():
    res_df = Queries.get_max_gap_per_stock_per_day(spark, df)
    res_df.show()
    pandas_df = res_df.toPandas()
    return render_template('table.html', tables=[pandas_df.to_html(classes='data')], titles=pandas_df.columns.values)


@app.route("/get_most_moved_stock")
def get_most_moved_stock():
    res_df = Queries.get_most_moved_stock(spark, df)
    res_df.show()
    pandas_df = res_df.toPandas()
    return render_template('table.html', tables=[pandas_df.to_html(classes='data')], titles=pandas_df.columns.values)


@app.route("/get_stdv_per_stock")
def get_stdv_per_stock():
    res_df = Queries.get_stdv_per_stock(spark, df)
    res_df.show()
    pandas_df = res_df.toPandas()
    return render_template('table.html', tables=[pandas_df.to_html(classes='data')], titles=pandas_df.columns.values)


@app.route("/get_mean_median_per_stock")
def get_mean_median_per_stock():
    res_df = Queries.get_mean_median_per_stock(spark, df)
    res_df.show()
    pandas_df = res_df.toPandas()
    return render_template('table.html', tables=[pandas_df.to_html(classes='data')], titles=pandas_df.columns.values)


@app.route("/get_avg_volume_per_stock")
def get_avg_volume_per_stock():
    res_df = Queries.get_avg_volume_per_stock(spark, df)
    res_df.show()
    pandas_df = res_df.toPandas()
    return render_template('table.html', tables=[pandas_df.to_html(classes='data')], titles=pandas_df.columns.values)


@app.route("/get_stock_with_max_avg_volume")
def get_stock_with_max_avg_volume():
    res_df = Queries.get_stock_with_max_avg_volume(spark, df)
    res_df.show()
    pandas_df = res_df.toPandas()
    return render_template('table.html', tables=[pandas_df.to_html(classes='data')], titles=pandas_df.columns.values)


@app.route("/get_peak_low_prices_per_stock")
def get_peak_low_prices_per_stock():
    res_df = Queries.get_peak_low_prices_per_stock(spark, df)
    res_df.show()
    pandas_df = res_df.toPandas()
    return render_template('table.html', tables=[pandas_df.to_html(classes='data')], titles=pandas_df.columns.values)

if __name__ == "__main__":
    app.run(debug=True, port=5000)

