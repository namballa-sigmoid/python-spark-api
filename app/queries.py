
class Queries:
    @staticmethod()
    def get_max_moved_stocks_per_day(spark, df):
        df.createOrReplaceTempView('stocks')
        return spark.sql('CREATE TEMPORARY VIEW query AS (SELECT Date, Stock, UpChange, DownChange, RANK() OVER (PARTITION BY Date ORDER BY UpChange DESC) AS MaxUpRank, RANK() OVER (PARTITION BY Date ORDER BY DownChange) AS MaxDownRank FROM (SELECT Stock, Date, ((High - Open)/Open) AS UpChange, ((Low - Open)/Open) AS DownChange FROM stocks))')

    @staticmethod()
    def get_most_traded_stock_per_day(spark, df):
        df.createOrReplaceTempView('stocks')
        return spark.sql('SELECT Date, Stock, Volume, RANK() OVER (PARTITION BY Date ORDER BY Volume DESC) AS MostTradedRank FROM stocks')

    @staticmethod()
    def get_max_gap_per_stock_per_day(spark, df):
        df.createOrReplaceTempView('stocks')
        return spark.sql('SELECT T3.Stock AS Stock, T3.Date AS Cur_Date, T4.Date AS Prev_Date, T3.Open AS Cur_Open, T4.Close AS Prev_Close, (T3.Open - T4.Close) AS Gap, RANK() OVER (PARTITION BY T3.Date ORDER BY (T3.Open - T4.Close) DESC) AS GapRank FROM (SELECT T1.Date AS Cur_Date, MAX(T2.Date) AS Prev_Date FROM stocks AS T1 INNER JOIN stocks AS T2 WHERE T1.Date > T2.Date GROUP BY Cur_Date ORDER BY Cur_Date) AS subTable INNER JOIN stocks AS T3 ON subTable.Cur_Date = T3.Date INNER JOIN stocks AS T4 ON subTable.Prev_Date = T4.Date WHERE T3.Stock = T4.Stock').show()

    @staticmethod()
    def get_most_moved_stock(spark, df):
        df.createOrReplaceTempView('stocks')
        return spark.sql('SELECT T1.Stock, min_date, max_date, (T2.Close - T1.Close) AS StockMoved, ROW_NUMBER() OVER (ORDER BY (T2.Close - T1.Close) DESC) AS MovementRank FROM (SELECT MIN(DATE) AS min_date, MAX(DATE) AS max_date FROM stocks) AS Subtable INNER JOIN stocks AS T1 ON Subtable.min_date = T1.Date INNER JOIN stocks AS T2 ON Subtable.max_date = T2.Date WHERE T1.Stock = T2.Stock').show()

    @staticmethod()
    def get_stdv_per_stock(spark, df):
        df.createOrReplaceTempView('stocks')
        spark.sql('SELECT Stock, STDDEV(Close) AS standard_deviation FROM stocks GROUP BY Stock').show()

    @staticmethod()
    def get_mean_median_per_stock(spark, df):
        df.createOrReplaceTempView('stocks')
        return spark.sql('SELECT T1.Stock, Mean, Median FROM (SELECT Stock, AVG(Close) AS Median FROM (SELECT Stock, Close, rn, (CASE WHEN cn%2=0 THEN (cn DIV 2) ELSE (cn DIV 2) + 1 END) AS m1, (cn DIV 2) + 1 AS m2 FROM (SELECT Stock, Close, ROW_NUMBER() OVER (PARTITION BY Stock ORDER BY Close) AS rn, COUNT(Close) OVER (PARTITION BY Stock) AS cn FROM stocks)) WHERE rn BETWEEN m1 AND m2 GROUP By Stock) AS T1 INNER JOIN (SELECT Stock, AVG(Close) AS Mean FROM stocks GROUP BY Stock) AS T2 ON T1.Stock = T2.Stock').show()

    @staticmethod()
    def get_avg_volume_per_stock(spark, df):
        df.createOrReplaceTempView('stocks')
        return spark.sql('SELECT Stock, AVG(Volume) AS Avg_Volume FROM stocks GROUP BY Stock').show()

    @staticmethod()
    def get_stock_with_max_avg_volume(spark, df):
        df.createOrReplaceTempView('stocks')
        return spark.sql('SELECT Stock, AVG(Volume) AS Avg_Volume FROM stocks GROUP BY Stock ORDER BY Avg_Volume DESC LIMIT 1').show()

    @staticmethod()
    def get_peak_low_prices_per_stock(spark, df):
        df.createOrReplaceTempView('stocks')
        return spark.sql('SELECT Stock, MAX(High) AS Highest_Price, MIN(Low) AS Lowest_Price FROM stocks GROUP BY Stock').show()

