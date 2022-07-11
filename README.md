# Tasks:

Use the API 
https://rapidapi.com/eec19846/api/investors-exchange-iex-trading/
to download data for 100 stocks and save in separate csv files for each stock.

Create Spark data frames by reading the csv files and do following analysis:

1. On each of the days find which stock has moved maximum %age wise in both directions (+ve, -ve).
2. Which stock was most traded stock on each day.
3. Which stock had the max gap up or gap down opening from the previous day close price I.e. (previous day close -  current day open price ).
4. Which stock has moved maximum from 1st Day data to the latest day Data.
5. Find the standard deviations for each stock over the period.
6. Mind the mean  and median prices for each stock.
7. Find the average volume over the period.
8. Find which stock has higher average volume.
9. Find the highest and lowest prices for a stock over the period of time.

Create REST APIs to query the data for above questions as you could query from the website.

For Spark you can use Scala or Pyspark.

For API again you can use Scala or Python.

# Project Stucture:

The folder 'csv_data' contains the required data in CSV format for 25 companies.

The folder 'server' contains the Flask app, where the routes are defined.

THe folder 'server/templates' contains the HTML files used to show the response.

The folder 'utils':
* companies.txt  --  A auto generated text file containing the Stock names.
* data_collection.py  --  The file contains code for collecting data in CSV format under folder 'csv_data'
* queries.py  --  The file contains SQL queries used to get the required data according to tasks mentioned above.
