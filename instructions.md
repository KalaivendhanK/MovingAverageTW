You have been provided with the below data files:

1) **StockPrices:**(/test/resources/input/stock_prices) <br/>
_Columns_: stockId,timeStamp,stockPrice
2) **StockInfo:** (/test/resources/input/stock_info) <br/>
_Columns_: stockId,stockName,stockCategory

You need to write a spark program in Scala using RDD/Dataframe/Dataset API’s for below tasks. You need to use below versions of tools

●  Spark 2.x <br/>
●  Scala 2.x <br/>
●  Sbt/Maven <br/>

<br/>
You need to pass the tests written for each of the tasks.

**Task 1:**
Compute the moving average for every stockID with a predefined moving window size

Window Size : 3 <br/>
===Output Data== <br/>
_Columns_: stockId,timeStamp,stockPrice,moving_average <br/>
_Ordered by_: stockId,timeStamp


**Task 2:**
Join with Projection - Join moving average calculated in Task1 with the stockInfo. ​ 

Window Size : 3 <br/>
===Output Data== <br/>
_Columns_: stockId,timeStamp,stockPrice,moving_average,stockName,stockCategory

**Task 3:**
Get the projected moving average data for a given stockId

stockId :103 <br/>
===Output Data== <br/>
_Columns_:stockId,timeStamp,stockPrice,moving_average,stockName,stockCategory 

**Extensions:**

**Task 4:**
Handle Nulls <br/>
File : test/resources/input/stock_prices_dirty/0.csv 
contains null values in stockPrice column.

Filter all the non null rows 
Columns: stockId,timeStamp,stockPrice

**Other Extensions**

 _Task 1 :_ Count of each files
 Load all the above files as spark dataframes and print the count of each of files.

 _Task 2 :_ Handle Nulls
File : test/resources/input/stock_prices_dirty/0.csv
 contains null values in amount column. Replace those null values with mean of column in spark dataframe.

_Task 3 :_ Add a Sequential Row ID
For a joined dataframe, add an id column which contains sequence numbers from 1 to number of rows

_Task 4 :_ Sales with 5% Discount
Add a column ​discount_amount​ to joined dataframe, which holds 5% discounted amount. <br/><br/>

**WHAT IS MOVING AVERAGE** <br/>
Moving Average is a lagging indicator, which means that it do not predict new trends but confirm trends once they have been established. <br/>
A moving average is commonly used with time series data to smooth out short-term fluctuations and highlight longer-term trends or cycles. <br/><br/>
To compute the moving average you would need to input a set of values and a window size,  and compute the moving average of the input. <br/>
If the input values are x1, x2, x3, ..., xn and the window value is k the moving average is computed using the following formula:

yk = [x(k-k+1) + x(k-k+2) + .... + xk]/k

(All the windows with less than k elements gave output as 0)