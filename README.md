# NASDAQ-Financial-Analysis-using-Hadoop-MapReduce
CSE587: Data Intensive Computing (Spring 2015)

Problem: 
(a) Use Mapreduce to compute the volatility of stocks in NASDAQ
(b) Evaluate the scalability of your implementation on different data sizes and
number of nodes.

Use Mapreduce on a Hadoop environment at CCR to compute the monthly volatility of stocks. You are given daily data of 2970 stocks on NASDAQ market for 3
years from 01/01/2012 to 12/31/2014 (except holidays, otherwise called trading days). Imagine that you are a data analyst working for an investment company, your daily job is to analyze stock
price data, and find out which stocks in a certain period have higher earnings potential, etc. One characteristic that is widely used by traders is the volatility index. You can get more details at
http://stockcharts.com/school/doku.php?id=chart_school:technical_indicators:standard_deviation_volatility. Your data is 2970 CSV format files (Comma Separated). Each file contains the data for one stock using its symbol as the file name. A stock list file is also provided.
