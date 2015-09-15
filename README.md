# NASDAQ Financial Analysis using Hadoop MapReduce

Used mapreduce  on a Hadoop environment at Center for Computational Research (CCR), Buffalo to compute stock volatility of companies listed on NASDAQ. Analysed scalability of the implementation on different data sizes and number of nodes.

**Dataset**: CSV records of 2970 stocks spanning over 3 years (~30,000 files, 30 mn rows).Each file contains the data for one stock using its symbol as the file name. A stock list file is provided for cross reference.
1. [Large Dataset](https://drive.google.com/open?id=0B5SqMYMuLpXyWFNKWXU1el9zZ0E)
2. [Medium Dataset](https://drive.google.com/open?id=0B5SqMYMuLpXyS2w4QTFzM1dFZ0k)
3. [Small Dataset](https://drive.google.com/open?id=0B5SqMYMuLpXybmYxUEJiMWE1Qkk)


**References**: [MapReduce: Simplified Data Processing on Large Clusters by Jeffrey Dean & Sanjay Ghemawat](http://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf)

Details about stock volatility [here] (http://stockcharts.com/school/doku.php?id=chart_school:technical_indicators:standard_deviation_volatility)
