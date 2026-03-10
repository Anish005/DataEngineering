## Call Center

You are given two DataFrames, calls_df and customers_df, which contain information about calls made by customers of a telecommunications company and information about the customers, respectively.

Write a function that returns the number of distinct customers who made calls on each date, along with the total duration of calls made on each date.

```python

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(calls_df, customers_df):
	# Write code here
    df = calls_df.join(customers_df,on="cust_id")\
  .groupBy('date').agg(F.count('cust_id').alias('num_customers'),\
  F.sum('duration').alias('total_duration')).orderBy('date')
    return(df)




```

