You are given a table named correct_social_media_post that contains posts from various social media platforms. Each post includes metadata like likes, shares, and the post content.

🎯 Your Task
Write a SQL query that performs the following operations:

Replace every case-sensitive occurrence of "Python" with "PySpark" in the text column.

Return all columns but in the following specific order:

comments

date

id

likes

platform

shares

text

Sort the final output by comments in ascending order.


```python

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(social_media):
	# Write code here
  return (
    social_media.withColumn("text",F.regexp_replace(F.col("text"),"Python","PySpark"))\
      .select("comments","date","id","likes","platform","shares","text").orderBy("comments")
  )



```