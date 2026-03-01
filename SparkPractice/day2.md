You're working as a Data Engineer at a video streaming platform. Your team wants to highlight popular and recently released videos on the homepage to boost user engagement.

You’ve been provided with a DataFrame called video_stream_df, which contains metadata about all the videos on the platform.

🎯 Task
Write a function that:

Accepts video_stream_df as input.

Returns a filtered DataFrame that includes only the videos which:

Have more than 1,000,000 views

Were released in the last 6 years (relative to the current year)

Reorders the columns in the following format:

duration, genre, release_year, title, video_id, view_count
Sort the data using column duration

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import datetime


spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()


def etl(video_stream_df):
   current_year = datetime.datetime.now().year
   filtered_df = video_stream_df.where(
    (F.col("view_count") > 1000000) & (F.col("release_year")>= current_year - 6))

   output_df = filtered_df.select("duration", "genre", "release_year", "title", "video_id", "view_count").orderBy("duration")
   return output_df


```