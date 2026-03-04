Write a function that performs the following transformations on the input DataFrame:

Extract Email Domain: From the email field, extract the domain name (text after @).

Anonymize Phone Numbers: Mask the first six digits of the phone number with asterisks (*) and retain only the last 4 digits.

The output should include the following columns:

anon_phone: Anonymized phone number (e.g., ******1234)

email_domain: Extracted domain name (e.g., example.com)

user_id: Original user ID
Finally, sort the result by phone number in ascending order (before masking).

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(input_df):
	# Write code here
    return (
        input_df
        # Extract email domain
        .withColumn("email_domain", F.split(F.col("email"), "@")[1])
        
        # Anonymize phone number: ****** + last 4 digits
        .withColumn("anon_phone", F.concat(F.lit("******"), F.substring(F.col("phone"), -4, 4)))
        
        # Sort by original phone number (before masking)
        .orderBy(F.col("phone").asc())
        
        # Select required columns
        .select("anon_phone", "email_domain", "user_id")
    )

```