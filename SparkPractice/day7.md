## Order Details
Find order details made by Jill and Eva.
Consider the Jill and Eva as first names of customers.
Output the order date, details and cost along with the first name.
Order records based on the customer id in ascending order.

```python
from pyspark.sql import functions as F

def join(cust_df, order_df):
    df_joined = cust_df.join(order_df, cust_df["cust_id"] == order_df["id"], how="inner")  # ✅ "inner" as string
    return df_joined

def filtered(df):
    return df.filter(                                        # ✅ use df, not df_joined
        (F.col("first_name") == 'Jill') | (F.col("first_name") == 'Eva')  # ✅ use | not or
    ).orderBy("id")

result_df = cust_df.transform(join(cust_df, order_df)).transform(filtered)  # ✅ pass order_df
```