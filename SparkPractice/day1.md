# 🏙️ Population of Japan

## 📌 Problem Description

You are given a table containing information about cities around the world.  
Each row represents a city along with its population and country code.

Write a query to **calculate the total population of all cities in Japan**.

The **country code for Japan is `JPN`**.

---

## 📂 Dataset

### Table Name
- `City` (LeetCode / SQL)
- `city_data` (PySpark DataFrame)
- `jcp_city_pop` (Spark SQL / DBT)

### Description
The dataset contains city-level demographic details for cities across multiple countries.

---

## 🧱 Table Schema

| Column Name    | Type     | Description                          |
|---------------|----------|--------------------------------------|
| `Id`          | INT      | Unique identifier for each city      |
| `Name`        | VARCHAR  | Name of the city                     |
| `COUNTRYCODE` | VARCHAR  | ISO country code                     |
| `DISTRICT`    | VARCHAR  | District or region of the city       |
| `POPULATION`  | INT      | Population of the city               |

---

## 📊 Example Input

| Id | Name      | COUNTRYCODE | DISTRICT | POPULATION |
|----|-----------|-------------|----------|------------|
| 1  | Tokyo     | JPN         | Kanto    | 13929286   |
| 2  | Osaka     | JPN         | Kansai   | 2691167    |
| 3  | Kyoto     | JPN         | Kansai   | 1474570    |
| 4  | Nagoya    | JPN         | Chubu    | 2304879    |
| 5  | Fukuoka   | JPN         | Kyushu   | 1587352    |
| 6  | Hiroshima | JPN         | Chugoku  | 1192011    |

---

## ✅ Expected Output

| Total Population |
|------------------|
| 23179265         |

---

## ⚠️ Constraints

- Column names and values are **case-sensitive**
- Filter must use `COUNTRYCODE = 'JPN'`
- Output should return **only one column**
- Output column name must be **`Total Population`**

---

## 🧠 Solution Approach

Filter the rows belonging to Japan using the country code `JPN` and compute the sum of the `POPULATION` column.

---

## ✅ Solution 1: Spark SQL

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()
def etl(city_data):
    
    city_data.createOrReplaceTempVi("jcp_city_pop")
    ans = spark.sql("""
            SELECT SUM(Population) AS total_population
            FROM jcp_city_pop
            WHERE CountryCode = 'JPN'
        """)
        return ans
```
## ✅ Solution 2: Pyspark Dataframe 

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()
def etl(city_data):
    ans = (
        city_data
        .filter(F.col("CountryCode") == "JPN")
        .agg(F.sum("Population").alias("total_population"))
    )
    return ans

```