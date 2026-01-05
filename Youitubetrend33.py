# Databricks notebook source
import requests
from pyspark.sql import Row

API_KEY = "I DONT WANT TO SHARE"
url = "WWW.Thanks"

params = {
    "part": "snippet,statistics",
    "chart": "mostPopular",
    "regionCode": "IN",
    "maxResults": 50,
    "key": API_KEY
}

response = requests.get(url, params=params)
data = response.json()

rows = []   # ✅ Separate list for Spark rows

for item in data.get("items", []):

    if "snippet" not in item or "statistics" not in item:
        continue

    rows.append(
        Row(
            title=item["snippet"].get("title"),
            channel=item["snippet"].get("channelTitle"),
            published_date=item["snippet"].get("publishedAt"),
            views=item["statistics"].get("viewCount", 0),
            likes=item["statistics"].get("likeCount", 0)
        )
    )

# ✅ Safety check
if not rows:
    raise ValueError("No valid records returned from API")

# ✅ Create Spark DataFrame
spark_df = spark.createDataFrame(rows)
spark_df.display()

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp

clean_df = (
    spark_df
    .withColumn("channel", col("channel").cast("string"))
    .withColumn("published_date", to_timestamp("published_date"))
    .withColumn("views", col("views").cast("int"))
    .withColumn("likes", col("likes").cast("int"))
    .dropDuplicates()
)
display(clean_df)

# COMMAND ----------

clean_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("youtube_trending")


