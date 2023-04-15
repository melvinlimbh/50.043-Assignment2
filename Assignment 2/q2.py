import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# you may add more import if you need to


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 2").getOrCreate()
# YOUR CODE GOES BELOW
"""
find highest and lowest ratings -> join
"""

csv_file_path ="hdfs:///assignment2/part1/input/TA_restaurants_curated_cleaned.csv"
df2 = spark.read.csv(csv_file_path, header= True, inferSchema=True)
print("=================BEFORE=================")
#df2.filter(df2["Rating"].isNotNull)
#df2.show()

#City|Price Range|max(Rating)| -> need to rename "max(Rating)" to "Rating"
best_places = df2.filter(df2["Price Range"] != "null").groupBy("City","Price Range").max("Rating").withColumnRenamed("max(Rating)","Rating")
best_places.show()

#City|Price Range|min(Rating)|
worst_places =  df2.filter(df2["Price Range"] != "null").groupBy("City","Price Range").min("Rating")
worst_places.show()