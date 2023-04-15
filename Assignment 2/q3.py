import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# you may add more import if you need to


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 3").getOrCreate()
# YOUR CODE GOES BELOW
"""
Reviews = "[[reviews], [dates]]" - find a way to split these
"""

csv_file_path ="hdfs:///assignment2/part1/input/TA_restaurants_curated_cleaned.csv"
df2 = spark.read.csv(csv_file_path, header= True, inferSchema=True)

reviews_and_dates = split(df2["Reviews"],"\\], \\[")
df2 = df2.withColumn("Review",reviews_and_dates.getItem(0)).withColumn("Date",reviews_and_dates.getItem(1))
# now is multiple reviews and dates in 1 line -> split again

df2.show()

df2 = df2.withColumn("Review", split(df2["Review"], "\\', \\'")).withColumn(
    "Date", split(df2["Date"], "\\', \\'")
)
