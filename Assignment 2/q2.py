import sys
from pyspark.sql import SparkSession

# you may add more import if you need to


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 2").getOrCreate()
# YOUR CODE GOES BELOW
"""
find distinct cities, filter by lowest and highest price ranges that are not null
"""

csv_file_path ="hdfs:///assignment2/part1/input/TA_restaurants_curated_cleaned.csv"
df2 = spark.read.csv(csv_file_path, header= True, inferSchema=True)
print("=================BEFORE=================")
df2.filter(df2["Price Range"].isNotNull) # remove null
df2.show()
print("=================AFTER=================")
df2.groupBy("City").max("Rating").show()
# print("MIN")
# df4 = df2.groupBy(df2["City"]).min().show()