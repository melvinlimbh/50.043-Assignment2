import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# you may add more import if you need to


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 2").getOrCreate()
# YOUR CODE GOES BELOW
"""
find highest and lowest ratings 
-> union to combine attributes selected 
-> join to have all the other columns
"""

csv_file_path ="hdfs:///assignment2/part1/input/TA_restaurants_curated_cleaned.csv"
df2 = spark.read.csv(csv_file_path, header= True, inferSchema=True)
print("=================BEFORE=================")
#df2.filter(df2["Rating"].isNotNull)
#df2.show()

#City|Price Range|max(Rating)| -> need to rename "max(Rating)" to "Rating"
best_places = df2.filter(df2["Price Range"] != "null").groupBy(["City","Price Range"]).max("Rating").withColumnRenamed("max(Rating)","Rating")
best_places.show()
print("BEST")

#City|Price Range|min(Rating)|
worst_places =  df2.filter(df2["Price Range"] != "null").groupBy(["City","Price Range"]).min("Rating").withColumnRenamed("min(Rating)","Rating")
worst_places.show()
print("WORSE")

union_places = best_places.union(worst_places)
union_places.show()
print("UNION")

combined = union_places.join(df2, on=["Price Range", "City", "Rating"], how="inner")
combined = combined.dropDuplicates(["Price Range", "City", "Rating"]).select(
        "_c0",
        "Name",
        "City",
        "Cuisine Style",
        "Ranking",
        "Rating",
        "Price Range",
        "Number of Reviews",
        "Reviews",
        "URL_TA",
        "ID_TA",
    )

combined.show()