import sys
from pyspark.sql import SparkSession

# you may add more import if you need to


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 3").getOrCreate()
# YOUR CODE GOES BELOW
"""
"[[reviews], [dates]]" - find a way to split these
"""
alphabets = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

sc = spark.sparkContext
csv_file_path ="hdfs:///assignment2/part1/input/TA_restaurants_curated_cleaned.csv"
df2 = spark.read.csv(csv_file_path, header= True, inferSchema=True)
df2.filter(df2["Reviews"].contains(alphabets))


# moddata = [("review", "Date")]
# distmodData = sc.parallelize(moddata)
# moddf = distmodData.toDF(["review", "Date"])
# df2.join(moddf, "inner").select(df2["ID_TA"]).show()
