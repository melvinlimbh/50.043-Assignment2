import sys
from pyspark.sql import SparkSession

# you may add more import if you need to


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 3").getOrCreate()
# YOUR CODE GOES BELOW
sc = SparkSession.sparkContext
csv_file_path ="hdfs:///assignment2/part1/input/TA_restaurants_curated_cleaned.csv"

df2 = spark.read.csv(csv_file_path, header= True, inferSchema=True)
#date = df2.select(df2["Reviews"])


moddata = [("review", "Date")]
distmodData = sc.parallelize(moddata)
moddf = distmodData.toDF(["module", "modname"])
df2.join(moddf, df2["module"] == moddf["module"], "inner")\
   .select(df2["ID_TA"]).show()
