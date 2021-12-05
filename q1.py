import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, length, asc
from pyspark.sql import Row
# you may add more import if you need to
from pyspark.sql.types import ArrayType, StringType

from ast import literal_eval

from pyspark.sql.functions import split, regexp_replace


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 1").getOrCreate()
# YOUR CODE GOES BELOW

df = spark.read.option("header",True)\
    .option("inferSchema", True)\
    .csv("hdfs://%s:9000/assignment2/part1/input/" % (hdfs_nn))

#
# THE EMPTY REVIEWS HAVE A LENGTH OF 14
df = df.filter(length('Reviews') > 14).filter(col("Rating") >= 1.0)
# The sanity check is done by checking the group by of the number of reviews 
df.groupBy('Reviews').agg({'Reviews': 'count'}).sort(desc("count(Reviews)")).show()

df.write.csv("hdfs://%s:9000/assignment2/output/question1/" % (hdfs_nn), header=True)
print("CSV Written")
