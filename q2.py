import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, length, asc, lit

# you may add more import if you need to


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 2").getOrCreate()
# YOUR CODE GOES BELOW

df = spark.read.option("header",True)\
    .option("inferSchema", True)\
    .csv("hdfs://%s:9000/assignment2/part1/input/" % (hdfs_nn))

# Drop the rows that have Price Range as null
df = df.na.drop(subset=["Price Range"])
df.printSchema()
feature_group = ["Price Range", "City"]
maxSet = df.groupBy(feature_group).agg({"Rating": "max"}).withColumn("Rating", col("max(Rating)"))
# maxSet.show()
minSet = df.groupBy(feature_group).agg({"Rating": "min"}).withColumn("Rating", col("min(Rating)"))

unionDf = maxSet.union(minSet)
# unionDf.show()

df_join = unionDf.join(df, on=["Price Range", "City", "Rating"], how='inner')
# df_join.sort(asc("City")).show()
df_join = df_join.sort(asc("City")).dropDuplicates(["Price Range", "City", "Rating"])

df_join.show()
# joinedMax = df.join(maxSet,feature_group).dropDuplicates().show()

# df_agg = df.groupby('date').agg(F.max('close').alias('close_agg')).withColumn("dummy",F.lit("dummmy")) # dummy column is needed as a workaround in spark issues of self join
# df_join = df.join(df_agg,on='date',how='left')
df_join.write.csv("hdfs://%s:9000/assignment2/output/question2/" % (hdfs_nn), header=True)
print("CSV Written")