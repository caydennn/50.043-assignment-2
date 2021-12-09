import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from pyspark.sql.types import StructField, StructType,StringType,ArrayType,IntegerType,DoubleType,BooleanType

# you may add more import if you need to


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 3").getOrCreate()

df = spark.read.option("header",True)\
    .option("inferSchema", True)\
    .csv("hdfs://%s:9000/assignment2/part1/input/" % (hdfs_nn))

split_cols = F.split(df["Reviews"],'\\], \\[')
df4 = df.withColumn('review',split_cols.getItem(0)).withColumn('date',split_cols.getItem(1))

df4 = df4.withColumn('review',F.split(F.col('review'),"\\', \\'"))
df4 = df4.withColumn('date',F.split(F.col('date'),"\\', \\'"))

df41 = df4.withColumn("new",F.arrays_zip("review","date"))\
    .withColumn("new",F.explode("new"))\
        .select("ID_TA",F.col("new.review").alias("review"),F.col("new.date").alias("date"))
df41 = df41.withColumn('review',F.regexp_replace('review',"'",""))
df41 = df41.withColumn('date',F.regexp_replace('date',"'",""))
df41 = df41.withColumn('review',F.regexp_replace('review',"\\[",""))
df41 = df41.withColumn('date',F.regexp_replace('date',"\\]",""))

df41.show()
df41.write.option("header",True).csv("hdfs://%s:9000/assignment2/output/question3/"% (hdfs_nn))