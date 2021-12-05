import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import Row
# you may add more import if you need to
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.functions import desc
from ast import literal_eval

from pyspark.sql.functions import split, regexp_replace


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 1").getOrCreate()
# YOUR CODE GOES BELOW

df = spark.read.option("header",True)\
    .option("inferSchema", True)\
    .csv("hdfs://%s:9000/assignment2/part1/input/" % (hdfs_nn))

# df.printSchema()



def castToArray(row):
    # row.Reviews = literal_eval(row.Reviews)

    row = row.asDict()
    row['Reviews'] = literal_eval(row['Reviews'])
    return Row(**row)

# df = df.foreach(castToArray)





df2 = df.withColumn(
    "ReviewsCleaned",
    split(regexp_replace("Reviews", r"(^\[\[)|(\]\]$)", ""), ", ")
)

df2 = df2.withColumn("ReviewsCleaned", col("ReviewsCleaned").cast(ArrayType(StringType())))
df2.show()
df2.printSchema()

df2.foreach(lambda row: print(row.ReviewsCleaned[0]))

# df = df.withColumn('Reviews', col('Reviews').cast(ArrayType(ArrayType(StringType()))))




# df = df.filter( (len(col("Reviews") > 0) or  len(col("Reviews")[0]) >= 0))
# df.groupBy('Reviews').agg({'Reviews': 'count'}).sort(desc("count(Reviews)")).show()

# def f(row):

#     if (len(row.Reviews) == 0 or len(row.Reviews[0]) == 0):

# df.select(df["Rating"]).distinct().collect()


# df.filter(col("Ranking") == 1.0).show()
# df.filter(col("Number of Reviews") == 0.0).show()
# df = df.filter(col("Rating") >= 1.0 ).filter(col("Number of Reviews") > 0.0).show()
# df.write.csv("hdfs://%s:9000/assignment2/output/question1/TA_restaurants_curated_cleaned.csv")
