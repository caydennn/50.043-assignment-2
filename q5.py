import sys 
from pyspark.sql import SparkSession
import ast

# you may add more import if you need to

# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 5").getOrCreate()
# YOUR CODE GOES BELOW

input = spark.read.option("header", True).option("inferSchema", "true")\
.parquet("hdfs://%s:9000/assignment2/part2/input/tmdb_5000_credits.parquet"% (hdfs_nn))

inputRdd = input.rdd

def get_actors(line):
    cast_ls = ast.literal_eval(line[2])
    answer = []
    actors = []
    for i in range(len(cast_ls)-1):
        for j in range(i+1,len(cast_ls)):
            if cast_ls[i]["name"] > cast_ls[j]["name"]:
                actor = cast_ls[i]["name"] + ',' + cast_ls[j]["name"]
            else:
                actor = cast_ls[j]["name"] + ',' + cast_ls[i]["name"]
            if actor not in actors:
                actors.append(actor)
                answer.append((actor, str(line[0]) +','+line[1]) )


    return answer

movie_with_actors = inputRdd.flatMap(get_actors)


counts = movie_with_actors.map(lambda line: (line[0], 1)).reduceByKey(lambda x,y: x+y).filter(lambda line: line[1] >= 2)





'''
{ "cayden, marcus" : 6
"cayden, felice": 1, # filtered out 
.
.
.
    }

'''

actorsCrossActressMore2 = movie_with_actors.join(counts).sortByKey().map(lambda line: (line[1][0].split(',')[0], line[1][0].split(',')[1], line[0].split(',')[0], line[0].split(',')[1])).distinct()

df = spark.createDataFrame(actorsCrossActressMore2).toDF("movie_id", "title", "actor1", "actor2")
 

df.show()

df.write.parquet("hdfs://%s:9000/assignment2/output/question5/" % (hdfs_nn)) 
print("pArquet wRitten")