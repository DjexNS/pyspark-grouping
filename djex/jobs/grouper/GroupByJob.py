from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from operator import itemgetter
from itertools import *

conf = SparkConf().setAppName('Grouper').setMaster("local").set("spark.speculation", "false")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

initial_dataframe = spark.read.csv("/home/djex/workspace/upwork/rana_khan_toronto/pyspark_grouping/test-input.csv")
# initial_dataframe.show(30)


def split_list(seq):
    groups = []
    for k, g in groupby(enumerate(seq), lambda x: x[0]-x[1]):
        groups.append(list(map(itemgetter(1), g)))
    return groups


mdf = initial_dataframe.groupBy("_c0", "_c1").agg(F.collect_list("_c2")).orderBy("_c1")\
    .withColumnRenamed("_c0", "tid")\
    .withColumnRenamed("_c1", "oid")\
    .withColumnRenamed("collect_list(_c2)", "timestamps")
mdf.show(30, False)

mdf1 = mdf.withColumn("is_properly_aggregated",
                      when((mdf['timestamps'][size('timestamps')-1] - mdf['timestamps'][0]) == size('timestamps') - 1, "true")
                      .otherwise("false")
                      )
mdf1.show(30)

mdf2 = mdf1.filter(F.col("is_properly_aggregated") == "false").sort("timestamps")
mdf2.show(30)

mdf3 = mdf2.select("*", F.col("timestamps"))



# mdf3 = mdf2.select("*", F.explode("res").alias("exploded_data"))







#//////////////////////////
# FOR THE END PHASE
# mdf2 = mdf1.filter(F.col("is_properly_aggregated") == "true").sort("timestamps")
# mdf2.show(30)
#
# mdf3 = mdf1.filter(F.col("is_properly_aggregated") == "false").sort("timestamps")
# mdf3.show(30)
#
# mdf2.union(mdf3).sort("timestamps").show(30)

# /////////////////////////
# mdf2 = mdf1.filter(F.col("is_properly_aggregated") == "true")
# mdf2.show(30)
#
# mdf3 = mdf2\
#     .withColumn("start_time", F.col("timestamps")[0])\
#     .withColumn("end_time", F.col("timestamps")[size("timestamps")-1])\
#     .withColumn("count", size(F.col("timestamps")))
# mdf3.show(30)
