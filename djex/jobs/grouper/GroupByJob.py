from itertools import *
from operator import itemgetter

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, ArrayType

conf = SparkConf().setAppName('UBI-test').setMaster("local").set("spark.speculation", "false")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

initial_dataframe = spark.read.csv("/home/djex/workspace/upwork/rana_khan_toronto/pyspark_grouping/test-input2.csv")


def split_list(input_seq):
    input_seq = list(map(int, input_seq))
    groups = []
    for k, g in groupby(enumerate(input_seq), lambda x: x[0]-x[1]):
        groups.append(list(map(itemgetter(1), g)))
    return groups


split_list_udf_ = udf(lambda sequence: split_list(sequence),
                      ArrayType(ArrayType(StringType()))
                      )

mdf = initial_dataframe.groupBy("_c0", "_c1").agg(F.collect_list("_c2")).orderBy("_c1")\
    .withColumnRenamed("_c0", "tid")\
    .withColumnRenamed("_c1", "oid")\
    .withColumnRenamed("collect_list(_c2)", "timestamps")

aggregatedDF = mdf.withColumn("is_properly_aggregated",
                      when((mdf['timestamps'][size('timestamps')-1] - mdf['timestamps'][0]) == size('timestamps') - 1, "true")
                      .otherwise("false")
                      )

goodDF = aggregatedDF.filter(F.col("is_properly_aggregated") == "true").sort("timestamps")

badDF = aggregatedDF.filter(F.col("is_properly_aggregated") == "false").sort("timestamps")

split_badDF = badDF.select("tid",
                   "oid",
                   split_list_udf_("timestamps")).withColumnRenamed("<lambda>(timestamps)", "split_timestamps")

exploded_df = split_badDF.select("*", F.explode("split_timestamps").alias("timestamps"))

exploded_df2 = exploded_df.drop("split_timestamps")

trueDF = goodDF.drop("is_properly_aggregated")

unionedDF = trueDF.union(exploded_df2).sort("tid", "timestamps")

final_df = unionedDF\
    .withColumn("start_time", F.col("timestamps")[0])\
    .withColumn("end_time", F.col("timestamps")[size("timestamps")-1])\
    .withColumn("count", size(F.col("timestamps")))\
    .drop("timestamps")
final_df.show(150, False)
