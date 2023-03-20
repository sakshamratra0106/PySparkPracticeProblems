from pyspark.sql import SparkSession
import sys
import logging
from operator import add

logging.basicConfig(level=logging.INFO)


def init_spark():
    spark = SparkSession.builder.appName("WordCharacterCount").getOrCreate()
    sc = spark.sparkContext
    return spark, sc


def WordCountFromTextmain(args):
    spark, sc = init_spark()
    logging.info("System Arguments passed are Are {}".format(
        args
    ))

    # Read Text Data Set
    # Output is in form of DF
    data = spark.read.text(args[1].split(",")[0])

    # data.show()

    lines = data.rdd.map(lambda r: r[0])
    counts = lines.flatMap(lambda x: x.split(" ")) \
        .map(lambda x: (x, 1)) \
        .reduceByKey(add)

    output = counts.collect()

    for word, count in output:
        if int(count) >= 10:
            print("{} has frequency {}".format(
                word, count
            ))


def WordCountFromCSVmain(args):
    spark, sc = init_spark()
    logging.info("System Arguments passed are Are {}".format(
        args
    ))

    # Read Text Data Set
    # Output is in form of DF
    data = spark.read.csv(args[1].split(",")[1], header=True)
    founders_frequency = data.select("funder").rdd \
        .map(lambda x: x[0]) \
        .map(lambda x: (x, 1)) \
        .reduceByKey(add) \
        .collect()

    for founder, count in founders_frequency:

        if int(count) >= 100:
            print("Founder {} has frequency {}".format(
                founder, count
            ))


def CharCountFromTextmain(args):
    spark, sc = init_spark()
    logging.info("System Arguments passed are Are {}".format(
        args
    ))

    # Read Text Data Set
    # Output is in form of DF
    data = spark.read.text(args[1].split(",")[0])

    # data.show()

    # flatMap has been added in place of MAP in below line
    lines = data.rdd.flatMap(lambda r: r[0])
    counts = lines.flatMap(lambda x: x.split(" ")) \
        .map(lambda x: (x, 1)) \
        .reduceByKey(add)

    output = counts.collect()

    for word, count in output:
        if int(count) >= 10:
            print("{} has frequency {}".format(
                word, count
            ))


if __name__ == '__main__':
    WordCountFromTextmain(sys.argv)
    WordCountFromCSVmain(sys.argv)
    CharCountFromTextmain(sys.argv)
