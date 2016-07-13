from tw_samples import *
from pyspark import SparkContext


inputFile = sys.argv[1]
sc = SparkContext('local', "Word Count")
text_file = sc.textFile(inputFile)

counts = text_file.flatMap(lambda line: line.split(" ")) \
    .map(lambda word: (word, 1)) \
    .reduceByKey(lambda a, b: a + b)

counts.saveAsTextFile("../../outputFile")
