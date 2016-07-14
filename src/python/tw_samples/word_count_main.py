from pyspark import SparkContext

from tw_samples import *
from tw_samples.python_word_count import wordcount

inputFile = sys.argv[1]
sc = SparkContext('local', "Word Count")
text_file = sc.textFile(inputFile)
counts = wordcount(text_file)

counts.saveAsTextFile("../../../outputFile")
