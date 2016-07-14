import unittest

from pyspark.context import SparkContext

from tw_samples import python_word_count


class PySparkTestCase(unittest.TestCase):
    def setUp(self):
        class_name = self.__class__.__name__
        self.sc = SparkContext('local', class_name)

    def tearDown(self):
        self.sc.stop()

    def test_should_be_able_to_word_count(self):
        rdd = self.sc.parallelize(["This is a text", "Another text", "More text", "a text"])
        result = python_word_count.wordcount(rdd)
        expected = [('a', 2), ('This', 1), ('text', 4), ('is', 1), ('Another', 1), ('More', 1)]
        self.assertEquals(expected, result.collect())
