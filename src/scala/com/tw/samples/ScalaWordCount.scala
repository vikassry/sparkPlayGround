package com.tw.samples

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ScalaWordCount{
    def main(args: Array[String]) {
        if (args.length < 1) {
            println("Usage: ScalaWordCount <file>")
            System.exit(1)
        }
        val inputFile:String = args(0)
        val sparkConf: SparkConf = new SparkConf().setAppName("JavaWordCount")
        val sc: SparkContext = new SparkContext(sparkConf)
        val textFile = sc.textFile(inputFile)
        val counts: RDD[(String, Int)] = wordCount(textFile)
        counts.saveAsTextFile("output")
    }

    def wordCount(textFile: RDD[String]): RDD[(String, Int)] = {
        val counts = textFile.flatMap(line => line.split(" "))
            .map(word => (word, 1))
            .reduceByKey(_ + _)
        counts
    }
}