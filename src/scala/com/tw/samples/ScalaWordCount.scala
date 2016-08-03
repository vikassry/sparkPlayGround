package com.tw.samples

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ScalaWordCount{
    def main(args: Array[String]) {
        if (args.length < 1) {
            println("Usage: ScalaWordCount <file>")
            System.exit(1)
        }
        val logFile:String = args(0)
        val sparkConf: SparkConf = new SparkConf().setAppName("JavaWordCount").setMaster("local")
        val sc: SparkContext = new SparkContext(sparkConf)
        val logData = sc.textFile(logFile, 2).repartition(2).cache()
        wordCount(logData)
    }

    def wordCount(textFile : RDD[String]): Unit ={
        val wordCounts: RDD[(String, Int)] = textFile.flatMap(_.split(" ").map(word => {(word, 1)})).reduceByKey((a, b)=>{a+b})
        val counts: Array[(String, Int)] = wordCounts.collect()
        println("All Word Counts :")
        counts.foreach(tuple=>{println(s"${tuple._1}:${tuple._2}")})
    }
}