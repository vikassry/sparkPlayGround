package com.tw.samples

import org.apache.spark.rdd.RDD

case class Olympics(olympicsData: RDD[String]) {

  def totalMedalsWon(country: String, year: Int): Int = {
    return 0
  }

  def countryWithMaxMedalsFor(year: Int): String = {
    return ""
  }

  def playersWithMedalsInMoreThanOneSport(year: Int): Array[(String, String)] = {
    return Array()
  }


  def findYearOfMaxMedals(countryName: String): Int = {
    return 0
  }

  def parse(record: String): Record = {
    val strings: Array[String] = record.split(",")
    Record(
      strings(0),
      strings(1),
      strings(2).toInt,
      strings(3),
      strings(4).toInt,
      strings(5).toInt,
      strings(6).toInt,
      strings(7).toInt)
  }

  case class Record(
                     Athlete: String,
                     Country: String,
                     Year: Int,
                     Sport: String,
                     Gold: Int,
                     Silver: Int,
                     Bronze: Int,
                     Total: Int) {
  }

}
