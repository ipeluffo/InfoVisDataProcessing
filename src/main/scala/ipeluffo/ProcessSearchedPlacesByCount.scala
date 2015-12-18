package ipeluffo

import java.io.File

import ipeluffo.util.Utils
import org.apache.hadoop.fs.FileUtil
import org.apache.spark.rdd.RDD

/**
  * Created by ipeluffo on 12/13/15.
  */
object ProcessSearchedPlacesByCount {

  def main (args: Array[String]): Unit = {
    if (args.length < 2) {
      println("Usage: ProcessSearchedPlacesByCount dataset_path has_header")
      println("dataset_path: Path to dataset file")
      println("has_header: boolean that indicates if dataset has header or not")
      System.exit(1)
    }

    val datasetFilePath = args(0)

    val sc = Utils.createSparkContext("InfoVisDataProcessing:ProcessSearchedPlacesByCount")
    val dataset = sc.textFile(datasetFilePath)

    val datasetWithoutHeader = if (args(1) != null && args(1).toBoolean) dropHeader(dataset) else dataset

    println("Count of entries: "+datasetWithoutHeader.count())

    val datasetMap = datasetWithoutHeader.map(l => {
      val parts = l.split(",")
      (parts(1)+":"+parts(2), 1)
    })

    val mostSearchedPlaces = datasetMap
      .reduceByKey((a, b) => a+b)
      .sortBy(t => t._2, ascending = false)
      .map(t => {
        val longLatParts = t._1.split(":")
        val long = longLatParts(0)
        val lat = longLatParts(1)
        val count = t._2
        long+","+lat+","+count
      })

    val tmpFile = "./mostSearchedPlacesTmp.csv"
    FileUtil.fullyDelete(new File(tmpFile))

    mostSearchedPlaces.saveAsTextFile(tmpFile)

    val mostSearchedPlacesCSVFile = "./mostSearchedPlaces.csv"
    FileUtil.fullyDelete(new File(mostSearchedPlacesCSVFile))

    Utils.merge(tmpFile, mostSearchedPlacesCSVFile)

    // Delete tmp "HDFS type" files
    FileUtil.fullyDelete(new File(tmpFile))
    println("Process finished. Count of unique records: "+mostSearchedPlaces.count())
  }

  def dropHeader(data: RDD[String]): RDD[String] = {
    data.mapPartitionsWithIndex((idx, lines) => {
      if (idx == 0) {
        lines.drop(1)
      }
      lines
    })
  }

}
