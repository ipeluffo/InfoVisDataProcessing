package ipeluffo

import java.io.File

import ipeluffo.util.Utils
import org.apache.hadoop.fs.FileUtil

/**
  * Created by ipeluffo on 12/14/15.
  */
object FinalDataProcessing {

  def main(args: Array[String]) {
    if (args.length < 2) {
      println("Usage: FinalDataProcessing original_dataset_path corners_dataset_path")
      println("original_dataset_path: Path to dataset file")
      println("corners_dataset_path: Path to dataset file of corners")
      System.exit(1)
    }

    val originalDatasetPath = args(0)
    val cornersDatasetPath = args(1)

    val sc = Utils.createSparkContext("InfoVisdataProcessing:FinalDataProcessing")

    // "date time,lng,lat"
    val dataset = sc.textFile(originalDatasetPath)
    println("Number of records for Original Dataset : "+dataset.count())

    // "lng,lat,count,lng_corner,lat_corner,street1,street2"
    val searchedCorners = sc.textFile(cornersDatasetPath)
    println("Number of records for Corners Dataset : "+searchedCorners.count())

    // ("lng:lat", "date time")
    // time format replaced to use only hour. For example: 06:25:33 ===> 06:00:00
    val datasetMap = dataset.map(l => {
      val parts = l.split(",")
      val dateTimeParts = parts(0).split(" ")
      (parts(1)+":"+parts(2), dateTimeParts(0)+" "+dateTimeParts(1).substring(0,2)+":00:00")
    })

    // ("lng:lat", (lng_corner, lat_corner, "street1 y street2"))
    val searchedCornersMap = searchedCorners.map(l => {
      val parts = l.split(",")
      // With this comparison we avoid the cases where the same corner has the streets in different order
      val corner = if (parts(5) < parts(6)) parts(5)+" y "+parts(6) else parts(6)+" y "+parts(5)
      (parts(0)+":"+parts(1), (parts(3), parts(4), corner))
    })

    // ("lng:lat", ("date time", (lng_corner, lat_corner, "street1 y street2")))
    val join = datasetMap.join(searchedCornersMap)

    // ("lng_corner:lat_corner:date Time", ("date time", lng_corner, lat_corner, "street1 y street2", 1))
    val joinMap = join.map(t => {
      (t._2._2._1+":"+t._2._2._2+":"+t._2._1+":"+t._2._2._3, (t._2._1, t._2._2._1, t._2._2._2, t._2._2._3, 1))
    })

    // "date, time, lng_corner, lat_corner, 'street1 y street2', count"
    val joinReduced = joinMap
      .reduceByKey((v1, v2) => (v1._1, v1._2, v1._3, v1._4, v1._5 + v2._5))
      .map(t => t._2)
      .sortBy(t => t._5, ascending=false)
      .map(t => t.productIterator.toList.mkString(","))

    // Save results
    val tmpFile = "./searchedCornersResultTmp.csv"
    FileUtil.fullyDelete(new File(tmpFile))

    joinReduced.saveAsTextFile(tmpFile)

    val mostSearchedPlacesCSVFile = "./searchedCornersResult.csv"
    FileUtil.fullyDelete(new File(mostSearchedPlacesCSVFile))

    Utils.merge(tmpFile, mostSearchedPlacesCSVFile)

    // Delete tmp "HDFS type" files
    FileUtil.fullyDelete(new File(tmpFile))

    println("Process finished. Count of records: "+joinReduced.count())
  }

}
