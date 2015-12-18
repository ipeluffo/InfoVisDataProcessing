package ipeluffo.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileUtil, FileSystem}
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext, Logging}

/**
  * Created by ipeluffo on 12/13/15.
  */
object Utils extends Logging {

  def createSparkContext(appName:String): SparkContext = {
    val conf = new SparkConf().setMaster("local[*]").setAppName(appName)
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    Utils.setSparkLogLevel(Level.WARN)
    return sc
  }

  def setSparkLogLevel(level:Level): Unit = {
    val log4jInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements
    if (!log4jInitialized) {
      logInfo(s"Setting log level to [WARN]...")
    }
    Logger.getRootLogger.setLevel(level)
  }

  def merge(srcPath: String, dstPath: String): Unit =  {
    val hadoopConfig = new Configuration()
    val hdfs = FileSystem.get(hadoopConfig)
    FileUtil.copyMerge(hdfs, new Path(srcPath), hdfs, new Path(dstPath), false, hadoopConfig, null)
  }

}