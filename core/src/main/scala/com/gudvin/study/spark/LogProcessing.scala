package com.gudvin.study.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by vinita on 6/27/16.
  */
object LogProcessing {

  def main(args: Array[String]) {
    val sparkHome = "/usr/local/spark-1.6.1-hadoop2.6-firsttime/"
    val sparkMasterUrl = "spark://vinita-Lenovo-G50-80:7077"

    val conf: SparkConf = new SparkConf()
      .setAppName("My First Spark Application With Jar 123")
      .setMaster(sparkMasterUrl)
      .setSparkHome(sparkHome)
      .setJars(Array("/media/vinita/Projects/Workspaces/IntellijJ/SparkStudy/out/artifacts/" +
        "SparkStudy_jar/SparkStudy.jar"))
      .set("spark.hadoop.validateOutputSpecs", "false")

    val sc = new SparkContext(conf)

    //Loading data
    val logData = Seq("INFO started","ERROR Failed connection refused"
      ,"INFO stopped","ERROR Failed connection refused", "ERROR Failed connection refused","ERROR overloaded memory")
    val loadedFileRDD: RDD[String] = sc.parallelize(logData)
   // val filteredErrorLog = loadedFileRDD.filter(_.contains("ERROR"))
   // val filteredConnectionRefused = filteredErrorLog.filter(_.contains("connection refused"))

    // Map
    val status = loadedFileRDD.map(_.substring(0,10)).map(x => {
      if(x.contains("ERROR")&&x.contains("connection refused")) (1)
      else (0)
    })


    status.saveAsTextFile("path")


    // Reduce
    val connectionDroppingFrequency = status.reduce(sum)

    //val connectionDroppingFrequency = filteredConnectionRefused.count()
    println("Cisco your connection drop frequency = "+ connectionDroppingFrequency)
  }

  def sum(lastOutput: Int, input: Int): Int = {
    lastOutput+input
  }
}