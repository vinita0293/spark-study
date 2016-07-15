package com.gudvin.study.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by vinita baniwal on 6/26/16.
  */

object SqlContextOperations {
  def main(args: Array[String]) {
    val sparkHome = "/usr/local/spark-1.6.1-hadoop2.6-firsttime/"
    val sparkMasterUrl = "spark://vinita-Lenovo-G50-80:7077"

    val conf: SparkConf = new SparkConf()
      .setAppName("My First Spark Application With Jar 123")
      .setMaster("local")
      .setSparkHome(sparkHome)
      .setJars(Array("/media/vinita/Projects/study_related/Workspaces/IntellijJ/SparkStudy/" +
        "out/artifacts/SparkStudy_jar/SparkStudy.jar"))
      .set("spark.hadoop.validateOutputSpecs", "false")

    val sc = new SparkContext(conf)
    val loadedFileRDD: RDD[String] = sc.textFile("/usr/local/spark-1.6.1-hadoop2.6-firsttime/NOTICE", 5) //A


    //count
   loadedFileRDD.count

    //
    //assert(sc==loadedFileRDD.context)
    println(sc, loadedFileRDD.context)

    //first element or row of rdd
    loadedFileRDD.first()

    loadedFileRDD.mapPartitions(x => x.map(_+""))

   val s=  loadedFileRDD.map(x => (x.charAt(0),x))//there are 2 col in rdd.

  }
}