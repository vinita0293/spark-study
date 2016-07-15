package com.gudvin.study.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by vinita baniwal on 6/26/16.
  */

object MySpark_PersisOrCache {
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

    val loadedFileRDD1 = loadedFileRDD.map(_+"it") //B
    loadedFileRDD1.persist(StorageLevel.DISK_ONLY_2)

    //below three lines are same
    loadedFileRDD1.cache()
    loadedFileRDD1.persist()
     loadedFileRDD1.persist(StorageLevel.MEMORY_ONLY)

   val rdd1_1 = loadedFileRDD1.map(_+"_t1") //C
   val rdd1_2 = loadedFileRDD1.map(_+"_t2") //D

  }
}