package com.gudvin.study.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by vinita on 6/26/16.
  */

object MySparkOnYarn {
  def main(args: Array[String]) {
    val sparkHome = "/usr/local/spark-1.6.1-hadoop2.6-firsttime/"
    val sparkMasterUrl = "yarn-client"

    val conf: SparkConf = new SparkConf()
      .setAppName("My First Spark Application With Jar 123")
      .setMaster(sparkMasterUrl)
      .setSparkHome(sparkHome)
      .setJars(Array("/media/vinita/Projects/study_related/Workspaces/IntellijJ/SparkStudy/" +
        "out/artifacts/SparkStudy_jar/SparkStudy.jar"))
      .set("spark.hadoop.validateOutputSpecs", "false")

    val sc = new SparkContext(conf)
    val loadedFileRDD: RDD[String] = sc.textFile("/usr/local/spark-1.6.1-hadoop2.6-firsttime/NOTICE", 5)

   /*val transformedRDD = loadedFileRDD.map(line => {
     (line.hashCode,line.length,line.contains("a"))
   })

    val transRDD2 = transformedRDD.map(x => (x._1 + "HashCode",x._2,x._3))
*/

    val t1 = loadedFileRDD.map(_.trim).map(_.capitalize)

    // Logs
    /*
         INFO started
         ERROR Failed connection refusd
         INFO stopped
     */

    //loadedFileRDD.filter(_.)

    //action
    //Type1
    //transformedRDD.count()

    //Type2
    t1.saveAsTextFile("/home/vinita/Documents/FirstOutput_withyarn")
    //transformedRDD.collect()
  }
}