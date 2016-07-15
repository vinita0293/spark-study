/*
package com.gudvin.study.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by vinita on 7/3/16.
  */

//adding extends Serializable wont help
class testing {
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
  val list = List(1,2,3)

  val rddList = sc.parallelize(list)

  def doIT =  {
    //again calling the fucntion someFunc
    val after = rddList.map(someFunc(_))
    //this will crash (spark lazy)
    after.collect().map(println(_))
  }

  def someFunc(a:Int) = a+1

}

                                                                                                                                                                                                                  object NOTworking extends App {
  new testing().doIT
}*/
