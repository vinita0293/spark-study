package com.gudvin.study.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by vinita baniwal on 6/26/16.
  */
case class Person(name: String, age: Long)//it  contains only the structure
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
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._// by using this we are able to dataframe in the code
    val loadedFileRDD: RDD[String] = sc.textFile("/usr/local/spark-1.6.1-hadoop2.6-firsttime/NOTICE", 5) //A

    //indexes are put after the each line of rdd.
    val df = loadedFileRDD.zipWithIndex().map(x => Person(x._1,x._2)).toDF()
    df.persist()
    df.registerTempTable("vini_table")

    val df_lt5 = sqlContext.sql("select * from vini_table where age < 5")
    df_lt5.show(false)//not truncate
    df_lt5.show()//truncate


  }
}