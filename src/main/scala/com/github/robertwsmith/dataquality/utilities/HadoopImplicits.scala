package com.github.robertwsmith.dataquality.utilities

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/** Some implicits to add Hadoop File System functionality to Spark.
  */
object HadoopImplicits {

  /** Adds a [[FileSystem]] to a [[SparkContext]]
    *
    * @param sparkContext active [[SparkContext]]
    */
  implicit class SparkContextHadoop(sparkContext: SparkContext) {
    lazy val hadoopFileSystem: FileSystem = {
      FileSystem.get(sparkContext.hadoopConfiguration)
    }
  }

  /** Adds a [[FileSystem]] to a [[SparkSession]]
    *
    * @param sparkSession active [[SparkSession]]
    */
  implicit class SparkSessionHadoop(sparkSession: SparkSession)
      extends SparkContextHadoop(sparkSession.sparkContext)

}
