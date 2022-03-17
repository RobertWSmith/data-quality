package com.github.robertwsmith.dataquality.utilities

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/** Some implicits to add Hive functionality to Spark.
  */
object HiveImplicits {

  /** Adds a [[HiveConf]] and a [[HiveMetaStoreClient]] to an active [[SparkContext]].
    *
    * @param sparkContext active [[SparkContext]]
    */
  implicit class SparkContextHive(sparkContext: SparkContext) {
    lazy val hiveConfiguration: HiveConf =
      new HiveConf(sparkContext.hadoopConfiguration, classOf[Configuration])

    lazy val hiveMetaStoreClient: HiveMetaStoreClient = new HiveMetaStoreClient(
      this.hiveConfiguration
    )
  }

  /** Adds a [[HiveConf]] and a [[HiveMetaStoreClient]] to an active [[SparkSession]].
    *
    * @param sparkSession active [[SparkSession]]
    */
  implicit class SparkSessionHive(sparkSession: SparkSession)
      extends SparkContextHive(sparkSession.sparkContext)

}
