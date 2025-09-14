package com.morillo.spark.delta

import org.apache.spark.sql.SparkSession
import com.typesafe.config.Config

object SparkSessionFactory {

  def createSparkSession(config: Config): SparkSession = {
    val appName = config.getString("app.name")
    val master = config.getString("spark.master")
    val warehouseDir = config.getString("spark.sql.warehouse.dir")

    val sparkBuilder = SparkSession.builder()
      .appName(appName)
      .master(master)
      .config("spark.sql.warehouse.dir", warehouseDir)
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")

    if (master.contains("local")) {
      sparkBuilder
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "4")
    }

    val spark = sparkBuilder.getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    spark
  }

  def createLocalSparkSession(appName: String = "DeltaLakeApp"): SparkSession = {
    SparkSession.builder()
      .appName(appName)
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "./spark-warehouse")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.default.parallelism", "4")
      .getOrCreate()
  }
}