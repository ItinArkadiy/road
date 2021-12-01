package com.epam.road

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object Application extends App {

  val spark: SparkSession = SparkSession.builder()
    .appName("road")
    .master("local[*]")
    .getOrCreate()

  val roadsDF: DataFrame = spark.read
    .parquet("road_counters_day_agg")
  val roadRes: DataFrame = roadsDF
    .groupBy(to_date(col("timestamp")).alias("date"), col("roadId"), col("name"))
    .agg(
      max(col("value")).alias("max_val"),
      min(col("value")).alias("min_val"),
      bround(avg(col("value")), 2).alias("avg_val"),
      sum(col("value")).alias("sum_val")
    )

  roadRes
    .coalesce(1)
    .write
    .option("header", "true")
    .csv("road_counters_day_agg_res")

}
