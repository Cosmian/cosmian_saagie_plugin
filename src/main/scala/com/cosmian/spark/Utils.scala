package com.cosmian.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkException
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column

object Utils {

  val orly = "Paris-Orly"
  val roissy = "Paris-Charles-de-Gaulle"
  val partitions = Array(
    "APT_NAME"
  ) // Array("STATE_NAME", "APT_NAME", "MONTH_NUM")

  def getSparkSession(): SparkSession = {
    val ACCESS_KEY = sys.env.get("AWS_ACCESS_KEY") match {
      case None => throw new SparkException("Please provide the AWS ACCESS_KEY")
      case Some(value) => value
    }

    val SECRET_KEY = sys.env.get("AWS_SECRET_KEY") match {
      case None => throw new SparkException("Please provide the AWS SECRET_KEY")
      case Some(value) => value
    }

    val ENDPOINT = sys.env.getOrElse("ENDPOINT", "s3.eu-west-3.amazonaws.com")

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Word Count")
      .config("spark.hadoop.fs.s3a.path.style.access", true)
      .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY)
      .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY)
      .config("spark.hadoop.fs.s3a.endpoint", ENDPOINT)
      .config(
        "spark.hadoop.fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem"
      )
      .config("spark.executor.instances", "4")
      .getOrCreate()
    // This does not seem necessary anymore
    // .config("com.amazonaws.services.s3.enableV4", true)
    // .config(
    //   "spark.driver.extraJavaOptions",
    //   "-Dcom.amazonaws.services.s3.enableV4=true"
    // )

    spark.sparkContext.setLogLevel("ERROR")

    spark
  }

  def orlyRoissyFilter(): Column = {
    (col("APT_NAME") === Utils.orly) || (col("APT_NAME") === Utils.roissy)
  }

  def otherFranceFilter(): Column = {
    (col("STATE_NAME") === "France") && (col(
      "APT_NAME"
    ) !== Utils.orly) && (col("APT_NAME") !== Utils.roissy)
  }

  def nonFranceFilter(): Column = {
    col("STATE_NAME") !== "France"
  }

  def allFranceFilter(): Column = {
    col("STATE_NAME") === "France"
  }

  def noFilter(): Column = {
    null
  }

  def printStats(name: String, df: DataFrame) = {
    println(
      s" -- ${name} Orly/Roissy  Count: " + df
        .filter(orlyRoissyFilter())
        .count()
    )
    println(
      s" -- ${name} Other France Count: " + df
        .filter(otherFranceFilter())
        .count()
    )
    println(s" -- ${name} Total        Count: " + df.count())
  }

}
