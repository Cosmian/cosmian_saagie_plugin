package com.cosmian.spark.fs

import org.apache.spark.sql.{Encoders, SparkSession}
import java.nio.file.Paths
import java.nio.file.Files
import org.apache.spark.SparkException

object CsvToParquet extends App {

  if (args.length == 0)
    throw new IllegalArgumentException("File name must be provided")
  val inputFileName = args(0)
  println(s"Input File: ${inputFileName}")

  val inputFile = Paths.get(inputFileName);
  if (!Files.exists(inputFile)) {
    throw new SparkException(s"Input CSV file: $inputFileName does not exist")
  }

  val outputFile = inputFile
    .getParent()
    .resolve(inputFile.getFileName().toString() + ".parquet")
  println(s"Output File: ${outputFile.toAbsolutePath().toString()}")

  val spark: SparkSession = SparkSession
    .builder()
    .master("local[1]")
    .appName("CsVToParquet")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  //read csv with options
  val df = spark.read
    .options(
      Map("inferSchema" -> "true", "delimiter" -> ",", "header" -> "true")
    )
    .csv(inputFileName)
  df.show()
  df.printSchema()

  df.write
    .parquet(outputFile.toAbsolutePath().toString())

}
