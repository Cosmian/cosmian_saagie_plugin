package com.cosmian.spark

import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.SparkException
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.BinaryType
import org.apache.spark.sql.functions._
import java.nio.charset.StandardCharsets
import com.cosmian.rest.kmip.objects.PublicKey
import com.cosmian.spark.fs.LocalResource
import com.cosmian.jna.Ffi
import com.cosmian.rest.abe.acccess_policy.Attr
import java.security.MessageDigest
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.io.ByteArrayOutputStream
import java.util.Optional
import com.cosmian.rest.abe.policy.Policy
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.Row
import java.util.Base64
import java.util.concurrent.TimeUnit

object Encrypt extends App {

  if (args.length < 2)
    throw new SparkException("the s3a input and output uris must be provided")
  val inputURI = args(0)
  val outputURI = args(1)

  println(s"Encrypting parquet file ${inputURI} --> ${outputURI}")

  // Get a Spark session
  val spark = Utils.getSparkSession()

  // the KMS implementation that reads local public key
  spark.sparkContext.hadoopConfiguration
    .set(
      CosmianAbeFactory.COSMIAN_KMS_CLIENT_CLASS,
      "com.cosmian.spark.LocalKms"
    )

  // the class that implements ABE inside Parquet Modular Encryption
  spark.sparkContext.hadoopConfiguration
    .set(
      "parquet.crypto.factory.class",
      "com.cosmian.spark.CosmianAbeFactory"
    )

  // Pre-print some statistics on the data that will be encrypted
  Utils.printStats(
    "Input Data frame",
    spark.read
      .parquet(inputURI)
  )

  println("Encrypting....")
  val start = System.nanoTime();

  // call the encrypt function defined below
  encrypt(spark, inputURI, outputURI)

  val encryptionTime =
    TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
  println(s"... time: $encryptionTime ms")

  // Perform the actual encryption from inputURI to outputURI
  def encrypt(spark: SparkSession, inputURI: String, outputURI: String) = {

    val inputDf = spark.read
      .parquet(inputURI)

    //3 partitions under 3 different attributes will be encrypted

    // This is the partition encrypted with attributes "Airport::OrlyRoissy"
    val attributesOrlyRoissy = "Airport::OrlyRoissy"
    inputDf
      .filter(Utils.orlyRoissyFilter())
      .write
      .mode(SaveMode.Overwrite)
      .partitionBy("APT_NAME")
      .option(
        CosmianAbeFactory.COSMIAN_ENCRYPTION_ATTRIBUTES,
        attributesOrlyRoissy
      )
      .parquet(outputURI)

    // This is the partition encrypted with attributes "Airport::OtherFrance"
    val otherFranceAttributes = "Airport::OtherFrance"
    inputDf
      .filter(Utils.otherFranceFilter())
      .write
      .mode(SaveMode.Append)
      .partitionBy("APT_NAME")
      .option(
        CosmianAbeFactory.COSMIAN_ENCRYPTION_ATTRIBUTES,
        otherFranceAttributes
      )
      .parquet(outputURI)

    // This is the partition encrypted with attributes "Airport::NonFrance"
    // This partition could be left un-encrypted by simply removing the `option`
    val nonFranceAttributes = "Airport::NonFrance"
    inputDf
      .filter(Utils.nonFranceFilter())
      .write
      .mode(SaveMode.Append)
      .partitionBy("APT_NAME")
      .option(
        CosmianAbeFactory.COSMIAN_ENCRYPTION_ATTRIBUTES,
        nonFranceAttributes
      )
      .parquet(outputURI)
  }
}
