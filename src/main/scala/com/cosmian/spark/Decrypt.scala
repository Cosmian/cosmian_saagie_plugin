package com.cosmian.spark

import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.SparkException
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import java.nio.charset.StandardCharsets
import org.apache.spark.sql.types.StructType
import java.security.MessageDigest
import com.cosmian.jna.Ffi
import com.cosmian.rest.kmip.objects.PrivateKey
import com.cosmian.spark.fs.LocalResource
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.Arrays
import com.cosmian.jna.abe.DecryptedHeader
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.Column
import java.util.concurrent.TimeUnit

object Decrypt extends App {

  if (args.length < 2)
    throw new SparkException("the s3a input and output uris must be provided")
  val inputURI = args(0)
  val outputURI = args(1)

  println(s"Decrypting parquet file ${inputURI} --> ${outputURI}")

  // one of
  // - all
  // - all_but_orly_roissy
  // - non_france
  val userKeyFile = if (args.length >= 3) {
    args(2)
  } else {
    ""
  }

  // the filter to apply - if an attempt is made to decrypt a partition
  // with the wrong ey, the whole decryption process will fail
  val filter = if (args.length >= 4) {
    args(3) match {
      case "orly_roissy"  => Utils.orlyRoissyFilter()
      case "other_france" => Utils.otherFranceFilter()
      case "all_france"   => Utils.allFranceFilter()
      case "non_france"   => Utils.nonFranceFilter()
      case "all"          => Utils.noFilter()
      case x              => throw new SparkException(s"unknown filter: $x")
    }
  } else {
    Utils.noFilter()
  }

  val spark = Utils.getSparkSession()
  if (userKeyFile != "") {
    // decryption is requested, set the Hadoop context
    // The id of the user decryption to be loaded by the KMS class
    spark.sparkContext.hadoopConfiguration
      .set(
        CosmianAbeFactory.COSMIAN_DECRYPTION_KEY_ID,
        userKeyFile
      )
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
    println {
      s"Using Decryption key ID ${userKeyFile} and filter: ${filter}"
    }
  }

  // Print some stats on what is going to be decrypted (slow)
  Utils.printStats(
    "Input Data frame",
    spark.read
      .parquet(inputURI)
  )

  println("Decrypting....")
  val start = System.nanoTime();

  // call the decrypt method below
  decrypt(spark, inputURI, outputURI, filter)

  val decryptionTime =
    TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
  println(s"...time: $decryptionTime ms")

  val df = spark.read
    .parquet(outputURI)
  Utils
    .printStats("Decrypted Data frame", df)

  def decrypt(
      spark: SparkSession,
      inputURI: String,
      outputURI: String,
      filter: Column
  ) = {

    var df = spark.read
      .parquet(inputURI)

    // if no filter is provided, attempt to decrypt anything that is encrypted
    if (filter != null) {
      df = df.filter(filter)
    }

    df.write
      .mode(SaveMode.Overwrite)
      .partitionBy("APT_NAME")
      .parquet(outputURI)

  }
}
