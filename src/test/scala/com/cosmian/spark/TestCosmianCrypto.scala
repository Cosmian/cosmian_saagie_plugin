package com.cosmian.spark

import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.BinaryType
import org.apache.hadoop.shaded.com.nimbusds.jose.util.StandardCharset
import java.nio.charset.StandardCharsets

import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.DataFrame
import java.io.File
import java.nio.file.Files
import java.nio.file.Paths
import scala.reflect.io.Directory
import com.cosmian.rest.kmip.objects.PublicKey
import org.apache.spark.sql.SaveMode
import com.cosmian.spark.fs.LocalResource
import com.cosmian.jna.Ffi
import java.util.Base64
import org.scalatest.BeforeAndAfterEach
import org.apache.spark.sql.Column
import java.util.concurrent.TimeUnit

class TestCosmianCrypto extends AnyFunSuite with BeforeAndAfterEach {

  var spark: SparkSession = _
  override def beforeEach() = {
    spark = SparkSession
      .builder()
      .appName("encrypt")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    spark.sparkContext.hadoopConfiguration.set(
      CosmianAbeFactory.COSMIAN_KMS_CLIENT_CLASS,
      "com.cosmian.spark.LocalKms"
    )

    super.beforeEach() // To be stackable, must call super.beforeEach
  }

  override def afterEach() = {
    try {
      super.afterEach() // To be stackable, must call super.afterEach
    } finally {
      spark.stop()
    }
  }

  test("the dataset should be encrypted") {

    if (
      !Paths
        .get("src/test/resources/keys/master_private_key.json")
        .toFile()
        .exists()
    ) {
      println("Generating keys")
      new KeyGenerator("src/test/resources/keys").generateKeys()
    }

    val inputURI = "src/test/resources/Airport_Traffic.partitioned.parquet"
    val encryptedURI =
      "src/test/resources/Airport_Traffic.cosmian.encrypted.parquet"
    val decryptedURI =
      "src/test/resources/Airport_Traffic.cosmian.decrypted.parquet"

    spark.sparkContext.hadoopConfiguration
      .set(
        CosmianAbeFactory.COSMIAN_DECRYPTION_KEY_ID,
        "all"
      )

    spark.sparkContext.hadoopConfiguration
      .set(
        "parquet.crypto.factory.class",
        "com.cosmian.spark.CosmianAbeFactory"
      )

    Utils.printStats(
      "Input Data frame",
      spark.read
        .parquet(inputURI)
    )

    // ----------------
    // Encryption
    // ----------------

    println("Encrypting....")
    val eStart = System.nanoTime();

    Encrypt.encrypt(spark, inputURI, encryptedURI)

    val encryptionTime =
      TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - eStart);
    println(s"Encryption: $encryptionTime ms")

    // ----------------
    // Decryption
    // ----------------

    println("Decrypting....")
    val dStart = System.nanoTime();

    Decrypt.decrypt(
      spark,
      encryptedURI,
      decryptedURI,
      Utils.allFranceFilter()
    )

    val decryptionTime =
      TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - dStart);
    println(s"Decryption: $decryptionTime ms")

    Utils.printStats(
      "Decrypted Data frame",
      spark.read
        .parquet(decryptedURI)
    )

  }

}
