# Cosmian Saagie Spark ABE Plugin

The project demonstrates [Attributes Based Encryption (ABE)](https://github.com/Cosmian/cosmian_spark_crypto) on Saagie/Spark and provides and Encrypt and a Decrypt class to be run in Spark 3.2 on Saagie.

The goal is to encrypt/decrypt Airport landing data across 3 attributes: Orly/roissy, Other France and Non France.

## Design

The project leverages Parquet Modular Encryption through the "com.cosmian:cosmian_spark_crypto:1.0.0" library. See The [Github repo](https://github.com/Cosmian/cosmian_spark_crypto) for details.


Encryption is performed in [Encrypt.scala](./src/main/scala/com/cosmian/spark/Encrypt.scala)

Decryption is performed in [Decrypt.scala](./src/main/scala/com/cosmian/spark/Decrypt.scala)

The keys are using [pre-generated keys](./src/main/resources/keys/) delivered through the [LocalKms.scala](./src/main/scala/com/cosmian/spark/LocalKms.scala) implementation.


## Build

This project requires scala 2.12 and sbt 1.6

To build the "fat" jar to be deployed, simply run 

```bash
sbt assembly
```

See the [parameters](./saagie-spark-params.txt) or on the Saagie platform.

## Testing

You need to have a local spark-3.2.1-bin-hadoop3.3 installation.
See these [instructions](https://spark.apache.org/downloads.html) to download and install.

To test the [TestCosmianCrypto.scala](./src/test/scala/com/cosmian/spark/TestCosmianCrypto.scala), run

```bash
 sbt "test:testOnly *TestCosmianCrypto"
 ```
