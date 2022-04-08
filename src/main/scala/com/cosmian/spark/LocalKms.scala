package com.cosmian.spark

import org.apache.parquet.crypto.KeyAccessDeniedException
import org.apache.hadoop.conf.Configuration
import com.cosmian.spark.fs.LocalResource
import java.nio.charset.StandardCharsets

class LocalKms extends KmsClient {

  @throws[KeyAccessDeniedException]
  override def initialize(
      configuration: Configuration,
      kmsInstanceID: String,
      kmsInstanceURL: String,
      accessToken: String
  ) = {};

  @throws[KeyAccessDeniedException]
  override def retrievePublicKey(): Array[Byte] = {
    val keyJson =
      LocalResource.loadStringResource("keys/master_public_key.json")
    keyJson.getBytes(StandardCharsets.UTF_8)
  }

  @throws[KeyAccessDeniedException]
  override def retrievePrivateKey(privateKeyId: String): Array[Byte] = {
    val keyJson =
      LocalResource.loadStringResource(s"keys/user_${privateKeyId}_key.json")
    keyJson.getBytes(StandardCharsets.UTF_8)

  }
}
