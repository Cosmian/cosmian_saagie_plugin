package com.cosmian.spark

import com.cosmian.rest.abe.Abe
import com.cosmian.RestClient
import java.util.Optional
import com.cosmian.rest.abe.policy.Policy
import java.util.logging.Logger
import java.nio.charset.StandardCharsets
import com.cosmian.CosmianException
import com.cosmian.rest.abe.acccess_policy.AccessPolicy
import com.cosmian.rest.abe.acccess_policy.Attr
import com.cosmian.rest.abe.acccess_policy.And
import com.cosmian.rest.abe.acccess_policy.Or

class KeyGenerator(val outputDirectory: String) {

  lazy val logger = Logger.getLogger(classOf[KeyGenerator].getName())
  val outDirectory = OutputDirectory(outputDirectory)
  val abe = new Abe(
    new RestClient(
      KeyGenerator.kmsServerUrl(),
      KeyGenerator.apiKey()
    )
  )

  /** Generate all the keys required for this Demo
    */
  def generateKeys() = {

    val airports = List("OrlyRoissy", "OtherFrance", "NonFrance")
    val policy = new Policy(100)
      .addAxis("Airport", airports.toArray, false)

    // Generate Master Keys
    val masterIds = abe.createMasterKeyPair(policy)
    logger.info(() =>
      s"Created Master Key: Private Key ID: ${masterIds(0)}, Public Key: ${masterIds(1)} "
    );

    // Serialize Private Master Key
    val privateMasterKeyId = masterIds(0)
    val privateMasterKey = abe.retrievePrivateMasterKey(privateMasterKeyId)
    try {
      this.outDirectory.fs.writeFile(
        this.outDirectory.directory
          .resolve(s"master_private_key.json")
          .toString(),
        privateMasterKey.toJson().getBytes(StandardCharsets.UTF_8)
      );
    } catch {
      case e: AppException =>
        throw AppException(
          s"Failed saving the master private key: ${e.getMessage()}: ${e.getCause()}"
        )
    }

    // Serialize the Public (Master) Key
    val publicMasterKeyId = masterIds(1)
    val publicMasterKey = abe.retrievePublicMasterKey(publicMasterKeyId)
    try {
      this.outDirectory.fs.writeFile(
        this.outDirectory.directory
          .resolve(s"master_public_key.json")
          .toString(),
        publicMasterKey.toJson().getBytes(StandardCharsets.UTF_8)
      );
    } catch {
      case e: AppException =>
        throw AppException(
          s"Failed saving the master public key: ${e.getMessage()}: ${e.getCause()}"
        )
    }

    // Generate and serialize the ALL France user decryption key
    val allFrance =
      KeyGenerator.axisPolicy("Airport", airports)
    generateUserKey("all", privateMasterKeyId, allFrance)

    // Generate and serialize the Other France user decryption key
    val allButOrlyRoissy =
      KeyGenerator.axisPolicy("Airport", Seq("OtherFrance", "NonFrance"))
    generateUserKey("all_but_orly_roissy", privateMasterKeyId, allButOrlyRoissy)

    // Generate and serialize the Non France user decryption key
    val nonFrance = KeyGenerator.axisPolicy("Airport", Seq("NonFrance"))
    generateUserKey("non_france", privateMasterKeyId, nonFrance)

  }

  @throws[CosmianException]
  @throws[AppException]
  def generateUserKey(
      name: String,
      privateMasterKeyId: String,
      accessPolicy: AccessPolicy
  ) = {
    val userDecryptionKeyId =
      abe.createUserDecryptionKey(accessPolicy, privateMasterKeyId)
    val userDecryptionKey = abe.retrieveUserDecryptionKey(userDecryptionKeyId)
    val keyFile = s"user_${name}_key.json"
    try {
      this.outDirectory.fs
        .writeFile(
          this.outDirectory.directory.resolve(keyFile).toString(),
          userDecryptionKey.toJson().getBytes(StandardCharsets.UTF_8)
        )
    } catch {
      case e: AppException =>
        throw new AppException(
          s"Failed saving the user decryption key: $keyFile : ${e.getMessage()}",
          e.getCause()
        )
    }
  }
}

object KeyGenerator {

  def main(args: Array[String]): Unit = {
    new KeyGenerator("src/main/resources/keys").generateKeys()
  }

  private def kmsServerUrl(): String = {
    val v = System.getenv("COSMIAN_SERVER_URL");
    if (v == null) {
      "http://localhost:9998"
    } else {
      v
    }
  }

  private def apiKey(): Optional[String] = {
    val v = System.getenv("COSMIAN_API_KEY");
    if (v == null) {
      Optional.empty()
    } else {
      return Optional.of(v)
    }
  }

  @throws[CosmianException]
  def axisPolicy(axisName: String, values: Seq[String]): AccessPolicy = {
    if (values.length == 0) {
      throw new CosmianException("The policy must have at least one entity");
    } else if (values.length == 1) {
      new Attr(axisName, values(0))
    } else {
      var valuesPolicy: AccessPolicy = new Attr(axisName, values(0));
      for (i <- 1 until values.length) {
        valuesPolicy = new Or(valuesPolicy, new Attr(axisName, values(i)))
      }
      valuesPolicy
    }
  }

}
