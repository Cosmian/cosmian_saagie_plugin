package com.cosmian.spark.fs

import java.io.InputStream
import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets

object LocalResource {

  def loadStringResource(resourceName: String): String = {
    var is: InputStream = null

    // println(
    //   s"LOCAL RESOURCE: ${LocalResource.getClass().getClassLoader().getResource(".")}"
    // )
    try {
      is = LocalResource
        .getClass()
        .getClassLoader()
        .getResourceAsStream(resourceName);
      val bytes = LocalFileSystem.read_all_bytes(is)
      new String(bytes, StandardCharsets.UTF_8)
    } finally {
      if (is != null) is.close()
    }
  }

}
