package com.cosmian.spark.fs

import java.io.IOException
import java.nio.file.Files
import java.nio.file.Paths

import com.cosmian.spark.AppException
import java.io.FileOutputStream
import java.io.FileInputStream
import java.io.FileNotFoundException
import java.io.OutputStream
import java.io.InputStream
import java.io.ByteArrayOutputStream

import scala.collection.JavaConverters._

class LocalFileSystem extends AppFileSystem {

  @throws[AppException]
  def writeFile(filePath: String, bytes: Array[Byte]) = {
    val path = Paths.get(filePath)
    val os = new FileOutputStream(path.toFile())
    try {
      os.write(bytes);
      os.flush();
    } catch {
      case e: IOException =>
        throw AppException(
          s"failed writing: ${path.toString()}: ${e.getMessage()}",
          e
        )
    } finally {
      os.close()
    }

  }

  @throws[AppException]
  def readFile(filePath: String): Array[Byte] = {
    val path = Paths.get(filePath)
    val is = new FileInputStream(path.toFile())
    try {
      LocalFileSystem.read_all_bytes(is)
    } catch {
      case e: IOException =>
        throw AppException(
          s"failed reading the file: ${path.toFile().getAbsolutePath}: ${e.getMessage()}",
          e
        )
    } finally {
      is.close()
    }
  }

  @throws[AppException]
  def getOutputStream(filePath: String, overwrite: Boolean): OutputStream = {
    val path = Paths.get(filePath)
    try {
      return new FileOutputStream(path.toFile(), !overwrite)
    } catch {
      case e: FileNotFoundException =>
        throw AppException(
          s"failed writing: ${path.toString()}: file not found",
          e
        );
    }
  }

  @throws[AppException]
  def listFiles(directoryPath: String): Iterator[String] = {
    try {
      Files
        .list(Paths.get(directoryPath))
        .map[String](p => p.toAbsolutePath().toString())
        .iterator()
        .asScala
    } catch {
      case e: IOException =>
        throw AppException(
          s"failed listing the files at: $directoryPath : ${e.getMessage()}",
          e
        )
    }
  }

  @throws[AppException]
  def isDirectory(directoryPath: String): Boolean = {
    Files.isDirectory(Paths.get(directoryPath))
  }

  @throws[AppException]
  def getInputStream(filePath: String): InputStream = {
    val path = Paths.get(filePath);
    try {
      return new FileInputStream(path.toFile());
    } catch {
      case e: FileNotFoundException =>
        throw new AppException(
          s"failed reading: ${path.toString()}: file not found",
          e
        );
    }
  }

}

object LocalFileSystem {

  @throws[AppException]
  def read_all_bytes(inputStream: InputStream): Array[Byte] = {
    val BUFFER_LENGTH = 4096;
    var buffer: Array[Byte] = new Array(BUFFER_LENGTH);
    var readLen: Int = 0;
    val outputStream = new ByteArrayOutputStream();

    while ({
      readLen = inputStream.read(buffer, 0, BUFFER_LENGTH)
      readLen
    }
      != -1)
      outputStream.write(buffer, 0, readLen);
    return outputStream.toByteArray();
  }
}
